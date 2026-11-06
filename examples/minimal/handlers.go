package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/opencairn-org/cairn-provider-sdk-go/fitmap"
	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

// reserve takes one call from the pool's per-minute budget.
func reserve(ctx context.Context, w *workersdk.Worker) error {
	return w.ReserveAPI(ctx, w.RateBucket("minute"), 1)
}

// ---------------------------------------------------------------------------
// fetch_source: one activity → FIT → ImportedActivity, archived and claim-checked
// ---------------------------------------------------------------------------

func makeFetchSource(client *acmeClient) workersdk.HandlerFunc {
	return func(ctx context.Context, w *workersdk.Worker, job workersdk.Job) error {
		var in workersdk.FetchSourceJob
		if err := json.Unmarshal(job.Body, &in); err != nil {
			return &port.TerminalError{Reason: "bad_payload", Cause: err}
		}
		if in.ExtID == "" {
			return &port.TerminalError{Reason: "missing_ext_id"}
		}
		err := fetchOnce(ctx, w, client, job, in)
		// A terminal failure publishes the failure envelope first, so core
		// fails the queue item with the true reason instead of waiting for
		// the stale reaper.
		var term *port.TerminalError
		if errors.As(err, &term) {
			w.PublishFailureResult(ctx, job, in.Ref(), term.Reason, term.Cause)
		}
		return err
	}
}

func fetchOnce(ctx context.Context, w *workersdk.Worker, client *acmeClient, job workersdk.Job, in workersdk.FetchSourceJob) error {
	tok, err := w.FetchToken(ctx, in.AccountID)
	if err != nil {
		return err // already terminal or NAK-with-delay
	}
	if err := reserve(ctx, w); err != nil {
		return err
	}
	detail, err := client.activity(ctx, tok.AccessToken, in.ExtID)
	if err != nil {
		return mapErr(err, "activity")
	}
	if err := reserve(ctx, w); err != nil {
		return err
	}
	raw, err := client.file(ctx, tok.AccessToken, in.ExtID)
	if err != nil {
		return mapErr(err, "file")
	}
	act, err := mapFIT(raw, in.Ref(), detail.Name)
	if err != nil {
		return err
	}
	// Archive the original file so parse_blob can re-map it without
	// provider budget. Best-effort: the import is durable without it.
	if id, size, aerr := w.ArchiveBlob(ctx, in.UserID, raw, fitmap.ContentTypeFIT); aerr != nil {
		w.Logger().Warn("archive failed", "ext_id", in.ExtID, "error", aerr)
	} else {
		act.RawBlobId, act.RawContentType, act.RawSizeBytes = id, fitmap.ContentTypeFIT, size
	}
	msgID := fmt.Sprintf("result:%s:d%d", in.JobID, job.DeliveryAttempt)
	return w.PublishResultViaBlob(ctx, job, msgID, w.NewJobResult(activityEvent(act)))
}

// mapFIT is the parser proper: FIT bytes in, an ImportedActivity out. A
// file the mapper rejects will not improve on retry.
func mapFIT(raw []byte, ref *workerv1.ExternalRef, title string) (*workerv1.ImportedActivity, error) {
	res, err := fitmap.FromFIT(bytes.NewReader(raw))
	if err != nil {
		return nil, &port.TerminalError{Reason: "unreadable_file", Cause: err}
	}
	act := res.Activity
	act.Ref = ref
	if act.Payload.Timezone == "" {
		act.Payload.Timezone = "UTC" // core rejects an activity without a zone; tz.FromOffset names a bare offset
	}
	if title != "" {
		act.Payload.Title = title
	}
	return act, nil
}

func activityEvent(act *workerv1.ImportedActivity) *workerv1.WorkerEvent {
	return &workerv1.WorkerEvent{Type: workerv1.WorkerEventType_WORKER_EVENT_TYPE_ACTIVITY, Entity: &workerv1.WorkerEvent_Activity{Activity: act}}
}

// ---------------------------------------------------------------------------
// parse_blob: re-map the archived FIT with the current mapper
// ---------------------------------------------------------------------------

func parseBlob(ctx context.Context, w *workersdk.Worker, job workersdk.Job) error {
	var in workersdk.ParseBlobJob
	if err := json.Unmarshal(job.Body, &in); err != nil {
		return &port.TerminalError{Reason: "bad_payload", Cause: err}
	}
	if err := w.CheckParserVersion(job, in.MinParserVersion); err != nil {
		var term *port.TerminalError
		if errors.As(err, &term) {
			w.PublishFailureResult(ctx, job, in.Ref(), term.Reason, term.Cause)
		}
		return err
	}
	url, err := w.BlobDownloadURL(ctx, in.Blob)
	if err != nil {
		return err
	}
	raw, err := workersdk.DownloadBlob(ctx, url)
	if err != nil {
		return fmt.Errorf("download archive: %w", err) // transient → NAK
	}
	act, err := mapFIT(raw, in.Ref(), "")
	if err != nil {
		w.PublishFailureResult(ctx, job, in.Ref(), "unreadable_file", err)
		return err
	}
	act.RawBlobId, act.RawContentType, act.RawSizeBytes = in.Blob.FallbackHandle, fitmap.ContentTypeFIT, int64(len(raw))
	return w.PublishResultViaBlob(ctx, job, "result:"+in.JobID, w.NewJobResult(activityEvent(act)))
}

// ---------------------------------------------------------------------------
// reconcile: list recent activity, fan out, advance the watermark
// ---------------------------------------------------------------------------

const (
	reconcilePerPage    = 100
	reconcileMaxEnqueue = 500
)

func makeReconcile(client *acmeClient) workersdk.HandlerFunc {
	return func(ctx context.Context, w *workersdk.Worker, job workersdk.Job) error {
		var in workersdk.ReconcileJob
		if err := json.Unmarshal(job.Body, &in); err != nil {
			return &port.TerminalError{Reason: "bad_payload", Cause: err}
		}
		tok, err := w.FetchToken(ctx, in.AccountID)
		if err != nil {
			return err
		}
		enqueued, highWater, err := reconcileEnqueue(ctx, w, client, in, tok.AccessToken)
		if err != nil {
			return err
		}
		// The watermark result goes on the account-suffixed subject, even
		// when nothing was found: it advances last_sync_at.
		res := w.NewJobResult()
		if !highWater.IsZero() {
			res.NewWatermark = timestamppb.New(highWater)
		}
		body, err := protojson.Marshal(res)
		if err != nil {
			return &port.TerminalError{Reason: "marshal_result", Cause: err}
		}
		w.Logger().Info("reconcile done", "account_id", in.AccountID, "enqueued", enqueued)
		return w.Enqueue(ctx, w.ResultSubject(workersdk.JobReconcile)+"."+in.AccountID, "result:"+in.JobID, body)
	}
}

// reconcileEnqueue walks the recent window and fans out a fetch per
// activity not in KnownExtIDs, returning the count and the newest start.
func reconcileEnqueue(ctx context.Context, w *workersdk.Worker, client *acmeClient, in workersdk.ReconcileJob, token string) (int, time.Time, error) {
	known := make(map[string]struct{}, len(in.KnownExtIDs))
	for _, id := range in.KnownExtIDs {
		known[id] = struct{}{}
	}
	maxEnqueue := in.MaxEnqueue
	if maxEnqueue <= 0 {
		maxEnqueue = reconcileMaxEnqueue
	}
	var highWater time.Time
	if in.Watermark != nil {
		highWater = *in.Watermark
	}
	enqueued := 0
	after := workersdk.ReconcileWindowStart(in.Watermark, time.Now())
	for page := 1; enqueued < maxEnqueue; page++ {
		if err := reserve(ctx, w); err != nil {
			return 0, time.Time{}, err
		}
		items, err := client.list(ctx, token, after, page, reconcilePerPage)
		if err != nil {
			return 0, time.Time{}, mapErr(err, "list")
		}
		n, err := enqueueNew(ctx, w, in, items, known, &highWater)
		if err != nil {
			return 0, time.Time{}, err
		}
		enqueued += n
		if len(items) < reconcilePerPage {
			break
		}
	}
	return enqueued, highWater, nil
}

// enqueueNew fans out the unknown items of one page and advances highWater.
func enqueueNew(ctx context.Context, w *workersdk.Worker, in workersdk.ReconcileJob, items []acmeActivity, known map[string]struct{}, highWater *time.Time) (int, error) {
	n := 0
	for _, a := range items {
		if _, ok := known[a.ID]; ok {
			continue
		}
		if err := w.EnqueueFetchSource(ctx, in.JobID, in.AccountID, in.UserID, "reconcile", a.ID); err != nil {
			return n, fmt.Errorf("enqueue fetch_source: %w", err)
		}
		n++
		if a.Start.After(*highWater) {
			*highWater = a.Start
		}
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// import_metrics: daily values as METRIC events (capability Weight)
// ---------------------------------------------------------------------------

func makeImportMetrics(client *acmeClient) workersdk.HandlerFunc {
	return func(ctx context.Context, w *workersdk.Worker, job workersdk.Job) error {
		var in workersdk.ImportMetricsJob
		if err := json.Unmarshal(job.Body, &in); err != nil {
			return &port.TerminalError{Reason: "bad_payload", Cause: err}
		}
		tok, err := w.FetchToken(ctx, in.AccountID)
		if err != nil {
			return err
		}
		if err := reserve(ctx, w); err != nil {
			return err
		}
		since, until := in.Window()
		metrics, err := client.metrics(ctx, tok.AccessToken, since, until)
		if err != nil {
			return mapErr(err, "metrics")
		}
		events := make([]*workerv1.WorkerEvent, 0, len(metrics))
		for _, m := range metrics {
			v := m.Value
			events = append(events, &workerv1.WorkerEvent{
				Type: workerv1.WorkerEventType_WORKER_EVENT_TYPE_METRIC,
				Entity: &workerv1.WorkerEvent_Metric{Metric: &workerv1.ImportedMetric{
					Ref: &workerv1.ExternalRef{UserId: in.UserID, Provider: workerProvider, ExternalAccountId: in.AccountID, ExternalId: m.Key + ":" + m.TS.UTC().Format(time.RFC3339)},
					Key: m.Key, Ts: timestamppb.New(m.TS), ValueNumeric: &v,
				}},
			})
		}
		msgID := fmt.Sprintf("metrics:%s:%d", in.AccountID, in.UntilUnix)
		return w.PublishResultViaBlob(ctx, job, msgID, w.NewJobResult(events...))
	}
}

// ---------------------------------------------------------------------------
// discover: paged listing for the import queue
// ---------------------------------------------------------------------------

const discoverMaxPagesPerCall = 25

func makeDiscover(w *workersdk.Worker, client *acmeClient) workersdk.DiscoverFunc {
	return func(ctx context.Context, req workersdk.DiscoverRequest) workersdk.DiscoverResponse {
		tok, err := w.FetchToken(ctx, req.AccountID)
		if err != nil {
			return workersdk.DiscoverResponse{Error: err.Error()}
		}
		var items []workersdk.DiscoverItem
		for i := 0; i < discoverMaxPagesPerCall; i++ {
			page := req.StartPage + i
			list, resp, stop := discoverPage(ctx, w, client, tok.AccessToken, page, items)
			if stop {
				return resp
			}
			items = append(items, list...)
		}
		return workersdk.DiscoverResponse{Items: items, NextPage: req.StartPage + discoverMaxPagesPerCall}
	}
}

// discoverPage lists one page; stop carries the final response when the
// walk ends here (end of data, rate limit or a listing error).
func discoverPage(ctx context.Context, w *workersdk.Worker, client *acmeClient, token string, page int, sofar []workersdk.DiscoverItem) ([]workersdk.DiscoverItem, workersdk.DiscoverResponse, bool) {
	if err := reserve(ctx, w); err != nil {
		return nil, workersdk.DiscoverResponse{Items: sofar, NextPage: page, RateLimited: true}, true
	}
	list, err := client.list(ctx, token, 0, page, 200)
	switch {
	case err != nil && len(sofar) == 0:
		return nil, workersdk.DiscoverResponse{Error: err.Error()}, true
	case err != nil:
		return nil, workersdk.DiscoverResponse{Items: sofar, NextPage: page}, true
	case len(list) == 0:
		return nil, workersdk.DiscoverResponse{Items: sofar, Complete: true}, true
	}
	items := make([]workersdk.DiscoverItem, 0, len(list))
	for _, a := range list {
		items = append(items, workersdk.DiscoverItem{ItemType: "activity", ExternalID: a.ID, ItemTime: a.Start.UTC().Format(time.RFC3339)})
	}
	return items, workersdk.DiscoverResponse{}, false
}

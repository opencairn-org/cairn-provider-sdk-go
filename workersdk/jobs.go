package workersdk

import (
	"context"
	"encoding/json"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
)

// ---------------------------------------------------------------------------
// Job payloads (server → worker, JSON) — provider contract §4. Field names
// are core's; a worker decodes job.Body into these and may embed them in
// its own structs for extra fields.
// ---------------------------------------------------------------------------

// Job kinds core routes to a pool (cairn.jobs.<kind>.<provider>.<name>).
const (
	JobFetchSource   = "fetch_source"
	JobParseBlob     = "parse_blob"
	JobBackfill      = "backfill"
	JobReconcile     = "reconcile"
	JobImportGear    = "import_gear"
	JobImportMetrics = "import_metrics"
)

// FetchSourceJob imports one activity by provider id. Reason is "backfill"
// | "webhook" | "reconcile" | "queue" (the import queue) | "refetch".
type FetchSourceJob struct {
	JobID        string `json:"job_id"`
	AccountID    string `json:"account_id"`
	UserID       string `json:"user_id"`
	Provider     string `json:"provider"`
	ExtID        string `json:"ext_id"`
	FetchStreams bool   `json:"fetch_streams"`
	Reason       string `json:"reason"`
}

// Ref is the ExternalRef of the job's activity.
func (j FetchSourceJob) Ref() *workerv1.ExternalRef {
	return &workerv1.ExternalRef{UserId: j.UserID, Provider: j.Provider, ExternalAccountId: j.AccountID, ExternalId: j.ExtID}
}

// BlobRef locates an archived blob for parse_blob: a presigned URL with
// its expiry and the stored key to re-presign from when it has expired.
type BlobRef struct {
	URL            string    `json:"url"`
	ExpiresAt      time.Time `json:"expires_at"`
	FallbackHandle string    `json:"fallback_handle"` // raw_blob_id (full S3 key)
}

// ParseBlobJob re-parses an archived blob with the current parser, without
// a provider API call. MinParserVersion is the source's current parser
// version; a worker whose parser is older hands the job back (ParserAllows,
// ParserTooOldBackoff).
type ParseBlobJob struct {
	JobID            string  `json:"job_id"`
	SourceID         string  `json:"source_id"`
	UserID           string  `json:"user_id"`
	Provider         string  `json:"provider"`
	AccountID        string  `json:"account_id"`
	ExtID            string  `json:"ext_id"`
	MinParserVersion string  `json:"min_parser_version"`
	Blob             BlobRef `json:"blob"`
}

// Ref is the ExternalRef of the job's source.
func (j ParseBlobJob) Ref() *workerv1.ExternalRef {
	return &workerv1.ExternalRef{UserId: j.UserID, Provider: j.Provider, ExternalAccountId: j.AccountID, ExternalId: j.ExtID}
}

// BackfillJob lists the account's history and fans out fetch_source jobs.
type BackfillJob struct {
	JobID         string     `json:"job_id"`
	AccountID     string     `json:"account_id"`
	UserID        string     `json:"user_id"`
	Provider      string     `json:"provider"`
	Since         *time.Time `json:"since,omitempty"`
	PageSize      int        `json:"page_size"`
	MaxActivities int        `json:"max_activities,omitempty"`
}

// ReconcileJob is the polling sync (provider contract §7): list recent
// activity, enqueue fetch_source for ids not in KnownExtIDs, publish the
// new watermark on ResultSubject(JobReconcile)+"."+AccountID even when
// nothing was found.
type ReconcileJob struct {
	JobID       string     `json:"job_id"`
	AccountID   string     `json:"account_id"`
	UserID      string     `json:"user_id"`
	Provider    string     `json:"provider"`
	Watermark   *time.Time `json:"watermark,omitempty"`
	MaxEnqueue  int        `json:"max_enqueue,omitempty"`
	KnownExtIDs []string   `json:"known_ext_ids,omitempty"`
	WebhookSeen bool       `json:"webhook_seen"`
	Reason      string     `json:"reason"`
	IssuedAt    time.Time  `json:"issued_at"`
}

// ImportGearJob imports the account's full gear list (capability Gear).
type ImportGearJob struct {
	JobID     string    `json:"job_id"`
	AccountID string    `json:"account_id"`
	UserID    string    `json:"user_id"`
	Provider  string    `json:"provider"`
	Reason    string    `json:"reason"`
	IssuedAt  time.Time `json:"issued_at"`
}

// ImportMetricsJob imports non-activity metrics (sleep, HRV, weight…) for
// the window; core sends it to pools declaring a metric data type.
type ImportMetricsJob struct {
	AccountID string `json:"account_id"`
	UserID    string `json:"user_id"`
	Provider  string `json:"provider"`
	SinceUnix int64  `json:"since_unix"`
	UntilUnix int64  `json:"until_unix"`
}

// Window is the job's [since, until) as times.
func (j ImportMetricsJob) Window() (since, until time.Time) {
	return time.Unix(j.SinceUnix, 0).UTC(), time.Unix(j.UntilUnix, 0).UTC()
}

// ---------------------------------------------------------------------------
// Fan-out, reconcile and re-parse helpers
// ---------------------------------------------------------------------------

// EnqueueFetchSource fans out one fetch_source sub-job to this pool. The
// job id is parentJobID+":"+extID and the deterministic msg-id
// "<reason>:<provider>:<account>:<ext id>" collapses duplicate enqueues
// within JetStream's dedup window.
func (w *Worker) EnqueueFetchSource(ctx context.Context, parentJobID, accountID, userID, reason, extID string) error {
	body, _ := json.Marshal(FetchSourceJob{
		JobID:        parentJobID + ":" + extID,
		AccountID:    accountID,
		UserID:       userID,
		Provider:     w.cfg.Provider,
		ExtID:        extID,
		FetchStreams: true,
		Reason:       reason,
	})
	msgID := reason + ":" + w.cfg.Provider + ":" + accountID + ":" + extID
	return w.Enqueue(ctx, w.JobSubject(JobFetchSource), msgID, body)
}

// ReconcileLookbackFloor bounds how far back a reconcile ever lists, even
// with no watermark: full history is the import queue's job.
const ReconcileLookbackFloor = 30 * 24 * time.Hour

// ReconcileWindowStart returns the lower bound of a reconcile listing: the
// later of (watermark − 1h drift margin) and now − ReconcileLookbackFloor,
// as a Unix timestamp (never negative). The margin re-covers activity the
// provider published just after the last import; the floor keeps a
// reconcile from backfilling ancient history.
func ReconcileWindowStart(watermark *time.Time, now time.Time) int64 {
	windowStart := now.Add(-ReconcileLookbackFloor)
	if watermark != nil {
		if wm := watermark.Add(-time.Hour); wm.After(windowStart) {
			windowStart = wm
		}
	}
	return max(windowStart.Unix(), 0)
}

// ParserTooOldAttempts bounds how often an old instance hands a re-parse
// back before failing it as parser_too_old (about 35 minutes with
// ParserTooOldBackoff); register parse_blob with HandleWithMaxDeliver and
// ParserTooOldAttempts+2.
const ParserTooOldAttempts = 10

// ParserTooOldBackoff is the NAK delay for delivery attempt n of a
// re-parse this instance is too old for: 30s doubling up to 10 minutes.
func ParserTooOldBackoff(attempt int) time.Duration {
	if attempt > 6 { // 30s·2^5 already exceeds the cap; avoid shifting past it
		return 10 * time.Minute
	}
	return min(30*time.Second<<max(attempt-1, 0), 10*time.Minute)
}

// CheckParserVersion is the rolling-upgrade guard of a parse_blob handler:
// nil when this worker may re-parse the job's source; otherwise a
// *port.NakWithDelayError (parser_too_old, so a newer instance of the pool
// takes it) or, from ParserTooOldAttempts on, a *port.TerminalError the
// caller should report with PublishFailureResult.
func (w *Worker) CheckParserVersion(job Job, minParserVersion string) error {
	if ParserAllows(w.cfg.Version, minParserVersion) {
		return nil
	}
	cause := &ParserTooOldError{Min: minParserVersion, Own: w.cfg.Version}
	if job.DeliveryAttempt >= ParserTooOldAttempts {
		return &port.TerminalError{Reason: "parser_too_old", Cause: cause}
	}
	return &port.NakWithDelayError{Reason: "parser_too_old", Delay: ParserTooOldBackoff(job.DeliveryAttempt), Cause: cause}
}

// ParserTooOldError says which versions clashed.
type ParserTooOldError struct{ Min, Own string }

func (e *ParserTooOldError) Error() string {
	return "source parsed by " + e.Min + ", this parser is " + e.Own
}

// ---------------------------------------------------------------------------
// Discover (request/reply) — provider contract §8
// ---------------------------------------------------------------------------

// DiscoverRequest is core's question on DiscoverSubject: what can be
// imported for the account? StartPage drives paged, resumable discovery:
// core calls repeatedly, advancing StartPage to the reply's NextPage, until
// Complete. 0/1 = the first page.
type DiscoverRequest struct {
	AccountID string `json:"account_id"`
	StartPage int    `json:"start_page,omitempty"`
}

// DiscoverItem is one importable thing; ItemTime is RFC 3339.
type DiscoverItem struct {
	ItemType   string `json:"item_type"`
	ExternalID string `json:"external_id"`
	ItemTime   string `json:"item_time"`
}

// DiscoverResponse answers a page. Complete is true only once the end of
// the data is known (a short page is not the end for every provider);
// NextPage is where core continues when !Complete; RateLimited tells core
// to wait for the budget before asking for NextPage. Error is set when
// nothing could be listed ("bad_request", "rate_limited: …", or the cause).
type DiscoverResponse struct {
	Items       []DiscoverItem `json:"items"`
	Complete    bool           `json:"complete"`
	NextPage    int            `json:"next_page,omitempty"`
	RateLimited bool           `json:"rate_limited,omitempty"`
	Error       string         `json:"error,omitempty"`
}

// DiscoverFunc lists one batch of pages from req.StartPage (already ≥ 1).
type DiscoverFunc func(ctx context.Context, req DiscoverRequest) DiscoverResponse

// RespondDiscover serves DiscoverSubject with fn until the subscription is
// closed. An undecodable request answers {"error":"bad_request"}.
func (w *Worker) RespondDiscover(ctx context.Context, fn DiscoverFunc) (port.Subscription, error) {
	return w.cfg.Bus.RespondTo(ctx, w.DiscoverSubject(), func(ctx context.Context, body []byte) ([]byte, error) {
		var req DiscoverRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return json.Marshal(DiscoverResponse{Error: "bad_request"})
		}
		req.StartPage = max(req.StartPage, 1)
		return json.Marshal(fn(ctx, req))
	})
}

// ---------------------------------------------------------------------------
// Webhook envelope — what core's forwarder publishes per POST
// ---------------------------------------------------------------------------

// WebhookEnvelope is the body of every WebhookEvent: core forwards the raw
// POST with its headers (lowercased keys) and the provider name.
type WebhookEnvelope struct {
	Provider string            `json:"provider"`
	Headers  map[string]string `json:"headers"`
	Body     []byte            `json:"body"`
}

// Envelope decodes the event's forwarder envelope. A failure is a
// *port.TerminalError (bad_envelope): the message cannot become valid on
// retry.
func (ev WebhookEvent) Envelope() (WebhookEnvelope, error) {
	var env WebhookEnvelope
	if err := json.Unmarshal(ev.Body, &env); err != nil {
		return WebhookEnvelope{}, &port.TerminalError{Reason: "bad_envelope", Cause: err}
	}
	return env, nil
}

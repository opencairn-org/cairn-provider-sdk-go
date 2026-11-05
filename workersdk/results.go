package workersdk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
)

// BlobHTTPClient performs the presigned PUTs and GETs (archive, claim-check,
// attachments). Replace it to tune timeouts or inject a transport.
var BlobHTTPClient = &http.Client{Timeout: 30 * time.Second}

// MaxBlobDownload caps DownloadBlob (64 MiB).
const MaxBlobDownload = 64 << 20

// NewJobResult stamps events with this worker's identity (name, parser
// version, package, release). Every JobResult a worker publishes starts
// here, with or without events.
func (w *Worker) NewJobResult(events ...*workerv1.WorkerEvent) *workerv1.JobResult {
	return &workerv1.JobResult{
		WorkerName:    w.cfg.Name,
		WorkerVersion: w.cfg.Version,
		WorkerPackage: w.cfg.Package,
		WorkerRelease: w.cfg.Release,
		Events:        events,
	}
}

// PublishResultViaBlob claim-checks an event-carrying JobResult: the full
// protojson body is PUT to the blob store via a server-minted presigned URL
// (kind "result", short-lived — the server deletes it after ingest, a bucket
// lifecycle rule reaps orphans), and only a small envelope with payload_ref
// travels over NATS on the job's reply subject. Results of any size stay
// publishable; the broker's max_payload no longer matters. msgID is the
// Nats-Msg-Id (dedup within the stream's window).
//
// Presign/upload failures are transient (a plain error, so the job NAKs):
// the blob store being briefly unreachable must not fail the activity.
func (w *Worker) PublishResultViaBlob(ctx context.Context, job Job, msgID string, jr *workerv1.JobResult) error {
	body, err := protojson.Marshal(jr)
	if err != nil {
		return &port.TerminalError{Reason: "marshal_result", Cause: err}
	}
	sum := sha256.Sum256(body)
	shaHex := hex.EncodeToString(sum[:])

	up, err := w.PresignUpload(ctx, PresignUploadRequest{
		Kind:          "result",
		ContentType:   "application/json",
		ContentLength: int64(len(body)),
		ContentSHA256: shaHex,
	})
	if err != nil {
		return fmt.Errorf("presign result upload: %w", err)
	}
	if err := PutPresigned(ctx, up, body, "application/json"); err != nil {
		return fmt.Errorf("upload result payload: %w", err)
	}

	envelope := &workerv1.JobResult{
		WorkerName:    jr.GetWorkerName(),
		WorkerVersion: jr.GetWorkerVersion(),
		WorkerPackage: jr.GetWorkerPackage(),
		WorkerRelease: jr.GetWorkerRelease(),
		NewWatermark:  jr.GetNewWatermark(),
		MoreAvailable: jr.GetMoreAvailable(),
		PayloadRef: &workerv1.PayloadRef{
			BlobId:        up.BlobID,
			SizeBytes:     int64(len(body)),
			ContentType:   "application/json",
			ContentSha256: shaHex,
		},
	}
	envBody, err := protojson.Marshal(envelope)
	if err != nil {
		return &port.TerminalError{Reason: "marshal_result", Cause: err}
	}
	return w.PublishResult(ctx, job, msgID, envBody)
}

// FailureClass maps a terminal reason to the proto error class core keys
// its handling on: needs_reauth → AUTH_EXPIRED, not_found → NOT_FOUND,
// everything else INVALID_INPUT.
func FailureClass(reason string) workerv1.ErrorClass {
	switch reason {
	case "needs_reauth":
		return workerv1.ErrorClass_ERROR_CLASS_AUTH_EXPIRED
	case "not_found":
		return workerv1.ErrorClass_ERROR_CLASS_NOT_FOUND
	default:
		return workerv1.ErrorClass_ERROR_CLASS_INVALID_INPUT
	}
}

// PublishFailureResult tells the server a job failed terminally, so the
// import-queue item is failed NOW with the true reason. Without it a Term'd
// job publishes nothing and the item sits in_progress until the stale
// reaper masks the cause as "no result after N dispatch attempts".
// Best-effort (failures are logged): the Term disposition that follows is
// authoritative for the job itself. A ref without an external id is not
// correlatable and is skipped.
func (w *Worker) PublishFailureResult(ctx context.Context, job Job, ref *workerv1.ExternalRef, reason string, cause error) {
	if ref.GetExternalId() == "" {
		return
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	jr := w.NewJobResult()
	jr.Error = &workerv1.WorkerError{Class: FailureClass(reason), Code: reason, Message: msg}
	jr.FailedRef = ref
	body, err := protojson.Marshal(jr)
	if err != nil {
		return
	}
	msgID := fmt.Sprintf("failed:%s:%s:d%d", ref.GetProvider(), ref.GetExternalId(), job.DeliveryAttempt)
	if perr := w.PublishResult(ctx, job, msgID, body); perr != nil {
		w.logger.Warn("failure result publish failed", "ext_id", ref.GetExternalId(), "reason", reason, "error", perr)
	}
}

// PutPresigned uploads body to a server-minted presigned URL, sending the
// required headers verbatim. Shared by the raw-archive, attachment and
// claim-check paths.
func PutPresigned(ctx context.Context, up PresignedURL, body []byte, contentType string) error {
	method := up.Method
	if method == "" {
		method = http.MethodPut
	}
	req, err := http.NewRequestWithContext(ctx, method, up.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range up.RequiredHeaders {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := BlobHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("blob PUT: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("blob PUT status %d", resp.StatusCode)
	}
	return nil
}

// ArchiveBlob uploads raw provider data (the parse_blob input) under the
// user's key space through a presigned PUT and returns the blob id to store
// as raw_blob_id plus its size. Best-effort for callers: an import is
// durable without the archive, so log and continue on error.
func (w *Worker) ArchiveBlob(ctx context.Context, userID string, body []byte, contentType string) (blobID string, sizeBytes int64, err error) {
	up, err := w.PresignUpload(ctx, PresignUploadRequest{
		UserID:        userID,
		ContentType:   contentType,
		ContentLength: int64(len(body)),
	})
	if err != nil {
		return "", 0, fmt.Errorf("presign upload: %w", err)
	}
	if err := PutPresigned(ctx, up, body, contentType); err != nil {
		return "", 0, err
	}
	return up.BlobID, int64(len(body)), nil
}

// DownloadBlob GETs a blob from a presigned URL (the parse_blob read path),
// capped at MaxBlobDownload. A failure is transient from the job's point
// of view (return it as is to NAK).
func DownloadBlob(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := BlobHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("blob GET: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("blob GET status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, MaxBlobDownload))
}

// ErrNoBlobHandle is the terminal reason when a parse_blob job carries
// neither a usable URL nor a handle to re-presign.
var ErrNoBlobHandle = errors.New("no_blob_handle")

// BlobDownloadURL resolves a usable download URL for a parse_blob job: the
// supplied one while it is valid, else a fresh presign from the stored
// blob key (fallback_handle == raw_blob_id). No handle is a
// *port.TerminalError (no_blob_handle); a presign failure is transient.
func (w *Worker) BlobDownloadURL(ctx context.Context, blob BlobRef) (string, error) {
	if blob.URL != "" && (blob.ExpiresAt.IsZero() || !time.Now().After(blob.ExpiresAt)) {
		return blob.URL, nil
	}
	if blob.FallbackHandle == "" {
		return "", &port.TerminalError{Reason: "no_blob_handle", Cause: ErrNoBlobHandle}
	}
	fresh, err := w.PresignDownload(ctx, PresignDownloadRequest{BlobID: blob.FallbackHandle})
	if err != nil {
		return "", fmt.Errorf("refresh presign: %w", err)
	}
	return fresh.URL, nil
}

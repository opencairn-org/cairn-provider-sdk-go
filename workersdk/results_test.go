package workersdk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/opencairn-org/cairn-provider-sdk-go/inmem"
	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
)

// capture records every message published on subject.
func capture(t *testing.T, bus *inmem.Bus, subject string) *[]port.Message {
	t.Helper()
	var got []port.Message
	if _, err := bus.Subscribe(context.Background(), port.ConsumerConfig{Subject: subject}, func(_ context.Context, m port.Message) error {
		got = append(got, m)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return &got
}

func TestPublishResultViaBlob_UploadsBodyAndPublishesEnvelope(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()

	var uploaded []byte
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("blob upload method = %s; want PUT", r.Method)
		}
		uploaded, _ = io.ReadAll(r.Body)
	}))
	defer srv.Close()

	var presignReq PresignUploadRequest
	_, _ = bus.RespondTo(ctx, "cairn.blobs.presign_upload.p", func(_ context.Context, body []byte) ([]byte, error) {
		if err := json.Unmarshal(body, &presignReq); err != nil {
			return nil, err
		}
		return json.Marshal(PresignedURL{Method: "PUT", URL: srv.URL, BlobID: "transfer/p/b1.json"})
	})
	published := capture(t, bus, "cairn.results.fetch_source.p.n")

	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus, Version: "1.2", Package: "pkg", Release: "v0.7.0"})
	full := w.NewJobResult(&workerv1.WorkerEvent{
		Type: workerv1.WorkerEventType_WORKER_EVENT_TYPE_ACTIVITY,
		Entity: &workerv1.WorkerEvent_Activity{Activity: &workerv1.ImportedActivity{
			Ref: &workerv1.ExternalRef{Provider: "p", ExternalId: "42"},
		}},
	})
	job := Job{Subject: "cairn.jobs.fetch_source.p.n"}
	if err := w.PublishResultViaBlob(ctx, job, "result:j1:d1", full); err != nil {
		t.Fatalf("PublishResultViaBlob: %v", err)
	}
	if presignReq.Kind != "result" || presignReq.ContentLength != int64(len(uploaded)) {
		t.Errorf("presign request = %+v", presignReq)
	}
	var stored workerv1.JobResult
	if err := protojson.Unmarshal(uploaded, &stored); err != nil {
		t.Fatalf("uploaded body is not a JobResult: %v", err)
	}
	if len(stored.GetEvents()) != 1 || stored.GetWorkerVersion() != "1.2" || stored.GetWorkerRelease() != "v0.7.0" {
		t.Fatalf("uploaded JobResult = %v", &stored)
	}
	var env workerv1.JobResult
	if err := protojson.Unmarshal((*published)[0].Body, &env); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(uploaded)
	ref := env.GetPayloadRef()
	if len(env.GetEvents()) != 0 || ref.GetBlobId() != "transfer/p/b1.json" || ref.GetContentSha256() != hex.EncodeToString(sum[:]) ||
		ref.GetSizeBytes() != int64(len(uploaded)) || env.GetWorkerPackage() != "pkg" || env.GetWorkerRelease() != "v0.7.0" {
		t.Errorf("envelope = %v", &env)
	}

	// Presign failure and upload failure are transient.
	srv.Close()
	if err := w.PublishResultViaBlob(ctx, job, "m", full); err == nil {
		t.Error("upload failure: want error")
	}
	bare := mustNew(t, Config{Name: "n", Provider: "p", Bus: inmem.New()})
	if err := bare.PublishResultViaBlob(ctx, job, "m", full); err == nil {
		t.Error("presign failure: want error")
	}
}

func TestPublishFailureResult(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()
	published := capture(t, bus, "cairn.results.fetch_source.p.n")
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})
	job := Job{Subject: "cairn.jobs.fetch_source.p.n", DeliveryAttempt: 2}
	ref := &workerv1.ExternalRef{UserId: "u", Provider: "p", ExternalAccountId: "acc-1", ExternalId: "42"}

	w.PublishFailureResult(ctx, job, ref, "needs_reauth", errors.New("token revoked"))
	w.PublishFailureResult(ctx, job, &workerv1.ExternalRef{}, "needs_reauth", nil) // nothing to correlate
	if len(*published) != 1 {
		t.Fatalf("published %d results; want 1", len(*published))
	}
	var env workerv1.JobResult
	if err := protojson.Unmarshal((*published)[0].Body, &env); err != nil {
		t.Fatal(err)
	}
	if env.GetError().GetCode() != "needs_reauth" || env.GetError().GetClass() != workerv1.ErrorClass_ERROR_CLASS_AUTH_EXPIRED ||
		env.GetError().GetMessage() != "token revoked" || env.GetFailedRef().GetExternalId() != "42" || env.GetWorkerName() != "n" {
		t.Errorf("failure envelope = %v", &env)
	}
	if (*published)[0].Headers["Nats-Msg-Id"] != "failed:p:42:d2" {
		t.Errorf("msg id = %q", (*published)[0].Headers["Nats-Msg-Id"])
	}
	if FailureClass("not_found") != workerv1.ErrorClass_ERROR_CLASS_NOT_FOUND || FailureClass("x") != workerv1.ErrorClass_ERROR_CLASS_INVALID_INPUT {
		t.Error("FailureClass")
	}
}

func TestBlobHelpers(t *testing.T) {
	ctx := context.Background()
	var gotHeader, gotCT, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader, gotCT, gotMethod = r.Header.Get("X-Amz-Meta"), r.Header.Get("Content-Type"), r.Method
		switch r.URL.Path {
		case "/fail":
			w.WriteHeader(http.StatusForbidden)
		case "/data":
			_, _ = w.Write([]byte("payload"))
		}
	}))
	defer srv.Close()

	up := PresignedURL{URL: srv.URL + "/ok", RequiredHeaders: map[string]string{"X-Amz-Meta": "m", "Content-Type": "application/x"}}
	if err := PutPresigned(ctx, up, []byte("b"), "application/json"); err != nil || gotHeader != "m" || gotCT != "application/x" || gotMethod != http.MethodPut {
		t.Errorf("put = %v (header %q ct %q method %q)", err, gotHeader, gotCT, gotMethod)
	}
	for name, u := range map[string]string{"403": srv.URL + "/fail", "unreachable": "http://127.0.0.1:1", "bad url": "://bad"} {
		if err := PutPresigned(ctx, PresignedURL{URL: u}, nil, "x"); err == nil {
			t.Errorf("%s put: want error", name)
		}
		if _, err := DownloadBlob(ctx, u); err == nil {
			t.Errorf("%s download: want error", name)
		}
	}
	if b, err := DownloadBlob(ctx, srv.URL+"/data"); err != nil || string(b) != "payload" {
		t.Errorf("download = %q, %v", b, err)
	}

	bus := inmem.New()
	_, _ = bus.RespondTo(ctx, "cairn.blobs.presign_upload.p", func(_ context.Context, body []byte) ([]byte, error) {
		var req PresignUploadRequest
		_ = json.Unmarshal(body, &req)
		if req.UserID != "u1" || req.ContentType != "application/octet-stream" || req.ContentLength != 3 {
			t.Errorf("archive presign request = %+v", req)
		}
		return json.Marshal(PresignedURL{URL: srv.URL + "/ok", BlobID: "users/u1/p/uploads/b"})
	})
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})
	id, size, err := w.ArchiveBlob(ctx, "u1", []byte("fit"), "application/octet-stream")
	if err != nil || id != "users/u1/p/uploads/b" || size != 3 {
		t.Errorf("archive = %q, %d, %v", id, size, err)
	}
	if _, _, err := mustNew(t, Config{Name: "n", Provider: "p", Bus: inmem.New()}).ArchiveBlob(ctx, "u1", nil, "x"); err == nil {
		t.Error("archive without presign responder: want error")
	}
	_, _ = bus.RespondTo(ctx, "cairn.blobs.presign_upload.p", func(context.Context, []byte) ([]byte, error) {
		return json.Marshal(PresignedURL{URL: srv.URL + "/fail"})
	})
	if _, _, err := w.ArchiveBlob(ctx, "u1", nil, "x"); err == nil {
		t.Error("archive upload failure: want error")
	}
}

func TestBlobDownloadURL(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})

	if u, err := w.BlobDownloadURL(ctx, BlobRef{URL: "http://fresh"}); err != nil || u != "http://fresh" {
		t.Errorf("fresh = %q, %v", u, err)
	}
	if u, err := w.BlobDownloadURL(ctx, BlobRef{URL: "http://fresh", ExpiresAt: time.Now().Add(time.Hour)}); err != nil || u != "http://fresh" {
		t.Errorf("unexpired = %q, %v", u, err)
	}
	var term *port.TerminalError
	if _, err := w.BlobDownloadURL(ctx, BlobRef{URL: "http://old", ExpiresAt: time.Now().Add(-time.Hour)}); !errors.As(err, &term) || term.Reason != "no_blob_handle" {
		t.Errorf("no handle = %v", err)
	}
	if _, err := w.BlobDownloadURL(ctx, BlobRef{FallbackHandle: "k"}); err == nil || errors.As(err, &term) {
		t.Errorf("presign failure = %v; want transient", err)
	}
	_, _ = bus.RespondTo(ctx, "cairn.blobs.presign_download.p", func(_ context.Context, body []byte) ([]byte, error) {
		var req PresignDownloadRequest
		_ = json.Unmarshal(body, &req)
		return json.Marshal(PresignedURL{URL: "http://signed/" + req.BlobID})
	})
	if u, err := w.BlobDownloadURL(ctx, BlobRef{FallbackHandle: "k"}); err != nil || u != "http://signed/k" {
		t.Errorf("re-presign = %q, %v", u, err)
	}
}

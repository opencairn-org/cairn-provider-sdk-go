package workersdk

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/inmem"
	"github.com/opencairn-org/cairn-provider-sdk-go/port"
)

func TestEnqueueFetchSource(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})
	got := capture(t, bus, w.JobSubject(JobFetchSource))
	for i := 0; i < 2; i++ { // the second enqueue dedups on the msg-id
		if err := w.EnqueueFetchSource(ctx, "job1", "acct", "u1", "reconcile", "42"); err != nil {
			t.Fatal(err)
		}
	}
	if len(*got) != 1 {
		t.Fatalf("published %d; want 1", len(*got))
	}
	var j FetchSourceJob
	if err := json.Unmarshal((*got)[0].Body, &j); err != nil {
		t.Fatal(err)
	}
	if j.JobID != "job1:42" || j.ExtID != "42" || !j.FetchStreams || j.Reason != "reconcile" || j.Provider != "p" || j.AccountID != "acct" {
		t.Errorf("job = %+v", j)
	}
	if (*got)[0].Headers["Nats-Msg-Id"] != "reconcile:p:acct:42" {
		t.Errorf("msg id = %q", (*got)[0].Headers["Nats-Msg-Id"])
	}
	if r := j.Ref(); r.GetUserId() != "u1" || r.GetExternalAccountId() != "acct" || r.GetExternalId() != "42" || r.GetProvider() != "p" {
		t.Errorf("ref = %v", r)
	}
	if r := (ParseBlobJob{UserID: "u", AccountID: "a", ExtID: "e", Provider: "p"}).Ref(); r.GetExternalId() != "e" || r.GetExternalAccountId() != "a" {
		t.Errorf("parse ref = %v", r)
	}
	since, until := (ImportMetricsJob{SinceUnix: 10, UntilUnix: 20}).Window()
	if since.Unix() != 10 || until.Unix() != 20 || since.Location() != time.UTC {
		t.Error("metrics window")
	}
}

func TestReconcileWindowStart(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	floor := now.Add(-ReconcileLookbackFloor).Unix()
	recent := now.Add(-3 * time.Hour)
	ancient := now.Add(-365 * 24 * time.Hour)

	if got := ReconcileWindowStart(nil, now); got != floor {
		t.Errorf("no watermark: %d; want floor %d", got, floor)
	}
	if got := ReconcileWindowStart(&recent, now); got != recent.Add(-time.Hour).Unix() {
		t.Errorf("recent watermark: %d; want watermark-1h", got)
	}
	if got := ReconcileWindowStart(&ancient, now); got != floor {
		t.Errorf("ancient watermark: %d; want floor %d", got, floor)
	}
	if got := ReconcileWindowStart(nil, time.Unix(0, 0)); got != 0 {
		t.Errorf("pre-epoch window: %d; want 0", got)
	}
}

func TestParserVersionGuards(t *testing.T) {
	if ParserTooOldBackoff(1) != 30*time.Second || ParserTooOldBackoff(2) != time.Minute || ParserTooOldBackoff(99) != 10*time.Minute {
		t.Error("backoff bounds")
	}
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: inmem.New(), Version: "1.1"})
	if err := w.CheckParserVersion(Job{DeliveryAttempt: 1}, "1.0"); err != nil {
		t.Errorf("allowed = %v", err)
	}
	var nak *port.NakWithDelayError
	if err := w.CheckParserVersion(Job{DeliveryAttempt: 1}, "1.9"); !errors.As(err, &nak) || nak.Reason != "parser_too_old" || nak.Delay != 30*time.Second {
		t.Errorf("newer source = %v", err)
	}
	var term *port.TerminalError
	var tooOld *ParserTooOldError
	err := w.CheckParserVersion(Job{DeliveryAttempt: ParserTooOldAttempts}, "1.9")
	if !errors.As(err, &term) || term.Reason != "parser_too_old" || !errors.As(err, &tooOld) || tooOld.Min != "1.9" || tooOld.Error() == "" {
		t.Errorf("exhausted = %v", err)
	}
}

func TestRespondDiscover(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus})
	var seen DiscoverRequest
	sub, err := w.RespondDiscover(ctx, func(_ context.Context, req DiscoverRequest) DiscoverResponse {
		seen = req
		return DiscoverResponse{Items: []DiscoverItem{{ItemType: "activity", ExternalID: "1", ItemTime: "2026-01-01T00:00:00Z"}}, Complete: true}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(ctx) }()
	out, err := bus.Request(ctx, "cairn.discover.p.n", []byte(`{"account_id":"a"}`), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp DiscoverResponse
	if json.Unmarshal(out, &resp) != nil || !resp.Complete || len(resp.Items) != 1 || seen.StartPage != 1 || seen.AccountID != "a" {
		t.Errorf("resp = %+v seen = %+v", resp, seen)
	}
	out, _ = bus.Request(ctx, "cairn.discover.p.n", []byte(`{`), time.Second)
	if json.Unmarshal(out, &resp) != nil || resp.Error != "bad_request" {
		t.Errorf("bad request = %s", out)
	}
}

func TestWebhookEnvelope(t *testing.T) {
	body, _ := json.Marshal(WebhookEnvelope{Provider: "p", Headers: map[string]string{"x-sig": "s"}, Body: []byte(`{"id":1}`)})
	env, err := (WebhookEvent{Body: body}).Envelope()
	if err != nil || env.Provider != "p" || env.Headers["x-sig"] != "s" || string(env.Body) != `{"id":1}` {
		t.Fatalf("envelope = %+v, %v", env, err)
	}
	var term *port.TerminalError
	if _, err := (WebhookEvent{Body: []byte("{")}).Envelope(); !errors.As(err, &term) || term.Reason != "bad_envelope" {
		t.Errorf("bad envelope = %v", err)
	}
}

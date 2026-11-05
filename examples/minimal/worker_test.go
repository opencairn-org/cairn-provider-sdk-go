package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/opencairn-org/cairn-provider-sdk-go/examples/minimal/fakeprovider"
	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	cairnv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/v1"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
	"github.com/opencairn-org/cairn-provider-sdk-go/sdktest"
	"github.com/opencairn-org/cairn-provider-sdk-go/webhook"
	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

const (
	owner   = "owner-1"
	account = "acct-1"
	user    = "user-1"
)

var t0 = time.Date(2026, 5, 1, 7, 0, 0, 0, time.UTC)

// stack is the fake core, the fake provider and a running worker.
type stack struct {
	core     *sdktest.Core
	provider *fakeprovider.Server
	w        *workersdk.Worker
}

func startStack(t *testing.T) *stack {
	t.Helper()
	provider := fakeprovider.New()
	api := httptest.NewServer(provider.Handler())
	t.Cleanup(api.Close)
	t.Setenv("ACME_BASE_URL", api.URL)
	t.Setenv("ACME_WEBHOOK_SECRET", "hook-secret")
	t.Setenv("CAIRN_WORKER_NAME", "")
	cfg, err := loadSettings()
	if err != nil {
		t.Fatal(err)
	}

	core := sdktest.Start(t, sdktest.WithRelease("v0.7.0"))
	cfg.env.NATSURL = core.URL
	bus := core.Connect(t, workerName)
	if _, err := newWorker(bus, cfg, cfg.env.NewLogger()); err != nil {
		t.Fatal(err)
	}
	// Build with sdktest defaults (fast heartbeat, batch of one).
	w := core.Worker(t, workersdk.Config{
		Name: workerName, Provider: workerProvider, Version: parserVersion, Package: workerPackage, Release: "v0.7.1", Bus: bus,
		Auth: newRefresher(cfg.baseURL), RefreshLock: workersdk.NewRefreshLock(bus, nil, "test", time.Minute), Manifest: manifest(), ProviderMeta: providerMeta(cfg.baseURL),
	})
	client := newClient(cfg.baseURL)
	registerHandlers(w, client, cfg)
	ctx := context.Background()
	if _, err := w.RespondDiscover(ctx, makeDiscover(w, client)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.RespondOAuthExchange(ctx, newExchanger(cfg.baseURL).Exchange); err != nil {
		t.Fatal(err)
	}
	core.Run(t, w)
	core.SetAccount(workerProvider, owner, account, user)
	return &stack{core: core, provider: provider, w: w}
}

// seedToken gives core a valid token for the account.
func (s *stack) seedToken(t *testing.T, expiresIn time.Duration) {
	t.Helper()
	access, refresh := s.provider.IssueToken(owner)
	s.core.SetToken(account, port.TokenState{AccessToken: access, RefreshToken: refresh, ExpiresAt: time.Now().Add(expiresIn), TokenType: "Bearer"},
		workersdk.OAuthClientCreds{ClientID: "cid", ClientSecret: "sec"})
}

func TestFetchSourceAndParseBlob(t *testing.T) {
	s := startStack(t)
	s.seedToken(t, time.Hour)
	s.provider.AddActivity(owner, "a1", "Commute", t0)

	s.core.PublishJob(t, s.w.JobSubject(workersdk.JobFetchSource), "f1", workersdk.FetchSourceJob{JobID: "j1", AccountID: account, UserID: user, Provider: workerProvider, ExtID: "a1", FetchStreams: true})
	res := s.core.AwaitResult(t, s.w.ResultSubject(workersdk.JobFetchSource), 10*time.Second)
	if res.GetWorkerVersion() != parserVersion || res.GetWorkerPackage() != workerPackage || res.GetWorkerRelease() != "v0.7.1" {
		t.Errorf("stamp = %v", res)
	}
	act := res.GetEvents()[0].GetActivity()
	p := act.GetPayload()
	if act.GetRef().GetExternalId() != "a1" || act.GetRef().GetUserId() != user || p.GetTitle() != "Commute" || p.GetType() != cairnv1.ActivityType_ACTIVITY_TYPE_RIDE ||
		p.GetDiscipline() != cairnv1.Discipline_DISCIPLINE_RIDE_ROAD || act.GetStream().GetSampleCount() != 30 || len(p.GetLaps()) != 1 || p.GetSummary().GetDistanceM() != 300 {
		t.Errorf("activity = %v", act)
	}
	if act.GetRawContentType() != "application/vnd.ant.fit" || act.GetRawSizeBytes() == 0 {
		t.Errorf("archive = %q %d", act.GetRawContentType(), act.GetRawSizeBytes())
	}
	raw, ok := s.core.Blobs.Get(act.GetRawBlobId())
	if !ok || len(raw) != int(act.GetRawSizeBytes()) {
		t.Fatalf("archived FIT missing: %v", ok)
	}

	// parse_blob re-maps the archive without touching the provider.
	calls := s.provider.Calls
	s.core.PublishJob(t, s.w.JobSubject(workersdk.JobParseBlob), "p1", workersdk.ParseBlobJob{JobID: "p1", SourceID: "src", UserID: user, Provider: workerProvider, AccountID: account, ExtID: "a1",
		MinParserVersion: "1.0", Blob: workersdk.BlobRef{URL: s.core.Blobs.URL() + "/" + act.GetRawBlobId(), FallbackHandle: act.GetRawBlobId()}})
	re := s.core.AwaitResult(t, s.w.ResultSubject(workersdk.JobParseBlob), 10*time.Second)
	if got := re.GetEvents()[0].GetActivity(); got.GetRawBlobId() != act.GetRawBlobId() || got.GetStream().GetSampleCount() != 30 || got.GetRef().GetExternalId() != "a1" {
		t.Errorf("reparse = %v", got)
	}
	if s.provider.Calls != calls {
		t.Error("parse_blob must not call the provider")
	}

	// A newer parser owns the source: this instance hands the job back and
	// eventually reports parser_too_old.
	s.core.PublishJob(t, s.w.JobSubject(workersdk.JobParseBlob), "p2", workersdk.ParseBlobJob{JobID: "p2", UserID: user, Provider: workerProvider, AccountID: account, ExtID: "a1", MinParserVersion: "1.9"})
	if err := s.w.CheckParserVersion(workersdk.Job{DeliveryAttempt: 1}, "1.9"); err == nil {
		t.Error("newer source must be refused")
	}

	// A missing activity fails the item with not_found.
	s.core.PublishJob(t, s.w.JobSubject(workersdk.JobFetchSource), "f2", workersdk.FetchSourceJob{JobID: "j2", AccountID: account, UserID: user, Provider: workerProvider, ExtID: "nope"})
	fail := s.core.AwaitResult(t, s.w.ResultSubject(workersdk.JobFetchSource), 10*time.Second)
	if fail.GetError().GetCode() != "not_found" || fail.GetError().GetClass() != workerv1.ErrorClass_ERROR_CLASS_NOT_FOUND || fail.GetFailedRef().GetExternalId() != "nope" {
		t.Errorf("failure = %v", fail)
	}
}

func TestTokenRefreshAndExchange(t *testing.T) {
	s := startStack(t)
	s.seedToken(t, -time.Minute) // expired: the first call refreshes
	s.provider.AddActivity(owner, "a1", "Ride", t0)

	s.core.PublishJob(t, s.w.JobSubject(workersdk.JobFetchSource), "f1", workersdk.FetchSourceJob{JobID: "j1", AccountID: account, UserID: user, Provider: workerProvider, ExtID: "a1"})
	s.core.AwaitResult(t, s.w.ResultSubject(workersdk.JobFetchSource), 10*time.Second)
	if s.provider.Refreshes != 1 || len(s.core.StoreCalls()) != 1 {
		t.Errorf("refreshes = %d stores = %d; want one each", s.provider.Refreshes, len(s.core.StoreCalls()))
	}
	if st, _ := s.core.Token(account); st.RefreshToken == "" || time.Until(st.ExpiresAt) < 59*time.Minute {
		t.Errorf("stored state = %+v", st)
	}

	// A revoked grant: refresh fails terminally, core is told.
	st, _ := s.core.Token(account)
	s.provider.RevokeRefresh(st.RefreshToken)
	st.ExpiresAt = time.Now().Add(-time.Minute)
	s.core.SetToken(account, st, workersdk.OAuthClientCreds{ClientID: "cid", ClientSecret: "sec"})
	w2 := s.core.Worker(t, workersdk.Config{Name: "acme-two", Provider: workerProvider, Auth: newRefresher(workersdk.EnvOr("ACME_BASE_URL", ""))})
	_, err := w2.FetchToken(context.Background(), account)
	var term *port.TerminalError
	if !errors.As(err, &term) || term.Reason != "invalid_grant" || len(s.core.ReauthCalls()) != 1 {
		t.Errorf("revoked = %v reauth %v", err, s.core.ReauthCalls())
	}

	// The connect flow's code exchange returns tokens plus the owner id.
	s.provider.GrantCode("code-1", owner)
	reply, err := s.core.ExchangeCode(workerProvider, workerName, workersdk.OAuthExchangeRequest{ConnectionID: "c", ClientID: "cid", ClientSecret: "sec", Code: "code-1", RedirectURI: "https://cairn.test/cb", CodeVerifier: "v"})
	if err != nil || reply.Error != "" || reply.AccessToken == "" || reply.RefreshToken == "" || reply.ProviderAccountID != owner || reply.Scope != "activity:read" {
		t.Errorf("exchange = %+v, %v", reply, err)
	}
	if reply, _ = s.core.ExchangeCode(workerProvider, workerName, workersdk.OAuthExchangeRequest{ClientID: "cid", ClientSecret: "sec", Code: "code-1"}); reply.Error != "exchange_failed" || reply.Message != "invalid_grant" {
		t.Errorf("reused code = %+v", reply)
	}
}

func TestReconcileDiscoverMetrics(t *testing.T) {
	s := startStack(t)
	s.seedToken(t, time.Hour)
	for i, id := range []string{"old", "a1", "a2", "a3"} {
		start := time.Now().Add(-time.Duration(3-i) * 24 * time.Hour)
		if id == "old" {
			start = time.Now().Add(-90 * 24 * time.Hour) // beyond the lookback floor
		}
		s.provider.AddActivity(owner, id, "Ride "+id, start)
	}
	s.provider.AddMetric(owner, "weight_kg", t0, 70.5)
	s.provider.AddMetric(owner, "weight_kg", t0.Add(24*time.Hour), 70.1)

	s.core.PublishJob(t, s.w.JobSubject(workersdk.JobReconcile), "r1", workersdk.ReconcileJob{JobID: "r1", AccountID: account, UserID: user, Provider: workerProvider, KnownExtIDs: []string{"a2"}})
	wm := s.core.AwaitResult(t, s.w.ResultSubject(workersdk.JobReconcile)+"."+account, 10*time.Second)
	if wm.GetNewWatermark() == nil || time.Since(wm.GetNewWatermark().AsTime()) > 2*time.Hour {
		t.Errorf("watermark = %v", wm.GetNewWatermark())
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ { // a1 and a3 are fetched; a2 is known, old is outside the window
		res := s.core.AwaitResult(t, s.w.ResultSubject(workersdk.JobFetchSource), 10*time.Second)
		seen[res.GetEvents()[0].GetActivity().GetRef().GetExternalId()] = true
	}
	if !seen["a1"] || !seen["a3"] {
		t.Errorf("fetched = %v", seen)
	}

	disc, err := s.core.Discover(workerProvider, workerName, workersdk.DiscoverRequest{AccountID: account})
	if err != nil || !disc.Complete || len(disc.Items) != 4 || disc.Items[0].ExternalID != "old" || disc.Items[0].ItemType != "activity" {
		t.Errorf("discover = %+v, %v", disc, err)
	}
	if disc, _ = s.core.Discover(workerProvider, workerName, workersdk.DiscoverRequest{AccountID: "unknown"}); disc.Error == "" {
		t.Errorf("discover unknown account = %+v", disc)
	}

	s.core.PublishJob(t, s.w.JobSubject(workersdk.JobImportMetrics), "m1", workersdk.ImportMetricsJob{AccountID: account, UserID: user, Provider: workerProvider, SinceUnix: t0.Unix(), UntilUnix: t0.Add(36 * time.Hour).Unix()})
	metrics := s.core.AwaitResult(t, s.w.ResultSubject(workersdk.JobImportMetrics), 10*time.Second)
	if len(metrics.GetEvents()) != 2 || metrics.GetEvents()[0].GetMetric().GetKey() != "weight_kg" || metrics.GetEvents()[0].GetMetric().GetValueNumeric() != 70.5 ||
		!metrics.GetEvents()[0].GetMetric().GetTs().AsTime().Equal(timestamppb.New(t0).AsTime()) {
		t.Errorf("metrics = %v", metrics.GetEvents())
	}
}

func TestWebhooks(t *testing.T) {
	s := startStack(t)
	s.seedToken(t, time.Hour)
	s.provider.AddActivity(owner, "a9", "Pushed", t0)
	key := s.w.WorkerKey()

	body, _ := json.Marshal(acmeEvent{OwnerID: owner, ActivityID: "a9", Type: "created"})
	sig := "sha256=" + webhook.Sign([]byte("hook-secret"), webhook.Hex, body)
	s.core.ForwardWebhook(t, key, workerProvider, map[string]string{"X-Acme-Signature": sig}, body)
	res := s.core.AwaitResult(t, s.w.ResultSubject(workersdk.JobFetchSource), 10*time.Second)
	if res.GetEvents()[0].GetActivity().GetRef().GetExternalId() != "a9" || res.GetEvents()[0].GetActivity().GetPayload().GetTitle() != "Pushed" {
		t.Errorf("webhook fetch = %v", res)
	}

	del, _ := json.Marshal(acmeEvent{OwnerID: owner, ActivityID: "a9", Type: "deleted"})
	s.core.ForwardWebhook(t, key, workerProvider, map[string]string{"X-Acme-Signature": webhook.Sign([]byte("hook-secret"), webhook.Hex, del)}, del)
	ev := s.core.AwaitMessage(t, "cairn.events.source.deleted_upstream", 10*time.Second)
	var deleted map[string]string
	if json.Unmarshal(ev.Body, &deleted) != nil || deleted["external_id"] != "a9" || deleted["account_id"] != account {
		t.Errorf("deleted event = %s", ev.Body)
	}

	// Forged and orphaned events never produce work.
	s.core.ForwardWebhook(t, key, workerProvider, map[string]string{"X-Acme-Signature": "sha256=00"}, body)
	orphan, _ := json.Marshal(acmeEvent{OwnerID: "stranger", ActivityID: "a9", Type: "created"})
	s.core.ForwardWebhook(t, key, workerProvider, map[string]string{"X-Acme-Signature": webhook.Sign([]byte("hook-secret"), webhook.Hex, orphan)}, orphan)

	v, err := s.core.VerifyWebhook(key, "GET", map[string]string{"verify_token": "hook-secret", "challenge": "c1"})
	if err != nil || v.Status != 200 || string(v.Body) != "c1" {
		t.Errorf("verify = %+v, %v", v, err)
	}
	if v, _ = s.core.VerifyWebhook(key, "GET", map[string]string{"verify_token": "nope"}); v.Status != 403 {
		t.Errorf("bad token = %+v", v)
	}
	if n := len(s.core.Results(t, s.w.ResultSubject(workersdk.JobFetchSource))); n != 1 {
		t.Errorf("results = %d; forged/orphaned events must not fetch", n)
	}
	hb := s.core.AwaitPresence(t, workerName, time.Second)
	if !hb.Webhooks || hb.WorkerKey != "acme-fetcher-acme" {
		t.Errorf("presence = %+v", hb)
	}
	if m := s.core.Manifest(t, workerName); m["provider_meta"] == nil {
		t.Error("provider_meta missing from the manifest")
	}
}

func TestMapErrAndSettings(t *testing.T) {
	var term *port.TerminalError
	var nak *port.NakWithDelayError
	if err := mapErr(errUnauthorized, "x"); !errors.As(err, &term) || term.Reason != "needs_reauth" {
		t.Error("401")
	}
	if err := mapErr(&rateLimitError{}, "x"); !errors.As(err, &nak) || nak.Delay != time.Second {
		t.Error("429")
	}
	if err := mapErr(errors.New("boom"), "x"); errors.As(err, &term) || errors.As(err, &nak) {
		t.Error("5xx")
	}
	t.Setenv("CAIRN_LOG_LEVEL", "bad")
	if _, err := loadSettings(); err == nil {
		t.Error("bad env: want error")
	}
	if err := manifest().Validate(); err != nil {
		t.Error(err)
	}
}

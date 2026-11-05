package sdktest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/natsbus"
	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

// refresher rotates the refresh token once; a reused one is terminal.
type refresher struct{ used map[string]bool }

func (r *refresher) Refresh(_ context.Context, cur port.TokenState, creds workersdk.OAuthClientCreds) (port.TokenState, error) {
	if r.used[cur.RefreshToken] || creds.ClientID == "" {
		return port.TokenState{}, &port.TerminalError{Reason: "invalid_grant"}
	}
	r.used[cur.RefreshToken] = true
	return port.TokenState{AccessToken: "fresh", RefreshToken: cur.RefreshToken + "x", ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func TestCoreDrivesAWorkerEndToEnd(t *testing.T) {
	core := Start(t, WithRelease("v0.7.0"))
	core.SetToken("acct-1", port.TokenState{AccessToken: "old", RefreshToken: "r1", ExpiresAt: time.Now().Add(-time.Minute), AppID: "app"}, workersdk.OAuthClientCreds{ClientID: "cid", ClientSecret: "sec"})
	core.SetTokenError("acct-gone", "needs_reauth", 0)
	core.SetAccount("acme", "owner-9", "acct-1", "user-1")
	core.SetCreds("acct-cred", "login", "pw")

	bus := core.Connect(t, "acme-fetcher")
	w := core.Worker(t, workersdk.Config{
		Name: "acme-fetcher", Provider: "acme", Version: "1.0", Package: "pkg", Release: "v0.7.3", Bus: bus,
		Auth:        &refresher{used: map[string]bool{}},
		RefreshLock: workersdk.NewRefreshLock(bus, nil, "test", time.Minute),
		Limiter:     workersdk.NewRateLimiter(bus, nil, map[string]natsbus.Limit{"acme:acme-fetcher:minute": {Capacity: 10, Window: natsbus.PerMinute}}),
		ProviderMeta: &workersdk.ProviderMeta{DisplayName: "Acme", AuthMethod: workersdk.AuthMethodOAuth2,
			OAuth: &workersdk.ProviderOAuth{AuthorizeURL: "https://acme.test/authorize"}},
	})
	w.Handle(w.JobSubject(workersdk.JobFetchSource), func(ctx context.Context, w *workersdk.Worker, job workersdk.Job) error {
		var in workersdk.FetchSourceJob
		if err := json.Unmarshal(job.Body, &in); err != nil {
			return &port.TerminalError{Reason: "bad_payload", Cause: err}
		}
		tok, err := w.FetchToken(ctx, in.AccountID)
		if err != nil {
			w.PublishFailureResult(ctx, job, in.Ref(), "needs_reauth", err)
			return err
		}
		if err := w.ReserveAPI(ctx, w.RateBucket("minute"), 1); err != nil {
			return err
		}
		id, size, err := w.ArchiveBlob(ctx, in.UserID, []byte("raw "+tok.AccessToken), "application/octet-stream")
		if err != nil {
			return err
		}
		act := &workerv1.ImportedActivity{Ref: in.Ref(), RawBlobId: id, RawSizeBytes: size}
		_ = w.PublishEvent(ctx, "example.fetched", "ev:"+in.ExtID, []byte(in.ExtID))
		return w.PublishResultViaBlob(ctx, job, "result:"+in.JobID, w.NewJobResult(&workerv1.WorkerEvent{
			Type: workerv1.WorkerEventType_WORKER_EVENT_TYPE_ACTIVITY, Entity: &workerv1.WorkerEvent_Activity{Activity: act},
		}))
	})
	w.WebhookEvent(func(ctx context.Context, w *workersdk.Worker, ev workersdk.WebhookEvent) error {
		env, err := ev.Envelope()
		if err != nil {
			return err
		}
		req, _ := json.Marshal(map[string]string{"provider": "acme", "external_id": env.Headers["x-owner"], "worker_name": w.Name()})
		out, err := w.Request(ctx, w.LookupSubject(), req, time.Second)
		if err != nil {
			return err
		}
		var acct struct {
			AccountID string `json:"account_id"`
			UserID    string `json:"user_id"`
		}
		_ = json.Unmarshal(out, &acct)
		return w.EnqueueFetchSource(ctx, "hook", acct.AccountID, acct.UserID, "webhook", string(env.Body))
	})
	w.WebhookVerify(func(_ context.Context, _ *workersdk.Worker, req workersdk.WebhookVerifyRequest) (workersdk.WebhookVerifyResponse, error) {
		return workersdk.WebhookVerifyResponse{Status: 200, ContentType: "text/plain", Body: []byte(req.Query["challenge"])}, nil
	})
	ctx := context.Background()
	if _, err := w.RespondDiscover(ctx, func(_ context.Context, req workersdk.DiscoverRequest) workersdk.DiscoverResponse {
		return workersdk.DiscoverResponse{Items: []workersdk.DiscoverItem{{ItemType: "activity", ExternalID: "1"}}, Complete: true, NextPage: req.StartPage}
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.RespondOAuthExchange(ctx, func(_ context.Context, req workersdk.OAuthExchangeRequest) workersdk.OAuthExchangeReply {
		return workersdk.OAuthExchangeReply{AccessToken: "at", ProviderAccountID: req.Code + "-owner"}
	}); err != nil {
		t.Fatal(err)
	}
	core.Run(t, w)

	// Presence and manifest, as core reads them.
	hb := core.AwaitPresence(t, "acme-fetcher", 5*time.Second)
	if hb.WorkerKey != "acme-fetcher-acme" || hb.Release != "v0.7.3" || !hb.Webhooks || hb.Provider != "acme" {
		t.Errorf("heartbeat = %+v", hb)
	}
	if m := core.Manifest(t, "acme-fetcher"); m == nil || m["provider_meta"] == nil {
		t.Errorf("manifest = %v", m)
	}
	if core.Manifest(t, "nobody") != nil {
		t.Error("absent manifest must be nil")
	}

	// A job: token refreshed under the pool lock and stored, result claim-checked.
	core.PublishJob(t, w.JobSubject(workersdk.JobFetchSource), "m1", workersdk.FetchSourceJob{JobID: "j1", AccountID: "acct-1", UserID: "user-1", Provider: "acme", ExtID: "42"})
	res := core.AwaitResult(t, w.ResultSubject(workersdk.JobFetchSource), 10*time.Second)
	act := res.GetEvents()[0].GetActivity()
	if act.GetRef().GetExternalId() != "42" || res.GetWorkerRelease() != "v0.7.3" {
		t.Fatalf("result = %v", res)
	}
	if raw, ok := core.Blobs.Get(act.GetRawBlobId()); !ok || string(raw) != "raw fresh" || len(core.Blobs.IDs()) != 2 {
		t.Errorf("archive = %q %v (ids %v)", raw, ok, core.Blobs.IDs())
	}
	if st, ok := core.Token("acct-1"); !ok || st.AccessToken != "fresh" || st.RefreshToken != "r1x" || len(core.StoreCalls()) != 1 || core.StoreCalls()[0].Provider != "acme" {
		t.Errorf("token store = %+v %v stores %+v", st, ok, core.StoreCalls())
	}
	if _, ok := core.RateLimit(t, "acme.acme-fetcher.minute"); !ok {
		t.Error("rate-limit bucket not written")
	}
	if ev := core.Events("cairn.events.example.fetched"); len(ev) != 1 || string(ev[0].Body) != "42" {
		t.Errorf("events = %v", ev)
	}
	if all := core.Results(t, "cairn.results.>"); len(all) != 1 {
		t.Errorf("results = %d", len(all))
	}

	// A failing job: failure envelope with the reason, needs_reauth recorded.
	core.PublishJob(t, w.JobSubject(workersdk.JobFetchSource), "m2", workersdk.FetchSourceJob{JobID: "j2", AccountID: "acct-gone", Provider: "acme", ExtID: "43"})
	fail := core.AwaitResult(t, w.ResultSubject(workersdk.JobFetchSource), 10*time.Second)
	if fail.GetError().GetCode() != "needs_reauth" || fail.GetFailedRef().GetExternalId() != "43" {
		t.Errorf("failure = %v", fail)
	}
	if _, ok := core.Token("acct-gone"); ok {
		t.Error("errored account must not read as a token")
	}

	// Request/replies core makes.
	disc, err := core.Discover("acme", "acme-fetcher", workersdk.DiscoverRequest{AccountID: "acct-1"})
	if err != nil || !disc.Complete || disc.NextPage != 1 {
		t.Errorf("discover = %+v, %v", disc, err)
	}
	ex, err := core.ExchangeCode("acme", "acme-fetcher", workersdk.OAuthExchangeRequest{Code: "c"})
	if err != nil || ex.ProviderAccountID != "c-owner" {
		t.Errorf("exchange = %+v, %v", ex, err)
	}
	if _, err := core.Discover("acme", "nobody", workersdk.DiscoverRequest{}); err == nil {
		t.Error("discover without responder: want error")
	}

	// Webhooks: a forwarded POST becomes a fetch job; the GET handshake answers.
	core.ForwardWebhook(t, w.WorkerKey(), "acme", map[string]string{"X-Owner": "owner-9"}, []byte("77"))
	hook := core.AwaitResult(t, w.ResultSubject(workersdk.JobFetchSource), 10*time.Second)
	if hook.GetEvents()[0].GetActivity().GetRef().GetExternalId() != "77" || hook.GetEvents()[0].GetActivity().GetRef().GetUserId() != "user-1" {
		t.Errorf("webhook result = %v", hook)
	}
	verify, err := core.VerifyWebhook(w.WorkerKey(), "GET", map[string]string{"challenge": "c1"})
	if err != nil || verify.Status != 200 || string(verify.Body) != "c1" {
		t.Errorf("verify = %+v, %v", verify, err)
	}

	// Credential providers.
	out, err := bus.Request(ctx, "cairn.creds.acme.get", []byte(`{"account_id":"acct-cred"}`), time.Second)
	if err != nil || string(out) != `{"client_id":"login","client_secret":"pw","email":"login","password":"pw"}` {
		t.Errorf("creds = %s, %v", out, err)
	}
	out, _ = bus.Request(ctx, "cairn.creds.acme.get", []byte(`{"account_id":"nope"}`), time.Second)
	if string(out) != `{"error":"account_gone"}` {
		t.Errorf("creds missing = %s", out)
	}
}

func TestResponderEdgeCases(t *testing.T) {
	core := Start(t)
	ctx := context.Background()
	bus := core.Connect(t, "w")
	client := workersdk.NewTokenClient(bus, "acme", time.Second)

	var term *port.TerminalError
	if _, _, err := client.Get(ctx, "unknown"); !errors.As(err, &term) || term.Reason != "account_gone" {
		t.Errorf("unknown account = %v", err)
	}
	core.SetTokenError("t", "transient", 7)
	var nak *port.NakWithDelayError
	if _, _, err := client.Get(ctx, "t"); !errors.As(err, &nak) || nak.Delay != 7*time.Second {
		t.Errorf("transient = %v", err)
	}
	if _, err := client.Store(ctx, "unknown", port.TokenState{AccessToken: "x"}, time.Time{}); !errors.As(err, &term) || term.Reason != "account_gone" {
		t.Errorf("store unknown = %v", err)
	}
	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	core.SetToken("a", port.TokenState{AccessToken: "a1", ExpiresAt: exp}, workersdk.OAuthClientCreds{})
	// A store from a stale state is refused and the winner returned.
	win, err := client.Store(ctx, "a", port.TokenState{AccessToken: "a2", ExpiresAt: exp.Add(time.Hour)}, exp.Add(-time.Minute))
	if err != nil || win == nil || win.AccessToken != "a1" {
		t.Errorf("stale store = %+v, %v", win, err)
	}
	if win, err := client.Store(ctx, "a", port.TokenState{AccessToken: "a2", ExpiresAt: exp.Add(time.Hour)}, exp); err != nil || win != nil {
		t.Errorf("fresh store = %+v, %v", win, err)
	}
	if err := client.MarkNeedsReauth(ctx, "a", "revoked"); err != nil || len(core.ReauthCalls()) != 1 || core.ReauthCalls()[0].Reason != "revoked" {
		t.Errorf("reauth = %v %+v", err, core.ReauthCalls())
	}
	if _, _, err := client.Get(ctx, "a"); !errors.As(err, &term) || term.Reason != "needs_reauth" {
		t.Errorf("after reauth = %v", err)
	}
	out, _ := bus.Request(ctx, "cairn.tokens.acme.store", []byte("{"), time.Second)
	if string(out) != `{"error":"bad_request","ok":false}` {
		t.Errorf("bad store = %s", out)
	}

	// Lookups prefer the subscription id, fall back to the external id.
	core.SetAccount("acme", "sub-1", "acct-sub", "u")
	core.SetAccount("acme", "owner-1", "acct-owner", "u")
	for req, want := range map[string]string{
		`{"external_id":"owner-1","subscription_id":"sub-1"}`: "acct-sub",
		`{"external_id":"owner-1"}`:                           "acct-owner",
		`{"external_id":"owner-1","subscription_id":"x"}`:     "acct-owner",
		`{"external_id":"nobody"}`:                            "",
	} {
		out, err := bus.Request(ctx, "cairn.accounts.lookup.acme.w", []byte(req), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var resp struct {
			AccountID string `json:"account_id"`
			Error     string `json:"error"`
		}
		_ = json.Unmarshal(out, &resp)
		if resp.AccountID != want || (want == "" && resp.Error != "not_found") {
			t.Errorf("lookup %s = %s", req, out)
		}
	}

	// Blob store: PUT failure, GET of a missing object, other methods.
	core.Blobs.FailPuts(true)
	if err := workersdk.PutPresigned(ctx, workersdk.PresignedURL{URL: core.Blobs.URL() + "/x"}, []byte("b"), "text/plain"); err == nil {
		t.Error("failing put: want error")
	}
	core.Blobs.FailPuts(false)
	if _, err := workersdk.DownloadBlob(ctx, core.Blobs.URL()+"/missing"); err == nil {
		t.Error("missing blob: want error")
	}
	core.Blobs.Put("seed", []byte("s"))
	if b, err := workersdk.DownloadBlob(ctx, core.Blobs.URL()+"/seed"); err != nil || string(b) != "s" {
		t.Errorf("seeded blob = %q, %v", b, err)
	}
	resp, err := core.NC.Request("cairn.blobs.presign_upload.acme", []byte(`{"user_id":"u","source_id":"s","content_type":"x"}`), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var up workersdk.PresignedURL
	_ = json.Unmarshal(resp.Data, &up)
	if up.BlobID != "users/u/acme/sources/s/1" || up.RequiredHeaders["Content-Type"] != "x" {
		t.Errorf("source presign = %+v", up)
	}
}

func TestAwaitMessageTimesOut(t *testing.T) {
	c := &capture{seen: map[string]int{}}
	c.cond = sync.NewCond(&c.mu)
	if _, ok := c.await("cairn.results.x", 10*time.Millisecond); ok {
		t.Error("empty capture must time out")
	}
	c.msgs = append(c.msgs, Message{Subject: "cairn.results.x"}, Message{Subject: "cairn.results.x"})
	if _, ok := c.await("cairn.results.x", time.Millisecond); !ok {
		t.Error("first")
	}
	if _, ok := c.await("cairn.results.x", time.Millisecond); !ok {
		t.Error("second")
	}
	if _, ok := c.await("cairn.results.x", time.Millisecond); ok {
		t.Error("third must time out")
	}
	if !subjectMatches("a.>", "a.b.c") || subjectMatches("a.>", "a") || subjectMatches("a.*", "a.b.c") || !subjectMatches("a.*", "a.b") || subjectMatches("a.b", "a.c") {
		t.Error("subjectMatches")
	}
	for subject, want := range map[string]string{
		"x": "", "cairn.tokens.acme.get": "acme", "cairn.creds.polar.get": "polar", "cairn.blobs.presign_upload.oura": "oura",
		"cairn.blobs.presign_download.whoop": "whoop", "cairn.accounts.lookup.acme.w1": "acme", "cairn.other.x": "",
	} {
		if got := providerOf(subject); got != want {
			t.Errorf("providerOf(%s) = %q", subject, got)
		}
	}
}

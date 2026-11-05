// Package sdktest is the integration-test kit for cairn provider workers:
// an embedded NATS JetStream server bootstrapped the way cairn-core does
// it (the CAIRN_* streams, the KV buckets and the core release key), fake
// core responders for everything a worker asks core (tokens, credentials,
// presigned blob URLs backed by an in-memory store, account lookup) and
// helpers to drive the worker like core would: publish a job and await its
// result, forward a webhook, run the discover and OAuth-exchange
// request/replies.
//
//	core := sdktest.Start(t)
//	core.SetToken("acct-1", port.TokenState{AccessToken: "tok", ExpiresAt: time.Now().Add(time.Hour)}, workersdk.OAuthClientCreds{})
//	w := core.Worker(t, workersdk.Config{Name: "acme-fetcher", Provider: "acme", Version: "1.0"})
//	w.Handle(w.JobSubject(workersdk.JobFetchSource), fetchSource)
//	core.Run(t, w)
//	core.PublishJob(t, w.JobSubject(workersdk.JobFetchSource), "m1", workersdk.FetchSourceJob{JobID: "j1", AccountID: "acct-1", ExtID: "42"})
//	res := core.AwaitResult(t, w.ResultSubject(workersdk.JobFetchSource), 5*time.Second)
//
// Everything is cleaned up through t.Cleanup. The server listens on a free
// loopback port without authentication: the auth-callout path is core's,
// covered by workersdk's own tests.
package sdktest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/opencairn-org/cairn-provider-sdk-go/natsbus"
	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

// Stream and bucket names, as cairn-core bootstraps them
// (internal/adapter/secondary/nats/bootstrap_streams.go).
const (
	StreamJobs     = "CAIRN_JOBS"
	StreamResults  = "CAIRN_RESULTS"
	StreamEvents   = "CAIRN_EVENTS"
	StreamDLQ      = "CAIRN_DLQ"
	StreamWebhooks = "CAIRN_WEBHOOKS"

	KVWorkerPresence  = "cairn_worker_presence"
	KVWorkerManifests = "cairn_worker_manifests"
	KVRateLimits      = "cairn_rate_limits"
	KVCoreInfo        = "cairn_core"
)

// Core is the fake cairn-core plus its NATS server.
type Core struct {
	// URL is the server's client URL (nats://127.0.0.1:<port>).
	URL string
	// NC and JS are core's own connection, for assertions beyond the helpers.
	NC *nats.Conn
	JS jetstream.JetStream
	// Blobs is the object store behind the presign responders.
	Blobs *BlobStore
	// Release is what cairn_core/info advertises.
	Release string

	srv     *server.Server
	mu      sync.Mutex
	tokens  map[string]*tokenRecord
	creds   map[string]credRecord
	lookups map[string]lookupRecord
	stores  []StoreCall
	reauths []ReauthCall
	results *capture
	events  *capture
}

// Option configures Start.
type Option func(*Core)

// WithRelease sets the release core advertises in cairn_core/info (the
// value a tagged core writes, e.g. "v0.7.0"). The default "dev" is what an
// untagged core writes and is compatible with every worker release.
func WithRelease(release string) Option {
	return func(c *Core) { c.Release = release }
}

// Start runs the server, bootstraps streams, buckets and the release key,
// and installs the fake responders. Everything stops with t.
func Start(t testing.TB, opts ...Option) *Core {
	t.Helper()
	so := natsserver.DefaultTestOptions
	so.Host, so.Port, so.JetStream, so.StoreDir, so.NoLog, so.NoSigs = "127.0.0.1", -1, true, t.TempDir(), true, true
	srv := natsserver.RunServer(&so)
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL(), nats.Name("sdktest-core"))
	if err != nil {
		t.Fatalf("sdktest: connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("sdktest: jetstream: %v", err)
	}
	c := &Core{
		URL: srv.ClientURL(), NC: nc, JS: js, Release: "dev", srv: srv,
		tokens: map[string]*tokenRecord{}, creds: map[string]credRecord{}, lookups: map[string]lookupRecord{},
	}
	for _, o := range opts {
		o(c)
	}
	c.Blobs = newBlobStore(t)
	if err := c.bootstrap(context.Background()); err != nil {
		t.Fatalf("sdktest: bootstrap: %v", err)
	}
	c.results = newCapture(t, nc, "cairn.results.>")
	c.events = newCapture(t, nc, "cairn.events.>")
	c.installResponders(t)
	return c
}

// bootstrap mirrors cairn-core's BootstrapStreams and the release key.
func (c *Core) bootstrap(ctx context.Context) error {
	for _, sc := range streamConfigs() {
		if _, err := c.JS.CreateOrUpdateStream(ctx, sc); err != nil {
			return fmt.Errorf("stream %s: %w", sc.Name, err)
		}
	}
	for _, kc := range kvConfigs() {
		if _, err := c.JS.CreateOrUpdateKeyValue(ctx, kc); err != nil {
			return fmt.Errorf("kv %s: %w", kc.Bucket, err)
		}
	}
	kv, err := c.JS.KeyValue(ctx, KVCoreInfo)
	if err != nil {
		return err
	}
	info, _ := json.Marshal(map[string]string{"release": c.Release})
	_, err = kv.Put(ctx, "info", info)
	return err
}

func streamConfigs() []jetstream.StreamConfig {
	return []jetstream.StreamConfig{
		{Name: StreamJobs, Subjects: []string{"cairn.jobs.>"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Discard: jetstream.DiscardNew, Duplicates: 5 * time.Minute},
		{Name: StreamResults, Subjects: []string{"cairn.results.>"}, Retention: jetstream.InterestPolicy, Storage: jetstream.FileStorage, Discard: jetstream.DiscardOld, Duplicates: time.Minute},
		{Name: StreamEvents, Subjects: []string{"cairn.events.>"}, Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage, Discard: jetstream.DiscardOld, MaxAge: 7 * 24 * time.Hour, Duplicates: time.Minute},
		{Name: StreamDLQ, Subjects: []string{"cairn.dlq.>"}, Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage, Discard: jetstream.DiscardOld, MaxAge: 30 * 24 * time.Hour},
		{Name: StreamWebhooks, Subjects: []string{"cairn.webhooks.>"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Discard: jetstream.DiscardOld, MaxAge: 24 * time.Hour, Duplicates: 5 * time.Minute},
	}
}

func kvConfigs() []jetstream.KeyValueConfig {
	return []jetstream.KeyValueConfig{
		{Bucket: KVWorkerPresence, TTL: 60 * time.Second, History: 1},
		{Bucket: KVWorkerManifests, TTL: 15 * time.Minute, History: 5},
		{Bucket: KVRateLimits, TTL: 15 * time.Minute, History: 1},
		{Bucket: KVCoreInfo, History: 1},
	}
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------

// Connect opens a worker-side bus on the server (dev mode, no token) with
// a short pull wait so a worker stops promptly at the end of a test.
func (c *Core) Connect(t testing.TB, workerName string) *natsbus.Bus {
	t.Helper()
	nc, err := nats.Connect(c.URL, nats.Name(workerName))
	if err != nil {
		t.Fatalf("sdktest: worker connect: %v", err)
	}
	bus, err := natsbus.NewBus(nc, natsbus.Options{ClientName: workerName + ":test", Logger: quiet(), FetchMaxWait: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("sdktest: worker bus: %v", err)
	}
	t.Cleanup(bus.Close)
	return bus
}

// Worker builds a worker on a fresh bus. A nil cfg.Bus is filled in; a
// zero HeartbeatInterval becomes 100ms so presence assertions are quick,
// and a zero PullBatchSize becomes 1 so a single job is dispatched as soon
// as it arrives instead of after the pull's max wait.
func (c *Core) Worker(t testing.TB, cfg workersdk.Config) *workersdk.Worker {
	t.Helper()
	if cfg.Bus == nil {
		cfg.Bus = c.Connect(t, cfg.Name)
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 100 * time.Millisecond
	}
	if cfg.PullBatchSize == 0 {
		cfg.PullBatchSize = 1
	}
	if cfg.Logger == nil {
		cfg.Logger = quiet()
	}
	w, err := workersdk.New(cfg)
	if err != nil {
		t.Fatalf("sdktest: new worker: %v", err)
	}
	return w
}

// Run starts w.Run in the background, waits for its first heartbeat and
// returns a stop function (also registered with t.Cleanup) that cancels
// the run and waits for it to return. Run's error, if any, fails the test.
func (c *Core) Run(t testing.TB, w *workersdk.Worker) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("sdktest: worker run: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Errorf("sdktest: worker did not stop")
			}
		})
	}
	t.Cleanup(stop)
	c.AwaitPresence(t, w.Name(), 5*time.Second)
	return stop
}

// Heartbeat is a presence record as core reads it.
type Heartbeat struct {
	WorkerName string          `json:"worker_name"`
	WorkerKey  string          `json:"worker_key"`
	InstanceID string          `json:"instance_id"`
	Version    string          `json:"version"`
	Release    string          `json:"release"`
	Provider   string          `json:"provider"`
	Package    string          `json:"package"`
	Webhooks   bool            `json:"webhooks"`
	LastSeen   time.Time       `json:"last_seen"`
	Raw        json.RawMessage `json:"-"`
}

// AwaitPresence waits for a heartbeat of the named pool.
func (c *Core) AwaitPresence(t testing.TB, workerName string, timeout time.Duration) Heartbeat {
	t.Helper()
	kv, err := c.JS.KeyValue(context.Background(), KVWorkerPresence)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		keys, _ := kv.Keys(context.Background())
		for _, k := range keys {
			if !strings.HasPrefix(k, workerName+".") {
				continue
			}
			if e, err := kv.Get(context.Background(), k); err == nil {
				var hb Heartbeat
				if json.Unmarshal(e.Value(), &hb) == nil {
					hb.Raw = e.Value()
					return hb
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("sdktest: no presence for %q within %s", workerName, timeout)
	return Heartbeat{}
}

// Manifest reads the pool's manifest as published (worker_name, version,
// provider, package, provider_meta…); nil when absent.
func (c *Core) Manifest(t testing.TB, workerName string) map[string]json.RawMessage {
	t.Helper()
	kv, err := c.JS.KeyValue(context.Background(), KVWorkerManifests)
	if err != nil {
		t.Fatal(err)
	}
	e, err := kv.Get(context.Background(), workerName)
	if err != nil {
		return nil
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(e.Value(), &out); err != nil {
		t.Fatalf("sdktest: manifest of %s: %v", workerName, err)
	}
	return out
}

// ---------------------------------------------------------------------------
// Driving the worker
// ---------------------------------------------------------------------------

// PublishJob publishes a job on the CAIRN_JOBS stream. body is marshalled
// as JSON unless it already is a []byte. msgID dedups within the stream's
// window, as core's publishes do.
func (c *Core) PublishJob(t testing.TB, subject, msgID string, body any) {
	t.Helper()
	raw, ok := body.([]byte)
	if !ok {
		var err error
		if raw, err = json.Marshal(body); err != nil {
			t.Fatal(err)
		}
	}
	msg := &nats.Msg{Subject: subject, Data: raw, Header: nats.Header{}}
	if msgID != "" {
		msg.Header.Set(jetstream.MsgIDHeader, msgID)
	}
	if _, err := c.JS.PublishMsg(context.Background(), msg); err != nil {
		t.Fatalf("sdktest: publish %s: %v", subject, err)
	}
}

// Message is a captured publish.
type Message struct {
	Subject string
	Headers map[string]string
	Body    []byte
}

// AwaitMessage returns the next unseen message on a cairn.results.> or
// cairn.events.> subject (exact or a NATS wildcard pattern), failing after
// timeout.
func (c *Core) AwaitMessage(t testing.TB, subject string, timeout time.Duration) Message {
	t.Helper()
	cap := c.results
	if strings.HasPrefix(subject, "cairn.events.") {
		cap = c.events
	}
	m, ok := cap.await(subject, timeout)
	if !ok {
		t.Fatalf("sdktest: no message on %s within %s", subject, timeout)
	}
	return m
}

// AwaitResult awaits the next JobResult on subject and resolves the
// claim-check from the blob store when the envelope carries a payload_ref.
func (c *Core) AwaitResult(t testing.TB, subject string, timeout time.Duration) *workerv1.JobResult {
	t.Helper()
	m := c.AwaitMessage(t, subject, timeout)
	return c.decodeResult(t, m.Body)
}

// Results returns every JobResult captured so far on subject (exact or
// wildcard), claim-checks resolved, oldest first.
func (c *Core) Results(t testing.TB, subject string) []*workerv1.JobResult {
	t.Helper()
	var out []*workerv1.JobResult
	for _, m := range c.results.all(subject) {
		out = append(out, c.decodeResult(t, m.Body))
	}
	return out
}

// Events returns every message captured so far on a cairn.events.> subject.
func (c *Core) Events(subject string) []Message { return c.events.all(subject) }

func (c *Core) decodeResult(t testing.TB, body []byte) *workerv1.JobResult {
	t.Helper()
	var jr workerv1.JobResult
	if err := protojson.Unmarshal(body, &jr); err != nil {
		t.Fatalf("sdktest: result is not protojson: %v", err)
	}
	ref := jr.GetPayloadRef()
	if ref == nil {
		return &jr
	}
	full, ok := c.Blobs.Get(ref.GetBlobId())
	if !ok {
		t.Fatalf("sdktest: claim-check blob %q missing", ref.GetBlobId())
	}
	var resolved workerv1.JobResult
	if err := protojson.Unmarshal(full, &resolved); err != nil {
		t.Fatalf("sdktest: claim-check body is not protojson: %v", err)
	}
	return &resolved
}

// Discover runs core's discover request/reply against the pool.
func (c *Core) Discover(provider, pool string, req workersdk.DiscoverRequest) (workersdk.DiscoverResponse, error) {
	var resp workersdk.DiscoverResponse
	err := c.request("cairn.discover."+provider+"."+pool, req, &resp)
	return resp, err
}

// ExchangeCode runs core's OAuth code exchange request/reply against the pool.
func (c *Core) ExchangeCode(provider, pool string, req workersdk.OAuthExchangeRequest) (workersdk.OAuthExchangeReply, error) {
	var resp workersdk.OAuthExchangeReply
	err := c.request("cairn.oauth."+provider+"."+pool+".exchange", req, &resp)
	return resp, err
}

// ForwardWebhook publishes a POST the way core's forwarder does: the raw
// body and lowercased headers in a WebhookEnvelope on
// cairn.webhooks.<workerKey>.event.
func (c *Core) ForwardWebhook(t testing.TB, workerKey, provider string, headers map[string]string, body []byte) {
	t.Helper()
	lower := map[string]string{}
	for k, v := range headers {
		lower[strings.ToLower(k)] = v
	}
	env, _ := json.Marshal(workersdk.WebhookEnvelope{Provider: provider, Headers: lower, Body: body})
	c.PublishJob(t, "cairn.webhooks."+workerKey+".event", "", env)
}

// VerifyWebhook relays a GET handshake to cairn.webhooks.<workerKey>.verify.
func (c *Core) VerifyWebhook(workerKey, method string, query map[string]string) (workersdk.WebhookVerifyResponse, error) {
	var resp workersdk.WebhookVerifyResponse
	err := c.request("cairn.webhooks."+workerKey+".verify", map[string]any{"method": method, "query": query}, &resp)
	return resp, err
}

// request is a core-style request/reply. A subject the CAIRN_WEBHOOKS
// stream also covers (cairn.webhooks.<key>.verify) first yields the
// stream's PubAck ({"stream":…,"seq":…}) on the reply inbox; it is skipped
// so the worker's answer comes back. (cairn-core's forwarder takes the
// first reply as is; see the README's notes for workers.)
func (c *Core) request(subject string, req, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	inbox := nats.NewInbox()
	sub, err := c.NC.SubscribeSync(inbox)
	if err != nil {
		return err
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := c.NC.PublishRequest(subject, inbox, body); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		msg, err := sub.NextMsg(time.Until(deadline))
		if err != nil {
			return fmt.Errorf("request %s: %w", subject, err)
		}
		if msg.Header.Get("Status") == "503" {
			return fmt.Errorf("request %s: no responders", subject)
		}
		if e := msg.Header.Get("Nats-Error"); e != "" {
			return fmt.Errorf("request %s: %s", subject, e)
		}
		if isPubAck(msg.Data) {
			continue
		}
		return json.Unmarshal(msg.Data, resp)
	}
}

// isPubAck recognises a JetStream publish acknowledgement.
func isPubAck(data []byte) bool {
	var ack struct {
		Stream string `json:"stream"`
		Seq    uint64 `json:"seq"`
	}
	return json.Unmarshal(data, &ack) == nil && ack.Stream != "" && ack.Seq > 0
}

// RateLimit reads a bucket's state from cairn_rate_limits by its KV key
// ("<provider>.<app>.<window>"); ok is false when absent.
func (c *Core) RateLimit(t testing.TB, key string) (state map[string]any, ok bool) {
	t.Helper()
	kv, err := c.JS.KeyValue(context.Background(), KVRateLimits)
	if err != nil {
		t.Fatal(err)
	}
	e, err := kv.Get(context.Background(), key)
	if err != nil {
		return nil, false
	}
	_ = json.Unmarshal(e.Value(), &state)
	return state, true
}

// ---------------------------------------------------------------------------
// Capture
// ---------------------------------------------------------------------------

// capture records every message on a wildcard subscription and hands
// unseen ones out by subject.
type capture struct {
	mu   sync.Mutex
	cond *sync.Cond
	msgs []Message
	seen map[string]int
}

func newCapture(t testing.TB, nc *nats.Conn, subject string) *capture {
	t.Helper()
	c := &capture{seen: map[string]int{}}
	c.cond = sync.NewCond(&c.mu)
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		headers := map[string]string{}
		for k, v := range m.Header {
			if len(v) > 0 {
				headers[k] = v[0]
			}
		}
		c.mu.Lock()
		c.msgs = append(c.msgs, Message{Subject: m.Subject, Headers: headers, Body: m.Data})
		c.cond.Broadcast()
		c.mu.Unlock()
	})
	if err != nil {
		t.Fatalf("sdktest: capture %s: %v", subject, err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return c
}

// all returns the captured messages matching subject, oldest first.
func (c *capture) all(subject string) []Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []Message
	for _, m := range c.msgs {
		if subjectMatches(subject, m.Subject) {
			out = append(out, m)
		}
	}
	return out
}

// await returns the next message on subject not handed out before.
func (c *capture) await(subject string, timeout time.Duration) (Message, bool) {
	deadline := time.Now().Add(timeout)
	timer := time.AfterFunc(timeout, func() {
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	})
	defer timer.Stop()
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		matched := 0
		for _, m := range c.msgs {
			if !subjectMatches(subject, m.Subject) {
				continue
			}
			if matched == c.seen[subject] {
				c.seen[subject]++
				return m, true
			}
			matched++
		}
		if !time.Now().Before(deadline) {
			return Message{}, false
		}
		c.cond.Wait()
	}
}

// subjectMatches reports whether subject matches the NATS pattern.
func subjectMatches(pattern, subject string) bool {
	if pattern == subject {
		return true
	}
	pt, st := strings.Split(pattern, "."), strings.Split(subject, ".")
	for i, p := range pt {
		if p == ">" {
			return i < len(st)
		}
		if i >= len(st) || (p != "*" && p != st[i]) {
			return false
		}
	}
	return len(pt) == len(st)
}

// StoreCall is one cairn.tokens.<p>.store request the worker made.
type StoreCall struct {
	Provider          string
	AccountID         string
	State             port.TokenState
	PreviousExpiresAt time.Time
}

// ReauthCall is one cairn.tokens.<p>.needs_reauth request.
type ReauthCall struct {
	Provider  string
	AccountID string
	Reason    string
}

# Writing a worker

A worker imports one provider's data into Cairn. This walks through every
part, in the order you build them; `examples/minimal` is the finished
result for an imaginary provider, "Acme", and every snippet below is taken
from it. Read cairn-core's
[provider contract](https://docs.opencairn.org/architecture/provider-contract)
once for the rules the SDK enforces.

## 0. Layout and identity

```
cmd/worker-acme/        main, handlers, client, mapping, webhooks
  minimal.go            config, wiring, manifest, provider meta
  client.go             the provider's HTTP client with typed errors
  handlers.go           fetch_source, parse_blob, reconcile, import_metrics, discover
  webhook.go            event + verify handlers
  worker_test.go        sdktest integration tests
```

Pick three constants:

```go
const (
    workerName     = "acme-fetcher"                  // the default pool name (CAIRN_WORKER_NAME overrides)
    workerProvider = "acme"                          // the provider token in every subject
    workerPackage  = "org.opencairn.examples.acme-importer" // stable package identity
    datasetVersion = 1                               // archive format
    parserVersion  = "1.0"                           // datasetVersion.M, bump M on every output change
)
var workerRelease = ""                               // -ldflags "-X main.workerRelease=v0.7.0"
```

`workerRelease` is your git tag and must share major.minor with the SDK
you build on (and so with core); `workersdk` refuses to run against an
incompatible core. Dataset and parser versions follow core ADR 0005.

## 1. Manifest and provider meta

The manifest declares the data types you supply; core routes work and
shows the user what to expect from it. The provider meta is what core needs
to connect an account without knowing your provider: the authorize URL,
scopes, PKCE, the labels of the two credential slots.

```go
func manifest() capability.Manifest {
    return capability.Manifest{
        capability.DataTypeActivity: {Read: true, Backfill: true, Granularity: "per-activity"},
        capability.DataTypeLap:      {Read: true, Backfill: true},
        capability.DataTypeWeight:   {Read: true, Backfill: true, Granularity: "daily"},
    }
}

func providerMeta(base string) *workersdk.ProviderMeta {
    return &workersdk.ProviderMeta{
        DisplayName: "Acme", AuthMethod: workersdk.AuthMethodOAuth2,
        ActivityURLTemplate: base + "/activities/{id}",
        OAuth: &workersdk.ProviderOAuth{AuthorizeURL: base + "/oauth/authorize", Scopes: []string{"activity:read"}, PKCE: true},
        ConfigFields: []workersdk.ProviderField{{Key: "client_id", Label: "Client ID"}, {Key: "client_secret", Label: "Client secret", Secret: true}},
    }
}
```

Declaring `DataTypeGear` makes core send `import_gear`; a metric type
(`Weight`, `Sleep`, `HRV` …) makes it send `import_metrics`. Providers that
use an API key or login instead of OAuth set `AuthMethodCredentials`; core
then answers `cairn.creds.<provider>.get` with the two slots.

## 2. Configuration and the bus

```go
env, err := workersdk.LoadEnv(workerName)   // CAIRN_* variables
bus, err := workersdk.Connect(env, logger)  // enrollment token in production, plain in dev stacks
defer bus.Close()
```

Read your own settings next to it (`ACME_BASE_URL`, a webhook secret).

## 3. The worker

```go
w, err := workersdk.New(workersdk.Config{
    Name: env.WorkerName, InstanceID: env.InstanceID,
    Version: parserVersion, Release: workerRelease, Package: workerPackage, Provider: workerProvider,
    Bus:         bus,
    Limiter:     workersdk.NewRateLimiter(bus, logger, map[string]natsbus.Limit{
        workerProvider + ":" + env.WorkerName + ":minute": {Capacity: 100, Window: natsbus.PerMinute},
    }),
    Auth:        &oauth.Refresher{Endpoint: oauth2.Endpoint{TokenURL: base + "/oauth/token", AuthStyle: oauth2.AuthStyleInParams}},
    RefreshLock: workersdk.NewRefreshLock(bus, logger, env.InstanceID, 30*time.Second),
    Logger:      logger, Manifest: manifest(), ProviderMeta: providerMeta(base),
})
```

- **Rate limits.** One bucket per provider window; `w.RateBucket("minute")`
  names the pool's bucket. Reserve before every call:
  `w.ReserveAPI(ctx, w.RateBucket("minute"), 1)` returns a
  `*port.NakWithDelayError` when the budget is gone — return it. Sync the
  limiter from the provider's usage headers with `w.ObserveUsage` and
  anchor a 429 with `w.ObserveRateLimit429`. Budgets are shared by every
  instance of the pool through the `cairn_rate_limits` KV bucket.
- **Tokens.** `Auth` refreshes expired tokens. `oauth.Refresher` covers RFC
  6749 endpoints (Basic or body client auth, rotated refresh tokens, scope);
  implement `workersdk.AuthHandler` yourself for a non-standard reply.
  `RefreshLock` makes a refresh happen once per pool, which single-use or
  rotating refresh tokens require.

## 4. Handlers per job type

Register one handler per job kind on `w.JobSubject(kind)`; each decodes its
payload (`workersdk.FetchSourceJob` etc.) and returns one of three outcomes:

| return                    | disposition          | when                                           |
| ------------------------- | -------------------- | ---------------------------------------------- |
| `nil`                     | ACK                  | done                                           |
| `*port.TerminalError`     | Term (no retry)      | bad payload, revoked token, 4xx that repeats    |
| `*port.NakWithDelayError` | NAK after the delay  | rate limit, provider told us when to come back |
| any other error           | NAK (consumer backoff) | 5xx, network, blob store down                |

Map your client's errors once (`mapErr` in the example) so every handler
agrees. `w.Logger()` is the structured logger.

### fetch_source

```go
tok, err := w.FetchToken(ctx, in.AccountID)      // terminal or NAK-with-delay errors pass through
raw, err := client.file(ctx, tok.AccessToken, in.ExtID)
res, err := fitmap.FromFIT(bytes.NewReader(raw))   // or your own mapping to cairnv1.ActivitySourcePayload
act := res.Activity
act.Ref = in.Ref()
id, size, err := w.ArchiveBlob(ctx, in.UserID, raw, fitmap.ContentTypeFIT)   // best-effort
act.RawBlobId, act.RawContentType, act.RawSizeBytes = id, fitmap.ContentTypeFIT, size
return w.PublishResultViaBlob(ctx, job, fmt.Sprintf("result:%s:d%d", in.JobID, job.DeliveryAttempt),
    w.NewJobResult(&workerv1.WorkerEvent{Type: workerv1.WorkerEventType_WORKER_EVENT_TYPE_ACTIVITY,
        Entity: &workerv1.WorkerEvent_Activity{Activity: act}}))
```

Before returning a terminal error, call
`w.PublishFailureResult(ctx, job, in.Ref(), reason, cause)` so core fails
the import-queue item with the true reason at once.

### parse_blob (archive dataset, re-parse)

Archive whatever your parser needs (the provider's original file or JSON)
with `ArchiveBlob`; `parse_blob` re-maps it without provider budget:

```go
if err := w.CheckParserVersion(job, in.MinParserVersion); err != nil { … }   // rolling-upgrade guard
url, err := w.BlobDownloadURL(ctx, in.Blob)   // re-presigns an expired URL from fallback_handle
raw, err := workersdk.DownloadBlob(ctx, url)
```

Register it with `w.HandleWithMaxDeliver(subject, parseBlob, workersdk.ParserTooOldAttempts+2)`.
Keep a golden test of the mapping per parser version (the Strava worker's
`TestParserGolden` is the pattern).

### reconcile and backfill

Reconcile is the polling sync: list from
`workersdk.ReconcileWindowStart(in.Watermark, time.Now())`, skip
`in.KnownExtIDs`, fan out with `w.EnqueueFetchSource(ctx, in.JobID, in.AccountID, in.UserID, "reconcile", id)`
(deterministic msg-ids collapse duplicates) and publish the watermark on
`w.ResultSubject(workersdk.JobReconcile) + "." + in.AccountID` even when
nothing was found. Full history comes through discover and the import
queue, not through reconcile.

### import_metrics

Emit `METRIC` events (`workerv1.ImportedMetric` with a key, a timestamp
and a numeric or JSON value) for the job's window
(`in.Window()`); publish with `PublishResultViaBlob`.

## 5. Discover

```go
w.RespondDiscover(ctx, func(ctx context.Context, req workersdk.DiscoverRequest) workersdk.DiscoverResponse { … })
```

List pages from `req.StartPage`, at most a few dozen per call; return
`Complete` only when the end of the data is known, `NextPage` otherwise,
`RateLimited` when the budget ran out.

## 6. OAuth code exchange

Core runs the authorize round-trip from your provider meta and asks the
pool to exchange the code:

```go
ex := &oauth.Exchanger{
    Endpoint:  oauth2.Endpoint{TokenURL: base + "/oauth/token", AuthStyle: oauth2.AuthStyleInParams},
    AccountID: oauth.AccountIDFromProfile(base+"/v1/me", "id"),      // or AccountIDFromExtra("athlete", "id")
}
w.RespondOAuthExchange(ctx, ex.Exchange)
```

`ExtraParams` adds token-request parameters; the PKCE verifier arrives in
the request when the meta declares PKCE. The reply must carry the
provider's stable account id.

## 7. Webhooks

Set a verify secret on exactly one instance of the pool and register:

```go
w.WebhookEvent(func(ctx context.Context, w *workersdk.Worker, ev workersdk.WebhookEvent) error {
    env, err := ev.Envelope()                                                     // provider, headers (lowercased), body
    if !webhook.Verify(secret, env.Body, env.Headers["x-acme-signature"], webhook.Hex) {
        return &port.TerminalError{Reason: "bad_signature"}
    }
    // resolve the owner on w.LookupSubject(), then w.EnqueueFetchSource(…, "webhook", id)
})
w.WebhookVerify(func(_ context.Context, _ *workersdk.Worker, req workersdk.WebhookVerifyRequest) (workersdk.WebhookVerifyResponse, error) { … })
```

Core's webhook URL for the pool is `/webhooks/<name>-<provider>`
(`w.WorkerKey()`). `webhook.VerifyTimestamped` covers timestamp+body
signatures, `webhook.FreshTimestamp` replay windows.

## 8. Testing

Unit tests run handlers on `inmem.New()` with responders registered by
hand. Integration tests use `sdktest`:

```go
core := sdktest.Start(t, sdktest.WithRelease("v0.7.0"))
core.SetToken("acct-1", port.TokenState{AccessToken: "tok", ExpiresAt: time.Now().Add(time.Hour)}, workersdk.OAuthClientCreds{})
core.SetAccount("acme", "owner-1", "acct-1", "user-1")
w := core.Worker(t, workersdk.Config{Name: "acme-fetcher", Provider: "acme", Version: "1.0", Auth: refresher})
registerHandlers(w, client, cfg)
core.Run(t, w)
core.PublishJob(t, w.JobSubject(workersdk.JobFetchSource), "m1", workersdk.FetchSourceJob{JobID: "j1", AccountID: "acct-1", ExtID: "42"})
res := core.AwaitResult(t, w.ResultSubject(workersdk.JobFetchSource), 10*time.Second)   // claim-check resolved
```

The kit also drives discover (`core.Discover`), the code exchange
(`core.ExchangeCode`), webhooks (`core.ForwardWebhook`,
`core.VerifyWebhook`), records token stores and `needs_reauth` calls, and
exposes the blob store (`core.Blobs`), presence and the manifest. Point the
provider client at an `httptest` server (the example's `fakeprovider`).

## 9. Conformance against core

With a dev core's NATS (or a `nats-server -js` seeded with the release
key) and the worker running, run core's checker:

```bash
cd ../cairn-core && go run ./cmd/worker-conformance --nats nats://127.0.0.1:4222 --provider acme [--gear] [--metrics]
```

It verifies presence, a durable consumer per required job subject, the
discover shape and (with `--active`) a reconcile round-trip.

## 10. Repository setup

Copy the Strava worker's `Makefile`, `.golangci.yml`,
`.pre-commit-config.yaml`, workflows and `.fleetlint.yaml`; add
`require github.com/opencairn-org/cairn-provider-sdk-go vX.Y.Z` and keep the
worker's release at the same major.minor. The Docker build is a plain
`go build`: the proto stubs come with the SDK.

Note for webhook `GET` handshakes: core's `CAIRN_WEBHOOKS` stream covers
`cairn.webhooks.>`, so the `.verify` request/reply also yields a JetStream
PubAck on the reply inbox; `sdktest` skips it, cairn-core's forwarder takes
the first reply (tracked as a core issue).

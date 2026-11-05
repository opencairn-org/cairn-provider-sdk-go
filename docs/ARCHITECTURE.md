# Architecture

The SDK is the provider-agnostic half of a Cairn worker. It was extracted
from cairn-provider-strava 0.7.0, where every package except the Strava
client and mapping already knew nothing about Strava, and it is the
implementation of cairn-core's provider contract for Go workers.

## Components

```
workersdk  Worker, Config, Job, HandlerFunc; Connect/LoadEnv; TokenClient/TokenCache; results and blobs;
           job payloads; discover, OAuth exchange and webhook responders; release and parser checks
natsbus    Bus (port.JobBus on nats.go + JetStream), RateLimiter (port.RateLimiter on a KV bucket)
oauth      TokenSource/Client (x/oauth2 over TokenCache), Refresher (AuthHandler), Exchanger
kvlock     Lock/Handle: leases by compare-and-set on port.KV
webhook    Sign/Verify/VerifyTimestamped/FreshTimestamp
fitmap     FromFIT/FromGPX → ImportedActivity (muktihari/fit, tkrajina/gpxgo)
capability DataType taxonomy, Manifest
port       JobBus, PullSubscription, KV, RateLimiter, TokenState, TerminalError, NakWithDelayError
inmem      Bus/KV fakes with synchronous delivery
sdktest    Core: embedded nats-server bootstrapped like core, fake responders, drive-and-await helpers
e2e        build tag e2e: a worker + its mock against the real cairn-core (compose services, fresh
           database, HTTP + bus assertions, worker-conformance), one spec per provider
proto      generated cairn.v1 and cairn.worker.v1 messages
```

## Boundaries

- `port` is the only dependency of `workersdk`'s core logic; NATS types
  stay inside `natsbus`. A worker's unit tests run on `inmem`, its
  integration tests on `sdktest`, and `e2e` proves it against a real core.
- The SDK owns the wire contract: subjects (`JobSubject`, `ResultSubject`,
  `LookupSubject`, `DiscoverSubject`, `OAuthExchangeSubject`, the webhook
  subjects), the JSON shapes of core's RPCs (tokens, creds, presign,
  lookup, discover, exchange, verify) and the proto messages. A worker
  touches none of them directly.
- A worker owns the provider: its HTTP client, its error classification
  onto the three outcomes, the mapping to proto, the archive format and the
  dataset/parser versions, the manifest and provider meta.
- Secrets never rest in the worker: tokens and app credentials come from
  core per call and are cached in memory only; blob I/O goes through
  presigned URLs.

## Data flow

1. `Connect` joins NATS (enrollment token → auth-callout in production,
   plain in dev stacks). `Run` checks core's release in `cairn_core/info`,
   publishes presence (`cairn_worker_presence`) and the manifest with the
   provider meta (`cairn_worker_manifests`), opens one durable pull consumer
   per registered job subject on `CAIRN_JOBS` and, when webhook handlers are
   registered, on `CAIRN_WEBHOOKS`.
2. Each job is dispatched to its handler with its ack deadline extended
   while it runs. The handler's return value is the disposition:
   nil → ACK, `*port.TerminalError` → Term, `*port.NakWithDelayError` → NAK
   with delay (and the pull loop pauses), any other error → NAK.
3. Handlers get tokens from `FetchToken` (cache → `cairn.tokens.<p>.get` →
   refresh through the `AuthHandler` under the pool-wide `kvlock` lease →
   `cairn.tokens.<p>.store`), reserve provider budget from the shared
   `cairn_rate_limits` bucket, archive raw data and publish results. An
   event-carrying result is claim-checked: the protojson body goes to the
   blob store via a presigned PUT and only the envelope with `payload_ref`
   crosses NATS on `cairn.results.<job>.<provider>.<pool>`.
4. Request/replies core initiates — discover, the OAuth code exchange, the
   webhook GET handshake — are served by `RespondDiscover`,
   `RespondOAuthExchange` and `WebhookVerify`.

## Decisions

- **Lockstep with core** (ADR 0002): the SDK's major.minor is the core
  contract version. Workers inherit compatibility from the SDK they link.
- **Refresh once per pool.** Providers with single-use or rotating refresh
  tokens break when two instances refresh the same account at once. The
  token cache takes a KV lease (`kvlock`) in the one shared bucket a worker
  may write and is meant for coordination state, `cairn_rate_limits`,
  under the `lock.` prefix; waiters re-read the stored token instead of
  refreshing again. Core's `stale_store` check stays as the second guard.
- **Rate limits are data, not code.** Windows and capacities are
  configured per bucket; the limiter is a CAS loop on the shared KV so
  every instance of a pool draws from one budget, and provider usage
  headers re-sync it.
- **Established libraries over hand-rolled code.** x/oauth2 for token
  grants, muktihari/fit and gpxgo for files, nats-server's test package
  for the kit.

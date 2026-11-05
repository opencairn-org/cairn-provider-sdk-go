# Cairn provider SDK for Go

The framework a [Cairn](https://github.com/opencairn-org/cairn-core) provider
worker is built on. A worker is a standalone process that imports one
platform's data (Strava, Polar, Oura, a folder of FIT files …) into Cairn
over the NATS control plane; this module holds everything that is the same
for every provider, so a new worker is the provider client, the mapping and
a few handlers.

```
workersdk/   the worker: NATS connect + auth-callout, job loop, presence and manifest, release check,
             token cache with pool-wide refresh lock, presign/claim-check/archive, job payloads, discover,
             OAuth exchange and webhook responders
natsbus/     the JetStream bus adapter and the shared KV rate limiter (per-second … per-day windows)
oauth/       x/oauth2 token source over Cairn's token store, RFC 6749 refresher, code-exchange helper
kvlock/      compare-and-set leases on a KV bucket (once-per-pool work)
webhook/     constant-time HMAC-SHA256 signature checks (hex/base64, body or timestamp+body)
fitmap/      FIT and GPX files to cairn.worker.v1.ImportedActivity
capability/  the data-type taxonomy and the capability manifest
port/        the interfaces the worker consumes (JobBus, KV, RateLimiter, typed errors)
inmem/       in-memory bus and KV for unit tests
sdktest/     embedded JetStream server bootstrapped like core + fake core responders, for integration tests
e2e/         a worker and its mock against the real cairn-core (make e2e PROVIDER=<name>), one spec per provider
proto/       the committed cairn.v1 / cairn.worker.v1 message stubs (scripts/sync-proto.sh)
examples/    minimal: a complete worker for a fake provider, the template for new workers
```

The contract itself — subjects, job and result shapes, versioning — is
documented in cairn-core's [provider contract](https://docs.opencairn.org/architecture/provider-contract)
and its ADRs [0005](https://github.com/opencairn-org/cairn-core/blob/main/docs/adr/0005-provider-versioning.md)
(versioning) and [0006](https://github.com/opencairn-org/cairn-core/blob/main/docs/adr/0006-job-routing-per-worker-pool.md)
(pools). The SDK implements it; workers do not need to read NATS subjects
themselves.

## Requirements

- Go (version in `go.mod`); no cgo.
- A cairn-core of the same major.minor (see Versioning) with NATS; for
  tests nothing but Go, `sdktest` starts its own server.

## Quickstart

```bash
go get github.com/opencairn-org/cairn-provider-sdk-go
```

```go
env, _ := workersdk.LoadEnv("acme-fetcher")      // CAIRN_NATS_URL, CAIRN_WORKER_NAME, enrollment token …
bus, _ := workersdk.Connect(env, logger)
w, _ := workersdk.New(workersdk.Config{
    Name: env.WorkerName, Provider: "acme", Version: "1.0", Release: release, Package: "org.example.acme",
    Bus: bus, Limiter: workersdk.NewRateLimiter(bus, logger, limits),
    Auth: &oauth.Refresher{Endpoint: endpoint}, RefreshLock: workersdk.NewRefreshLock(bus, logger, env.InstanceID, 30*time.Second),
    Manifest: manifest, ProviderMeta: meta,
})
w.Handle(w.JobSubject(workersdk.JobFetchSource), fetchSource)
w.Handle(w.JobSubject(workersdk.JobReconcile), reconcile)
w.RespondDiscover(ctx, discover)
w.RespondOAuthExchange(ctx, (&oauth.Exchanger{Endpoint: endpoint, AccountID: oauth.AccountIDFromProfile(me, "id")}).Exchange)
return w.Run(ctx)
```

[docs/writing-a-worker.md](docs/writing-a-worker.md) walks through every
step, and [examples/minimal](examples/minimal) is the complete, tested
worker it describes. Run it against a dev core with

```bash
export CAIRN_NATS_URL=nats://localhost:4222 CAIRN_WORKER_NAME=acme-fetcher ACME_BASE_URL=http://localhost:8099
go run ./examples/minimal
```

## Versioning

The SDK's **major.minor equals the cairn-core contract it implements**
([ADR 0002](docs/adr/0002-release-lockstep-with-core.md), adapted from
core's ADR 0005): SDK `v0.7.x` targets core `0.7`. A worker built on SDK
`vX.Y.z` tags itself `vX.Y.n`, passes it as `Config.Release`, and the
release check in `workersdk` refuses an incompatible core. Within a minor
the exported API is append-only; `make api-compat` (gorelease) guards it and
a breaking change ships only with a new major. `proto/` is regenerated from
core at every core minor (`scripts/sync-proto.sh ../cairn-core`).

## Develop

`make check` is the definition of green (lint, race tests, coverage floor,
govulncheck, licenses, gitleaks, API compatibility, build, fleetlint);
`make tools` installs the pinned tools and `make help` lists the targets.
See [CONTRIBUTING.md](CONTRIBUTING.md) and
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

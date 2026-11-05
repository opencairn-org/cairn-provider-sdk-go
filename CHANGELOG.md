# Changelog

All notable changes to the Cairn provider SDK for Go are documented here. The
format follows [Keep a Changelog](https://keepachangelog.com/), and the project
follows [Semantic Versioning](https://semver.org/) with the major.minor of each
release matching the cairn-core contract it implements
([ADR 0002](docs/adr/0002-release-lockstep-with-core.md)). Dates are ISO-8601.

## [Unreleased]

### Added
- Initial extraction of the provider-agnostic worker code from
  cairn-provider-strava 0.7.0: `workersdk` (worker loop, presence, manifest,
  provider meta, OAuth code exchange responder, token cache, results,
  claim-check, archive helpers, generic job payloads, discover), `natsbus`
  (JetStream bus, KV rate limiter), `capability`, `port`, `inmem` and the
  committed `cairn.v1` / `cairn.worker.v1` stubs under `proto/`
  (`scripts/sync-proto.sh` regenerates them from cairn-core).
- `kvlock`: compare-and-set lease over a NATS KV bucket.
- `oauth`: `golang.org/x/oauth2` token source over core's token store with
  a cross-instance refresh lock, an RFC 6749 refresher and a code-exchange
  helper (Basic or body client auth, PKCE verifier, extra params, custom
  token endpoint, provider account id).
- `natsbus.RateLimiter`: explicit per-bucket windows (second, minute,
  15 minutes, hour, day) next to the suffix convention.
- `webhook`: constant-time HMAC-SHA256 verification over the body or
  timestamp+body, hex or base64.
- `fitmap`: FIT and GPX files to `cairn.worker.v1.ImportedActivity`.
- `sdktest`: embedded JetStream server bootstrapped like cairn-core plus fake
  core responders for integration tests.
- `e2e` (build tag `e2e`, `make e2e PROVIDER=<name>` / `make e2e-all`): a
  worker against the real cairn-core — compose dev services, a freshly built
  core with its own database, the provider's mock and the worker binary;
  connect, discover, full sync, reconcile, webhooks, re-parse, rate limit,
  token refresh and `worker-conformance`, driven by a spec per provider
  (`e2e/providers/<name>.yaml`) with a Markdown report per run.

### Fixed
- `natsbus.RateLimiter`: a deleted or purged rate-limit key (an operator's
  `nats kv del`, a reset between test runs) kept its marker revision, so
  the create path's compare-and-set never succeeded again and every
  reserve of that bucket was denied with a short NAK until the bucket TTL
  expired. The create path now goes through the KV `Create`, which steps
  over the marker.
- `examples/minimal`: a complete worker for a fake provider.

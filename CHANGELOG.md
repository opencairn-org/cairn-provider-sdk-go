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
  (`e2e/providers/<name>.yaml`, or a spec a worker repository owns via
  `make e2e SPEC=/abs/path/<name>.yaml`) with a Markdown report per run;
  concurrent runs queue on `flock /tmp/cairn-e2e.lock`. Specs give reasons
  for skipped steps (`skip:`) and mark known core defects (`xfail:`); a failed
  run keeps its full logs next to the report.
- `tz`: an IANA zone for a provider's bare UTC offset (`FromOffset`) or a
  local wall-clock time next to UTC (`FromLocal`), including the half- and
  quarter-hour offsets in use.
- `examples/minimal`: a complete worker for a fake provider.
- `sdktest.ValidateMetric`: core's import rules for a METRIC event (user,
  account and external_id set, a timestamp, one value, a health data type
  or valid store key, the value shape a reserved key dictates). Results are
  checked like activities, and a value sent both as a data type and as its
  reserved store twin (`HRV` and `hrv_rmssd_ms`) fails the test.
- `e2e`: the `metrics store` step (`metrics.store_keys`) finds each key in
  core's metrics store with the worker's provenance and its own
  external_id, fails on any metric core refused, and checks that a second
  import updates the rows instead of adding new ones.

### Fixed
- `workersdk.Run`: presence and manifest are published once every job
  consumer and webhook subscriber exists, so a heartbeat means the worker
  takes work. A cancel during startup is a clean stop (nil, as after it),
  and a startup error stops the consumers already started instead of
  leaving their loops running after `Run` returned. Workers' tests that
  stop `run` on the first heartbeat failed ~1 in 7 under load
  (`subscribe …: context canceled`).
- `fitmap`: activities carry the timezone of the FIT file's local timestamp;
  core rejects an activity without one.
- `sdktest`: results are checked against core's activity rules (known type,
  time order, durations, IANA timezone) and fail the test where core would
  reject the import. The example worker set no timezone and every import of
  it would have been rejected; it now does.
- `e2e`: steps that passed a faulty worker (negative controls in
  `e2e/README.md`) now fail it: activity data checks for a loadable IANA
  timezone (core only refuses an empty one); reparse compares the activity,
  laps and source stream before and after, once core's
  `activity.changed` marks the re-merge; rate limit 429 watches the worker's
  provider calls through an API tap (a reverse proxy behind `${MOCK_URL}`)
  and fails a call inside the `Retry-After`; the new `webhook forged` step
  (`steps.webhook_forged`, mock emit `"forge": true`) fails a worker that
  accepts an event signed with the wrong secret; import metrics seeds a
  distinct value per data type and finds each, from the provider, in its
  own series. A failed import reports core's `last_error` and ends the wait
  at once.
- `e2e`: binaries and process logs go to the user cache directory
  (`E2E_OUT_DIR` overrides) instead of `e2e/out`: gorelease zips ignored
  files as well, and `make check` failed once a few runs' output passed its
  500 MB module limit.
- `e2e`: the import-metrics step reads core's health series (`GET
  /api/health`), where worker METRIC events land, instead of `/api/metrics`.
- `natsbus.RateLimiter`: a deleted or purged rate-limit key (an operator's
  `nats kv del`, a reset between test runs) kept its marker revision, so
  the create path's compare-and-set never succeeded again and every
  reserve of that bucket was denied with a short NAK until the bucket TTL
  expired. The create path now goes through the KV `Create`, which steps
  over the marker.

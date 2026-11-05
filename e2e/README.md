# End-to-end: a worker against the real cairn-core

`sdktest` is the fake core for a worker's own test suite; this package is its
counterpart against the real thing. One run brings up cairn-core's compose dev
services, builds and starts core on a free port with a fresh database, starts
the provider's mock server and the worker binary, then drives the same HTTP
routes the web UI uses — connect, discover, full sync, reconcile, webhooks,
re-parse, rate limits, token refresh — and finishes with core's
`worker-conformance`. Every step is asserted through core's API and the NATS
bus, never through the worker's internals.

```bash
export PATH="$HOME/go/bin:$PATH"
make e2e PROVIDER=strava                                   # one spec: e2e/providers/strava.yaml
make e2e SPEC=/home/me/cairn-provider-polar/e2e/polar.yaml # a spec the worker repository owns
make e2e-all                                               # every spec in e2e/providers ("_*.yaml" excluded)
```

A worker repository keeps its spec at `<worker>/e2e/<name>.yaml` and runs
it with `make -C ../cairn-provider-sdk-go e2e SPEC=$PWD/e2e/<name>.yaml`
(`SPEC` is an absolute path). With `repo:` left empty, the worker repository
is the parent of the spec's directory; `repo:` may also be absolute or
relative to the SDK root (the in-tree Strava spec uses the latter).

Runs share the compose NATS and MinIO, and each run resets the `CAIRN_*`
streams, so the Makefile wraps every run in `flock /tmp/cairn-e2e.lock`
(`E2E_LOCK` overrides the path): concurrent `make e2e` invocations queue
behind each other instead of clobbering a running one. Reports go to the
SDK's `e2e/reports/<provider>-<timestamp>.md` (gitignored), binaries and
process logs to `e2e/out/<provider>/`.

The package is behind the `e2e` build tag: `make check` lints it
(`--build-tags e2e`) but does not run it.

## Prerequisites

- `../cairn-core` checked out next to this repository (`E2E_CORE_DIR`
  overrides), with Go and git; the harness builds `cmd/server` and runs
  `cmd/worker-conformance` from it.
- `podman compose` (or `docker compose`: `E2E_COMPOSE="docker compose"`) for
  `docker-compose.dev.yml` — Postgres on 5432, NATS on 4222, MinIO on 9000.
  The first run pulls the images.
- The worker repository at the spec's `repo` path with a buildable worker and
  mock package.

Environment switches: `E2E_REUSE_STACK=1` skips `compose up` (the services
must already answer), `E2E_DOWN=1` stops them afterwards, `E2E_KEEP_DB=1`
keeps the run's database (otherwise it is dropped, as are `cairn_e2e_*`
databases of earlier runs), `E2E_TIMEOUT` bounds the whole run (default 25m),
`E2E_CORE_LOG_LEVEL` / `E2E_WORKER_LOG_LEVEL` tune the logs.

## What a run does

1. **stack** — `compose up -d`, waits for Postgres/NATS/MinIO, creates
   `cairn_e2e_<timestamp>`, drops the `CAIRN_*` streams and the presence,
   manifest and rate-limit buckets (core recreates them; deleting rather
   than purging also clears the streams' duplicate windows, which would
   otherwise swallow a job whose deterministic msg-id an earlier run used),
   builds core stamped with
   `git describe` (e.g. `v0.7.0-1-gcebd00c`, a 0.7 release for the
   compatibility check) and starts `cairn serve` with auto-migrate, the
   bootstrap admin, a 2s reconcile tick, no geocoder and no tile server.
2. **build worker+mock** — `go build` in the worker repository; the worker is
   stamped `-X <release_var>=v0.7.0`.
3. **core release** — `cairn_core/info` carries the built release and
   `/api/version` agrees.
4. **admin login** — `POST /auth/password` as the bootstrap admin, then
   `POST /auth/password/required` to replace the one-time password; the
   session cookie authenticates everything after.
5. **mock up** — the mock binds `${MOCK_ADDR}` and answers its health route.
6. **worker presence** — the heartbeat in `cairn_worker_presence` carries the
   release, the worker key and the expected capabilities; `GET
   /api/admin/workers` shows the pool online and compatible and `GET
   /api/admin/providers` / `GET /api/connections` list the provider from its
   manifest.
7. **connect account** — `POST /api/connections` stores the app credentials
   (or login + secret); for OAuth the harness follows `GET
   /auth/oauth/<provider>/connect?connection=…&worker=…` to the mock's
   authorize endpoint (must be the mock: `provider_meta` is built from the
   worker's base-URL override), the mock's redirect to core's callback and
   core's redirect to `/connections?connected=<provider>` (the pool answered
   the code exchange over NATS). `GET /api/accounts` then lists the account on
   the pool; its poll interval is parked at a day.
8. **reconcile on connect** — the scheduler's next tick reconciles the new
   account (`cairn.jobs.reconcile.*` → `cairn.results.reconcile.<provider>.<account>`),
   `last_sync_at` gets set; seeded activities are older than the 30-day
   reconcile window, so nothing is imported yet.
9. **gear on connect** — `import_gear` runs; its GEAR events land as pending
   refs (`GearService/ListPendingGearRefs`, the user decides), the harness
   decides "new gear" for each (`CreateGearFromRef`) and `ListGear` then
   shows gear with an external ref of the provider (specs with `expect.gear`).
10. **discover** — `POST /api/accounts/{id}/sync/preview` reports the seeded
    total, complete.
11. **full sync** — `POST /api/accounts/{id}/sync/start {skip_existing}`; the
    import queue dispatches `fetch_source` (reason `queue`) until
    `imported == expected` with an empty queue and no failed item.
12. **activity data** — `ActivityService/ListActivities` (providers filter,
    sources inline): count, `activity_type`, source stamps (provider, pool,
    `raw_blob_id` when `expect.archive`), `GET /api/activities/{id}/laps`,
    the per-source stream (`GetActivityStream`: channels and samples, polled
    because core stores it a moment after the activity), gear on the
    activity; the merged stream's channels are reported once they
    materialise.
13. **reparse** — `POST /api/activities/{id}/sources/{sid}/reimport {mode:
    reparse}` → a `parse_blob` job and result on the bus, the source back to
    `REIMPORT_STATUS_CURRENT` with a new `last_reimported_at` and
    `source_worker_version == expect.parser_version`.
14. **rate limit 429** — the mock serves one 429 (`Retry-After: 2`) on the
    next API call; `POST /api/accounts/{id}/import-one` for a new activity
    must still land (the worker NAKs with delay and completes on redelivery);
    the mock counted exactly one 429.
15. **reconcile** — a recent activity is added to the mock and the account's
    poll interval set to 1s: the next tick publishes a scheduled reconcile,
    the worker enqueues `fetch_source` with reason `reconcile` for it, and it
    appears.
16. **webhook event** — the mock delivers a provider-shaped event to
    `POST /webhooks/<worker_key>`; core forwards it on
    `cairn.webhooks.<worker_key>.event`, the worker resolves the owner and
    enqueues `fetch_source` with reason `webhook`; the activity appears.
17. **webhook verify** — `GET /webhooks/<worker_key>?<verify_query>` must
    answer 200 with `verify_expect` in the body. Core before commit
    `cebd00c` (`fix/webhook-verify-stream`) answers 502: the `CAIRN_WEBHOOKS`
    stream covered `cairn.webhooks.>` and its pub-ack reached the forwarder
    before the worker's reply. The harness checks whether core's checkout
    contains the fix; without it the step is reported as **XFAIL** (expected
    failure, not counted), with it a failure is a failure.
18. **import metrics** — metric providers: samples seeded on the mock,
    `POST /api/accounts/{id}/import-health?days=N`, an `import_metrics` job,
    `GET /api/metrics?keys=…` lists them.
19. **token refresh** — the mock issues the exchange token with an
    `expires_in` inside the SDK's 60s refresh skew, so the first job
    refreshed it through `cairn.tokens.<provider>.store`; the mock must count
    exactly one refresh and no 401 for the whole run (the stale token was
    never sent, the refreshed one was cached and persisted).
20. **worker-conformance** — `go run ./cmd/worker-conformance --nats …
    --provider … --worker-name … <flags> [--active --account-id …]` in
    cairn-core exits 0 (output in the report). A core whose tool still
    awaits the pre-pool subject `cairn.results.reconcile.<provider>.<account>`
    (before `fix(conformance)`) fails only the `--active` round-trip; the
    harness detects that checkout and reports the step as **XFAIL**.

Critical steps (1–7) abort the run; every later step is independent and is
reported PASS / FAIL / SKIP / XFAIL / XPASS with its duration. On any FAIL the
report ends with the last 50 lines of the worker, mock and core logs.

## Adding a provider

Copy `providers/_template.yaml` to `<worker>/e2e/<name>.yaml` in the worker
repository (or to `providers/<name>.yaml` here). The worker needs
env overrides for its API and OAuth base URLs (`CAIRN_<NAME>_API_BASE_URL`,
`CAIRN_<NAME>_OAUTH_BASE_URL`) so `provider_meta` points at the mock, and its
repository needs a standalone mock, `cmd/mock-<name>`, that:

- binds `--listen <addr>` and serves the provider's OAuth authorize (auto-
  approving, redirecting to `redirect_uri` with `code` and `state`), token
  (code and refresh grants; `--exchange-token-ttl` for the first token) and
  API endpoints the worker uses, with the provider's rate-limit headers;
- seeds `--activities N` activities `--seed-age` ago (older than 30 days), with
  everything the spec expects (laps, streams, gear);
- implements the **mock control API** under `/_mock` (`control_path`):

| route | body | effect |
|---|---|---|
| `GET /_mock/healthz` | | 200 `{"ok":true}` once ready |
| `GET /_mock/stats` | | `{"refreshes","exchanges","api_calls","unauthorized","rate_limited_served","webhooks_sent","activities"}` |
| `POST /_mock/activities` | `{"name","start":RFC3339,"streams":bool}` | adds an activity → `{"id":"<external id>"}` |
| `POST /_mock/webhooks/emit` | `{"url","aspect","activity_id"}` | POSTs the provider's (signed) event for that activity to `url` → `{"status","body"}` |
| `POST /_mock/fail-next` | `{"status","count","retry_after"}` | the next `count` API calls answer `status` (429 with `Retry-After`) |
| `POST /_mock/metrics` | `{"key","ts","value"}` | seeds a metric sample (metric providers) |
| `POST /_mock/reset` | | clears injected failures |

The harness binds every mock on `127.0.0.1` and hands the worker
`${MOCK_URL}` with that address on purpose: core validates `provider_meta`
URLs and accepts plain `http` only on a loopback host (`localhost`, `127.0.0.1`),
`https` everywhere else. A mock that renders its authorize URL with another
host name, or a worker that rewrites the override, fails the connect step with
"not the mock" — no TLS front is needed as long as the loopback address is
kept.

Steps a provider cannot support are switched off in `steps:`; a provider whose
terms forbid raw archives sets `expect.archive: false` (no re-parse). Run
`make e2e PROVIDER=<name>` and read the report.

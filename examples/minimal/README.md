# examples/minimal

A complete Cairn worker for "Acme", an imaginary provider whose API
(`fakeprovider`) has the shape most real ones share: OAuth 2 with rotating
refresh tokens, a profile call, a paged activity list, a detail call, an
original FIT file per activity, daily metrics and signed webhooks. It is the
template for new workers; `docs/writing-a-worker.md` explains each file.

```
minimal.go      configuration, wiring, manifest, provider meta
client.go       the Acme HTTP client with typed errors and mapErr
handlers.go     fetch_source, parse_blob, reconcile, import_metrics, discover
webhook.go      HMAC-verified events and the GET handshake
worker_test.go  sdktest integration tests against fakeprovider
fakeprovider/   the Acme API as an http.Handler
```

Run the tests with `go test ./examples/minimal/...`; run the worker against
a dev core with

```bash
export CAIRN_NATS_URL=nats://localhost:4222 CAIRN_WORKER_NAME=acme-fetcher ACME_BASE_URL=http://localhost:8099
go run ./examples/minimal
```

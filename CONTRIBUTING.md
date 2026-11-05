# Contributing to the Cairn provider SDK for Go

Contributions are welcome. Open an issue first for non-trivial changes so the
direction is agreed before the work starts.

## License of contributions

The project is Apache-2.0. By submitting a contribution you agree it is
licensed under the same terms (Section 5 of the Apache License). There is no
CLA. Sign off your commits (`git commit -s`) to assert the
[Developer Certificate of Origin](https://developercertificate.org/).

## Setup

- Go (version in `go.mod`), `make`, and the pinned gate tools: `make tools`
  installs golangci-lint, govulncheck, go-licenses, fleetlint, gorelease and
  gremlins; [gitleaks](https://github.com/gitleaks/gitleaks) comes from its
  own release. `make proto-tools` adds buf and protoc-gen-go for
  `scripts/sync-proto.sh`.
- Git hooks: `prek install` runs `.pre-commit-config.yaml` — hygiene, secret
  and workflow checks and conventional commit messages on commit, `make check`
  before push.
- The tests need no external services: `sdktest` starts an embedded NATS
  JetStream server.

## Working rules

- `make check` must pass: format, golangci-lint, `go vet`, tests with the race
  detector, the coverage floor, govulncheck, the license allowlist, a tidy
  module graph, gitleaks, the API compatibility check, the build and
  fleetlint. CI (`.github/workflows/check.yml`) runs the same target, and the
  release workflow gates on it.
- Commit messages follow Conventional Commits (`feat:`, `fix:`, `refactor:` …).
- This is a library: exported API changes are compared with the previous
  release by gorelease (`make api-compat`). A breaking change means a new
  major version, never a silenced check. The SDK's major.minor follows the
  cairn-core contract it implements (`docs/adr/0002`).
- `proto/` is generated from cairn-core's contract; do not edit it by hand.
  Regenerate with `scripts/sync-proto.sh ../cairn-core` and commit the result.
- Handlers map failures onto the SDK's three outcomes: `*port.TerminalError`
  (Term), `*port.NakWithDelayError` (rate limit) or any other error (NAK and
  retry). New features come with tests on the in-memory bus or `sdktest`.
- User-facing changes get a CHANGELOG.md entry; `make release V=vX.Y.Z` refuses
  a tag without its section; that section becomes the GitHub release notes.

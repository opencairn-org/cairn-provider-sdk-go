# 2. The SDK releases in lockstep with cairn-core (major.minor)

Status: accepted (2026-11-05). Adapts cairn-core ADR 0005 ("Providers carry three versions:
release, dataset, parser") to a library that several workers share.

## Context

cairn-core ADR 0005 gives every provider worker a **release** version (`v0.7.1`, the git tag)
that states the core contract it implements: the proto messages, the NATS subjects, the job
and result shapes. Before 1.0 a worker is compatible with core when both have the same major
and minor; patch levels are free. From 1.0 on, a worker is compatible when the major matches
and its minor is not above core's. Core publishes its release in the `cairn_core` KV bucket
(key `info`, `{"release": "vX.Y.Z"}`); a worker refuses to start, and stops, against an
incompatible core. A dev build (no tag) is exempt.

With this SDK, the contract is no longer implemented in each worker but in one shared module:
the committed proto stubs under `proto/`, the subjects in `workersdk`, the wire shapes of
tokens, presign, discover, the OAuth exchange and the claim-checked results. A worker's release
therefore says little on its own; what matters is the SDK release it was built with.

## Decision

- The SDK's **major.minor equals the cairn-core contract it implements**. SDK `v0.7.x`
  implements core `0.7`; the first SDK release after core `0.8.0` is `v0.8.0`. Patch releases
  carry fixes and additive helpers that do not change the wire contract.
- `scripts/sync-proto.sh` regenerates `proto/` from cairn-core's `api/proto` at every core
  minor release; the regenerated stubs are committed and the SDK minor is bumped with them.
- A worker's own release stays the compatibility signal for core (presence `release`,
  `JobResult.worker_release`): a worker built on SDK `vX.Y.z` tags itself `vX.Y.n` and passes
  `workersdk.Config.Release`. `workersdk.ReleaseCompatible` encodes the rule, so the worker
  cannot drift from the SDK it links.
- Within a minor, the exported Go API is append-only. `make api-compat` (gorelease against the
  previous tag) runs in `make check` and in the release workflow; a breaking Go API change is
  shipped only together with a new SDK major (and then module path `/v2`), never by silencing
  the check.
- Dataset and parser versions (ADR 0005's other two) stay per worker: they describe a worker's
  archive format and parsed output, which the SDK does not own.

## Consequences

- Every core minor release is followed by an SDK minor release, and then by worker releases on
  it; the roadmap item "Phase 0" of cairn-core's `docs/provider-roadmap.md` counts on this.
- A worker pinned to an older SDK minor keeps working against a newer core only from 1.0 on,
  exactly as ADR 0005 states for workers.
- The SDK's `CHANGELOG.md` names the core release each SDK minor targets.

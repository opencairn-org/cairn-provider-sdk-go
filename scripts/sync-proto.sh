#!/usr/bin/env bash
# Regenerate the committed Go proto stubs from the cairn-core proto contract.
#
# The SDK is the one place provider workers get the cairn.v1 /
# cairn.worker.v1 message stubs from: workers import
# github.com/opencairn-org/cairn-provider-sdk-go/proto/... and never run
# buf themselves. Run this whenever the contract changes (a core minor
# release, docs/adr/0002), then commit the result.
#
# Usage:
#   scripts/sync-proto.sh [path-to-cairn-core]   # default: ../cairn-core
#
# Needs buf and protoc-gen-go on PATH (`make proto-tools`). The stubs are
# generated straight from core's api/proto with buf's managed mode
# rewriting go_package to this module (scripts/buf.gen.yaml), so the
# embedded descriptors are consistent with the import paths.
set -euo pipefail

CORE="${1:-../cairn-core}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$CORE/api/proto"
[ -f "$SRC/buf.yaml" ] || { echo "missing $SRC/buf.yaml — is $CORE a cairn-core checkout?" >&2; exit 1; }

echo "generating proto stubs from $SRC"
rm -rf "$HERE/proto/cairn"
(cd "$SRC" && buf generate --template "$HERE/scripts/buf.gen.yaml" -o "$HERE/proto")
(cd "$HERE" && go build ./proto/... && go vet ./proto/...)
echo "proto stubs synced + build OK"

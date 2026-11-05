# Security policy

## Supported versions

The latest release (the newest `v*` tag). Older releases do not receive fixes.

## Reporting a vulnerability

Please do not open a public issue. Report suspected vulnerabilities privately
through GitHub's "Report a vulnerability" button on the Security tab, or by
email to `security@johanneskueber.com`. You will get an acknowledgement within
7 days and a fix or a decision within 30.

Vulnerabilities in the Cairn server belong to
[cairn-core](https://github.com/opencairn-org/cairn-core); vulnerabilities in
a provider worker built on this SDK belong to that worker's repository unless
the cause is in the SDK.

## Verifying releases

Releases are git tags signed by the maintainer; the GitHub release is created
by `.github/workflows/release.yml` after `make check` and the API
compatibility check pass. The Go module proxy and checksum database
(`sum.golang.org`) verify the module contents.

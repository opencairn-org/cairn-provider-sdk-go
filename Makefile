# Task-runner contract (https://fleetlint.org/baseline/#task-runner-contract). `make check` is the definition of green.
SHELL := bash
.SHELLFLAGS := -euo pipefail -c
.ONESHELL:
.DEFAULT_GOAL := check-fast
MAIN_BRANCH ?= main
# Total statement coverage the cover target enforces (generated proto/ and examples/ excluded; the example is still tested).
COVER_MIN   ?= 90
PKGS        := ./...
# Packages with hand-written SDK code; proto/ holds generated stubs, examples/ is tested but not gated.
COVER_PKGS   = $(shell go list ./... | grep -v -e /proto/ -e /examples/)
LICENSES    ?= MIT,Apache-2.0,BSD-2-Clause,BSD-3-Clause,ISC
# Each pin carries a `# renovate:` comment so Renovate bumps it (customManagers in renovate.json5).
# renovate: datasource=go depName=github.com/golangci/golangci-lint/v2
GOLANGCI_VERSION    ?= v2.15.0
# renovate: datasource=go depName=golang.org/x/vuln
GOVULNCHECK_VERSION ?= v1.9.0
# renovate: datasource=go depName=github.com/google/go-licenses/v2
GO_LICENSES_VERSION ?= v2.0.1
# renovate: datasource=go depName=github.com/fleetlint/fleetlint
FLEETLINT_VERSION   ?= v0.10.0
# golang.org/x/exp has no tags: pinned to a commit (renovate: datasource=git-refs is not set up for it)
GORELEASE_VERSION   ?= f0251eb4aa3e
# renovate: datasource=go depName=github.com/go-gremlins/gremlins
GREMLINS_VERSION    ?= v0.6.0
# renovate: datasource=go depName=github.com/bufbuild/buf
BUF_VERSION           ?= v1.75.0
# renovate: datasource=go depName=google.golang.org/protobuf
PROTOC_GEN_GO_VERSION ?= v1.36.12
# Mutation test efficacy floor for the workersdk package.
MUTATION_MIN        ?= 70
# Tools are built with this module's toolchain: golangci-lint refuses code that targets a newer Go than it was built with.
TOOLCHAIN   := $(shell go env GOVERSION)

.PHONY: help tools proto-tools proto fmt lint vet test cover audit api-compat bench mutate build check-fast check release tidy clean e2e e2e-all

help: ## list targets
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*##' '{printf "  %-12s %s\n", $$1, $$2}'

tools: ## install the pinned Go tools the gates use (gitleaks comes from its own release)
	GOTOOLCHAIN=$(TOOLCHAIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)
	GOTOOLCHAIN=$(TOOLCHAIN) go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	GOTOOLCHAIN=$(TOOLCHAIN) go install github.com/google/go-licenses/v2@$(GO_LICENSES_VERSION)
	go install github.com/fleetlint/fleetlint/cmd/fleetlint@$(FLEETLINT_VERSION)
	GOTOOLCHAIN=$(TOOLCHAIN) go install golang.org/x/exp/cmd/gorelease@$(GORELEASE_VERSION)
	GOTOOLCHAIN=$(TOOLCHAIN) go install github.com/go-gremlins/gremlins/cmd/gremlins@$(GREMLINS_VERSION)

proto-tools: ## install buf + protoc-gen-go for scripts/sync-proto.sh
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

proto: ## regenerate proto/ from ../cairn-core (CORE=path overrides)
	scripts/sync-proto.sh $${CORE:-../cairn-core}

fmt: ## format in place
	golangci-lint fmt

lint: ## format check + linters + vet, warnings as errors (e2e/ included under its build tag)
	golangci-lint fmt --diff
	golangci-lint run
	golangci-lint run --build-tags e2e ./e2e/...
	go vet $(PKGS)
	go vet -tags e2e ./e2e/...

vet: ## go vet only
	go vet $(PKGS)

test: ## all tests with the race detector, shuffled
	go test -race -shuffle=on -count=1 $(PKGS)

cover: ## tests with coverage; fail if the total is below COVER_MIN
	go test -count=1 -coverprofile=cover.out $(COVER_PKGS)
	total=$$(go tool cover -func=cover.out | awk '/^total:/ {sub("%", "", $$3); print $$3}')
	echo "coverage $$total% (minimum $(COVER_MIN)%)"
	if [ "$${total%.*}" -lt $(COVER_MIN) ]; then echo "coverage below $(COVER_MIN)%"; exit 1; fi

# go-licenses spots stdlib packages by GOROOT; pass the active toolchain's, not the one it was built with
audit: api-compat ## vulnerabilities, licenses, tidy module graph, secrets, API compatibility
	govulncheck $(PKGS)
	GOROOT=$$(go env GOROOT) go-licenses check $(PKGS) --allowed_licenses=$(LICENSES)
	go mod tidy -diff
	gitleaks git --no-banner --redact .

# This is a library: a breaking change in a minor release is found by every worker at once, so the
# exported API is compared with the previous release tag (fleetlint release/api-compatibility/go).
# Before the first tag there is nothing to compare (-base=none); gorelease still validates the module itself.
api-compat: ## exported API vs the previous v* tag (gorelease); breaking change = bump the major
	base=$$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null || echo none)
	gorelease -base="$$base"

# End-to-end: a provider worker against a real cairn-core (../cairn-core, its compose dev services via
# podman). Not part of check; see e2e/README.md. PROVIDER=<name> runs e2e/providers/<name>.yaml, SPEC=/abs/path.yaml a
# spec a worker repository owns. Runs share the compose NATS/MinIO, so flock serialises concurrent invocations.
# E2E_REUSE_STACK=1 skips compose up, E2E_DOWN=1 stops it after.
E2E_LOCK ?= /tmp/cairn-e2e.lock
e2e: ## end-to-end run of one spec: make e2e PROVIDER=strava | make e2e SPEC=/abs/path/<name>.yaml
	@test -n "$(PROVIDER)$(SPEC)" || { echo "usage: make e2e PROVIDER=<name> (e2e/providers/<name>.yaml) or make e2e SPEC=/abs/path/<name>.yaml"; exit 1; }
	flock $(E2E_LOCK) env E2E_PROVIDER=$(PROVIDER) E2E_SPEC=$(SPEC) go test -tags e2e -count=1 -timeout 30m -v ./e2e

e2e-all: ## end-to-end run of every spec in e2e/providers
	flock $(E2E_LOCK) env E2E_PROVIDER=all go test -tags e2e -count=1 -timeout 90m -v ./e2e

bench: ## benchmarks (not part of check)
	go test -run='^$$' -bench=. -benchmem $(PKGS)

mutate: ## mutation testing of the workersdk package (weekly in CI, not part of check)
	# A cached base test run makes gremlins derive a near-zero timeout and every mutant times out.
	GOFLAGS=-count=1 gremlins unleash --threshold-efficacy $(MUTATION_MIN) ./workersdk

build: ## compile every package and the example worker
	go build $(PKGS)
	CGO_ENABLED=0 go build -trimpath -o /dev/null ./examples/minimal

check-fast: ## before every commit (< 30 s)
	golangci-lint fmt --diff
	golangci-lint run --new-from-rev=origin/$(MAIN_BRANCH) || golangci-lint run
	go test -short $(PKGS)

check: lint test cover audit build ## definition of green (< 10 min)
	fleetlint check --fail-on error

release: ## refuse dirty/non-main -> check -> signed tag V (its CHANGELOG.md section must exist); pushing the tag runs the release workflow
	@test -n "$(V)" || { echo "usage: make release V=vX.Y.Z"; exit 1; }
	@test -z "$$(git status --porcelain)" || { echo "dirty tree"; exit 1; }
	@test "$$(git branch --show-current)" = "$(MAIN_BRANCH)" || { echo "not on $(MAIN_BRANCH)"; exit 1; }
	@grep -qF "## [$(V:v%=%)]" CHANGELOG.md || { echo "CHANGELOG.md has no section for $(V)"; exit 1; }
	$(MAKE) check
	git tag -s "$(V)" -m "$(V)"
	@echo "tagged $(V) — push with: git push origin $(V)"

tidy: ## go mod tidy
	go mod tidy

clean: ## remove build and coverage output
	rm -rf cover.out

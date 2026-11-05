//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stepStatus is the outcome of one step.
type stepStatus string

const (
	statusPass  stepStatus = "PASS"
	statusFail  stepStatus = "FAIL"
	statusSkip  stepStatus = "SKIP"
	statusXFail stepStatus = "XFAIL" // failed as expected (known core issue)
	statusXPass stepStatus = "XPASS" // passed although a known issue was expected
)

// knownIssue is a failure a step attributes to a documented defect outside
// the worker; the step is reported XFAIL instead of FAIL.
type knownIssue struct {
	issue string
	err   error
}

func (k *knownIssue) Error() string { return k.err.Error() }
func (k *knownIssue) Unwrap() error { return k.err }

type stepResult struct {
	Name   string
	Status stepStatus
	Dur    time.Duration
	Msg    string
}

// runner executes the steps of one provider spec and collects the report.
type runner struct {
	t     *testing.T
	ctx   context.Context
	spec  *Spec
	stack *Stack
	core  *coreClient
	prov  *providerRun
	// acct is the linked external account; hb the worker's first heartbeat.
	acct account
	hb   *presence
	// marks remembers bus cursors per step for the report.
	results []stepResult
	aborted string
	started time.Time
}

func newRunner(ctx context.Context, t *testing.T, spec *Spec, stack *Stack) *runner {
	return &runner{t: t, ctx: ctx, spec: spec, stack: stack, core: newCoreClient(stack.CoreURL), prov: newProviderRun(spec, stack), started: time.Now()}
}

// step runs fn when enabled and records the outcome; it returns true on pass.
func (r *runner) step(name string, enabled bool, fn func() (string, error)) bool {
	return r.stepWith(name, enabled, "", fn)
}

// critical is a step whose failure skips every later step.
func (r *runner) critical(name string, fn func() (string, error)) {
	if !r.step(name, true, fn) && r.aborted == "" {
		r.aborted = name
	}
}

// stepWith runs fn; a non-empty known issue turns a failure into XFAIL.
func (r *runner) stepWith(name string, enabled bool, known string, fn func() (string, error)) bool {
	switch {
	case !enabled:
		r.record(name, statusSkip, 0, "disabled in spec")
		return false
	case r.aborted != "":
		r.record(name, statusSkip, 0, "skipped after "+r.aborted+" failed")
		return false
	}
	start := time.Now()
	msg, err := fn()
	dur := time.Since(start)
	var ki *knownIssue
	if errors.As(err, &ki) {
		known = ki.issue
	}
	switch {
	case err != nil && known != "":
		r.record(name, statusXFail, dur, fmt.Sprintf("expected failure (%s): %v", known, err))
		return false
	case err != nil:
		r.record(name, statusFail, dur, err.Error())
		return false
	case known != "":
		r.record(name, statusXPass, dur, "passed although a known issue was expected ("+known+"): "+msg)
	default:
		r.record(name, statusPass, dur, msg)
	}
	return true
}

func (r *runner) record(name string, status stepStatus, dur time.Duration, msg string) {
	r.results = append(r.results, stepResult{Name: name, Status: status, Dur: dur, Msg: msg})
	r.t.Logf("%-5s %-28s %8s  %s", status, name, dur.Round(time.Millisecond), firstLine(msg))
	if status == statusFail {
		r.t.Errorf("step %q failed: %s", name, msg)
	}
}

// failed reports whether any step failed outright.
func (r *runner) failed() bool {
	for _, s := range r.results {
		if s.Status == statusFail {
			return true
		}
	}
	return false
}

// writeReport renders reports/<provider>-<timestamp>.md.
func (r *runner) writeReport() (string, error) {
	dir := filepath.Join(r.stack.SDKRoot, "e2e", "reports")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	path := filepath.Join(dir, fmt.Sprintf("%s-%s.md", r.spec.Provider, r.started.UTC().Format("20060102-150405")))
	return path, os.WriteFile(path, []byte(r.report()), 0o600)
}

func (r *runner) report() string {
	var b strings.Builder
	verdict := "PASS"
	if r.failed() {
		verdict = "FAIL"
	}
	fmt.Fprintf(&b, "# e2e %s — %s\n\n", r.spec.Provider, verdict)
	fmt.Fprintf(&b, "- started: %s, total %s\n", r.started.UTC().Format(time.RFC3339), time.Since(r.started).Round(time.Millisecond))
	fmt.Fprintf(&b, "- core: %s (%s@%s) at %s, db %s, webhook-verify fix: %v, conformance --active fix: %v\n",
		r.stack.Release, r.stack.CoreBranch, r.stack.CoreCommit, r.stack.CoreURL, r.stack.DBName, r.stack.FixWebhookVerify, r.stack.FixConformanceActive)
	fmt.Fprintf(&b, "- spec: %s\n", r.spec.Path)
	fmt.Fprintf(&b, "- worker: %s (%s) release %s from %s; mock %s\n", r.spec.Worker.Name, r.spec.WorkerKey(), workerRelease, r.prov.repo, r.prov.mockURL)
	if r.hb != nil {
		fmt.Fprintf(&b, "- presence: package %s, parser %s, release %s, webhooks %v\n", r.hb.Package, r.hb.Version, r.hb.Release, r.hb.Webhooks)
	}
	if r.acct.ID != "" {
		fmt.Fprintf(&b, "- account: %s (provider id %s, pool %s)\n", r.acct.ID, r.acct.ProviderAccountID, r.acct.AssignedWorker)
	}
	b.WriteString("\n| step | status | time | detail |\n|---|---|---|---|\n")
	for _, s := range r.results {
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", s.Name, s.Status, s.Dur.Round(time.Millisecond), mdCell(s.Msg))
	}
	if r.failed() {
		r.appendLogs(&b)
	}
	return b.String()
}

// appendLogs adds the last 50 lines of the worker, mock and core logs.
func (r *runner) appendLogs(b *strings.Builder) {
	for _, l := range []struct {
		name  string
		lines []string
	}{
		{"worker", r.prov.worker.Tail(50)}, {"mock", r.prov.mock.Tail(50)}, {"core", r.stack.CoreTail(50)},
	} {
		fmt.Fprintf(b, "\n## %s log (last %d lines)\n\n```\n%s\n```\n", l.name, len(l.lines), strings.Join(l.lines, "\n"))
	}
}

func mdCell(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	return strings.ReplaceAll(strings.TrimSpace(s), "\n", "<br>")
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i] + " …"
	}
	return s
}

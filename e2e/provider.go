//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// workerRelease is stamped into the worker binary; same major.minor as
// the SDK and core (ADR 0005), so the compatibility check is exercised for real.
const workerRelease = "v0.7.0"

// providerRun is one provider's processes and the values its spec expands.
type providerRun struct {
	spec  *Spec
	stack *Stack
	// repo is the worker repository on disk.
	repo string
	// mockURL is the mock's base URL (http://127.0.0.1:<port>).
	mockURL string
	mock    *proc
	worker  *proc
	vars    map[string]string
	ctl     *mockControl
}

func newProviderRun(spec *Spec, stack *Stack) *providerRun {
	return &providerRun{spec: spec, stack: stack, repo: spec.RepoDir(stack.SDKRoot)}
}

// binaries are built into <out>/<provider>/.
func (r *providerRun) outDir() string { return filepath.Join(r.stack.OutDir, r.spec.Provider) }

// build compiles the mock and the worker (release stamped) from the repo.
func (r *providerRun) build(ctx context.Context) error {
	if err := os.MkdirAll(r.outDir(), 0o750); err != nil {
		return err
	}
	if _, err := os.Stat(r.repo); err != nil {
		return fmt.Errorf("worker repo %s: %w", r.repo, err)
	}
	env := map[string]string{"CGO_ENABLED": "0"}
	r.stack.log("building %s mock %s", r.spec.Provider, r.spec.Mock.Package)
	if _, err := run(ctx, r.repo, env, "go", "build", "-trimpath", "-o", r.mockBin(), r.spec.Mock.Package); err != nil {
		return err
	}
	r.stack.log("building %s worker %s (%s=%s)", r.spec.Provider, r.spec.Worker.Package, r.spec.Worker.ReleaseVar, workerRelease)
	_, err := run(ctx, r.repo, env, "go", "build", "-trimpath",
		"-ldflags", "-X "+r.spec.Worker.ReleaseVar+"="+workerRelease, "-o", r.workerBin(), r.spec.Worker.Package)
	return err
}

func (r *providerRun) mockBin() string   { return filepath.Join(r.outDir(), "mock-"+r.spec.Provider) }
func (r *providerRun) workerBin() string { return filepath.Join(r.outDir(), "worker-"+r.spec.Provider) }

// startMock runs the mock on a free port and waits for its health route.
func (r *providerRun) startMock(ctx context.Context) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)
	r.mockURL = "http://" + addr
	r.vars = map[string]string{
		"MOCK_URL": r.mockURL, "MOCK_ADDR": addr, "NATS_URL": r.stack.NATSURL, "CORE_URL": r.stack.CoreURL,
		"WORKER_NAME": r.spec.Worker.Name, "WORKER_KEY": r.spec.WorkerKey(), "RELEASE": workerRelease, "OUT_DIR": r.outDir(),
	}
	args := append([]string{r.mockBin()}, expandList(r.spec.Mock.Args, r.vars)...)
	p, err := startProc(ctx, r.outDir(), "mock", r.repo, expandMap(r.spec.Mock.Env, r.vars), args...)
	if err != nil {
		return err
	}
	r.mock = p
	r.ctl = &mockControl{base: r.mockURL + r.spec.Mock.ControlPath}
	return waitUntil(ctx, 30*time.Second, "mock "+r.spec.Mock.HealthPath, func() (bool, error) {
		if exited, err := p.Exited(); exited {
			return false, p.exitError(err, 20)
		}
		return httpOK(ctx, r.mockURL+r.spec.Mock.HealthPath), nil
	})
}

// workerEnv is the worker's environment: the SDK's CAIRN_* plus the spec's.
func (r *providerRun) workerEnv() map[string]string {
	env := map[string]string{
		"CAIRN_NATS_URL":           r.stack.NATSURL,
		"CAIRN_WORKER_NAME":        r.spec.Worker.Name,
		"CAIRN_WORKER_INSTANCE_ID": "e2e-1",
		"CAIRN_LOG_LEVEL":          envOr("E2E_WORKER_LOG_LEVEL", "debug"),
	}
	for k, v := range expandMap(r.spec.Worker.Env, r.vars) {
		env[k] = v
	}
	return env
}

// startWorker runs the worker and waits for its presence and for core to
// list the provider (the manifest watcher bound its provider_meta).
func (r *providerRun) startWorker(ctx context.Context, core *coreClient) (*presence, error) {
	p, err := startProc(ctx, r.outDir(), "worker", r.repo, r.workerEnv(), r.workerBin())
	if err != nil {
		return nil, err
	}
	r.worker = p
	var hb *presence
	err = waitUntil(ctx, 60*time.Second, "worker presence", func() (bool, error) {
		if exited, err := p.Exited(); exited {
			return false, p.exitError(err, 30)
		}
		hb, err = r.stack.bus.Presence(ctx, r.spec.Worker.Name)
		return err == nil, nil
	})
	if err != nil {
		return nil, err
	}
	err = waitUntil(ctx, 60*time.Second, "provider listed by core", func() (bool, error) {
		provs, err := core.adminProviders(ctx)
		if err != nil {
			return false, err
		}
		for _, p := range provs {
			if p.Provider == r.spec.Provider {
				return true, nil
			}
		}
		return false, nil
	})
	return hb, err
}

// stop ends the worker and the mock.
func (r *providerRun) stop() {
	if r.worker != nil {
		r.worker.Stop()
	}
	if r.mock != nil {
		r.mock.Stop()
	}
}

// mockControl is the harness side of the mock control contract (README).
type mockControl struct {
	base string
}

// mockStats is GET <control>/stats.
type mockStats struct {
	Refreshes         int `json:"refreshes"`
	Exchanges         int `json:"exchanges"`
	APICalls          int `json:"api_calls"`
	Unauthorized      int `json:"unauthorized"`
	RateLimitedServed int `json:"rate_limited_served"`
	WebhooksSent      int `json:"webhooks_sent"`
	Activities        int `json:"activities"`
}

func (m *mockControl) call(ctx context.Context, method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, m.base+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("mock %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &httpError{Method: method, Path: "mock" + path, Status: resp.StatusCode, Body: string(raw)}
	}
	if out != nil && len(raw) > 0 {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func (m *mockControl) stats(ctx context.Context) (mockStats, error) {
	var s mockStats
	err := m.call(ctx, http.MethodGet, "/stats", nil, &s)
	return s, err
}

// addActivity seeds one activity starting at start and returns its external id.
func (m *mockControl) addActivity(ctx context.Context, name string, start time.Time) (string, error) {
	var out struct {
		ID string `json:"id"`
	}
	err := m.call(ctx, http.MethodPost, "/activities", map[string]any{"name": name, "start": start.UTC().Format(time.RFC3339), "streams": true}, &out)
	return out.ID, err
}

// emitWebhook has the mock deliver a provider-shaped event to core; forged
// signs it with a secret (or token) the worker does not hold.
func (m *mockControl) emitWebhook(ctx context.Context, url, aspect, activityID string, forged bool) (int, string, error) {
	var out struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	err := m.call(ctx, http.MethodPost, "/webhooks/emit", map[string]any{"url": url, "aspect": aspect, "activity_id": activityID, "forge": forged}, &out)
	return out.Status, out.Body, err
}

// failNext makes the next count API calls answer status.
func (m *mockControl) failNext(ctx context.Context, status, count, retryAfter int) error {
	return m.call(ctx, http.MethodPost, "/fail-next", map[string]int{"status": status, "count": count, "retry_after": retryAfter}, nil)
}

func (m *mockControl) reset(ctx context.Context) error {
	return m.call(ctx, http.MethodPost, "/reset", nil, nil)
}

// addMetric seeds one metric sample (metric providers).
func (m *mockControl) addMetric(ctx context.Context, key string, ts time.Time, value float64) error {
	return m.call(ctx, http.MethodPost, "/metrics", map[string]any{"key": key, "ts": ts.UTC().Format(time.RFC3339), "value": value}, nil)
}

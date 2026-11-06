//go:build e2e

// Package e2e drives a provider worker against a real cairn-core: the
// compose dev services, a freshly built core on a free port with its own
// database, the provider's mock server and the worker binary. Each provider
// is described by a spec in providers/<name>.yaml; the steps assert through
// core's HTTP API and the NATS bus, the way the web UI and core do.
//
//	make e2e PROVIDER=strava
//	make e2e-all
package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Spec describes one provider worker and what the harness asserts for it.
type Spec struct {
	// Provider is the provider token in every subject (e.g. "strava").
	Provider string `yaml:"provider"`
	// Repo is the worker repository: absolute, relative to the SDK root, or
	// empty for the parent of the spec's directory (<worker>/e2e/<name>.yaml).
	Repo        string          `yaml:"repo"`
	Worker      WorkerSpec      `yaml:"worker"`
	Mock        MockSpec        `yaml:"mock"`
	Auth        AuthSpec        `yaml:"auth"`
	Expect      ExpectSpec      `yaml:"expect"`
	Steps       StepsSpec       `yaml:"steps"`
	Webhook     WebhookSpec     `yaml:"webhook"`
	Metrics     MetricsSpec     `yaml:"metrics"`
	Conformance ConformanceSpec `yaml:"conformance"`
	// Wait bounds every "wait until core shows X" poll (default 2m).
	Wait time.Duration `yaml:"wait"`
	// Skip names the reason a disabled step does not apply, by step name.
	Skip map[string]string `yaml:"skip"`
	// XFail marks steps that fail because of a documented defect outside the
	// worker, by step name; such a failure is XFAIL, a pass XPASS.
	XFail map[string]string `yaml:"xfail"`
	// Path is where the spec was read from (set by LoadSpecFile).
	Path string `yaml:"-"`
}

// WorkerSpec is how the worker binary is built and started.
type WorkerSpec struct {
	// Package is the go package path inside Repo (e.g. ./cmd/worker-strava).
	Package string `yaml:"package"`
	// Name is the pool name (CAIRN_WORKER_NAME); the worker key is <name>-<provider>.
	Name string `yaml:"name"`
	// ReleaseVar is the ldflags variable stamped with the release (e.g. main.workerRelease).
	ReleaseVar string `yaml:"release_var"`
	// Env is extra environment; ${MOCK_URL}, ${MOCK_ADDR}, ${NATS_URL}, ${CORE_URL}, ${WORKER_NAME},
	// ${WORKER_KEY}, ${RELEASE} and ${OUT_DIR} expand.
	Env map[string]string `yaml:"env"`
}

// MockSpec is the provider's standalone mock server.
type MockSpec struct {
	Package string            `yaml:"package"`
	Args    []string          `yaml:"args"`
	Env     map[string]string `yaml:"env"`
	// HealthPath answers 200 once the mock is up (default /_mock/healthz).
	HealthPath string `yaml:"health_path"`
	// ControlPath is the prefix of the mock control API (default /_mock).
	ControlPath string `yaml:"control_path"`
}

// AuthSpec is how the account is connected.
type AuthSpec struct {
	// Method is "oauth2" (authorize round-trip through the mock) or
	// "credentials" (core creates the account from the two config fields).
	Method string `yaml:"method"`
	// ClientID and ClientSecret fill the connection's two config fields:
	// the app credentials for OAuth, login and secret for credentials.
	ClientID     string `yaml:"client_id"`
	ClientSecret string `yaml:"client_secret"`
	Label        string `yaml:"label"`
}

// ExpectSpec is what a full sync must produce.
type ExpectSpec struct {
	// Activities the mock seeds and the full sync imports.
	Activities int `yaml:"activities"`
	// ActivityType is the proto enum name of the imported activities (e.g. ACTIVITY_TYPE_RIDE).
	ActivityType string `yaml:"activity_type"`
	Streams      bool   `yaml:"streams"`
	Laps         bool   `yaml:"laps"`
	// Gear expects at least one gear item from the connect-time import_gear.
	Gear bool `yaml:"gear"`
	// ParserVersion is the source_worker_version a re-parse stamps.
	ParserVersion string `yaml:"parser_version"`
	// Capabilities are data types the presence manifest must declare.
	Capabilities []string `yaml:"capabilities"`
	// Archive is false for providers whose terms forbid raw archives (no parse_blob).
	Archive bool `yaml:"archive"`
}

// StepsSpec switches the data steps on; connect always runs.
type StepsSpec struct {
	Discover  bool `yaml:"discover"`
	FullSync  bool `yaml:"full_sync"`
	Reconcile bool `yaml:"reconcile"`
	Webhook   bool `yaml:"webhook"`
	// WebhookForged delivers an event signed with the wrong secret; the
	// worker must drop it (needs the mock's emit "forge").
	WebhookForged bool `yaml:"webhook_forged"`
	WebhookVerify bool `yaml:"webhook_verify"`
	Reparse       bool `yaml:"reparse"`
	ImportMetrics bool `yaml:"import_metrics"`
	RateLimit     bool `yaml:"rate_limit"`
	TokenRefresh  bool `yaml:"token_refresh"`
	Conformance   bool `yaml:"conformance"`
}

// WebhookSpec drives the webhook steps.
type WebhookSpec struct {
	// Aspect is the event kind the mock emits (default "create").
	Aspect string `yaml:"aspect"`
	// VerifyQuery is the GET handshake query; ${CHALLENGE} expands to a random token.
	VerifyQuery map[string]string `yaml:"verify_query"`
	// VerifyExpect must appear in the handshake body; ${CHALLENGE} expands.
	VerifyExpect string `yaml:"verify_expect"`
}

// MetricsSpec drives import_metrics for metric providers.
type MetricsSpec struct {
	// Keys are core data types (Steps, HRV, RestingHR, Sleep, Weight) that
	// must show up in GET /api/health after the import; core drops METRIC
	// events whose key is not such a data type.
	Keys []string `yaml:"keys"`
	Days int      `yaml:"days"`
}

// ConformanceSpec configures cairn-core's worker-conformance run.
type ConformanceSpec struct {
	// Flags are passed through (e.g. --gear, --metrics, --parse-blob=false).
	Flags []string `yaml:"flags"`
	// Active adds --active --account-id <the connected account>.
	Active bool `yaml:"active"`
}

// LoadSpec reads providers/<name>.yaml under dir and applies defaults.
func LoadSpec(dir, name string) (*Spec, error) {
	return LoadSpecFile(filepath.Join(dir, name+".yaml"))
}

// LoadSpecFile reads a spec from path (a worker repository's e2e/<name>.yaml
// or one under providers/) and applies defaults.
func LoadSpecFile(path string) (*Spec, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // G304: the spec the operator named
	if err != nil {
		return nil, err
	}
	var s Spec
	if err := yaml.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("spec %s: %w", path, err)
	}
	s.Path = path
	s.defaults()
	if err := s.validate(); err != nil {
		return nil, fmt.Errorf("spec %s: %w", path, err)
	}
	return &s, nil
}

// RepoDir resolves Repo against the SDK root; an empty Repo is the parent of
// the spec file's directory.
func (s *Spec) RepoDir(sdkRoot string) string {
	switch {
	case s.Repo == "":
		return filepath.Clean(filepath.Join(filepath.Dir(s.Path), ".."))
	case filepath.IsAbs(s.Repo):
		return filepath.Clean(s.Repo)
	}
	return filepath.Clean(filepath.Join(sdkRoot, s.Repo))
}

// ListSpecs returns the spec names in dir, templates excluded.
func ListSpecs(dir string) ([]string, error) {
	entries, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		n := strings.TrimSuffix(filepath.Base(e), ".yaml")
		if !strings.HasPrefix(n, "_") {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (s *Spec) defaults() {
	if s.Worker.Name == "" {
		s.Worker.Name = s.Provider + "-fetcher"
	}
	if s.Worker.ReleaseVar == "" {
		s.Worker.ReleaseVar = "main.workerRelease"
	}
	if s.Mock.HealthPath == "" {
		s.Mock.HealthPath = "/_mock/healthz"
	}
	if s.Mock.ControlPath == "" {
		s.Mock.ControlPath = "/_mock"
	}
	if s.Auth.Method == "" {
		s.Auth.Method = "oauth2"
	}
	if s.Auth.Label == "" {
		s.Auth.Label = "e2e " + s.Provider
	}
	if s.Webhook.Aspect == "" {
		s.Webhook.Aspect = "create"
	}
	if s.Metrics.Days == 0 {
		s.Metrics.Days = 7
	}
	if s.Wait == 0 {
		s.Wait = 2 * time.Minute
	}
}

func (s *Spec) validate() error {
	switch {
	case s.Provider == "":
		return fmt.Errorf("provider is required")
	case s.Worker.Package == "":
		return fmt.Errorf("worker.package is required")
	case s.Mock.Package == "":
		return fmt.Errorf("mock.package is required")
	case s.Auth.Method != "oauth2" && s.Auth.Method != "credentials":
		return fmt.Errorf("auth.method must be oauth2 or credentials")
	case s.Steps.Reparse && !s.Expect.Archive:
		return fmt.Errorf("steps.reparse needs expect.archive (no parse_blob without a raw archive)")
	case s.Steps.WebhookForged && !s.Steps.Webhook:
		return fmt.Errorf("steps.webhook_forged needs steps.webhook")
	}
	for field, m := range map[string]map[string]string{"skip": s.Skip, "xfail": s.XFail} {
		for name := range m {
			if !slices.Contains(stepNames, name) {
				return fmt.Errorf("%s: unknown step %q (steps: %v)", field, name, stepNames)
			}
		}
	}
	return nil
}

// WorkerKey is the pool's composite identity and webhook path token.
func (s *Spec) WorkerKey() string { return s.Worker.Name + "-" + s.Provider }

// expand substitutes ${VAR} placeholders from vars.
func expand(in string, vars map[string]string) string {
	return os.Expand(in, func(k string) string { return vars[k] })
}

// expandMap expands every value of m.
func expandMap(m map[string]string, vars map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = expand(v, vars)
	}
	return out
}

// expandList expands every element of l.
func expandList(l []string, vars map[string]string) []string {
	out := make([]string, 0, len(l))
	for _, v := range l {
		out = append(out, expand(v, vars))
	}
	return out
}

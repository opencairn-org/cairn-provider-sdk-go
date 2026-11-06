//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Compose dev services of cairn-core (docker-compose.dev.yml), on the host.
const (
	composeFile  = "docker-compose.dev.yml"
	natsURL      = "nats://127.0.0.1:4222"
	natsMonitor  = "http://127.0.0.1:8222/healthz"
	minioHealth  = "http://127.0.0.1:9000/minio/health/live"
	pgHostPort   = "127.0.0.1:5432"
	minioKey     = "cairn"
	minioSecret  = "cairn-secret"
	minioBucket  = "cairn-e2e"
	e2eDBPrefix  = "cairn_e2e_"
	fixWebhookID = "cebd00c" // core: keep the webhook verify handshake out of CAIRN_WEBHOOKS
	// conformanceOldSubject is the pre-pool format the tool used before fix(conformance).
	conformanceOldSubject = `"cairn.results.reconcile.%s.%s"`
)

// Stack is the shared environment: compose services, the core server and
// the harness's own NATS connection.
type Stack struct {
	SDKRoot  string
	CoreRoot string
	// OutDir holds binaries and process logs of this run.
	OutDir string
	// NATSURL is the compose NATS; CoreURL the core server started by the harness.
	NATSURL string
	CoreURL string
	DBName  string
	// Release is the version core reports (git describe of its checkout).
	Release    string
	CoreCommit string
	CoreBranch string
	// FixWebhookVerify is true when core's checkout contains the verify-stream fix.
	FixWebhookVerify bool
	// FixConformanceActive is true when core's worker-conformance awaits the
	// reconcile result on the pool's subject (ADR 0006).
	FixConformanceActive bool
	// FixMetricsStore is true when core files non-data-type METRIC keys in
	// its metrics store instead of dropping them.
	FixMetricsStore bool
	// Admin is the bootstrap admin the harness logs in as.
	AdminEmail    string
	AdminPassword string

	core *proc
	bus  *busObserver
	log  func(format string, args ...any)
}

// defaultOutDir is outside the module: gorelease zips ignored files too and
// refuses a module over 500 MB, which a few runs' binaries exceed.
func defaultOutDir() string {
	if cache, err := os.UserCacheDir(); err == nil {
		return filepath.Join(cache, "cairn-provider-sdk-go", "e2e")
	}
	return filepath.Join(os.TempDir(), "cairn-provider-sdk-go-e2e")
}

// coreLog is the log of the core server the stack started.
func (s *Stack) coreLog() string { return filepath.Join(s.OutDir, "core.log") }

// NewStack resolves paths from the SDK root (the e2e package's parent).
func NewStack(sdkRoot string, logf func(string, ...any)) *Stack {
	coreRoot := envOr("E2E_CORE_DIR", filepath.Join(sdkRoot, "..", "cairn-core"))
	return &Stack{
		SDKRoot: sdkRoot, CoreRoot: coreRoot,
		OutDir:        envOr("E2E_OUT_DIR", defaultOutDir()),
		NATSURL:       envOr("E2E_NATS_URL", natsURL),
		AdminEmail:    envOr("E2E_ADMIN_EMAIL", "admin@e2e.local"),
		AdminPassword: envOr("E2E_ADMIN_PASSWORD", "e2e-admin-password-1"),
		log:           logf,
	}
}

// Up brings the services and core up: compose (unless E2E_REUSE_STACK=1), a
// fresh database, purged NATS state, the core binary on a free port.
func (s *Stack) Up(ctx context.Context) error {
	if err := os.MkdirAll(s.OutDir, 0o750); err != nil {
		return err
	}
	if os.Getenv("E2E_REUSE_STACK") != "1" {
		if err := s.composeUp(ctx); err != nil {
			return err
		}
	}
	if err := s.waitServices(ctx); err != nil {
		return err
	}
	if err := s.freshDatabase(ctx); err != nil {
		return err
	}
	bus, err := newBusObserver(ctx, s.NATSURL)
	if err != nil {
		return err
	}
	s.bus = bus
	if err := bus.purge(ctx); err != nil {
		return err
	}
	if err := s.buildCore(ctx); err != nil {
		return err
	}
	return s.startCore(ctx)
}

// Down stops core and the observer; compose stays up unless E2E_DOWN=1.
func (s *Stack) Down(ctx context.Context) {
	if s.core != nil {
		s.core.Stop()
	}
	if s.bus != nil {
		s.bus.close()
	}
	if os.Getenv("E2E_KEEP_DB") != "1" && s.DBName != "" {
		_, _ = s.psql(ctx, "DROP DATABASE IF EXISTS "+s.DBName+" WITH (FORCE)")
	}
	if os.Getenv("E2E_DOWN") == "1" {
		_, _ = s.compose(ctx, "down")
	}
}

// compose runs podman compose against core's dev file.
func (s *Stack) compose(ctx context.Context, args ...string) (string, error) {
	tool := strings.Fields(envOr("E2E_COMPOSE", "podman compose"))
	full := append(append(tool, "-f", composeFile), args...)
	return run(ctx, s.CoreRoot, nil, full...)
}

func (s *Stack) composeUp(ctx context.Context) error {
	s.log("compose up (%s)", s.CoreRoot)
	_, err := s.compose(ctx, "up", "-d")
	return err
}

// waitServices waits for Postgres, NATS and MinIO to answer.
func (s *Stack) waitServices(ctx context.Context) error {
	deadline := time.Now().Add(2 * time.Minute)
	for _, check := range []struct {
		name string
		ok   func() bool
	}{
		{"postgres", func() bool { return tcpOpen(pgHostPort) }},
		{"nats", func() bool { return httpOK(ctx, natsMonitor) }},
		{"minio", func() bool { return httpOK(ctx, minioHealth) }},
	} {
		for !check.ok() {
			if time.Now().After(deadline) {
				return fmt.Errorf("%s not ready after 2m", check.name)
			}
			if err := sleepCtx(ctx, time.Second); err != nil {
				return err
			}
		}
		s.log("%s ready", check.name)
	}
	return nil
}

// psql runs one statement through the compose postgres container.
func (s *Stack) psql(ctx context.Context, stmt string) (string, error) {
	return s.compose(ctx, "exec", "-T", "postgres", "psql", "-U", "cairn", "-d", "cairn", "-At", "-v", "ON_ERROR_STOP=1", "-c", stmt)
}

// freshDatabase creates a unique database and drops the ones earlier runs left.
func (s *Stack) freshDatabase(ctx context.Context) error {
	if os.Getenv("E2E_KEEP_DB") != "1" {
		old, err := s.psql(ctx, "SELECT datname FROM pg_database WHERE datname LIKE '"+e2eDBPrefix+"%'")
		if err != nil {
			return err
		}
		for _, db := range strings.Fields(old) {
			if strings.HasPrefix(db, e2eDBPrefix) {
				_, _ = s.psql(ctx, "DROP DATABASE IF EXISTS "+db+" WITH (FORCE)")
			}
		}
	}
	s.DBName = e2eDBPrefix + time.Now().UTC().Format("20060102_150405")
	// The port answers before Postgres accepts sessions; a few retries cover it.
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if _, err = s.psql(ctx, "CREATE DATABASE "+s.DBName); err == nil {
			break
		}
		if err := sleepCtx(ctx, 2*time.Second); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	s.log("database %s created", s.DBName)
	return nil
}

// buildCore compiles cairn-core's server, stamped with its git describe.
func (s *Stack) buildCore(ctx context.Context) error {
	describe, err := run(ctx, s.CoreRoot, nil, "git", "describe", "--tags", "--always", "--dirty")
	if err != nil {
		return err
	}
	s.Release = strings.TrimSpace(describe)
	commit, _ := run(ctx, s.CoreRoot, nil, "git", "rev-parse", "--short", "HEAD")
	branch, _ := run(ctx, s.CoreRoot, nil, "git", "branch", "--show-current")
	s.CoreCommit, s.CoreBranch = strings.TrimSpace(commit), strings.TrimSpace(branch)
	_, err = run(ctx, s.CoreRoot, nil, "git", "merge-base", "--is-ancestor", fixWebhookID, "HEAD")
	s.FixWebhookVerify = err == nil
	tool, _ := os.ReadFile(filepath.Join(s.CoreRoot, "cmd", "worker-conformance", "main.go")) //nolint:gosec // G304: core's checkout
	s.FixConformanceActive = !strings.Contains(string(tool), conformanceOldSubject)
	route, _ := os.ReadFile(filepath.Join(s.CoreRoot, "internal", "usecase", "metrics", "import.go")) //nolint:gosec // G304: core's checkout
	s.FixMetricsStore = strings.Contains(string(route), "func RouteImport")
	s.log("building core %s (%s@%s)", s.Release, s.CoreBranch, s.CoreCommit)
	_, err = run(ctx, s.CoreRoot, map[string]string{"CGO_ENABLED": "0"}, "go", "build", "-trimpath",
		"-ldflags", "-X main.Version="+s.Release, "-o", filepath.Join(s.OutDir, "cairn"), "./cmd/server")
	return err
}

// coreEnv is the server's configuration for this run. The public URL names
// localhost (WebAuthn wants a domain for its RP id); the listener is loopback.
func (s *Stack) coreEnv(addr, public string) map[string]string {
	return map[string]string{ //nolint:gosec // G101: throwaway secrets of the local dev stack
		"CAIRN_DATABASE_URL":                      "postgres://cairn:cairn@" + pgHostPort + "/" + s.DBName + "?sslmode=disable",
		"CAIRN_DATABASE_AUTO_MIGRATE":             "true",
		"CAIRN_NATS_URL":                          s.NATSURL,
		"CAIRN_STORAGE_ENDPOINT":                  "http://127.0.0.1:9000",
		"CAIRN_STORAGE_ACCESS_KEY_ID":             minioKey,
		"CAIRN_STORAGE_SECRET_ACCESS_KEY":         minioSecret,
		"CAIRN_STORAGE_BUCKET":                    minioBucket,
		"CAIRN_AUTH_SESSION_SECRET":               "e2e-session-secret-not-for-production",
		"CAIRN_AUTH_MASTER_ENCRYPTION_KEY":        "e2e-master-encryption-key-not-for-production",
		"CAIRN_INSTANCE_BOOTSTRAP_ADMIN_EMAIL":    s.AdminEmail,
		"CAIRN_INSTANCE_BOOTSTRAP_ADMIN_PASSWORD": "e2e-bootstrap-password",
		"CAIRN_INSTANCE_PUBLIC_URL":               public,
		"CAIRN_HTTP_ADDR":                         addr,
		"CAIRN_HTTP_PUBLIC_BASE_URL":              public,
		"CAIRN_EMAIL_ADAPTER":                     "none",
		"CAIRN_LOG_FORMAT":                        "text",
		"CAIRN_LOG_LEVEL":                         envOr("E2E_CORE_LOG_LEVEL", "info"),
		// Reconcile ticks fast; accounts are due by their poll interval (set per step).
		"CAIRN_SCHEDULER_TICK_INTERVAL":    "2s",
		"CAIRN_SCHEDULER_POLLING_INTERVAL": "1h",
		// No internet: no reverse geocoding, no OSM tiles.
		"CAIRN_GEOCODER_ENABLED": "false",
		"CAIRN_MAP_TILE_URL":     "http://127.0.0.1:9/{z}/{x}/{y}.png",
	}
}

// startCore runs the server on a free port and waits for /readyz.
func (s *Stack) startCore(ctx context.Context) error {
	port, err := freePort()
	if err != nil {
		return err
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)
	s.CoreURL = "http://localhost:" + strconv.Itoa(port)
	p, err := startProc(ctx, s.OutDir, "core", s.CoreRoot, s.coreEnv(addr, s.CoreURL), filepath.Join(s.OutDir, "cairn"), "serve")
	if err != nil {
		return err
	}
	s.core = p
	s.log("core starting on %s", s.CoreURL)
	return waitUntil(ctx, 2*time.Minute, "core /readyz", func() (bool, error) {
		if exited, err := p.Exited(); exited {
			return false, p.exitError(err, 30)
		}
		return httpOK(ctx, s.CoreURL+"/healthz") && httpOK(ctx, s.CoreURL+"/readyz"), nil
	})
}

// CoreTail is the last n lines of the core log.
func (s *Stack) CoreTail(n int) []string { return s.core.Tail(n) }

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	return port, l.Close()
}

func tcpOpen(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func httpOK(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// waitUntil polls cond every 500ms until it is true, errors, or d elapses.
func waitUntil(ctx context.Context, d time.Duration, what string, cond func() (bool, error)) error {
	deadline := time.Now().Add(d)
	for {
		ok, err := cond()
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: not reached within %s", what, d)
		}
		if err := sleepCtx(ctx, 500*time.Millisecond); err != nil {
			return err
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

package workersdk

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/opencairn-org/cairn-provider-sdk-go/natsbus"
)

// EnvConfig is the environment every cairn worker reads at start-up:
//
//	CAIRN_NATS_URL                        e.g. nats://nats:4222 (default nats://localhost:4222)
//	CAIRN_WORKER_ENROLLMENT_TOKEN         from POST /api/admin/worker-enrollments (production)
//	CAIRN_NATS_USER / CAIRN_NATS_PASSWORD optional static credentials (dev stacks)
//	CAIRN_WORKER_NAME                     the pool name; must equal the enrollment's name
//	CAIRN_WORKER_INSTANCE_ID              optional stable id per instance (default: hostname)
//	CAIRN_LOG_LEVEL                       debug|info|warn|error (default info)
//
// Provider-specific settings (API base URLs, webhook verify tokens) are the
// worker's own; read them next to LoadEnv.
type EnvConfig struct {
	NATSURL         string
	EnrollmentToken string
	NATSUser        string
	NATSPassword    string
	WorkerName      string
	InstanceID      string
	LogLevel        slog.Level
}

// LoadEnv reads EnvConfig from the environment. defaultName is the pool
// name used when CAIRN_WORKER_NAME is unset (e.g. "acme-fetcher"); the
// instance id falls back to the hostname and then to defaultName. The only
// value that can be malformed is CAIRN_LOG_LEVEL.
func LoadEnv(defaultName string) (EnvConfig, error) {
	cfg := EnvConfig{
		NATSURL:         EnvOr("CAIRN_NATS_URL", "nats://localhost:4222"),
		EnrollmentToken: os.Getenv("CAIRN_WORKER_ENROLLMENT_TOKEN"),
		NATSUser:        os.Getenv("CAIRN_NATS_USER"),
		NATSPassword:    os.Getenv("CAIRN_NATS_PASSWORD"),
		WorkerName:      EnvOr("CAIRN_WORKER_NAME", defaultName),
		InstanceID:      EnvOr("CAIRN_WORKER_INSTANCE_ID", HostnameOr(defaultName)),
	}
	if err := cfg.LogLevel.UnmarshalText([]byte(EnvOr("CAIRN_LOG_LEVEL", "info"))); err != nil {
		return EnvConfig{}, fmt.Errorf("CAIRN_LOG_LEVEL: %w", err)
	}
	return cfg, nil
}

// EnvOr returns the environment variable k, or fallback when unset or empty.
func EnvOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

// HostnameOr returns the hostname, or fallback when it cannot be read.
func HostnameOr(fallback string) string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return fallback
}

// NewLogger is a JSON logger on stderr at the configured level.
func (c EnvConfig) NewLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: c.LogLevel}))
}

// Connect opens the NATS connection a worker runs on and wraps it in a Bus.
//
// With an enrollment token (production) the worker presents only the token
// on CONNECT; core's auth-callout admits the connection by minting a
// user-JWT scoped to the pool's permissions for the user nkey nats-server
// assigns it (cairn-core docs/architecture.md §4.5). A restart is a new
// callout; a single-use enrollment then needs a new token.
//
// Without a token (dev stacks whose NATS runs without auth-callout) the
// worker connects anonymously, or with CAIRN_NATS_USER/PASSWORD when set.
// There is no per-worker permission scoping on that path. That is the only
// difference between dev and production bring-up.
func Connect(cfg EnvConfig, logger *slog.Logger) (*natsbus.Bus, error) {
	if logger == nil {
		logger = slog.Default()
	}
	opts := []nats.Option{
		nats.Name(cfg.WorkerName),
		nats.Timeout(10 * time.Second),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("nats disconnect", "error", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			logger.Info("nats reconnected", "url", c.ConnectedUrl())
		}),
	}

	switch {
	case cfg.EnrollmentToken != "":
		// Production path: core's auth-callout binds the JWT to the server-assigned user nkey.
		logger.Info("connecting to NATS (enrollment-token auth-callout)",
			"url", cfg.NATSURL,
			"worker_name", cfg.WorkerName,
		)
		opts = append(opts, nats.Token(cfg.EnrollmentToken))

	default:
		logger.Warn("connecting to NATS WITHOUT enrollment token (dev mode — no permission scoping)",
			"url", cfg.NATSURL,
			"worker_name", cfg.WorkerName,
			"static_user", cfg.NATSUser != "",
		)
		if cfg.NATSUser != "" {
			opts = append(opts, nats.UserInfo(cfg.NATSUser, cfg.NATSPassword))
		}
	}

	nc, err := nats.Connect(cfg.NATSURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	// Workers don't declare streams; the server does.
	bus, err := natsbus.NewBus(nc, natsbus.Options{ClientName: cfg.WorkerName + ":" + cfg.InstanceID, Logger: logger})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("wrap into Bus: %w", err)
	}
	return bus, nil
}

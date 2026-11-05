// Command minimal is a complete cairn provider worker for the imaginary
// "Acme" platform (examples/minimal/fakeprovider). It is the template the
// real workers are built from and shows every part of the contract:
//
//   - manifest and provider meta (core builds the connect flow from them)
//   - the OAuth code exchange and token refresh (package oauth)
//   - fetch_source: fetch the activity and its FIT file, map it with
//     fitmap, archive the raw bytes, publish the claim-checked result
//   - parse_blob: re-map an archived FIT without a provider call
//   - reconcile: list recent activity, fan out fetch_source, advance the
//     watermark
//   - import_metrics: daily metrics as METRIC events
//   - discover: paged listing for the import queue
//   - webhooks: HMAC-verified events and the GET handshake
//   - the shared rate limiter and the pool-wide refresh lock
//
// Environment: the CAIRN_* variables of workersdk.LoadEnv plus
//
//	ACME_BASE_URL           the provider API (default https://api.acme.test)
//	ACME_WEBHOOK_SECRET     set on exactly one instance to own the webhook subject
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/oauth2"

	"github.com/opencairn-org/cairn-provider-sdk-go/capability"
	"github.com/opencairn-org/cairn-provider-sdk-go/natsbus"
	"github.com/opencairn-org/cairn-provider-sdk-go/oauth"
	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

const (
	workerName     = "acme-fetcher"
	workerProvider = "acme"
	workerPackage  = "org.opencairn.examples.acme-importer"

	// Versions (cairn-core ADR 0005): the archive is the provider's FIT
	// file (dataset 1); bump the parser's M with every change to the
	// mapped output.
	datasetVersion = 1
	parserVersion  = "1.0"
)

// workerRelease is stamped at build time: -ldflags "-X main.workerRelease=v0.7.0".
var workerRelease = ""

// manifest declares the data types this worker supplies.
func manifest() capability.Manifest {
	return capability.Manifest{
		capability.DataTypeActivity: {Read: true, Backfill: true, Granularity: "per-activity"},
		capability.DataTypeLap:      {Read: true, Backfill: true},
		capability.DataTypeWeight:   {Read: true, Backfill: true, Granularity: "daily"},
	}
}

// providerMeta tells core how to connect an Acme account.
func providerMeta(base string) *workersdk.ProviderMeta {
	return &workersdk.ProviderMeta{
		DisplayName:         "Acme",
		ActivityURLTemplate: base + "/activities/{id}",
		AuthMethod:          workersdk.AuthMethodOAuth2,
		OAuth: &workersdk.ProviderOAuth{
			AuthorizeURL: base + "/oauth/authorize",
			Scopes:       []string{"activity:read"},
			PKCE:         true,
		},
		ConfigFields: []workersdk.ProviderField{
			{Key: "client_id", Label: "Client ID", Help: "From the Acme developer portal."},
			{Key: "client_secret", Label: "Client secret", Secret: true},
		},
	}
}

// settings is the worker's own configuration next to workersdk.EnvConfig.
type settings struct {
	env           workersdk.EnvConfig
	baseURL       string
	webhookSecret string
}

func loadSettings() (settings, error) {
	env, err := workersdk.LoadEnv(workerName)
	if err != nil {
		return settings{}, err
	}
	return settings{env: env, baseURL: workersdk.EnvOr("ACME_BASE_URL", "https://api.acme.test"), webhookSecret: os.Getenv("ACME_WEBHOOK_SECRET")}, nil
}

func main() {
	cfg, err := loadSettings()
	if err != nil {
		fmt.Fprintln(os.Stderr, "load config:", err)
		os.Exit(1)
	}
	logger := cfg.env.NewLogger()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

// run wires the bus, the worker and its handlers, then blocks in Run.
func run(ctx context.Context, cfg settings, logger *slog.Logger) error {
	bus, err := workersdk.Connect(cfg.env, logger)
	if err != nil {
		return fmt.Errorf("connect bus: %w", err)
	}
	defer bus.Close()

	w, err := newWorker(bus, cfg, logger)
	if err != nil {
		return err
	}
	client := newClient(cfg.baseURL)
	registerHandlers(w, client, cfg)

	discSub, err := w.RespondDiscover(ctx, makeDiscover(w, client))
	if err != nil {
		return fmt.Errorf("register discover responder: %w", err)
	}
	defer func() { _ = discSub.Close(context.Background()) }()

	exSub, err := w.RespondOAuthExchange(ctx, newExchanger(cfg.baseURL).Exchange)
	if err != nil {
		return fmt.Errorf("register oauth exchange responder: %w", err)
	}
	defer func() { _ = exSub.Close(context.Background()) }()

	logger.Info("acme worker starting", "instance", cfg.env.InstanceID, "version", parserVersion, "release", workerRelease, "pool", w.JobSubject("*"))
	return w.Run(ctx)
}

// newWorker builds the Worker: limiter and refresh lock on the shared KV,
// the RFC 6749 refresher as AuthHandler.
func newWorker(bus *natsbus.Bus, cfg settings, logger *slog.Logger) (*workersdk.Worker, error) {
	limits := map[string]natsbus.Limit{
		// Acme allows 100 requests per minute per app; the pool shares it.
		workerProvider + ":" + cfg.env.WorkerName + ":minute": {Capacity: 100, Window: natsbus.PerMinute},
	}
	return workersdk.New(workersdk.Config{
		Name:         cfg.env.WorkerName,
		InstanceID:   cfg.env.InstanceID,
		Version:      parserVersion,
		Release:      workerRelease,
		Package:      workerPackage,
		Provider:     workerProvider,
		Bus:          bus,
		Limiter:      workersdk.NewRateLimiter(bus, logger, limits),
		Auth:         newRefresher(cfg.baseURL),
		RefreshLock:  workersdk.NewRefreshLock(bus, logger, cfg.env.InstanceID, 30*time.Second),
		Logger:       logger,
		Manifest:     manifest(),
		ProviderMeta: providerMeta(cfg.baseURL),
	})
}

// newRefresher is the token refresh for Acme's standard endpoint; Acme
// rotates refresh tokens, which is why the worker sets RefreshLock.
func newRefresher(base string) *oauth.Refresher {
	return &oauth.Refresher{Endpoint: oauth2.Endpoint{TokenURL: base + "/oauth/token", AuthStyle: oauth2.AuthStyleInParams}}
}

// newExchanger answers core's code exchange; the account id comes from a
// profile call since the token reply does not carry it.
func newExchanger(base string) *oauth.Exchanger {
	return &oauth.Exchanger{
		Endpoint:  oauth2.Endpoint{TokenURL: base + "/oauth/token", AuthStyle: oauth2.AuthStyleInParams},
		AccountID: oauth.AccountIDFromProfile(base+"/v1/me", "id"),
	}
}

// registerHandlers wires the job handlers and, when this instance owns the
// webhook, the webhook handlers.
func registerHandlers(w *workersdk.Worker, client *acmeClient, cfg settings) {
	w.Handle(w.JobSubject(workersdk.JobFetchSource), makeFetchSource(client))
	w.HandleWithMaxDeliver(w.JobSubject(workersdk.JobParseBlob), parseBlob, workersdk.ParserTooOldAttempts+2)
	w.Handle(w.JobSubject(workersdk.JobReconcile), makeReconcile(client))
	w.Handle(w.JobSubject(workersdk.JobImportMetrics), makeImportMetrics(client))
	if cfg.webhookSecret != "" {
		w.WebhookEvent(makeWebhookEvent([]byte(cfg.webhookSecret)))
		w.WebhookVerify(makeWebhookVerify(cfg.webhookSecret))
	}
}

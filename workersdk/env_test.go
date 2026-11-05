package workersdk

import (
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestLoadEnv(t *testing.T) {
	t.Setenv("CAIRN_NATS_URL", "nats://n:4222")
	t.Setenv("CAIRN_WORKER_NAME", "")
	t.Setenv("CAIRN_WORKER_INSTANCE_ID", "i1")
	t.Setenv("CAIRN_LOG_LEVEL", "debug")
	t.Setenv("CAIRN_WORKER_ENROLLMENT_TOKEN", "tok")
	cfg, err := LoadEnv("acme-fetcher")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NATSURL != "nats://n:4222" || cfg.WorkerName != "acme-fetcher" || cfg.InstanceID != "i1" || cfg.LogLevel != slog.LevelDebug || cfg.EnrollmentToken != "tok" {
		t.Errorf("cfg = %+v", cfg)
	}
	if cfg.NewLogger() == nil {
		t.Error("NewLogger nil")
	}
	t.Setenv("CAIRN_LOG_LEVEL", "loud")
	if _, err := LoadEnv("x"); err == nil {
		t.Error("bad log level: want error")
	}
	if HostnameOr("x") == "" || EnvOr("CAIRN_NOPE_UNSET", "fb") != "fb" {
		t.Error("fallbacks")
	}
}

func startNATS(t *testing.T) string {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}

func TestConnect(t *testing.T) {
	url := startNATS(t)
	for name, cfg := range map[string]EnvConfig{
		"anonymous":   {NATSURL: url, WorkerName: "w"},
		"static user": {NATSURL: url, WorkerName: "w", NATSUser: "u", NATSPassword: "p"},
	} {
		t.Run(name, func(t *testing.T) {
			bus, err := Connect(cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			bus.Close()
		})
	}
	if _, err := Connect(EnvConfig{NATSURL: "nats://127.0.0.1:1"}, quietLogger()); err == nil {
		t.Error("unreachable server: want error")
	}
}

// startAuthCalloutNATS runs core's auth-callout nats.conf (no nkey users) with a
// fake callout that admits token and binds the JWT to req.UserNkey, as core does.
func startAuthCalloutNATS(t *testing.T, token string) (url string, nkeyOnConnect func() []string) {
	t.Helper()
	acct, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	acctPub, _ := acct.PublicKey()
	t.Setenv("CAIRN_NATS_PASSWORD", "core-pw")
	t.Setenv("CAIRN_NATS_AUTH_ISSUER", acctPub)
	opts, err := server.ProcessConfigFile("testdata/nats-auth-callout.conf")
	if err != nil {
		t.Fatal(err)
	}
	opts.Host, opts.Port, opts.HTTPPort, opts.StoreDir = "127.0.0.1", -1, 0, t.TempDir()
	opts.NoLog, opts.NoSigs = true, true
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(s.Shutdown)

	core, err := nats.Connect(s.ClientURL(), nats.UserInfo("cairn-core", "core-pw"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(core.Close)
	var mu sync.Mutex
	var seen []string
	_, err = core.Subscribe("$SYS.REQ.USER.AUTH", func(msg *nats.Msg) {
		req, err := jwt.DecodeAuthorizationRequestClaims(string(msg.Data))
		if err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		mu.Lock()
		seen = append(seen, req.ConnectOptions.Nkey)
		mu.Unlock()
		resp := jwt.NewAuthorizationResponseClaims(req.UserNkey)
		resp.Audience = req.Server.ID
		resp.IssuerAccount = acctPub
		if req.ConnectOptions.Token == token {
			uc := jwt.NewUserClaims(req.UserNkey)
			uc.Audience = "CAIRN"
			uc.Pub.Allow.Add(">")
			uc.Sub.Allow.Add(">")
			if resp.Jwt, err = uc.Encode(acct); err != nil {
				t.Errorf("encode user jwt: %v", err)
				return
			}
		} else {
			resp.Error = "invalid_token"
		}
		enc, err := resp.Encode(acct)
		if err != nil {
			t.Errorf("encode response: %v", err)
			return
		}
		_ = msg.Respond([]byte(enc))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Flush(); err != nil {
		t.Fatal(err)
	}
	return s.ClientURL(), func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), seen...) }
}

func TestConnect_EnrollmentToken_AuthCallout(t *testing.T) {
	url, seen := startAuthCalloutNATS(t, "enroll-1")
	bus, err := Connect(EnvConfig{NATSURL: url, WorkerName: "w", EnrollmentToken: "enroll-1"}, quietLogger())
	if err != nil {
		t.Fatalf("token-only connect without a placeholder nkey user: %v", err)
	}
	bus.Close()
	if got := seen(); len(got) != 1 || got[0] != "" {
		t.Errorf("CONNECT nkeys = %q; want one token-only connect", got)
	}
	_, err = Connect(EnvConfig{NATSURL: url, WorkerName: "w", EnrollmentToken: "wrong"}, quietLogger())
	if err == nil || !strings.Contains(err.Error(), "Authorization Violation") {
		t.Fatalf("wrong token: err = %v; want an authorization violation", err)
	}
}

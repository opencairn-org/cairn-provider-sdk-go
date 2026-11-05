package oauth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/opencairn-org/cairn-provider-sdk-go/inmem"
	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

// tokenEndpoint is a fake provider token endpoint that records the last
// request and answers per code/refresh token.
type tokenEndpoint struct {
	srv       *httptest.Server
	lastForm  url.Values
	lastAuth  string
	lastReq   *http.Request
	status    int
	body      string
	profileID string
}

func newTokenEndpoint(t *testing.T) *tokenEndpoint {
	t.Helper()
	te := &tokenEndpoint{status: http.StatusOK, profileID: `"u-77"`}
	te.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/profile" {
			if r.Header.Get("Authorization") != "Bearer at" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = io.WriteString(w, `{"user":{"id":`+te.profileID+`}}`)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		te.lastForm, _ = url.ParseQuery(string(raw))
		te.lastAuth = r.Header.Get("Authorization")
		te.lastReq = r
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(te.status)
		body := te.body
		if body == "" {
			body = `{"access_token":"at","refresh_token":"rt2","token_type":"bearer","expires_in":3600,"scope":"read","athlete":{"id":4242}}`
		}
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(te.srv.Close)
	return te
}

func TestRefresher(t *testing.T) {
	ctx := context.Background()
	te := newTokenEndpoint(t)
	r := &Refresher{Endpoint: oauth2.Endpoint{TokenURL: te.srv.URL + "/token", AuthStyle: oauth2.AuthStyleInHeader}}
	cur := port.TokenState{AccessToken: "old", RefreshToken: "rt1", Scope: "old-scope", AppID: "app"}
	creds := workersdk.OAuthClientCreds{ClientID: "cid", ClientSecret: "csec"}

	got, err := r.Refresh(ctx, cur, creds)
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != "at" || got.RefreshToken != "rt2" || got.TokenType != "bearer" || got.Scope != "read" || got.AppID != "app" ||
		time.Until(got.ExpiresAt) < 59*time.Minute {
		t.Errorf("state = %+v", got)
	}
	if te.lastForm.Get("grant_type") != "refresh_token" || te.lastForm.Get("refresh_token") != "rt1" || !strings.HasPrefix(te.lastAuth, "Basic ") {
		t.Errorf("request form=%v auth=%q", te.lastForm, te.lastAuth)
	}

	// Body auth, no rotated refresh token, no expires_in, no scope: carried over.
	r.Endpoint.AuthStyle = oauth2.AuthStyleInParams
	r.DefaultExpiry = 2 * time.Hour
	te.body = `{"access_token":"at2"}`
	got, err = r.Refresh(ctx, cur, creds)
	if err != nil || got.RefreshToken != "rt1" || got.Scope != "old-scope" || got.TokenType != "Bearer" || time.Until(got.ExpiresAt) < 119*time.Minute {
		t.Errorf("carry-over = %+v, %v", got, err)
	}
	if te.lastForm.Get("client_id") != "cid" || te.lastForm.Get("client_secret") != "csec" || te.lastAuth != "" {
		t.Errorf("body auth form=%v auth=%q", te.lastForm, te.lastAuth)
	}
	if ScopeString([]string{"a", "b"}) != "a b" {
		t.Error("ScopeString")
	}
}

func TestRefresher_Errors(t *testing.T) {
	ctx := context.Background()
	te := newTokenEndpoint(t)
	r := &Refresher{Endpoint: oauth2.Endpoint{TokenURL: te.srv.URL + "/token", AuthStyle: oauth2.AuthStyleInParams}}
	cur := port.TokenState{RefreshToken: "rt1"}
	creds := workersdk.OAuthClientCreds{ClientID: "cid", ClientSecret: "csec"}
	var term *port.TerminalError

	if _, err := r.Refresh(ctx, port.TokenState{}, creds); !errors.As(err, &term) || term.Reason != "no_refresh_token" {
		t.Errorf("no refresh token = %v", err)
	}
	if _, err := r.Refresh(ctx, cur, workersdk.OAuthClientCreds{}); !errors.As(err, &term) || term.Reason != "missing_client_credentials" {
		t.Errorf("no creds = %v", err)
	}
	cases := []struct {
		status int
		body   string
		reason string // "" = transient
	}{
		{400, `{"error":"invalid_grant"}`, "invalid_grant"},
		{401, `{"error":"invalid_client"}`, "invalid_client"},
		{400, `nope`, "refresh_rejected"},
		{403, `{}`, "refresh_rejected"},
		{429, `{"error":"rate_limited"}`, ""},
		{502, ``, ""},
	}
	for _, c := range cases {
		te.status, te.body = c.status, c.body
		_, err := r.Refresh(ctx, cur, creds)
		isTerm := errors.As(err, &term)
		switch {
		case err == nil:
			t.Errorf("%d %s: want error", c.status, c.body)
		case c.reason == "" && isTerm:
			t.Errorf("%d %s: %v; want transient", c.status, c.body, err)
		case c.reason != "" && (!isTerm || term.Reason != c.reason):
			t.Errorf("%d %s: %v; want terminal %s", c.status, c.body, err, c.reason)
		}
	}
	// Custom classifier: everything transient.
	r.Classify = func(*oauth2.RetrieveError) string { return "" }
	te.status, te.body = 400, `{"error":"invalid_grant"}`
	if _, err := r.Refresh(ctx, cur, creds); err == nil || errors.As(err, &term) {
		t.Errorf("custom classify = %v; want transient", err)
	}
	r.Endpoint.TokenURL = "http://127.0.0.1:1"
	if _, err := r.Refresh(ctx, cur, creds); err == nil || errors.As(err, &term) {
		t.Errorf("network error = %v; want transient", err)
	}
	if DefaultClassify(&oauth2.RetrieveError{}) != "" {
		t.Error("no response: want transient")
	}
}

func TestExchanger(t *testing.T) {
	ctx := context.Background()
	te := newTokenEndpoint(t)
	ex := &Exchanger{
		Endpoint:    oauth2.Endpoint{TokenURL: te.srv.URL + "/token", AuthStyle: oauth2.AuthStyleInParams},
		ExtraParams: map[string]string{"action": "requesttoken"},
		AccountID:   AccountIDFromExtra("athlete", "id"),
	}
	req := workersdk.OAuthExchangeRequest{ConnectionID: "c1", ClientID: "cid", ClientSecret: "csec", Code: "good", RedirectURI: "https://cairn.test/cb", CodeVerifier: "ver"}
	got := ex.Exchange(ctx, req)
	if got.Error != "" || got.AccessToken != "at" || got.RefreshToken != "rt2" || got.ProviderAccountID != "4242" || got.Scope != "read" ||
		got.TokenType != "bearer" || time.Until(got.ExpiresAt) < 59*time.Minute {
		t.Errorf("reply = %+v", got)
	}
	f := te.lastForm
	if f.Get("grant_type") != "authorization_code" || f.Get("code") != "good" || f.Get("redirect_uri") != "https://cairn.test/cb" ||
		f.Get("code_verifier") != "ver" || f.Get("action") != "requesttoken" || f.Get("client_secret") != "csec" {
		t.Errorf("form = %v", f)
	}

	// Account id from a profile call with the fresh token.
	ex.AccountID = AccountIDFromProfile(te.srv.URL+"/profile", "user", "id")
	if got = ex.Exchange(ctx, req); got.Error != "" || got.ProviderAccountID != "u-77" {
		t.Errorf("profile id = %+v", got)
	}
	te.profileID = "12"
	if got = ex.Exchange(ctx, req); got.ProviderAccountID != "12" {
		t.Errorf("numeric id = %+v", got)
	}
	ex.AccountID = AccountIDFromProfile(te.srv.URL+"/profile", "user", "missing")
	if got = ex.Exchange(ctx, req); got.Error != "transient" {
		t.Errorf("missing profile id = %+v", got)
	}
	ex.AccountID = AccountIDFromProfile("http://127.0.0.1:1/profile", "id")
	if got = ex.Exchange(ctx, req); got.Error != "transient" {
		t.Errorf("unreachable profile = %+v", got)
	}
	ex.AccountID = nil
	if got = ex.Exchange(ctx, req); got.Error != "" || got.ProviderAccountID != "" {
		t.Errorf("no resolver = %+v", got)
	}

	if got = ex.Exchange(ctx, workersdk.OAuthExchangeRequest{Code: "x"}); got.Error != "bad_request" {
		t.Errorf("missing client id = %+v", got)
	}
	te.status, te.body = 400, `{"error":"invalid_grant"}`
	if got = ex.Exchange(ctx, req); got.Error != "exchange_failed" || got.Message != "invalid_grant" {
		t.Errorf("rejected = %+v", got)
	}
	te.status, te.body = 418, `teapot`
	if got = ex.Exchange(ctx, req); got.Error != "exchange_failed" || got.Message != "exchange_failed" {
		t.Errorf("odd 4xx = %+v", got)
	}
	te.status, te.body = 503, ``
	if got = ex.Exchange(ctx, req); got.Error != "transient" {
		t.Errorf("5xx = %+v", got)
	}
	ex.Endpoint.TokenURL = "http://127.0.0.1:1"
	if got = ex.Exchange(ctx, req); got.Error != "transient" {
		t.Errorf("network = %+v", got)
	}
}

func TestAccountIDFromExtra(t *testing.T) {
	ctx := context.Background()
	tok := (&oauth2.Token{}).WithExtra(map[string]any{"userid": float64(12345678901), "athlete": map[string]any{"id": "a1"}, "ok": true})
	for _, c := range []struct {
		path []string
		want string
	}{
		{[]string{"userid"}, "12345678901"},
		{[]string{"athlete", "id"}, "a1"},
		{[]string{"ok"}, "true"},
	} {
		if got, err := AccountIDFromExtra(c.path...)(ctx, tok, nil); err != nil || got != c.want {
			t.Errorf("%v = %q, %v", c.path, got, err)
		}
	}
	for _, path := range [][]string{{}, {"nope"}, {"userid", "x"}, {"athlete", "missing"}} {
		if _, err := AccountIDFromExtra(path...)(ctx, tok, nil); err == nil {
			t.Errorf("%v: want error", path)
		}
	}
	if renderID(int64(5)) != "5" || renderID([]int{}) != "" {
		t.Error("renderID")
	}
}

func TestAccountIDFromProfile_BadReplies(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/500":
			w.WriteHeader(http.StatusInternalServerError)
		case "/garbage":
			_, _ = io.WriteString(w, "{")
		case "/array":
			_, _ = io.WriteString(w, `{"user":[1]}`)
		}
	}))
	defer srv.Close()
	for _, p := range []string{"/500", "/garbage", "/array"} {
		if _, err := AccountIDFromProfile(srv.URL+p, "user", "id")(ctx, nil, srv.Client()); err == nil {
			t.Errorf("%s: want error", p)
		}
	}
	if _, err := AccountIDFromProfile("://bad")(ctx, nil, srv.Client()); err == nil {
		t.Error("bad url: want error")
	}
}

func TestTokenSourceAndClient(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()
	gets := 0
	_, _ = bus.RespondTo(ctx, "cairn.tokens.p.get", func(context.Context, []byte) ([]byte, error) {
		gets++
		return json.Marshal(map[string]any{"access_token": "at", "expires_at": time.Now().Add(time.Hour)})
	})
	w, err := workersdk.New(workersdk.Config{Name: "n", Provider: "p", Bus: bus})
	if err != nil {
		t.Fatal(err)
	}
	src := WorkerTokenSource(ctx, w, "acc")
	tok, err := src.Token()
	if err != nil || tok.AccessToken != "at" || tok.TokenType != "Bearer" || tok.RefreshToken != "" {
		t.Fatalf("token = %+v, %v", tok, err)
	}

	var gotAuth string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
	}))
	defer api.Close()
	client := Client(ctx, w.Tokens(), "acc", &http.Client{Timeout: 3 * time.Second})
	resp, err := client.Get(api.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if gotAuth != "Bearer at" || client.Timeout != 3*time.Second || gets != 1 {
		t.Errorf("auth=%q timeout=%v gets=%d", gotAuth, client.Timeout, gets)
	}
	if Client(ctx, w.Tokens(), "acc", nil) == nil {
		t.Error("nil base")
	}

	// Terminal errors pass through x/oauth2 unchanged.
	_, _ = bus.RespondTo(ctx, "cairn.tokens.p.get", func(context.Context, []byte) ([]byte, error) {
		return json.Marshal(map[string]any{"error": "needs_reauth"})
	})
	_, err = TokenSource(ctx, w.Tokens(), "acc2").Token()
	var term *port.TerminalError
	if !errors.As(err, &term) || term.Reason != "needs_reauth" {
		t.Errorf("err = %v", err)
	}
}

package sdktest

import (
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type tokenRecord struct {
	state      port.TokenState
	creds      workersdk.OAuthClientCreds
	errCode    string
	retryAfter int
}

type credRecord struct{ clientID, clientSecret string }

type lookupRecord struct{ accountID, userID string }

// SetToken sets what cairn.tokens.<p>.get answers for the account: the
// state plus the connection's OAuth app credentials. Stores update it.
func (c *Core) SetToken(accountID string, state port.TokenState, creds workersdk.OAuthClientCreds) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tokens[accountID] = &tokenRecord{state: state, creds: creds}
}

// SetTokenError makes cairn.tokens.<p>.get answer {"error": code} for the
// account ("account_gone", "needs_reauth", "transient" with retryAfter).
func (c *Core) SetTokenError(accountID, code string, retryAfter int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tokens[accountID] = &tokenRecord{errCode: code, retryAfter: retryAfter}
}

// Token returns the account's current state (after any stores).
func (c *Core) Token(accountID string) (port.TokenState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok := c.tokens[accountID]
	if !ok || rec.errCode != "" {
		return port.TokenState{}, false
	}
	return rec.state, true
}

// StoreCalls lists the token stores the worker made, oldest first.
func (c *Core) StoreCalls() []StoreCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]StoreCall(nil), c.stores...)
}

// ReauthCalls lists the needs_reauth calls the worker made.
func (c *Core) ReauthCalls() []ReauthCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ReauthCall(nil), c.reauths...)
}

// SetCreds sets what cairn.creds.<p>.get answers for a credential-provider
// account (client_id / client_secret, the connection's two slots).
func (c *Core) SetCreds(accountID, clientID, clientSecret string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creds[accountID] = credRecord{clientID: clientID, clientSecret: clientSecret}
}

// SetAccount makes cairn.accounts.lookup.<provider>.<pool> resolve the
// provider's owner id (or subscription id) to a Cairn account and user.
func (c *Core) SetAccount(provider, externalID, accountID, userID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookups[provider+":"+externalID] = lookupRecord{accountID: accountID, userID: userID}
}

// installResponders subscribes the fake core RPCs with provider wildcards.
func (c *Core) installResponders(t testing.TB) {
	t.Helper()
	for subject, h := range map[string]nats.MsgHandler{
		"cairn.tokens.*.get":             c.tokenGet,
		"cairn.tokens.*.store":           c.tokenStore,
		"cairn.tokens.*.needs_reauth":    c.tokenNeedsReauth,
		"cairn.creds.*.get":              c.credsGet,
		"cairn.blobs.presign_upload.*":   c.Blobs.presignUpload,
		"cairn.blobs.presign_download.*": c.Blobs.presignDownload,
		"cairn.accounts.lookup.*.*":      c.lookup,
	} {
		sub, err := c.NC.Subscribe(subject, h)
		if err != nil {
			t.Fatalf("sdktest: responder %s: %v", subject, err)
		}
		t.Cleanup(func() { _ = sub.Unsubscribe() })
	}
	if err := c.NC.Flush(); err != nil {
		t.Fatal(err)
	}
}

// providerOf extracts the provider token from core's RPC subjects:
// cairn.tokens.<p>.<op>, cairn.creds.<p>.get, cairn.blobs.presign_*.<p>,
// cairn.accounts.lookup.<p>.<pool>.
func providerOf(subject string) string {
	rest, ok := strings.CutPrefix(subject, "cairn.")
	if !ok {
		return ""
	}
	for _, prefix := range []string{"blobs.presign_upload.", "blobs.presign_download.", "accounts.lookup.", "tokens.", "creds."} {
		if tail, ok := strings.CutPrefix(rest, prefix); ok {
			p, _, _ := strings.Cut(tail, ".")
			return p
		}
	}
	return ""
}

func reply(m *nats.Msg, v any) {
	b, _ := json.Marshal(v)
	_ = m.Respond(b)
}

func (c *Core) tokenGet(m *nats.Msg) {
	var req struct {
		AccountID string `json:"account_id"`
	}
	_ = json.Unmarshal(m.Data, &req)
	c.mu.Lock()
	rec, ok := c.tokens[req.AccountID]
	c.mu.Unlock()
	switch {
	case !ok:
		reply(m, map[string]any{"error": "account_gone"})
	case rec.errCode != "":
		reply(m, map[string]any{"error": rec.errCode, "retry_after": rec.retryAfter})
	default:
		reply(m, map[string]any{
			"access_token": rec.state.AccessToken, "refresh_token": rec.state.RefreshToken, "expires_at": rec.state.ExpiresAt,
			"scope": rec.state.Scope, "token_type": rec.state.TokenType, "app_id": rec.state.AppID,
			"client_id": rec.creds.ClientID, "client_secret": rec.creds.ClientSecret,
		})
	}
}

func (c *Core) tokenStore(m *nats.Msg) {
	var req struct {
		AccountID         string    `json:"account_id"`
		AccessToken       string    `json:"access_token"`
		RefreshToken      string    `json:"refresh_token"`
		ExpiresAt         time.Time `json:"expires_at"`
		Scope             string    `json:"scope"`
		TokenType         string    `json:"token_type"`
		PreviousExpiresAt time.Time `json:"previous_expires_at"`
	}
	if json.Unmarshal(m.Data, &req) != nil {
		reply(m, map[string]any{"ok": false, "error": "bad_request"})
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok := c.tokens[req.AccountID]
	if !ok || rec.errCode == "account_gone" {
		reply(m, map[string]any{"ok": false, "error": "account_gone"})
		return
	}
	state := port.TokenState{AccessToken: req.AccessToken, RefreshToken: req.RefreshToken, ExpiresAt: req.ExpiresAt, Scope: req.Scope, TokenType: req.TokenType, AppID: rec.state.AppID}
	c.stores = append(c.stores, StoreCall{Provider: providerOf(m.Subject), AccountID: req.AccountID, State: state, PreviousExpiresAt: req.PreviousExpiresAt})
	// Core refuses a store that did not start from the current state.
	if !req.PreviousExpiresAt.IsZero() && !rec.state.ExpiresAt.IsZero() && !req.PreviousExpiresAt.Equal(rec.state.ExpiresAt) {
		reply(m, map[string]any{"ok": false, "error": "stale_store"})
		return
	}
	rec.state, rec.errCode = state, ""
	reply(m, map[string]any{"ok": true})
}

func (c *Core) tokenNeedsReauth(m *nats.Msg) {
	var req struct {
		AccountID string `json:"account_id"`
		Reason    string `json:"reason"`
	}
	_ = json.Unmarshal(m.Data, &req)
	c.mu.Lock()
	c.reauths = append(c.reauths, ReauthCall{Provider: providerOf(m.Subject), AccountID: req.AccountID, Reason: req.Reason})
	if rec, ok := c.tokens[req.AccountID]; ok {
		rec.errCode = "needs_reauth"
	}
	c.mu.Unlock()
	reply(m, map[string]any{"ok": true})
}

func (c *Core) credsGet(m *nats.Msg) {
	var req struct {
		AccountID string `json:"account_id"`
	}
	_ = json.Unmarshal(m.Data, &req)
	c.mu.Lock()
	rec, ok := c.creds[req.AccountID]
	c.mu.Unlock()
	if !ok {
		reply(m, map[string]any{"error": "account_gone"})
		return
	}
	reply(m, map[string]any{"client_id": rec.clientID, "client_secret": rec.clientSecret, "email": rec.clientID, "password": rec.clientSecret})
}

func (c *Core) lookup(m *nats.Msg) {
	var req struct {
		Provider       string `json:"provider"`
		ExternalID     string `json:"external_id"`
		SubscriptionID string `json:"subscription_id"`
	}
	_ = json.Unmarshal(m.Data, &req)
	provider := req.Provider
	if provider == "" {
		provider = providerOf(m.Subject)
	}
	c.mu.Lock()
	rec, ok := c.lookups[provider+":"+req.SubscriptionID]
	if !ok || req.SubscriptionID == "" {
		rec, ok = c.lookups[provider+":"+req.ExternalID]
	}
	c.mu.Unlock()
	if !ok {
		reply(m, map[string]any{"error": "not_found"})
		return
	}
	reply(m, map[string]any{"account_id": rec.accountID, "user_id": rec.userID})
}

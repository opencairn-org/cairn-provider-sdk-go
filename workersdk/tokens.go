package workersdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/kvlock"
	"github.com/opencairn-org/cairn-provider-sdk-go/port"
)

// ---------------------------------------------------------------------------
// AuthHandler — provider-specific OAuth refresh logic
//
// The SDK calls Refresh whenever the cached AccessToken is expired (or
// expiring within refreshSkew). The worker's concrete implementation
// knows the provider's refresh endpoint, body shape, and error semantics;
// oauth.Refresher covers the RFC 6749 case.
//
// This is THE only worker-side abstraction the core defines for auth.
// Everything else — endpoints, scopes, client_id/secret, error strings
// — lives in the worker's own package.
//
// Threat-model note: the SDK passes the FULL TokenState (incl. refresh
// token) to Refresh. The worker is trusted to handle it correctly; if
// the worker is compromised, refresh tokens leak. NATS Account isolation
// (cairn-core docs/architecture.md §4) is what prevents cross-provider
// blast radius.
// ---------------------------------------------------------------------------

// AuthHandler is implemented by each provider-specific worker.
//
// Refresh trades the current token state for a fresh one by calling
// the provider's OAuth refresh endpoint. Returns a wrapped
// *port.TerminalError when the provider permanently rejects
// (invalid_grant, expired refresh token, revoked grant); the SDK then
// flags the account needs_reauth on the server and Term()s the
// in-flight job.
//
// Implementations should:
//   - Use the AccessToken's provider for the HTTP call (no provider
//     argument needed; the worker is provider-specific by definition).
//   - Set TokenType="Bearer" on the returned state if the provider
//     doesn't specify.
//   - Preserve Scope unless the provider returned a new one.
//   - Compute ExpiresAt as now + provider's expires_in.
type AuthHandler interface {
	Refresh(ctx context.Context, current port.TokenState, creds OAuthClientCreds) (port.TokenState, error)
}

// OAuthClientCreds is the per-account OAuth app identity (the user's own
// client_id/client_secret) the server returns alongside the token. Provider
// credentials are per-user in Cairn — there is no instance-global app — so the
// worker cannot hold them in env; it receives them per refresh from the server,
// which stores them encrypted (user_provider_configs).
type OAuthClientCreds struct {
	ClientID     string
	ClientSecret string
}

// ---------------------------------------------------------------------------
// Token client — core's cairn.tokens.<provider>.* request/reply
// ---------------------------------------------------------------------------

// TokenClient speaks core's token RPCs for one provider: get, store and
// needs_reauth on cairn.tokens.<provider>.*. The Worker's cache and
// oauth.TokenSource build on it; use it directly only for flows the cache
// does not cover.
type TokenClient struct {
	bus      port.JobBus
	provider string
	timeout  time.Duration
}

// NewTokenClient returns a client for provider's token subjects; timeout
// caps each round-trip (5s when zero).
func NewTokenClient(bus port.JobBus, provider string, timeout time.Duration) *TokenClient {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &TokenClient{bus: bus, provider: provider, timeout: timeout}
}

// Provider is the provider the client is scoped to.
func (c *TokenClient) Provider() string { return c.provider }

// Get fetches the account's current token state and the OAuth app
// credentials of its connection. Errors from core map to
// *port.TerminalError (account_gone, needs_reauth) or
// *port.NakWithDelayError (transient).
func (c *TokenClient) Get(ctx context.Context, accountID string) (port.TokenState, OAuthClientCreds, error) {
	body, _ := json.Marshal(map[string]string{"account_id": accountID})
	subj := "cairn.tokens." + c.provider + ".get"
	resp, err := c.bus.Request(ctx, subj, body, c.timeout)
	if err != nil {
		return port.TokenState{}, OAuthClientCreds{}, fmt.Errorf("token get RPC: %w", err)
	}
	var reply tokenGetReply
	if err := json.Unmarshal(resp, &reply); err != nil {
		return port.TokenState{}, OAuthClientCreds{}, fmt.Errorf("token get decode: %w", err)
	}
	if reply.Error != "" {
		return port.TokenState{}, OAuthClientCreds{}, mapReplyError(reply.Error, reply.RetryAfter)
	}
	state := port.TokenState{
		AccessToken:  reply.AccessToken,
		RefreshToken: reply.RefreshToken,
		ExpiresAt:    reply.ExpiresAt,
		Scope:        reply.Scope,
		TokenType:    reply.TokenType,
		AppID:        reply.AppID,
	}
	creds := OAuthClientCreds{
		ClientID:     reply.ClientID,
		ClientSecret: reply.ClientSecret,
	}
	return state, creds, nil
}

// Store persists a refreshed state. previousExpiresAt is the expiry of the
// state the refresh started from: core refuses the write as stale_store
// when another instance stored a newer state meanwhile, in which case
// Store re-fetches and returns the winning state (nil when ours was
// stored).
func (c *TokenClient) Store(ctx context.Context, accountID string, state port.TokenState, previousExpiresAt time.Time) (*port.TokenState, error) {
	body, _ := json.Marshal(tokenStoreRequest{ //nolint:gosec // G117: the token store RPC exists to carry these tokens
		AccountID:         accountID,
		AccessToken:       state.AccessToken,
		RefreshToken:      state.RefreshToken,
		ExpiresAt:         state.ExpiresAt,
		Scope:             state.Scope,
		TokenType:         state.TokenType,
		PreviousExpiresAt: previousExpiresAt,
	})
	subj := "cairn.tokens." + c.provider + ".store"
	resp, err := c.bus.Request(ctx, subj, body, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("token store RPC: %w", err)
	}
	var reply tokenStoreReply
	if err := json.Unmarshal(resp, &reply); err != nil {
		return nil, fmt.Errorf("token store decode: %w", err)
	}
	if reply.OK {
		return nil, nil
	}
	switch reply.Error {
	case "stale_store":
		st, _, err := c.Get(ctx, accountID)
		if err != nil {
			return nil, err
		}
		return &st, nil
	case "account_gone":
		return nil, &port.TerminalError{Reason: "account_gone", Cause: errors.New("account no longer exists")}
	default:
		return nil, fmt.Errorf("token store error: %s", reply.Error)
	}
}

// MarkNeedsReauth tells core the account's grant is gone for good.
func (c *TokenClient) MarkNeedsReauth(ctx context.Context, accountID, reason string) error {
	body, _ := json.Marshal(map[string]string{
		"account_id": accountID,
		"reason":     reason,
	})
	subj := "cairn.tokens." + c.provider + ".needs_reauth"
	_, err := c.bus.Request(ctx, subj, body, c.timeout)
	return err
}

// ---------------------------------------------------------------------------
// Token cache — worker-driven refresh
// ---------------------------------------------------------------------------

const refreshSkew = 60 * time.Second

// Token is the value handlers receive. Only AccessToken is exposed —
// they shouldn't need the refresh token for normal API calls.
type Token struct {
	AccessToken string
	ExpiresAt   time.Time
	Scope       string
	AppID       string // the provider app the token belongs to (rate-limit scope)
}

// TokenCacheConfig configures a TokenCache.
type TokenCacheConfig struct {
	// Client is required.
	Client *TokenClient
	// Auth refreshes expired states; nil makes the cache read-only (an
	// expired token is an error).
	Auth AuthHandler
	// Lock, when set, makes a refresh happen once per pool: instances
	// contend for lease "refresh.<provider>.<account>" and the losers wait
	// and re-read the state the winner stored. Needed for providers whose
	// refresh tokens are single-use or rotate (Oura, WHOOP, Withings,
	// Wahoo); harmless elsewhere. A lock the KV refuses (missing bucket or
	// permission) is logged once per refresh and skipped.
	Lock *kvlock.Lock
	// Logger is optional.
	Logger *slog.Logger
}

// TokenCache caches the FULL TokenState per account in-process and
// proactively refreshes via the AuthHandler when expiry is near.
//
// Server flow per Get call:
//  1. Cache hit + not expiring? Return.
//  2. Cache miss or expiring? RPC cairn.tokens.<provider>.get to get
//     the latest state from the server.
//  3. Still expiring (or fresh from server but already expired)?
//     Take the pool-wide refresh lock when configured, re-read, then
//     call auth.Refresh(state).
//  4. Refresh OK? RPC cairn.tokens.<provider>.store to persist.
//     On stale_store, re-fetch and use whatever the other worker wrote.
//  5. Refresh permanently failed? RPC cairn.tokens.<provider>.needs_reauth,
//     return *port.TerminalError so the in-flight handler Term()s.
type TokenCache struct {
	client *TokenClient
	auth   AuthHandler // nil → no refresh ability; pure read-through cache
	lock   *kvlock.Lock
	logger *slog.Logger

	mu    sync.Mutex
	cache map[string]*cacheEntry
}

type cacheEntry struct {
	state     port.TokenState
	creds     OAuthClientCreds // per-account OAuth app creds from the last server get
	refreshMu sync.Mutex       // serialises refresh attempts for one account
}

// NewTokenCache constructs a cache; it panics on a nil Client.
func NewTokenCache(cfg TokenCacheConfig) *TokenCache {
	if cfg.Client == nil {
		panic("workersdk: TokenCacheConfig.Client required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &TokenCache{
		client: cfg.Client,
		auth:   cfg.Auth,
		lock:   cfg.Lock,
		logger: cfg.Logger.With("component", "token_cache"),
		cache:  map[string]*cacheEntry{},
	}
}

// Client is the underlying RPC client.
func (c *TokenCache) Client() *TokenClient { return c.client }

// Get returns a valid access token for the account. Refreshes via the
// AuthHandler if needed.
func (c *TokenCache) Get(ctx context.Context, accountID string) (Token, error) {
	entry := c.entryFor(accountID)

	entry.refreshMu.Lock()
	defer entry.refreshMu.Unlock()

	// Fast-path: cached + fresh enough? Return without touching server.
	if hasFresh(entry.state) {
		return tokenFromState(entry.state), nil
	}

	// Pull latest from server (might have been refreshed by another instance).
	// The reply also carries the account's per-user OAuth app creds, which the
	// AuthHandler needs to talk to the provider's token endpoint.
	if err := c.reload(ctx, accountID, entry); err != nil {
		return Token{}, err
	}
	if hasFresh(entry.state) {
		return tokenFromState(entry.state), nil
	}

	// Need to actually refresh.
	if c.auth == nil {
		return Token{}, fmt.Errorf("token cache: no AuthHandler configured for provider %q; cannot refresh", c.client.provider)
	}
	if c.lock == nil {
		return c.refresh(ctx, accountID, entry)
	}
	return c.refreshLocked(ctx, accountID, entry)
}

// reload replaces the entry with the server's current state.
func (c *TokenCache) reload(ctx context.Context, accountID string, entry *cacheEntry) error {
	state, creds, err := c.client.Get(ctx, accountID)
	if err != nil {
		return err
	}
	entry.state = state
	entry.creds = creds
	return nil
}

// refreshLocked refreshes under the pool-wide lease. While another
// instance holds it, this one waits for the lease to end and re-reads: the
// holder's stored token is then fresh and no second refresh happens.
func (c *TokenCache) refreshLocked(ctx context.Context, accountID string, entry *cacheEntry) (Token, error) {
	name := "refresh." + c.client.provider + "." + accountID
	for attempt := 0; ; attempt++ {
		h, err := c.lock.Acquire(ctx, name)
		switch {
		case err == nil:
			return c.refreshHolding(ctx, accountID, entry, h)
		case !errors.Is(err, kvlock.ErrHeld):
			c.logger.Warn("refresh lock unavailable; refreshing without it", "account_id", accountID, "error", err)
			return c.refresh(ctx, accountID, entry)
		}
		if attempt >= 2 {
			return Token{}, fmt.Errorf("token cache: refresh lock for %s stays held: %w", accountID, err)
		}
		if err := c.lock.Wait(ctx, name); err != nil {
			return Token{}, fmt.Errorf("token cache: wait for refresh lock: %w", err)
		}
		if err := c.reload(ctx, accountID, entry); err != nil {
			return Token{}, err
		}
		if hasFresh(entry.state) {
			return tokenFromState(entry.state), nil
		}
	}
}

// refreshHolding re-reads under the lease (the previous holder may just
// have stored a fresh token), refreshes if still needed and releases.
func (c *TokenCache) refreshHolding(ctx context.Context, accountID string, entry *cacheEntry, h *kvlock.Handle) (Token, error) {
	defer func() {
		if err := h.Release(ctx); err != nil {
			c.logger.Debug("refresh lock release failed", "account_id", accountID, "error", err)
		}
	}()
	if err := c.reload(ctx, accountID, entry); err != nil {
		return Token{}, err
	}
	if hasFresh(entry.state) {
		return tokenFromState(entry.state), nil
	}
	return c.refresh(ctx, accountID, entry)
}

// refresh calls the AuthHandler and stores the result.
func (c *TokenCache) refresh(ctx context.Context, accountID string, entry *cacheEntry) (Token, error) {
	state := entry.state
	c.logger.Info("refreshing OAuth token",
		"account_id", accountID,
		"current_expires_at", state.ExpiresAt,
	)
	fresh, err := c.auth.Refresh(ctx, state, entry.creds)
	if err != nil {
		var term *port.TerminalError
		if errors.As(err, &term) {
			_ = c.client.MarkNeedsReauth(ctx, accountID, term.Reason)
			return Token{}, err
		}
		return Token{}, fmt.Errorf("refresh failed: %w", err)
	}

	// Store the new state. On stale_store (another instance won), refetch.
	stored, err := c.client.Store(ctx, accountID, fresh, state.ExpiresAt)
	if err != nil {
		return Token{}, err
	}
	if stored != nil {
		entry.state = *stored
	} else {
		entry.state = fresh
	}
	return tokenFromState(entry.state), nil
}

func (c *TokenCache) entryFor(accountID string) *cacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.cache[accountID]
	if !ok {
		e = &cacheEntry{}
		c.cache[accountID] = e
	}
	return e
}

func hasFresh(s port.TokenState) bool {
	return s.AccessToken != "" && time.Until(s.ExpiresAt) > refreshSkew
}

func tokenFromState(s port.TokenState) Token {
	return Token{
		AccessToken: s.AccessToken,
		ExpiresAt:   s.ExpiresAt,
		Scope:       s.Scope,
		AppID:       s.AppID,
	}
}

func mapReplyError(reason string, retryAfterSeconds int) error {
	switch reason {
	case "account_gone":
		return &port.TerminalError{Reason: "account_gone", Cause: errors.New("account not found")}
	case "needs_reauth":
		return &port.TerminalError{Reason: "needs_reauth", Cause: errors.New("account needs re-authorization")}
	case "transient":
		delay := time.Duration(retryAfterSeconds) * time.Second
		if delay <= 0 {
			delay = 30 * time.Second
		}
		return &port.NakWithDelayError{Reason: "token_fetch_transient", Delay: delay}
	default:
		return fmt.Errorf("token RPC error: %s", reason)
	}
}

// ---------------------------------------------------------------------------
// Wire types — must match the token exchange handler in cairn-core
// ---------------------------------------------------------------------------

type tokenGetReply struct {
	AccessToken  string    `json:"access_token,omitempty"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	ExpiresAt    time.Time `json:"expires_at,omitempty"`
	Scope        string    `json:"scope,omitempty"`
	TokenType    string    `json:"token_type,omitempty"`
	ClientID     string    `json:"client_id,omitempty"`
	ClientSecret string    `json:"client_secret,omitempty"`
	Error        string    `json:"error,omitempty"`
	RetryAfter   int       `json:"retry_after,omitempty"`
	AppID        string    `json:"app_id,omitempty"`
}

type tokenStoreRequest struct {
	AccountID         string    `json:"account_id"`
	AccessToken       string    `json:"access_token"`
	RefreshToken      string    `json:"refresh_token,omitempty"`
	ExpiresAt         time.Time `json:"expires_at"`
	Scope             string    `json:"scope,omitempty"`
	TokenType         string    `json:"token_type,omitempty"`
	PreviousExpiresAt time.Time `json:"previous_expires_at"`
}

type tokenStoreReply struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// Package oauth connects golang.org/x/oauth2 to cairn's token model.
//
// Tokens belong to core: a worker fetches the account's state over
// cairn.tokens.<provider>.get, refreshes it with the connection's own OAuth
// app credentials and stores the result back (workersdk.TokenCache). This
// package adds the three pieces a provider client needs on top of that:
//
//   - TokenSource / Client: an oauth2.TokenSource (and *http.Client) for one
//     account, so generated API clients and x/oauth2-aware libraries work
//     unchanged. Refreshes go through the cache, which serialises them per
//     pool with the KV lease when the worker configured a RefreshLock
//     (workersdk.NewRefreshLock): providers whose refresh tokens are
//     single-use or rotate (Oura, WHOOP, Withings, Wahoo) refresh once,
//     never twice.
//   - Refresher: a workersdk.AuthHandler for RFC 6749 refresh_token grants
//     with Basic or body client auth and an optional classifier.
//   - Exchanger: the worker side of core's authorization-code exchange
//     (cairn.oauth.<provider>.<pool>.exchange): Basic or body auth, PKCE
//     verifier, extra token parameters, a non-standard token endpoint and a
//     hook that resolves the provider's account id.
package oauth

import (
	"context"
	"net/http"
	"time"

	"golang.org/x/oauth2"

	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

// tokenSource adapts a TokenCache to oauth2.TokenSource for one account.
type tokenSource struct {
	ctx       context.Context
	cache     *workersdk.TokenCache
	accountID string
}

// TokenSource returns an oauth2.TokenSource for the account, backed by the
// cache (core's token store plus refresh through the worker's AuthHandler
// and the pool-wide lock). ctx bounds every Token call; use a request
// context per job. Errors are the cache's: a *port.TerminalError for an
// account that needs re-authorization or is gone, a
// *port.NakWithDelayError when core is briefly unavailable — return them
// from the handler as they are.
func TokenSource(ctx context.Context, cache *workersdk.TokenCache, accountID string) oauth2.TokenSource {
	return &tokenSource{ctx: ctx, cache: cache, accountID: accountID}
}

// WorkerTokenSource is TokenSource on the worker's own cache.
func WorkerTokenSource(ctx context.Context, w *workersdk.Worker, accountID string) oauth2.TokenSource {
	return TokenSource(ctx, w.Tokens(), accountID)
}

// Token implements oauth2.TokenSource. The refresh token is not exposed.
func (s *tokenSource) Token() (*oauth2.Token, error) {
	tok, err := s.cache.Get(s.ctx, s.accountID)
	if err != nil {
		return nil, err
	}
	return &oauth2.Token{AccessToken: tok.AccessToken, TokenType: "Bearer", Expiry: tok.ExpiresAt}, nil
}

// Client is an *http.Client that sends the account's bearer token on every
// request (oauth2.NewClient on TokenSource). base, when non-nil, supplies
// the transport and timeout under the OAuth transport.
func Client(ctx context.Context, cache *workersdk.TokenCache, accountID string, base *http.Client) *http.Client {
	if base != nil {
		ctx = context.WithValue(ctx, oauth2.HTTPClient, base)
	}
	c := oauth2.NewClient(ctx, TokenSource(ctx, cache, accountID))
	if base != nil && c.Timeout == 0 {
		c.Timeout = base.Timeout
	}
	return c
}

// withHTTPClient makes x/oauth2 use hc for token-endpoint calls.
func withHTTPClient(ctx context.Context, hc *http.Client) context.Context {
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}
	return context.WithValue(ctx, oauth2.HTTPClient, hc)
}

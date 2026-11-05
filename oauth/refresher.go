package oauth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang.org/x/oauth2"

	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

// Refresher is a workersdk.AuthHandler for providers whose refresh is a
// plain RFC 6749 refresh_token grant: POST Endpoint.TokenURL with
// grant_type=refresh_token, the client credentials as Basic auth or in
// the body (Endpoint.AuthStyle), and a JSON reply with access_token,
// expires_in, an optional rotated refresh_token and scope. Providers with
// a wrapped or otherwise non-standard reply (Withings) implement
// workersdk.AuthHandler themselves.
type Refresher struct {
	// Endpoint names the token URL and how the client authenticates;
	// AuthStyleAutoDetect tries both and remembers what worked.
	Endpoint oauth2.Endpoint
	// HTTPClient performs the request; 15s timeout when nil.
	HTTPClient *http.Client
	// Classify maps a token-endpoint rejection to a terminal reason. nil
	// uses DefaultClassify. Return "" to treat the rejection as transient.
	Classify func(err *oauth2.RetrieveError) string
	// DefaultExpiry is used when the reply carries no expires_in
	// (some providers omit it for non-expiring tokens); 0 = 1 hour.
	DefaultExpiry time.Duration
}

var _ workersdk.AuthHandler = (*Refresher)(nil)

// Refresh implements workersdk.AuthHandler.
func (r *Refresher) Refresh(ctx context.Context, current port.TokenState, creds workersdk.OAuthClientCreds) (port.TokenState, error) {
	if current.RefreshToken == "" {
		return port.TokenState{}, &port.TerminalError{Reason: "no_refresh_token", Cause: errors.New("token state missing refresh_token")}
	}
	if creds.ClientID == "" {
		return port.TokenState{}, &port.TerminalError{Reason: "missing_client_credentials", Cause: errors.New("server returned no OAuth app credentials for account")}
	}
	cfg := oauth2.Config{ClientID: creds.ClientID, ClientSecret: creds.ClientSecret, Endpoint: r.Endpoint}
	// An expired placeholder forces x/oauth2 to run the refresh grant now.
	stale := &oauth2.Token{AccessToken: "expired", RefreshToken: current.RefreshToken, Expiry: time.Unix(1, 0)}
	tok, err := cfg.TokenSource(withHTTPClient(ctx, r.HTTPClient), stale).Token()
	if err != nil {
		return port.TokenState{}, r.mapError(err)
	}
	return tokenState(tok, current, r.DefaultExpiry), nil
}

// mapError turns an x/oauth2 error into the SDK's outcomes.
func (r *Refresher) mapError(err error) error {
	var re *oauth2.RetrieveError
	if !errors.As(err, &re) {
		return fmt.Errorf("oauth refresh: %w", err) // network, decode: transient
	}
	classify := r.Classify
	if classify == nil {
		classify = DefaultClassify
	}
	if reason := classify(re); reason != "" {
		return &port.TerminalError{Reason: reason, Cause: fmt.Errorf("oauth refresh: %w", err)}
	}
	return fmt.Errorf("oauth refresh: %w", err)
}

// DefaultClassify is the RFC 6749 §5.2 reading of a token-endpoint
// rejection: invalid_grant (revoked or expired refresh token),
// invalid_client / unauthorized_client (app misconfigured) and any other
// 400/401 are terminal — the user must re-authorize; 429 and 5xx are
// transient. The returned reason becomes needs_reauth's reason.
func DefaultClassify(err *oauth2.RetrieveError) string {
	switch err.ErrorCode {
	case "invalid_grant", "invalid_client", "unauthorized_client", "invalid_request", "invalid_scope", "unsupported_grant_type":
		return err.ErrorCode
	}
	if err.Response == nil {
		return ""
	}
	switch err.Response.StatusCode {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
		return "refresh_rejected"
	}
	return ""
}

// tokenState converts an x/oauth2 token to the state core stores,
// carrying the previous refresh token and scope forward when the reply
// omits them.
func tokenState(tok *oauth2.Token, current port.TokenState, defaultExpiry time.Duration) port.TokenState {
	out := port.TokenState{
		AccessToken:  tok.AccessToken,
		RefreshToken: tok.RefreshToken,
		ExpiresAt:    tok.Expiry.UTC(),
		Scope:        current.Scope,
		TokenType:    tok.TokenType,
		AppID:        current.AppID,
	}
	if out.RefreshToken == "" {
		out.RefreshToken = current.RefreshToken
	}
	if s, _ := tok.Extra("scope").(string); s != "" {
		out.Scope = s
	}
	if out.TokenType == "" {
		out.TokenType = "Bearer"
	}
	if tok.Expiry.IsZero() {
		if defaultExpiry <= 0 {
			defaultExpiry = time.Hour
		}
		out.ExpiresAt = time.Now().UTC().Add(defaultExpiry)
	}
	return out
}

// ScopeString joins scopes the way most providers return them (space).
func ScopeString(scopes []string) string { return strings.Join(scopes, " ") }

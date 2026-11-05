//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// connectAccount links the provider account the way the connections page
// does: store the app credentials, then either the OAuth round-trip through
// the mock's authorize endpoint or, for credential providers, nothing more
// (core creates the account with the connection).
func (r *providerRun) connectAccount(ctx context.Context, core *coreClient) (account, error) {
	a := r.spec.Auth
	connID, err := core.createConnection(ctx, r.spec.Provider, a.Label, a.ClientID, a.ClientSecret, r.spec.Worker.Name)
	if err != nil {
		return account{}, fmt.Errorf("create connection: %w", err)
	}
	if a.Method == "oauth2" {
		if err := r.oauthRoundTrip(ctx, core, connID); err != nil {
			return account{}, err
		}
	}
	var acct account
	err = waitUntil(ctx, 30*time.Second, "external account", func() (bool, error) {
		acct, err = core.account(ctx, r.spec.Provider)
		return err == nil, nil
	})
	return acct, err
}

// oauthRoundTrip follows the three redirects of the connect flow by hand:
// core → provider authorize (the mock auto-approves) → core callback.
func (r *providerRun) oauthRoundTrip(ctx context.Context, core *coreClient, connID string) error {
	start := "/auth/oauth/" + r.spec.Provider + "/connect?connection=" + url.QueryEscape(connID) + "&worker=" + url.QueryEscape(r.spec.Worker.Name)
	resp, err := core.do(ctx, http.MethodGet, start, nil, nil, http.StatusFound)
	if err != nil {
		return fmt.Errorf("connect start: %w", err)
	}
	authorize := resp.Header.Get("Location")
	if !strings.HasPrefix(authorize, r.mockURL) {
		return fmt.Errorf("connect start redirected to %q, not the mock %s (provider_meta authorize URL?)", authorize, r.mockURL)
	}
	callback, err := followOnce(ctx, authorize)
	if err != nil {
		return fmt.Errorf("mock authorize: %w", err)
	}
	cb, err := url.Parse(callback)
	if err != nil || !strings.HasPrefix(callback, r.stack.CoreURL) || cb.Query().Get("code") == "" {
		return fmt.Errorf("mock authorize redirected to %q; want core's callback with a code", callback)
	}
	resp, err = core.do(ctx, http.MethodGet, cb.RequestURI(), nil, nil, http.StatusFound)
	if err != nil {
		return fmt.Errorf("core callback: %w", err)
	}
	done, _ := url.Parse(resp.Header.Get("Location"))
	if done == nil || done.Query().Get("error") != "" || done.Query().Get("connected") != r.spec.Provider {
		return fmt.Errorf("core callback redirected to %q; want /connections?connected=%s", resp.Header.Get("Location"), r.spec.Provider)
	}
	return nil
}

// followOnce GETs u without following redirects and returns the Location.
func followOnce(ctx context.Context, u string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	c := &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound && resp.StatusCode != http.StatusSeeOther && resp.StatusCode != http.StatusTemporaryRedirect {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	return resp.Header.Get("Location"), nil
}

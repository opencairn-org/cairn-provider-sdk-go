package workersdk

import (
	"context"
	"encoding/json"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/port"
)

// ProviderMeta is what the worker declares about connecting its provider.
// Core stores it from the manifest (provider_meta), bound to the first
// package that published it, and drives the connect form and the authorize
// URL with it; core compiles no provider knowledge in. Field names are the
// wire names core validates (cairn-core internal/domain/provider.go).
type ProviderMeta struct {
	DisplayName  string `json:"display_name"`
	Description  string `json:"description,omitempty"`
	SetupDocsURL string `json:"setup_docs_url,omitempty"`
	// ActivityURLTemplate deep-links the original activity; "{id}" is the
	// provider's external id.
	ActivityURLTemplate string `json:"activity_url_template,omitempty"`
	// AuthMethod is AuthMethodOAuth2 or AuthMethodCredentials.
	AuthMethod string         `json:"auth_method"`
	OAuth      *ProviderOAuth `json:"oauth,omitempty"`
	// ConfigFields label the connection's two credential slots, client_id
	// and client_secret, in the connect form.
	ConfigFields []ProviderField `json:"config_fields,omitempty"`
}

// Auth methods a provider connects with.
const (
	AuthMethodOAuth2      = "oauth2"
	AuthMethodCredentials = "credentials"
)

// ProviderOAuth is the authorize side of an OAuth2 provider; the code
// exchange runs here, in the worker (RespondOAuthExchange).
type ProviderOAuth struct {
	AuthorizeURL string   `json:"authorize_url"`
	Scopes       []string `json:"scopes,omitempty"`
	// ScopeSeparator joins Scopes in the authorize URL; " " unless the
	// provider deviates (Strava uses ",").
	ScopeSeparator string `json:"scope_separator,omitempty"`
	PKCE           bool   `json:"pkce,omitempty"`
	// AuthorizeParams are extra authorize query parameters.
	AuthorizeParams map[string]string `json:"authorize_params,omitempty"`
}

// ProviderField labels one credential slot ("client_id" | "client_secret").
type ProviderField struct {
	Key         string `json:"key"`
	Label       string `json:"label"`
	Help        string `json:"help,omitempty"`
	Secret      bool   `json:"secret,omitempty"`
	Placeholder string `json:"placeholder,omitempty"`
}

// OAuthExchangeRequest is core's request on cairn.oauth.<provider>.<name>.exchange:
// trade the callback's code for tokens with the connection's app credentials.
type OAuthExchangeRequest struct {
	ConnectionID string `json:"connection_id"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Code         string `json:"code"`
	RedirectURI  string `json:"redirect_uri"`
	CodeVerifier string `json:"code_verifier,omitempty"` // PKCE, when the meta declares it
}

// OAuthExchangeReply is the worker's answer: the token state plus the
// provider's account id, or an error ("exchange_failed" | "bad_request" |
// "transient") with a message.
type OAuthExchangeReply struct {
	AccessToken       string    `json:"access_token,omitempty"`
	RefreshToken      string    `json:"refresh_token,omitempty"`
	ExpiresAt         time.Time `json:"expires_at,omitempty"`
	Scope             string    `json:"scope,omitempty"`
	TokenType         string    `json:"token_type,omitempty"`
	ProviderAccountID string    `json:"provider_account_id,omitempty"`
	Error             string    `json:"error,omitempty"`
	Message           string    `json:"message,omitempty"`
}

// OAuthExchangeFunc performs the provider's code exchange.
type OAuthExchangeFunc func(ctx context.Context, req OAuthExchangeRequest) OAuthExchangeReply

// OAuthExchangeSubject is the request/reply subject this pool answers the
// authorization-code exchange on.
func (w *Worker) OAuthExchangeSubject() string {
	return "cairn.oauth." + w.cfg.Provider + "." + w.cfg.Name + ".exchange"
}

// RespondOAuthExchange serves the exchange subject with fn until the
// subscription is closed.
func (w *Worker) RespondOAuthExchange(ctx context.Context, fn OAuthExchangeFunc) (port.Subscription, error) {
	return w.cfg.Bus.RespondTo(ctx, w.OAuthExchangeSubject(), func(ctx context.Context, body []byte) ([]byte, error) {
		var req OAuthExchangeRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return json.Marshal(OAuthExchangeReply{Error: "bad_request", Message: "undecodable request"}) //nolint:gosec // G117: the reply carries the tokens core stores; the bus is the trusted channel
		}
		reply := fn(ctx, req)
		if reply.Error != "" {
			w.logger.Warn("oauth exchange refused", "connection_id", req.ConnectionID, "error", reply.Error, "message", reply.Message)
		}
		return json.Marshal(reply) //nolint:gosec // G117: see above
	})
}

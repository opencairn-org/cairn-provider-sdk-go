package oauth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/oauth2"

	"github.com/opencairn-org/cairn-provider-sdk-go/port"
	"github.com/opencairn-org/cairn-provider-sdk-go/workersdk"
)

// Exchanger answers core's authorization-code exchange
// (workersdk.Worker.RespondOAuthExchange) for an RFC 6749 token endpoint.
// Core supplies the connection's client id and secret, the code, the
// redirect URI and, when the provider meta declares PKCE, the verifier;
// the reply carries the tokens and the provider's account id.
//
//	ex := &oauth.Exchanger{
//	    Endpoint:  oauth2.Endpoint{TokenURL: "https://api.ouraring.com/oauth/token", AuthStyle: oauth2.AuthStyleInHeader},
//	    AccountID: oauth.AccountIDFromProfile("https://api.ouraring.com/v2/usercollection/personal_info", "id"),
//	}
//	sub, err := w.RespondOAuthExchange(ctx, ex.Exchange)
type Exchanger struct {
	// Endpoint names the token URL and how the client authenticates
	// (Basic header or form body).
	Endpoint oauth2.Endpoint
	// ExtraParams are added to the token request (Withings
	// action=requesttoken, Google access_type=offline on the token leg).
	ExtraParams map[string]string
	// AccountID resolves the provider's stable account id from the token
	// reply (AccountIDFromExtra) or with a call using the fresh token
	// (AccountIDFromProfile). nil leaves provider_account_id empty, which
	// core accepts only for providers without webhooks or a one-holder rule.
	AccountID func(ctx context.Context, tok *oauth2.Token, client *http.Client) (string, error)
	// HTTPClient performs the requests; 15s timeout when nil.
	HTTPClient *http.Client
	// Classify maps a rejection to the reply's message; nil uses
	// DefaultClassify (an empty reason reads as "exchange_failed").
	Classify func(err *oauth2.RetrieveError) string
	// DefaultExpiry applies when the reply has no expires_in; 0 = 1 hour.
	DefaultExpiry time.Duration
}

// Exchange is a workersdk.OAuthExchangeFunc.
func (e *Exchanger) Exchange(ctx context.Context, req workersdk.OAuthExchangeRequest) workersdk.OAuthExchangeReply {
	if req.ClientID == "" || req.Code == "" {
		return workersdk.OAuthExchangeReply{Error: "bad_request", Message: "client_id and code are required"}
	}
	cfg := oauth2.Config{ClientID: req.ClientID, ClientSecret: req.ClientSecret, Endpoint: e.Endpoint, RedirectURL: req.RedirectURI}
	opts := make([]oauth2.AuthCodeOption, 0, len(e.ExtraParams)+1)
	if req.CodeVerifier != "" {
		opts = append(opts, oauth2.VerifierOption(req.CodeVerifier))
	}
	for k, v := range e.ExtraParams {
		opts = append(opts, oauth2.SetAuthURLParam(k, v))
	}
	hc := e.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}
	tok, err := cfg.Exchange(withHTTPClient(ctx, hc), req.Code, opts...)
	if err != nil {
		return e.failure(err)
	}
	st := tokenState(tok, port.TokenState{}, e.DefaultExpiry)
	reply := workersdk.OAuthExchangeReply{
		AccessToken: st.AccessToken, RefreshToken: st.RefreshToken, ExpiresAt: st.ExpiresAt,
		Scope: st.Scope, TokenType: st.TokenType,
	}
	if e.AccountID != nil {
		id, err := e.AccountID(ctx, tok, oauth2.NewClient(withHTTPClient(ctx, hc), oauth2.StaticTokenSource(tok)))
		if err != nil {
			return workersdk.OAuthExchangeReply{Error: "transient", Message: "resolve account id: " + err.Error()}
		}
		reply.ProviderAccountID = id
	}
	return reply
}

// failure maps an exchange error to the reply's error class.
func (e *Exchanger) failure(err error) workersdk.OAuthExchangeReply {
	var re *oauth2.RetrieveError
	if !errors.As(err, &re) {
		return workersdk.OAuthExchangeReply{Error: "transient", Message: err.Error()}
	}
	classify := e.Classify
	if classify == nil {
		classify = DefaultClassify
	}
	reason := classify(re)
	if reason == "" && re.Response != nil && re.Response.StatusCode >= 500 {
		return workersdk.OAuthExchangeReply{Error: "transient", Message: err.Error()}
	}
	if reason == "" {
		reason = "exchange_failed"
	}
	return workersdk.OAuthExchangeReply{Error: "exchange_failed", Message: reason}
}

// AccountIDFromExtra reads the account id from the token reply's extra
// fields, following path into nested objects: Strava's athlete id is
// AccountIDFromExtra("athlete", "id"), Withings' userid is
// AccountIDFromExtra("userid"). Numbers are rendered without exponent.
func AccountIDFromExtra(path ...string) func(context.Context, *oauth2.Token, *http.Client) (string, error) {
	return func(_ context.Context, tok *oauth2.Token, _ *http.Client) (string, error) {
		if len(path) == 0 {
			return "", errors.New("oauth: AccountIDFromExtra needs a path")
		}
		id := dig(tok.Extra(path[0]), path[1:])
		if id == "" {
			return "", fmt.Errorf("oauth: token reply has no %v", path)
		}
		return id, nil
	}
}

// AccountIDFromProfile GETs url with the fresh access token and reads the
// id at path in the JSON reply (Oura: personal_info "id"; WHOOP: profile
// "user_id"; Polar v4: "polar-user-id"). A non-2xx reply is an error.
func AccountIDFromProfile(url string, path ...string) func(context.Context, *oauth2.Token, *http.Client) (string, error) {
	return func(ctx context.Context, _ *oauth2.Token, client *http.Client) (string, error) {
		body, err := getJSON(ctx, client, url)
		if err != nil {
			return "", fmt.Errorf("oauth: profile %s: %w", url, err)
		}
		id := dig(body, path)
		if id == "" {
			return "", fmt.Errorf("oauth: profile %s has no %v", url, path)
		}
		return id, nil
	}
}

// getJSON GETs url with Accept: application/json and decodes the object.
func getJSON(ctx context.Context, client *http.Client, url string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var body map[string]any
	if err := decodeJSON(resp, &body); err != nil {
		return nil, err
	}
	return body, nil
}

// dig follows path through nested JSON objects and renders the scalar at
// the end; "" when the path does not resolve.
func dig(v any, path []string) string {
	for _, key := range path {
		m, ok := v.(map[string]any)
		if !ok {
			return ""
		}
		v = m[key]
	}
	return renderID(v)
}

// renderID formats a JSON scalar as an id string.
func renderID(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(x, 10)
	case bool:
		return strconv.FormatBool(x)
	default:
		return ""
	}
}

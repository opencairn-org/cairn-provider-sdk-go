package workersdk

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/inmem"
)

func TestManifestCarriesProviderMeta(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()
	meta := &ProviderMeta{
		DisplayName: "Polar", AuthMethod: AuthMethodOAuth2,
		OAuth:        &ProviderOAuth{AuthorizeURL: "https://auth.example/authorize", Scopes: []string{"a"}, PKCE: true},
		ConfigFields: []ProviderField{{Key: "client_id", Label: "Client ID"}},
	}
	w, err := New(Config{Name: "n", Provider: "p", Package: "pkg", Version: "1.0", Bus: bus, ProviderMeta: meta})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.publishManifest(ctx); err != nil {
		t.Fatal(err)
	}
	kv, _ := bus.KV(kvWorkerManifests)
	e, err := kv.Get(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(e.Value, &got); err != nil {
		t.Fatal(err)
	}
	var back ProviderMeta
	if err := json.Unmarshal(got["provider_meta"], &back); err != nil || back.DisplayName != "Polar" || !back.OAuth.PKCE || back.ConfigFields[0].Key != "client_id" {
		t.Fatalf("provider_meta round trip: %+v %v", back, err)
	}

	// Without meta the key is absent (parser-only pools).
	w2, _ := New(Config{Name: "m", Provider: "p", Bus: bus})
	_ = w2.publishManifest(ctx)
	e, _ = kv.Get(ctx, "m")
	var bare map[string]json.RawMessage
	if json.Unmarshal(e.Value, &bare) != nil || bare["provider_meta"] != nil {
		t.Errorf("provider_meta must be omitted: %s", e.Value)
	}
}

func TestRespondOAuthExchange(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()
	w, err := New(Config{Name: "n", Provider: "p", Bus: bus})
	if err != nil {
		t.Fatal(err)
	}
	if w.OAuthExchangeSubject() != "cairn.oauth.p.n.exchange" {
		t.Fatalf("subject %q", w.OAuthExchangeSubject())
	}
	var seen OAuthExchangeRequest
	sub, err := w.RespondOAuthExchange(ctx, func(_ context.Context, req OAuthExchangeRequest) OAuthExchangeReply {
		seen = req
		if req.Code != "good" {
			return OAuthExchangeReply{Error: "exchange_failed", Message: "invalid_grant"}
		}
		return OAuthExchangeReply{AccessToken: "at", RefreshToken: "rt", ExpiresAt: time.Unix(1900000000, 0).UTC(), ProviderAccountID: "42"}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(ctx) }()

	ask := func(body string) OAuthExchangeReply {
		raw, err := bus.Request(ctx, "cairn.oauth.p.n.exchange", []byte(body), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var r OAuthExchangeReply
		if err := json.Unmarshal(raw, &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := ask(`{"connection_id":"c1","client_id":"id","client_secret":"s","code":"good","redirect_uri":"https://cairn.test/cb","code_verifier":"v"}`)
	if r.AccessToken != "at" || r.ProviderAccountID != "42" || r.ExpiresAt.Unix() != 1900000000 || r.Error != "" {
		t.Errorf("reply %+v", r)
	}
	if seen.ConnectionID != "c1" || seen.ClientSecret != "s" || seen.CodeVerifier != "v" || seen.RedirectURI != "https://cairn.test/cb" {
		t.Errorf("request %+v", seen)
	}
	if r = ask(`{"code":"bad"}`); r.Error != "exchange_failed" || r.Message != "invalid_grant" {
		t.Errorf("refused reply %+v", r)
	}
	if r = ask(`not json`); r.Error != "bad_request" {
		t.Errorf("undecodable reply %+v", r)
	}
}

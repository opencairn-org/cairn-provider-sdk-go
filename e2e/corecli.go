//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

// coreClient talks to core the way the web app does: a session cookie from
// the password login, JSON on /api/*, Connect JSON on /cairn.v1.*.
type coreClient struct {
	base string
	http *http.Client
}

func newCoreClient(base string) *coreClient {
	jar, _ := cookiejar.New(nil)
	return &coreClient{base: base, http: &http.Client{
		Jar:           jar,
		Timeout:       60 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// httpError is a non-2xx reply.
type httpError struct {
	Method, Path string
	Status       int
	Body         string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("%s %s: %d %s", e.Method, e.Path, e.Status, strings.TrimSpace(e.Body))
}

// do sends a request; body is JSON-encoded unless it is nil, a string or a
// url.Values (form). The reply body is decoded into out when non-nil.
func (c *coreClient) do(ctx context.Context, method, path string, body any, out any, want ...int) (*http.Response, error) {
	var rdr io.Reader
	ct := ""
	switch b := body.(type) {
	case nil:
	case url.Values:
		rdr, ct = strings.NewReader(b.Encode()), "application/x-www-form-urlencoded"
	case string:
		rdr, ct = strings.NewReader(b), "application/json"
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			return nil, err
		}
		rdr, ct = bytes.NewReader(raw), "application/json"
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return nil, err
	}
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if !statusWanted(resp.StatusCode, want) {
		return resp, &httpError{Method: method, Path: path, Status: resp.StatusCode, Body: string(raw)}
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp, fmt.Errorf("%s %s: decode: %w: %s", method, path, err, truncate(raw, 300))
		}
	}
	return resp, nil
}

func statusWanted(status int, want []int) bool {
	if len(want) == 0 {
		return status >= 200 && status < 300
	}
	for _, w := range want {
		if status == w {
			return true
		}
	}
	return false
}

func (c *coreClient) get(ctx context.Context, path string, out any) error {
	_, err := c.do(ctx, http.MethodGet, path, nil, out)
	return err
}

func (c *coreClient) post(ctx context.Context, path string, body, out any) error {
	_, err := c.do(ctx, http.MethodPost, path, body, out)
	return err
}

func (c *coreClient) put(ctx context.Context, path string, body, out any) error {
	_, err := c.do(ctx, http.MethodPut, path, body, out)
	return err
}

// connect calls a Connect RPC with the JSON codec (what the SPA sends).
func (c *coreClient) connect(ctx context.Context, service, method string, req, out any) error {
	return c.post(ctx, "/cairn.v1."+service+"/"+method, req, out)
}

// login signs in as the bootstrap admin, replacing the one-time password
// when core asks for it (users.must_change_password on the first login).
func (c *coreClient) login(ctx context.Context, email, bootstrapPassword, password string) error {
	for _, pw := range []string{bootstrapPassword, password} {
		resp, err := c.do(ctx, http.MethodPost, "/auth/password", url.Values{
			"identifier": {email}, "password": {pw}, "redirect": {"/"},
		}, nil, http.StatusSeeOther)
		if err != nil {
			return err
		}
		loc, _ := url.Parse(resp.Header.Get("Location"))
		switch {
		case loc == nil:
			return fmt.Errorf("login: no redirect")
		case loc.Query().Get("set_password") != "":
			return c.post(ctx, "/auth/password/required", map[string]any{
				"challenge_token": loc.Query().Get("set_password"), "password": password,
			}, nil)
		case loc.Query().Get("error") == "invalid_credentials":
			continue // the password was already replaced by an earlier run
		case loc.Query().Get("error") != "":
			return fmt.Errorf("login: %s", loc.Query().Get("error"))
		default:
			return nil
		}
	}
	return fmt.Errorf("login: neither the bootstrap nor the e2e password was accepted")
}

// --- accounts and connections (REST, internal/adapter/primary/rest) ---

type connectionsReply struct {
	Connections []struct {
		ID       string `json:"id"`
		Provider string `json:"provider"`
	} `json:"connections"`
	Providers []struct {
		Provider    string `json:"provider"`
		DisplayName string `json:"display_name"`
		OAuth       bool   `json:"oauth"`
		AuthMethod  string `json:"auth_method"`
		WorkerPools []struct {
			Name       string `json:"name"`
			Release    string `json:"release"`
			Online     bool   `json:"online"`
			Compatible bool   `json:"compatible"`
		} `json:"worker_pools"`
	} `json:"providers"`
}

func (c *coreClient) connections(ctx context.Context) (connectionsReply, error) {
	var out connectionsReply
	err := c.get(ctx, "/api/connections", &out)
	return out, err
}

func (c *coreClient) createConnection(ctx context.Context, provider, label, clientID, clientSecret, worker string) (string, error) {
	var out struct {
		ID string `json:"id"`
	}
	err := c.post(ctx, "/api/connections", map[string]string{
		"provider": provider, "label": label, "client_id": clientID, "client_secret": clientSecret, "worker": worker,
	}, &out)
	return out.ID, err
}

// account is one row of GET /api/accounts.
type account struct {
	ID                string         `json:"id"`
	Provider          string         `json:"provider"`
	ProviderAccountID string         `json:"provider_account_id"`
	Status            string         `json:"status"`
	Imported          int            `json:"imported"`
	ProviderTotal     int            `json:"provider_total"`
	RateLimited       bool           `json:"rate_limited"`
	Discovering       bool           `json:"discovering"`
	AutoImport        bool           `json:"auto_import_enabled"`
	AssignedWorker    string         `json:"assigned_worker"`
	LastSyncAt        *string        `json:"last_sync_at"`
	Watermark         *string        `json:"watermark"`
	Queue             map[string]int `json:"queue"`
	OutOfDate         int            `json:"out_of_date"`
}

func (c *coreClient) accounts(ctx context.Context) ([]account, error) {
	var out struct {
		Accounts []account `json:"accounts"`
	}
	err := c.get(ctx, "/api/accounts", &out)
	return out.Accounts, err
}

// account finds the provider's account (the run links exactly one).
func (c *coreClient) account(ctx context.Context, provider string) (account, error) {
	accts, err := c.accounts(ctx)
	if err != nil {
		return account{}, err
	}
	for _, a := range accts {
		if a.Provider == provider {
			return a, nil
		}
	}
	return account{}, fmt.Errorf("no %s account", provider)
}

type previewReply struct {
	Total          int  `json:"total"`
	AlreadyPresent int  `json:"already_present"`
	New            int  `json:"new"`
	Complete       bool `json:"complete"`
}

func (c *coreClient) syncPreview(ctx context.Context, accountID string) (previewReply, error) {
	var out previewReply
	err := c.post(ctx, "/api/accounts/"+accountID+"/sync/preview", "{}", &out)
	return out, err
}

func (c *coreClient) syncStart(ctx context.Context, accountID string, skipExisting bool) error {
	return c.post(ctx, "/api/accounts/"+accountID+"/sync/start", map[string]bool{"skip_existing": skipExisting}, nil)
}

func (c *coreClient) importOne(ctx context.Context, accountID, extID string) error {
	return c.post(ctx, "/api/accounts/"+accountID+"/import-one", map[string]string{"external_id": extID}, nil)
}

func (c *coreClient) setPollInterval(ctx context.Context, accountID string, seconds int) error {
	return c.put(ctx, "/api/accounts/"+accountID+"/config", map[string]int{"poll_interval_seconds": seconds}, nil)
}

func (c *coreClient) importHealth(ctx context.Context, accountID string, days int) error {
	return c.post(ctx, fmt.Sprintf("/api/accounts/%s/import-health?days=%d", accountID, days), "{}", nil)
}

// --- activities (Connect ActivityService + REST detail routes) ---

// activity is the subset of cairn.v1.Activity the steps look at (protojson names).
type activity struct {
	ID        string           `json:"id"`
	Type      string           `json:"type"`
	Title     string           `json:"title"`
	StartTime string           `json:"startTime"`
	Summary   map[string]any   `json:"summary"`
	Sources   []activitySource `json:"sources"`
	GearID    string           `json:"gearId"`
	GearName  string           `json:"gearName"`
}

// activitySource mirrors cairn.v1.ActivitySource (protojson names).
type activitySource struct {
	ID                  string `json:"id"`
	Provider            string `json:"provider"`
	ExternalID          string `json:"externalId"`
	SourceWorkerName    string `json:"sourceWorkerName"`
	SourceWorkerVersion string `json:"sourceWorkerVersion"`
	SourceWorkerPackage string `json:"sourceWorkerPackage"`
	RawBlobID           string `json:"rawBlobId"`
	Status              string `json:"status"`
	ReimportStatus      string `json:"reimportStatus"`
	ImportedAt          string `json:"importedAt"`
	LastReimportedAt    string `json:"lastReimportedAt"`
}

// listActivities lists the caller's activities with at least one source from provider.
func (c *coreClient) listActivities(ctx context.Context, provider string) ([]activity, error) {
	var out struct {
		Activities []activity `json:"activities"`
	}
	req := map[string]any{"providers": []string{provider}, "includeSources": true, "page": map[string]any{"pageSize": 200}}
	err := c.connect(ctx, "ActivityService", "ListActivities", req, &out)
	return out.Activities, err
}

func (c *coreClient) listSources(ctx context.Context, activityID string) ([]activitySource, error) {
	var out struct {
		Sources []activitySource `json:"sources"`
	}
	err := c.connect(ctx, "ActivityService", "ListActivitySources", map[string]string{"activityId": activityID}, &out)
	return out.Sources, err
}

func (c *coreClient) reimportSource(ctx context.Context, activityID, sourceID, mode string) error {
	return c.post(ctx, "/api/activities/"+activityID+"/sources/"+sourceID+"/reimport", map[string]string{"mode": mode}, nil)
}

func (c *coreClient) laps(ctx context.Context, activityID string) ([]map[string]any, error) {
	var out struct {
		Laps []map[string]any `json:"laps"`
	}
	err := c.get(ctx, "/api/activities/"+activityID+"/laps", &out)
	return out.Laps, err
}

// sourceStream returns the channels and sample count the worker delivered
// for one source (GetActivityStream at full resolution).
func (c *coreClient) sourceStream(ctx context.Context, sourceID string) ([]string, int, error) {
	var out struct {
		Stream struct {
			Channels    []string `json:"channels"`
			SampleCount int      `json:"sampleCount"`
		} `json:"stream"`
	}
	err := c.connect(ctx, "ActivityService", "GetActivityStream", map[string]any{"activitySourceId": sourceID, "maxResolutionHz": 0}, &out)
	return out.Stream.Channels, out.Stream.SampleCount, err
}

// mergedStream returns the channel names present in the merged stream.
func (c *coreClient) mergedStream(ctx context.Context, activityID string) ([]string, error) {
	var out struct {
		Grid     []any          `json:"grid"`
		Channels map[string]any `json:"channels"`
	}
	if err := c.get(ctx, "/api/activities/"+activityID+"/merged-stream", &out); err != nil {
		return nil, err
	}
	var names []string
	for k := range out.Channels {
		names = append(names, k)
	}
	return names, nil
}

// gear is the subset of cairn.v1.Gear the steps look at.
type gear struct {
	ID           string `json:"id"`
	Kind         string `json:"kind"`
	Name         string `json:"name"`
	Brand        string `json:"brand"`
	Retired      bool   `json:"retired"`
	ExternalRefs []struct {
		Provider string `json:"provider"`
	} `json:"externalRefs"`
}

// gearRef is a provider gear record (cairn.v1.GearExternalRef); pending while gearId is empty.
type gearRef struct {
	Provider          string `json:"provider"`
	ExternalAccountID string `json:"externalAccountId"`
	ExternalID        string `json:"externalId"`
	ProviderName      string `json:"providerName"`
	GearID            string `json:"gearId"`
	Kind              string `json:"kind"`
	Brand             string `json:"brand"`
}

// listPendingGearRefs lists provider gear waiting for the user's decision.
func (c *coreClient) listPendingGearRefs(ctx context.Context) ([]gearRef, error) {
	var out struct {
		Refs []gearRef `json:"refs"`
	}
	err := c.connect(ctx, "GearService", "ListPendingGearRefs", map[string]any{}, &out)
	return out.Refs, err
}

// createGearFromRef decides "new gear" for a pending ref and returns the gear.
func (c *coreClient) createGearFromRef(ctx context.Context, ref gearRef) (gear, error) {
	var out struct {
		Gear gear `json:"gear"`
	}
	err := c.connect(ctx, "GearService", "CreateGearFromRef", map[string]string{
		"provider": ref.Provider, "externalAccountId": ref.ExternalAccountID, "externalId": ref.ExternalID,
	}, &out)
	return out.Gear, err
}

func (c *coreClient) listGear(ctx context.Context) ([]gear, error) {
	var out struct {
		Gear []gear `json:"gear"`
	}
	err := c.connect(ctx, "GearService", "ListGear", map[string]any{"includeRetired": true}, &out)
	return out.Gear, err
}

// metrics lists stored metric samples for keys.
// healthSeries is core's merged daily series for one data type; worker METRIC
// events land in health_samples, which /api/metrics does not read.
func (c *coreClient) healthSeries(ctx context.Context, dataType string, days int) ([]map[string]any, error) {
	var out struct {
		Series []map[string]any `json:"series"`
	}
	err := c.get(ctx, fmt.Sprintf("/api/health?type=%s&days=%d", url.QueryEscape(dataType), days), &out)
	return out.Series, err
}

// --- admin fleet ---

type adminProvider struct {
	Provider    string `json:"provider"`
	Package     string `json:"package"`
	DisplayName string `json:"display_name"`
	AuthMethod  string `json:"auth_method"`
	OAuth       bool   `json:"oauth"`
}

func (c *coreClient) adminProviders(ctx context.Context) ([]adminProvider, error) {
	var out struct {
		Providers []adminProvider `json:"providers"`
	}
	err := c.get(ctx, "/api/admin/providers", &out)
	return out.Providers, err
}

type adminWorker struct {
	WorkerName  string `json:"worker_name"`
	WorkerKey   string `json:"worker_key"`
	Provider    string `json:"provider"`
	Release     string `json:"release"`
	CoreRelease string `json:"core_release"`
	// Compatible is core's release check; Known that both releases parsed.
	Compatible bool   `json:"release_compatible"`
	Known      bool   `json:"release_known"`
	LastSeen   string `json:"last_seen"`
}

func (c *coreClient) adminWorkers(ctx context.Context) ([]adminWorker, error) {
	var out struct {
		Workers []adminWorker `json:"workers"`
	}
	err := c.get(ctx, "/api/admin/workers", &out)
	return out.Workers, err
}

// webhookVerify performs the provider's GET handshake through core.
func (c *coreClient) webhookVerify(ctx context.Context, workerKey string, query map[string]string) (int, string, error) {
	q := url.Values{}
	for k, v := range query {
		q.Set(k, v)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/webhooks/"+workerKey+"?"+q.Encode(), nil)
	if err != nil {
		return 0, "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, string(raw), nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

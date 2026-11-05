package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/port"
)

// Typed client errors, so handlers can map them onto the SDK's outcomes.
var (
	errUnauthorized = errors.New("acme: 401 unauthorized")
	errNotFound     = errors.New("acme: 404 not found")
)

// rateLimitError is a 429; RetryAfter comes from the header when present.
type rateLimitError struct{ RetryAfter time.Duration }

func (e *rateLimitError) Error() string { return "acme: 429 rate limited" }

// acmeActivity is the list/detail shape.
type acmeActivity struct {
	ID    string    `json:"id"`
	Name  string    `json:"name"`
	Start time.Time `json:"start_time"`
}

type acmeMetric struct {
	Key   string    `json:"key"`
	TS    time.Time `json:"ts"`
	Value float64   `json:"value"`
}

// acmeClient is the read-only API client; the access token comes per call
// from the worker's token cache.
type acmeClient struct {
	base string
	http *http.Client
}

func newClient(base string) *acmeClient {
	return &acmeClient{base: base, http: &http.Client{Timeout: 30 * time.Second}}
}

func (c *acmeClient) list(ctx context.Context, token string, after int64, page, perPage int) ([]acmeActivity, error) {
	q := url.Values{"after": {strconv.FormatInt(after, 10)}, "page": {strconv.Itoa(page)}, "per_page": {strconv.Itoa(perPage)}}
	var out []acmeActivity
	return out, c.getJSON(ctx, token, "/v1/activities?"+q.Encode(), &out)
}

func (c *acmeClient) activity(ctx context.Context, token, id string) (acmeActivity, error) {
	var out acmeActivity
	return out, c.getJSON(ctx, token, "/v1/activities/"+url.PathEscape(id), &out)
}

func (c *acmeClient) file(ctx context.Context, token, id string) ([]byte, error) {
	resp, err := c.get(ctx, token, "/v1/activities/"+url.PathEscape(id)+"/file.fit")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(io.LimitReader(resp.Body, 32<<20))
}

func (c *acmeClient) metrics(ctx context.Context, token string, since, until time.Time) ([]acmeMetric, error) {
	q := url.Values{"since": {strconv.FormatInt(since.Unix(), 10)}, "until": {strconv.FormatInt(until.Unix(), 10)}}
	var out []acmeMetric
	return out, c.getJSON(ctx, token, "/v1/metrics?"+q.Encode(), &out)
}

func (c *acmeClient) getJSON(ctx context.Context, token, path string, v any) error {
	resp, err := c.get(ctx, token, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(v); err != nil {
		return fmt.Errorf("acme: decode %s: %w", path, err)
	}
	return nil
}

// get issues an authenticated GET and classifies non-2xx replies.
func (c *acmeClient) get(ctx context.Context, token, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("acme: %w", err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return nil, errUnauthorized
	case http.StatusNotFound:
		return nil, errNotFound
	case http.StatusTooManyRequests:
		secs, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
		return nil, &rateLimitError{RetryAfter: time.Duration(secs) * time.Second}
	}
	return nil, fmt.Errorf("acme: %s: status %d", path, resp.StatusCode)
}

// mapErr translates a client error into the SDK's three outcomes:
// terminal (Term), rate-limited (NAK with delay) or transient (NAK).
func mapErr(err error, op string) error {
	var rl *rateLimitError
	switch {
	case errors.Is(err, errUnauthorized):
		return &port.TerminalError{Reason: "needs_reauth", Cause: err}
	case errors.Is(err, errNotFound):
		return &port.TerminalError{Reason: "not_found", Cause: err}
	case errors.As(err, &rl):
		return &port.NakWithDelayError{Reason: "rate_limited", Delay: max(rl.RetryAfter, time.Second), Cause: err}
	default:
		return fmt.Errorf("acme %s: %w", op, err)
	}
}

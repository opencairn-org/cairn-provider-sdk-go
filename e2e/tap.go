//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"
)

// apiTap is a reverse proxy between the worker and the mock: the worker's
// ${MOCK_URL} points at it, so the harness sees when each provider call
// started and ended without any help from the mock.
type apiTap struct {
	srv *http.Server
	url string

	mu    sync.Mutex
	calls []tapCall
}

// tapCall is one proxied request.
type tapCall struct {
	Method, Path string
	Status       int
	Start, End   time.Time
}

// startTap listens on a free loopback port and forwards to target.
func startTap(target string) (*apiTap, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	t := &apiTap{url: "http://" + ln.Addr().String()}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.FlushInterval = -1 // streamed replies (SSE) pass straight through
	t.srv = &http.Server{ReadHeaderTimeout: 10 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rec := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		proxy.ServeHTTP(rec, req)
		t.mu.Lock()
		t.calls = append(t.calls, tapCall{Method: req.Method, Path: req.URL.Path, Status: rec.status, Start: start, End: time.Now()})
		t.mu.Unlock()
	})}
	go func() { _ = t.srv.Serve(ln) }()
	return t, nil
}

func (t *apiTap) stop() {
	if t != nil {
		_ = t.srv.Shutdown(context.Background())
	}
}

// mark is a cursor into the recorded calls.
func (t *apiTap) mark() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.calls)
}

// since returns the calls recorded after mark, in completion order.
func (t *apiTap) since(mark int) []tapCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]tapCall(nil), t.calls[mark:]...)
}

// statusWriter remembers the status the mock answered with.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// earlyRetry checks the provider calls after the harness armed one failure:
// the first call to start got it; no call may start before retryAfter has
// passed since that reply. Calls already in flight when it came back are
// concurrent, not retries. It describes what it saw either way.
func earlyRetry(calls []tapCall, retryAfter time.Duration) (string, error) {
	if len(calls) == 0 {
		return "", errors.New("the worker made no provider call after the 429 was armed")
	}
	failed := calls[0]
	for _, c := range calls[1:] {
		if c.Start.Before(failed.Start) {
			failed = c
		}
	}
	var next *tapCall
	for i, c := range calls {
		if c.Start.After(failed.End) && (next == nil || c.Start.Before(next.Start)) {
			next = &calls[i]
		}
	}
	seen := fmt.Sprintf("%s %s answered %d", failed.Method, failed.Path, failed.Status)
	if next == nil {
		return seen + ", no provider call after it", nil
	}
	gap := next.Start.Sub(failed.End)
	seen += fmt.Sprintf(", next call %s %s %s later", next.Method, next.Path, gap.Round(time.Millisecond))
	if gap < retryAfter {
		return seen, fmt.Errorf("%s: inside the Retry-After of %s, the worker retried without waiting", seen, retryAfter)
	}
	return seen, nil
}

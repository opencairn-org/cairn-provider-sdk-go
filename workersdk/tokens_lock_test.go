package workersdk

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/inmem"
	"github.com/opencairn-org/cairn-provider-sdk-go/kvlock"
	"github.com/opencairn-org/cairn-provider-sdk-go/natsbus"
	"github.com/opencairn-org/cairn-provider-sdk-go/port"
)

// tokenServer is a fake core token store shared by several worker instances.
type tokenServer struct {
	mu     sync.Mutex
	state  port.TokenState
	stores int
}

func (s *tokenServer) install(t *testing.T, bus *inmem.Bus) {
	t.Helper()
	ctx := context.Background()
	_, _ = bus.RespondTo(ctx, "cairn.tokens.p.get", func(context.Context, []byte) ([]byte, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		return json.Marshal(tokenGetReply{AccessToken: s.state.AccessToken, RefreshToken: s.state.RefreshToken, ExpiresAt: s.state.ExpiresAt})
	})
	_, _ = bus.RespondTo(ctx, "cairn.tokens.p.store", func(_ context.Context, body []byte) ([]byte, error) {
		var req tokenStoreRequest
		_ = json.Unmarshal(body, &req)
		s.mu.Lock()
		defer s.mu.Unlock()
		if !req.PreviousExpiresAt.Equal(s.state.ExpiresAt) {
			return json.Marshal(tokenStoreReply{Error: "stale_store"})
		}
		s.stores++
		s.state = port.TokenState{AccessToken: req.AccessToken, RefreshToken: req.RefreshToken, ExpiresAt: req.ExpiresAt}
		return json.Marshal(tokenStoreReply{OK: true})
	})
}

// slowAuth refreshes after a delay and counts calls; single-use refresh tokens.
type slowAuth struct {
	mu    sync.Mutex
	delay time.Duration
	calls int
	used  map[string]bool
}

func (a *slowAuth) Refresh(_ context.Context, cur port.TokenState, _ OAuthClientCreds) (port.TokenState, error) {
	a.mu.Lock()
	a.calls++
	if a.used == nil {
		a.used = map[string]bool{}
	}
	if a.used[cur.RefreshToken] {
		a.mu.Unlock()
		return port.TokenState{}, &port.TerminalError{Reason: "invalid_grant"}
	}
	a.used[cur.RefreshToken] = true
	a.mu.Unlock()
	time.Sleep(a.delay)
	return port.TokenState{AccessToken: "new-" + cur.RefreshToken, RefreshToken: cur.RefreshToken + "x", ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func TestTokenCache_RefreshLock_OnePoolWideRefresh(t *testing.T) {
	bus := inmem.New()
	srv := &tokenServer{state: port.TokenState{AccessToken: "old", RefreshToken: "r1", ExpiresAt: time.Now().Add(-time.Minute)}}
	srv.install(t, bus)
	auth := &slowAuth{delay: 30 * time.Millisecond}
	kv, _ := bus.KV(KVRateLimits)

	const n = 4
	tokens := make([]Token, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lock := &kvlock.Lock{KV: kv, Owner: "i" + string(rune('0'+i)), TTL: time.Minute, PollInterval: time.Millisecond}
			w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus, Auth: auth, RefreshLock: lock})
			tokens[i], errs[i] = w.FetchToken(context.Background(), "acc")
		}(i)
	}
	wg.Wait()
	for i := range tokens {
		if errs[i] != nil || tokens[i].AccessToken != "new-r1" {
			t.Errorf("instance %d: %+v, %v", i, tokens[i], errs[i])
		}
	}
	if auth.calls != 1 || srv.stores != 1 {
		t.Errorf("refreshes = %d, stores = %d; want exactly one (single-use refresh token)", auth.calls, srv.stores)
	}
	if _, err := kv.Get(context.Background(), "lock.refresh.p.acc"); err != nil {
		t.Errorf("lease key not in the rate-limit bucket: %v", err)
	}
}

func TestTokenCache_RefreshLock_Fallbacks(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()
	srv := &tokenServer{state: port.TokenState{AccessToken: "old", RefreshToken: "r1", ExpiresAt: time.Now().Add(-time.Minute)}}
	srv.install(t, bus)

	// A KV that cannot hold the lease: refresh runs without it.
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus, Auth: &slowAuth{}, RefreshLock: &kvlock.Lock{KV: failingKV{}}, Logger: quietLogger()})
	if tok, err := w.FetchToken(ctx, "acc"); err != nil || tok.AccessToken != "new-r1" {
		t.Fatalf("unlocked fallback = %+v, %v", tok, err)
	}

	// A lease that never ends: give up after waiting, with a clear error.
	srv.state = port.TokenState{AccessToken: "old2", RefreshToken: "r2", ExpiresAt: time.Now().Add(-time.Minute)}
	kv, _ := bus.KV(KVRateLimits)
	holder := &kvlock.Lock{KV: kv, Owner: "other", TTL: time.Hour}
	if _, err := holder.Acquire(ctx, "refresh.p.acc"); err != nil {
		t.Fatal(err)
	}
	stuck := &kvlock.Lock{KV: kv, Owner: "me", TTL: time.Hour, PollInterval: time.Millisecond}
	w = mustNew(t, Config{Name: "n", Provider: "p", Bus: bus, Auth: &slowAuth{}, RefreshLock: stuck, Logger: quietLogger()})
	wctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := w.FetchToken(wctx, "acc"); err == nil || !strings.Contains(err.Error(), "refresh lock") {
		t.Fatalf("held forever = %v", err)
	}
}

func TestTokenCache_RefreshLock_WaiterReusesStoredToken(t *testing.T) {
	ctx := context.Background()
	bus := inmem.New()
	srv := &tokenServer{state: port.TokenState{AccessToken: "old", RefreshToken: "r1", ExpiresAt: time.Now().Add(-time.Minute)}}
	srv.install(t, bus)
	kv, _ := bus.KV(KVRateLimits)
	lock := &kvlock.Lock{KV: kv, Owner: "a", TTL: time.Minute, PollInterval: time.Millisecond}

	// Another instance holds the lease and stores a fresh token before releasing.
	h, err := lock.Acquire(ctx, "refresh.p.acc")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		srv.mu.Lock()
		srv.state = port.TokenState{AccessToken: "theirs", RefreshToken: "r2", ExpiresAt: time.Now().Add(time.Hour)}
		srv.mu.Unlock()
		_ = h.Release(ctx)
	}()
	auth := &slowAuth{}
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus, Auth: auth, RefreshLock: lock})
	tok, err := w.FetchToken(ctx, "acc")
	if err != nil || tok.AccessToken != "theirs" || auth.calls != 0 {
		t.Fatalf("waiter = %+v, %v (refreshes %d); want the holder's token without refreshing", tok, err, auth.calls)
	}

	// Terminal refresh under the lock still marks needs_reauth and releases.
	srv.state = port.TokenState{AccessToken: "old", RefreshToken: "r1", ExpiresAt: time.Now().Add(-time.Minute)}
	reauth := 0
	_, _ = bus.RespondTo(ctx, "cairn.tokens.p.needs_reauth", func(context.Context, []byte) ([]byte, error) {
		reauth++
		return []byte(`{"ok":true}`), nil
	})
	auth.used = map[string]bool{"r1": true}
	w2 := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus, Auth: auth, RefreshLock: lock})
	var term *port.TerminalError
	if _, err := w2.FetchToken(ctx, "acc"); !errors.As(err, &term) || reauth != 1 {
		t.Fatalf("terminal under lock = %v (reauth %d)", err, reauth)
	}
	if _, err := lock.Acquire(ctx, "refresh.p.acc"); err != nil {
		t.Fatalf("lease not released after a failed refresh: %v", err)
	}
}

// failingKV errors on every call.
type failingKV struct{}

func (failingKV) Get(context.Context, string) (port.KVEntry, error) {
	return port.KVEntry{}, errors.New("kv down")
}

func (failingKV) Put(context.Context, string, []byte) (uint64, error) {
	return 0, errors.New("kv down")
}

func (failingKV) CompareAndSet(context.Context, string, []byte, uint64) (uint64, bool, error) {
	return 0, false, errors.New("kv down")
}

func TestNewRateLimiterAndRefreshLock(t *testing.T) {
	bus := inmem.New()
	if NewRateLimiter(bus, nil, map[string]natsbus.Limit{"p:n:short": {Capacity: 1}}) == nil || NewRefreshLock(bus, nil, "i", 0) == nil {
		t.Error("want a limiter and a lock when the bucket exists")
	}
	kvless := kvlessBus{newPullBus()}
	if NewRateLimiter(kvless, quietLogger(), nil) != nil || NewRefreshLock(kvless, quietLogger(), "i", time.Second) != nil {
		t.Error("want nil without the bucket")
	}
	if NewTokenClient(bus, "p", 0).timeout != 5*time.Second || NewTokenClient(bus, "p", 0).Provider() != "p" {
		t.Error("token client defaults")
	}
	w := mustNew(t, Config{Name: "n", Provider: "p", Bus: bus, Version: "1.0", Package: "pkg", Release: "v1"})
	if w.Tokens() == nil || w.Tokens().Client() == nil || w.Provider() != "p" || w.Version() != "1.0" || w.Package() != "pkg" || w.Release() != "v1" {
		t.Error("accessors")
	}
	defer func() {
		if recover() == nil {
			t.Error("NewTokenCache without a client must panic")
		}
	}()
	NewTokenCache(TokenCacheConfig{})
}

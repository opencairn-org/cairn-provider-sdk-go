package kvlock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/inmem"
	"github.com/opencairn-org/cairn-provider-sdk-go/port"
)

func newLock(t *testing.T) (*Lock, port.KV) {
	t.Helper()
	kv, err := inmem.New().KV("cairn_rate_limits")
	if err != nil {
		t.Fatal(err)
	}
	return &Lock{KV: kv, Owner: "a", TTL: time.Minute, PollInterval: time.Millisecond}, kv
}

func TestAcquireReleaseAndHeld(t *testing.T) {
	ctx := context.Background()
	a, kv := newLock(t)
	b := &Lock{KV: kv, Owner: "b", TTL: time.Minute}

	h, err := a.Acquire(ctx, "refresh.p.acct")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Get(ctx, "lock.refresh.p.acct"); err != nil {
		t.Fatalf("lease key missing: %v", err)
	}
	_, err = b.Acquire(ctx, "refresh.p.acct")
	var held *HeldError
	if !errors.As(err, &held) || !errors.Is(err, ErrHeld) || held.Owner != "a" || held.Until.IsZero() || held.Error() == "" {
		t.Fatalf("second acquire = %v; want held by a", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Acquire(ctx, "refresh.p.acct"); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestExpiredLeaseIsTakenOver(t *testing.T) {
	ctx := context.Background()
	a, kv := newLock(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return now }
	h, err := a.Acquire(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	b := &Lock{KV: kv, Owner: "b", now: func() time.Time { return now.Add(2 * time.Minute) }}
	if _, err := b.Acquire(ctx, "x"); err != nil {
		t.Fatalf("takeover of an expired lease: %v", err)
	}
	// The stale holder's release loses its CAS and reports nothing.
	if err := h.Release(ctx); err != nil {
		t.Fatalf("stale release: %v", err)
	}
	if _, err := a.Acquire(ctx, "x"); !errors.Is(err, ErrHeld) {
		t.Fatalf("b's lease must survive a's stale release: %v", err)
	}
}

func TestCorruptEntryIsFree(t *testing.T) {
	ctx := context.Background()
	a, kv := newLock(t)
	if _, err := kv.Put(ctx, "lock.x", []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Acquire(ctx, "x"); err != nil {
		t.Fatalf("corrupt entry must not pin the lock: %v", err)
	}
}

func TestDoSerialisesHolders(t *testing.T) {
	ctx := context.Background()
	a, kv := newLock(t)
	var mu sync.Mutex
	var inside, maxInside, runs int
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l := &Lock{KV: kv, Owner: "w", TTL: time.Minute, PollInterval: time.Millisecond}
			if i == 0 {
				l = a
			}
			err := l.Do(ctx, "job", func(context.Context) error {
				mu.Lock()
				inside++
				maxInside = max(maxInside, inside)
				runs++
				mu.Unlock()
				time.Sleep(2 * time.Millisecond)
				mu.Lock()
				inside--
				mu.Unlock()
				return nil
			})
			if err != nil {
				t.Errorf("Do: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if maxInside != 1 || runs != 8 {
		t.Fatalf("maxInside=%d runs=%d; want 1 and 8", maxInside, runs)
	}
	want := errors.New("boom")
	if err := a.Do(ctx, "job", func(context.Context) error { return want }); !errors.Is(err, want) {
		t.Fatalf("Do must return fn's error: %v", err)
	}
}

func TestWaitStopsWithContext(t *testing.T) {
	a, _ := newLock(t)
	if _, err := a.Acquire(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := a.Wait(ctx, "x"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait = %v; want deadline", err)
	}
	if err := a.Do(ctx, "x", func(context.Context) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do = %v; want deadline", err)
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

// casFailKV reads free but fails every write.
type casFailKV struct{ failingKV }

func (casFailKV) Get(context.Context, string) (port.KVEntry, error) {
	return port.KVEntry{}, port.ErrKVKeyNotFound
}

// casLoseKV reads free and loses every CAS.
type casLoseKV struct{ casFailKV }

func (casLoseKV) CompareAndSet(context.Context, string, []byte, uint64) (uint64, bool, error) {
	return 0, false, nil
}

func TestKVErrorsSurface(t *testing.T) {
	ctx := context.Background()
	l := &Lock{KV: failingKV{}}
	if _, err := l.Acquire(ctx, "x"); err == nil {
		t.Error("acquire: want read error")
	}
	if err := l.Wait(ctx, "x"); err == nil {
		t.Error("wait: want read error")
	}
	if err := l.Do(ctx, "x", nil); err == nil {
		t.Error("do: want read error")
	}
	l = &Lock{KV: casFailKV{}}
	if _, err := l.Acquire(ctx, "x"); err == nil {
		t.Error("acquire: want cas error")
	}
	if err := (&Handle{lock: l, key: "k"}).Release(ctx); err == nil {
		t.Error("release: want cas error")
	}
	l = &Lock{KV: casLoseKV{}}
	if _, err := l.Acquire(ctx, "x"); !errors.Is(err, ErrHeld) {
		t.Errorf("lost cas = %v; want held", err)
	}
	if l.key("n") != "lock.n" || (&Lock{Prefix: "p."}).key("n") != "p.n" || l.ttl() != 30*time.Second {
		t.Error("defaults")
	}
}

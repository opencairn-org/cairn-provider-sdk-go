// Package kvlock is a lease-style mutex over a NATS KV bucket, built on
// compare-and-set, for work that must run once per worker pool rather than
// once per instance: refreshing a single-use OAuth refresh token, renewing a
// provider webhook subscription, running a nightly cleanup.
//
// A lock is a KV entry holding its owner and an expiry. Acquire creates the
// entry with CAS (revision 0) or takes over an expired or released one with
// CAS on its revision, so two instances never both win. Release writes a
// tombstone with the holder's revision; a holder that crashed simply lets
// the lease expire. There is no renewal: keep TTL above the longest critical
// section and do not rely on the lock for correctness of the protected
// write itself — core's token store still checks previous_expires_at.
//
// Which bucket: core's worker permission template
// (cairn-core internal/domain/worker_enrollment.go, StandardPermissionTemplate)
// lets a worker write exactly three buckets — cairn_worker_presence,
// cairn_worker_manifests and cairn_rate_limits — and read cairn_core. The
// rate-limit bucket is the one meant for shared, short-lived coordination
// state (15-minute TTL, history 1), so locks live there under the key
// prefix "lock." next to the "<provider>.<app>.<window>" budget keys. Core
// reads budget keys by exact name and never lists the bucket, so the extra
// keys are invisible to it. workersdk.NewRefreshLock wires this up.
package kvlock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/port"
)

// Lock hands out leases on names under one key prefix.
type Lock struct {
	// KV is the bucket the leases live in (cairn_rate_limits for workers).
	KV port.KV
	// Prefix is prepended to every lease name; "lock." when empty. Names
	// and prefix must be valid KV key material ([-/_=.a-zA-Z0-9]).
	Prefix string
	// TTL bounds a holder's lease; an expired lease can be taken over.
	// 30s when zero. Keep it above the longest critical section.
	TTL time.Duration
	// Owner names the holder in the entry (diagnostics); the instance id
	// is a good choice.
	Owner string
	// PollInterval paces Wait; 100ms when zero.
	PollInterval time.Duration

	// now is overridable in tests.
	now func() time.Time
}

// ErrHeld is wrapped by the error Acquire returns while another holder's
// lease is live; HeldError carries who and until when.
var ErrHeld = errors.New("kvlock: held")

// HeldError reports a live lease held by someone else.
type HeldError struct {
	Owner string
	Until time.Time
}

func (e *HeldError) Error() string {
	return fmt.Sprintf("kvlock: held by %q until %s", e.Owner, e.Until.UTC().Format(time.RFC3339))
}

// Is makes errors.Is(err, ErrHeld) true.
func (e *HeldError) Is(target error) bool { return target == ErrHeld }

// Handle is a held lease.
type Handle struct {
	lock *Lock
	key  string
	rev  uint64
	// Until is when the lease expires unless released.
	Until time.Time
}

type entry struct {
	Owner     string    `json:"owner,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	Released  bool      `json:"released,omitempty"`
}

// free reports whether the stored entry no longer protects anything.
func (e entry) free(now time.Time) bool {
	return e.Released || e.ExpiresAt.IsZero() || !now.Before(e.ExpiresAt)
}

func (l *Lock) clock() time.Time {
	if l.now != nil {
		return l.now()
	}
	return time.Now()
}

func (l *Lock) ttl() time.Duration {
	if l.TTL > 0 {
		return l.TTL
	}
	return 30 * time.Second
}

func (l *Lock) key(name string) string {
	prefix := l.Prefix
	if prefix == "" {
		prefix = "lock."
	}
	return prefix + name
}

// read returns the current entry and revision; a missing key reads as free
// with revision 0 (CAS then creates it).
func (l *Lock) read(ctx context.Context, key string) (entry, uint64, error) {
	e, err := l.KV.Get(ctx, key)
	if errors.Is(err, port.ErrKVKeyNotFound) {
		return entry{Released: true}, 0, nil
	}
	if err != nil {
		return entry{}, 0, fmt.Errorf("kvlock: read %s: %w", key, err)
	}
	var cur entry
	if json.Unmarshal(e.Value, &cur) != nil {
		cur = entry{Released: true} // a corrupt entry must not pin the lock
	}
	return cur, e.Revision, nil
}

// Acquire takes the lease on name or returns a *HeldError (errors.Is
// ErrHeld) when another holder's lease is live. It never blocks; see Wait
// and Do.
func (l *Lock) Acquire(ctx context.Context, name string) (*Handle, error) {
	key := l.key(name)
	cur, rev, err := l.read(ctx, key)
	if err != nil {
		return nil, err
	}
	now := l.clock()
	if !cur.free(now) {
		return nil, &HeldError{Owner: cur.Owner, Until: cur.ExpiresAt}
	}
	until := now.Add(l.ttl())
	payload, _ := json.Marshal(entry{Owner: l.Owner, ExpiresAt: until})
	newRev, ok, err := l.KV.CompareAndSet(ctx, key, payload, rev)
	if err != nil {
		return nil, fmt.Errorf("kvlock: acquire %s: %w", key, err)
	}
	if !ok {
		// Lost the race to a concurrent Acquire: whoever won holds it now.
		return nil, &HeldError{Until: until}
	}
	return &Handle{lock: l, key: key, rev: newRev, Until: until}, nil
}

// Release ends the lease. A lease that expired and was taken over in the
// meantime is left alone (the CAS loses), which is not an error.
func (h *Handle) Release(ctx context.Context) error {
	payload, _ := json.Marshal(entry{Owner: h.lock.Owner, Released: true})
	if _, _, err := h.lock.KV.CompareAndSet(ctx, h.key, payload, h.rev); err != nil {
		return fmt.Errorf("kvlock: release %s: %w", h.key, err)
	}
	return nil
}

// Wait blocks until no live lease exists on name or ctx ends. It does not
// acquire: call Acquire afterwards (and expect it may still lose).
func (l *Lock) Wait(ctx context.Context, name string) error {
	interval := l.PollInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	key := l.key(name)
	for {
		cur, _, err := l.read(ctx, key)
		if err != nil {
			return err
		}
		if cur.free(l.clock()) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// Do runs fn under the lease on name, waiting for a live lease to end
// first. It returns fn's error; a failure to acquire or release is
// returned as is.
func (l *Lock) Do(ctx context.Context, name string, fn func(ctx context.Context) error) error {
	for {
		h, err := l.Acquire(ctx, name)
		if err == nil {
			ferr := fn(ctx)
			if rerr := h.Release(ctx); rerr != nil && ferr == nil {
				return rerr
			}
			return ferr
		}
		if !errors.Is(err, ErrHeld) {
			return err
		}
		if err := l.Wait(ctx, name); err != nil {
			return err
		}
	}
}

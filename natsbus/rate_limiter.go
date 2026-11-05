package natsbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/port"
)

// RateLimiter implements port.RateLimiter over the cairn_rate_limits
// NATS KV bucket. Each bucket key holds a JSON-encoded rateLimitState;
// concurrent updates are arbitrated by JetStream KV's CAS semantics.
//
// Design rationale (see also cairn-core docs/architecture.md §6.3):
//
//   - Workers Reserve BEFORE every external API call. The Reserve call
//     is async-safe via CompareAndSet; on lost CAS the caller retries.
//   - On 429 from the upstream API, the worker calls ForceRefill with
//     the authoritative reset time from the Retry-After header. All
//     other workers see this on their next Reserve via the bumped KV
//     revision.
//   - The KV bucket has a 15-minute TTL — short enough that crashed
//     workers don't pin counters, long enough to cover the typical short
//     window. Longer quotas are tracked by storing windowStart and
//     comparing on every read; a worker that keeps reserving keeps the
//     entry alive, and an idle bucket resetting early is harmless.
type RateLimiter struct {
	kv port.KV

	// Bucket limits. Keyed by bucket name; populated from the caller's
	// map. Empty map = no enforced limits. A zero Window falls back to the
	// suffix convention (windowFor).
	limits map[string]Limit

	now func() time.Time

	// sleep backs off between failed CAS attempts so concurrent reservers on
	// one bucket desynchronise instead of livelocking (thundering herd).
	// Injectable for tests (set to a no-op for deterministic, fast runs).
	sleep func(time.Duration)
}

// Limit is one bucket's budget: Capacity requests per rolling Window.
type Limit struct {
	Capacity int
	// Window is the rolling window; zero means "by bucket-name suffix"
	// (see NewRateLimiter).
	Window time.Duration
}

// Rolling windows providers commonly enforce. Combine with Limit:
// {Capacity: 100, Window: natsbus.PerMinute}.
const (
	PerSecond     = time.Second
	PerMinute     = time.Minute
	Per15Minutes  = 15 * time.Minute
	PerHour       = time.Hour
	PerDay        = 24 * time.Hour
	defaultWindow = Per15Minutes
)

// NewRateLimiter constructs a rate limiter on top of an existing KV
// bucket handle (typically the cairn_rate_limits bucket the cairn-core
// server bootstraps).
//
// `capacities` maps bucket-name → capacity. The adapter has NO built-in
// per-provider knowledge — workers configure their own. The rolling window
// comes from the bucket-name suffix: "<...>:short" and ":webhook" are 15
// minutes, ":daily" 24 hours, ":hourly"/":hour" one hour, ":minute" one
// minute, ":second" one second; other suffixes default to 15 minutes.
// NewRateLimiterWithLimits sets windows explicitly instead.
//
// Pass an empty map (or nil) for a permissive limiter — buckets not
// listed in the map are treated as unlimited. That's intentional:
// unknown buckets DON'T fail-closed, because rate-limit policy is a
// worker concern and the core should not block traffic for buckets it
// has no opinion about.
func NewRateLimiter(kv port.KV, capacities map[string]int) *RateLimiter {
	limits := make(map[string]Limit, len(capacities))
	for k, v := range capacities {
		limits[k] = Limit{Capacity: v}
	}
	return NewRateLimiterWithLimits(kv, limits)
}

// NewRateLimiterWithLimits is NewRateLimiter with an explicit window per
// bucket (a zero Window keeps the suffix convention). Bucket names are
// still free-form; a worker with per-second and per-minute limits on one
// API uses two buckets ("acme:w1:second", "acme:w1:minute") and reserves
// from both before a call.
func NewRateLimiterWithLimits(kv port.KV, limits map[string]Limit) *RateLimiter {
	own := make(map[string]Limit, len(limits))
	for k, v := range limits {
		own[k] = v
	}
	return &RateLimiter{
		kv:     kv,
		limits: own,
		now:    func() time.Time { return time.Now().UTC() },
		sleep:  time.Sleep,
	}
}

// casBackoff returns a jittered backoff for the given (0-based) CAS attempt.
// Exponential up to a cap, with full jitter so contenders don't re-collide in
// lockstep. attempt 0 → up to ~0.4ms; grows to a ~20ms cap.
func casBackoff(attempt int) time.Duration {
	const base = 200 * time.Microsecond
	const maxSpan = 20 * time.Millisecond
	shift := attempt
	if shift > 6 {
		shift = 6
	}
	span := base << shift
	if span > maxSpan {
		span = maxSpan
	}
	return time.Duration(rand.Int63n(int64(span) + 1)) //nolint:gosec // G404: backoff jitter, not security-sensitive
}

// rateLimitState is the JSON value stored under each bucket key.
type rateLimitState struct {
	Capacity      int       `json:"capacity"`
	Used          int       `json:"used"`
	WindowStart   time.Time `json:"window_start"`
	WindowResetAt time.Time `json:"window_reset_at"`
}

// windowDurations maps the bucket-name suffix to the rolling window.
// Naming convention only — providers can use any of these suffixes or
// register their own buckets with custom names (in which case the
// default 15-min window applies) or explicit Limit windows.
var windowDurations = map[string]time.Duration{
	"short":   Per15Minutes,
	"webhook": Per15Minutes,
	"daily":   PerDay,
	"hourly":  PerHour,
	"hour":    PerHour,
	"minute":  PerMinute,
	"second":  PerSecond,
}

func windowFor(bucket string) time.Duration {
	for suffix, d := range windowDurations {
		if strings.HasSuffix(bucket, ":"+suffix) {
			return d
		}
	}
	return defaultWindow
}

// windowOf is the bucket's configured window, else the suffix convention.
func (r *RateLimiter) windowOf(bucket string) time.Duration {
	if l, ok := r.limits[bucket]; ok && l.Window > 0 {
		return l.Window
	}
	return windowFor(bucket)
}

// kvKey turns a bucket name into a valid NATS KV key. Bucket names use the
// "<provider>:<window>" convention, but NATS KV keys reject ':' — its allowed
// charset is [-/_=.a-zA-Z0-9]. Map ':' to '.' (a natural token separator) so
// "strava:short" → "strava.short". The bucket name is still what capacities and
// windowFor key off; only the KV I/O uses this.
func kvKey(bucket string) string {
	return strings.ReplaceAll(bucket, ":", ".")
}

// Reserve atomically consumes `tokens` from the bucket. Returns
// (true, 0, nil) on success; (false, retryAfter, nil) when the bucket
// is exhausted (caller should NakWithDelay the job).
//
// Implementation: read current state, advance the window if expired,
// either grant or compute retry-after, then CAS the new state. If the
// CAS fails (another worker raced us), retry the whole loop.
func (r *RateLimiter) Reserve(
	ctx context.Context,
	bucket string,
	tokens int,
) (bool, time.Duration, error) {
	if tokens <= 0 {
		return true, 0, nil
	}

	capacity := r.limits[bucket].Capacity
	if capacity == 0 {
		// Unknown bucket: behave permissively (workers can still call APIs
		// when no policy is configured). Operator may add via NewRateLimiter.
		return true, 0, nil
	}
	window := r.windowOf(bucket)

	// With N workers contending for one bucket, lockstep CAS retries
	// livelock (every collision loses to a contender that collides next).
	// Jittered backoff between attempts desynchronises contenders so they
	// converge in a handful of rounds even at high N. 200 attempts is a
	// generous ceiling; with backoff it's rarely approached.
	const maxAttempts = 200
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			r.sleep(casBackoff(attempt))
		}
		settled, granted, retryAfter, err := r.tryReserve(ctx, bucket, tokens, capacity, window)
		if err != nil {
			return false, 0, err
		}
		if settled {
			return granted, retryAfter, nil
		}
		// CAS lost: another worker advanced; back off (top of loop) and retry.
	}
	// Persistent contention is NOT an error — surfacing it as one made the
	// worker NAK-and-instantly-redeliver (busy-loop). Return a soft "couldn't
	// reserve, retry after a short jittered delay" so it flows through the
	// caller's NakWithDelay backoff path, same as an exhausted bucket.
	return false, 50*time.Millisecond + casBackoff(8), nil
}

// tryReserve runs one read-advance-CAS round. settled is false only when the
// CAS lost to a concurrent writer and the caller should retry.
func (r *RateLimiter) tryReserve(
	ctx context.Context,
	bucket string,
	tokens, capacity int,
	window time.Duration,
) (settled, granted bool, retryAfter time.Duration, err error) {
	state, rev, err := r.readState(ctx, bucket)
	if err != nil {
		return false, false, 0, err
	}

	now := r.now()
	state = rollWindow(state, now, capacity, window)

	if state.Used+tokens > state.Capacity {
		return true, false, max(state.WindowResetAt.Sub(now), 0), nil
	}

	state.Used += tokens
	ok, err := r.writeState(ctx, bucket, state, rev)
	if err != nil {
		return false, false, 0, err
	}
	return ok, ok, 0, nil
}

// rollWindow starts a fresh window when the previous one has expired. It
// carries the last known capacity forward — SyncUsage may have learned the
// provider's real limit, which beats the configured guess.
func rollWindow(state rateLimitState, now time.Time, capacity int, window time.Duration) rateLimitState {
	if !state.WindowStart.IsZero() && now.Before(state.WindowResetAt) {
		return state
	}
	carried := state.Capacity
	if carried <= 0 {
		carried = capacity
	}
	return rateLimitState{
		Capacity:      carried,
		Used:          0,
		WindowStart:   now,
		WindowResetAt: now.Add(window),
	}
}

// ForceRefill is called after a 429 from the upstream API. Sets the
// bucket to (available, windowResetsAt) authoritatively; the CAS is
// unconditional (Put rather than Update) because the worker that just
// hit 429 has the latest information from the provider's response.
func (r *RateLimiter) ForceRefill(
	ctx context.Context,
	bucket string,
	available int,
	windowResetsAt time.Time,
) error {
	capacity := r.limits[bucket].Capacity
	if capacity == 0 {
		capacity = available
	}
	used := capacity - available
	if used < 0 {
		used = 0
	}
	state := rateLimitState{
		Capacity:      capacity,
		Used:          used,
		WindowStart:   r.now(),
		WindowResetAt: windowResetsAt,
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("force-refill: marshal state: %w", err)
	}
	if _, err := r.kv.Put(ctx, kvKey(bucket), payload); err != nil {
		return fmt.Errorf("force-refill: kv put %s: %w", bucket, err)
	}
	return nil
}

// SyncUsage overwrites the bucket with the provider-reported (used, limit)
// pair — authoritative accounting straight from response headers. Like
// ForceRefill the write is unconditional: the caller just heard the truth
// from the provider, so losing a concurrent local reservation is fine (the
// next response re-syncs).
func (r *RateLimiter) SyncUsage(
	ctx context.Context,
	bucket string,
	used, limit int,
	windowResetsAt time.Time,
) error {
	if limit <= 0 {
		return nil
	}
	if used < 0 {
		used = 0
	}
	state := rateLimitState{
		Capacity:      limit,
		Used:          used,
		WindowStart:   r.now(),
		WindowResetAt: windowResetsAt,
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("sync-usage: marshal state: %w", err)
	}
	if _, err := r.kv.Put(ctx, kvKey(bucket), payload); err != nil {
		return fmt.Errorf("sync-usage: kv put %s: %w", bucket, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// State I/O helpers
// ---------------------------------------------------------------------------

func (r *RateLimiter) readState(
	ctx context.Context,
	bucket string,
) (rateLimitState, uint64, error) {
	entry, err := r.kv.Get(ctx, kvKey(bucket))
	if err != nil {
		if errors.Is(err, port.ErrKVKeyNotFound) {
			return rateLimitState{}, 0, nil
		}
		return rateLimitState{}, 0, fmt.Errorf("kv get %s: %w", bucket, err)
	}
	if len(entry.Value) == 0 {
		return rateLimitState{}, entry.Revision, nil
	}
	var state rateLimitState
	if err := json.Unmarshal(entry.Value, &state); err != nil {
		// Treat malformed entries as empty so a corrupted KV value
		// doesn't pin the bucket. The next Reserve will overwrite.
		return rateLimitState{}, entry.Revision, nil
	}
	return state, entry.Revision, nil
}

func (r *RateLimiter) writeState(
	ctx context.Context,
	bucket string,
	state rateLimitState,
	expectedRev uint64,
) (bool, error) {
	payload, err := json.Marshal(state)
	if err != nil {
		return false, fmt.Errorf("marshal state: %w", err)
	}
	// CompareAndSet with expectedRev=0 means "create only if no entry
	// exists" — both nats.KV and the in-memory fake implement this
	// semantics. Using Put unconditionally would clobber a concurrently-
	// created entry and lose tokens (200 concurrent workers all racing
	// the initial create would each Put their own initial state).
	_, ok, err := r.kv.CompareAndSet(ctx, kvKey(bucket), payload, expectedRev)
	if err != nil {
		return false, fmt.Errorf("kv cas %s: %w", bucket, err)
	}
	return ok, nil
}

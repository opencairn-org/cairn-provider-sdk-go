package workersdk

import (
	"log/slog"
	"time"

	"github.com/opencairn-org/cairn-provider-sdk-go/kvlock"
	"github.com/opencairn-org/cairn-provider-sdk-go/natsbus"
	"github.com/opencairn-org/cairn-provider-sdk-go/port"
)

// KVRateLimits is the shared KV bucket of provider budgets (and, under the
// "lock." prefix, of pool-wide leases). Core bootstraps it with a 15-minute
// TTL; the worker permission template grants writes to it.
const KVRateLimits = "cairn_rate_limits"

// NewRateLimiter builds the shared, KV-backed rate limiter over
// KVRateLimits with natsbus.NewRateLimiterWithLimits semantics: limits maps
// bucket names (RateBucket or the worker's own per-app scheme) to a
// capacity per window. It returns nil (no local limiting) when the bucket
// is unreachable — the server has not bootstrapped it yet, or the worker
// lacks the permission — so a missing bucket degrades to relying on the
// provider's 429s through ObserveRateLimit429.
func NewRateLimiter(bus port.JobBus, logger *slog.Logger, limits map[string]natsbus.Limit) port.RateLimiter {
	if logger == nil {
		logger = slog.Default()
	}
	kv, err := bus.KV(KVRateLimits)
	if err != nil {
		logger.Warn("rate-limit KV bucket unavailable; running without local rate limiting", "error", err)
		return nil
	}
	return natsbus.NewRateLimiterWithLimits(kv, limits)
}

// NewRefreshLock builds the pool-wide lease a TokenCache serialises
// refreshes with (Config.RefreshLock). It lives in KVRateLimits under the
// "lock." prefix: of the three buckets core lets a worker write (presence,
// manifests, rate limits) it is the one for shared short-lived state, and
// its 15-minute TTL reaps a crashed holder's lease by itself; see package
// kvlock. owner names the holder (the instance id). ttl bounds a holder's
// lease and should exceed a provider token round-trip by a wide margin
// (30s when zero). Returns nil, like NewRateLimiter, when the bucket is
// unreachable: refreshes then run unlocked, as before the lock existed.
func NewRefreshLock(bus port.JobBus, logger *slog.Logger, owner string, ttl time.Duration) *kvlock.Lock {
	if logger == nil {
		logger = slog.Default()
	}
	kv, err := bus.KV(KVRateLimits)
	if err != nil {
		logger.Warn("rate-limit KV bucket unavailable; token refreshes run without the pool lock", "error", err)
		return nil
	}
	return &kvlock.Lock{KV: kv, Prefix: "lock.", Owner: owner, TTL: ttl}
}

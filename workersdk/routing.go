package workersdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/mod/semver"

	"github.com/opencairn-org/cairn-provider-sdk-go/port"
)

// kvCoreInfo holds core's release under key "info" (cairn-core ADR 0005).
const kvCoreInfo = "cairn_core"

// JobSubject is this worker pool's subject for a job kind:
// cairn.jobs.<job>.<provider>.<name> (cairn-core ADR 0006).
func (w *Worker) JobSubject(job string) string {
	return "cairn.jobs." + job + "." + w.cfg.Provider + "." + w.cfg.Name
}

// Name is the pool name (Config.Name).
func (w *Worker) Name() string { return w.cfg.Name }

// ResultSubject is where this pool publishes a job kind's results:
// cairn.results.<job>.<provider>.<name> (reconcile appends the account).
func (w *Worker) ResultSubject(job string) string {
	return "cairn.results." + job + "." + w.cfg.Provider + "." + w.cfg.Name
}

// LookupSubject resolves a provider owner id to this pool's connection.
func (w *Worker) LookupSubject() string {
	return "cairn.accounts.lookup." + w.cfg.Provider + "." + w.cfg.Name
}

// RateBucket is this pool's rate-limit bucket for a window (budgets are
// per pool, i.e. per provider app): <provider>:<name>:<window>.
func (w *Worker) RateBucket(window string) string {
	return w.cfg.Provider + ":" + w.cfg.Name + ":" + window
}

// DiscoverSubject is the request/reply subject this pool answers discovery on.
func (w *Worker) DiscoverSubject() string {
	return "cairn.discover." + w.cfg.Provider + "." + w.cfg.Name
}

// HandleWithMaxDeliver is Handle with a per-subject redelivery cap: a job
// that is handed back on purpose (parser_too_old) needs more deliveries than
// the bus default before it is dead-lettered.
func (w *Worker) HandleWithMaxDeliver(subject string, fn HandlerFunc, maxDeliver int) {
	w.Handle(subject, fn)
	w.maxDeliver[subject] = maxDeliver
}

// ErrIncompatibleCore is returned when core's release has another
// major.minor than this worker's.
var ErrIncompatibleCore = errors.New("workersdk: core release incompatible")

// checkCoreRelease compares core's published release with ours (cairn-core
// ADR 0005): before 1.0 both share major.minor; from 1.0 on, same major and
// our minor not above core's. Missing info or an untagged build on either
// side is not an error.
func (w *Worker) checkCoreRelease(ctx context.Context) error {
	if semver.MajorMinor(w.cfg.Release) == "" {
		return nil
	}
	kv, err := w.cfg.Bus.KV(kvCoreInfo)
	if err != nil {
		w.logger.Debug("core info unavailable", "error", err)
		return nil
	}
	e, err := kv.Get(ctx, "info")
	if err != nil {
		if !errors.Is(err, port.ErrKVKeyNotFound) {
			w.logger.Debug("core info read failed", "error", err)
		}
		return nil
	}
	var info struct {
		Release string `json:"release"`
	}
	if json.Unmarshal(e.Value, &info) != nil {
		return nil
	}
	if ReleaseCompatible(info.Release, w.cfg.Release) {
		return nil
	}
	return fmt.Errorf("%w: core %s, worker %s", ErrIncompatibleCore, info.Release, w.cfg.Release)
}

// ReleaseCompatible implements the release rule; untagged builds pass.
func ReleaseCompatible(core, worker string) bool {
	cMaj, cMin, cok := releaseMajorMinor(core)
	wMaj, wMin, wok := releaseMajorMinor(worker)
	switch {
	case !cok || !wok:
		return true
	case cMaj != wMaj:
		return false
	case cMaj == 0:
		return cMin == wMin
	}
	return wMin <= cMin
}

func releaseMajorMinor(v string) (major, minor int, ok bool) {
	mm := semver.MajorMinor(v)
	if mm == "" {
		return 0, 0, false
	}
	a, b, _ := strings.Cut(strings.TrimPrefix(mm, "v"), ".")
	major, err1 := strconv.Atoi(a)
	minor, err2 := strconv.Atoi(b)
	return major, minor, err1 == nil && err2 == nil
}

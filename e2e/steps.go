//go:build e2e

package e2e

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
)

// pollNever parks the account's reconcile cadence; steps lower it on demand.
const pollNever = 86400

// runAll executes the provider's steps in order and writes the report.

// stepNames are the steps in run order; spec skip/xfail keys name them.
var stepNames = []string{
	"build worker+mock", "core release", "admin login", "mock up", "worker presence", "connect account",
	"reconcile on connect", "gear on connect", "discover", "full sync", "activity data", "reparse",
	"rate limit 429", "reconcile", "webhook event", "webhook forged", "webhook verify", "import metrics", "token refresh",
	"worker-conformance",
}

func (r *runner) runAll() {
	defer r.prov.stop()
	r.critical("build worker+mock", r.stepBuild)
	r.critical("core release", r.stepCoreRelease)
	r.critical("admin login", r.stepLogin)
	r.critical("mock up", r.stepMock)
	r.critical("worker presence", r.stepWorker)
	r.critical("connect account", r.stepConnect)
	r.step("reconcile on connect", true, r.stepReconcileOnConnect)
	r.step("gear on connect", r.spec.Expect.Gear, r.stepGear)
	r.step("discover", r.spec.Steps.Discover, r.stepDiscover)
	r.step("full sync", r.spec.Steps.FullSync, r.stepFullSync)
	r.step("activity data", r.spec.Steps.FullSync, r.stepActivityData)
	r.step("reparse", r.spec.Steps.Reparse, r.stepReparse)
	r.step("rate limit 429", r.spec.Steps.RateLimit, r.stepRateLimit)
	r.step("reconcile", r.spec.Steps.Reconcile, r.stepReconcile)
	r.step("webhook event", r.spec.Steps.Webhook, r.stepWebhook)
	r.step("webhook forged", r.spec.Steps.WebhookForged, r.stepWebhookForged)
	r.stepWith("webhook verify", r.spec.Steps.WebhookVerify, r.verifyKnownIssue(), r.stepWebhookVerify)
	r.step("import metrics", r.spec.Steps.ImportMetrics, r.stepImportMetrics)
	r.step("token refresh", r.spec.Steps.TokenRefresh, r.stepTokenRefresh)
	r.step("worker-conformance", r.spec.Steps.Conformance, r.stepConformance)
}

func (r *runner) stepBuild() (string, error) {
	return "binaries in " + r.prov.outDir(), r.prov.build(r.ctx)
}

// stepCoreRelease checks the release core advertises to workers.
func (r *runner) stepCoreRelease() (string, error) {
	rel, err := r.stack.bus.CoreRelease(r.ctx)
	if err != nil {
		return "", fmt.Errorf("cairn_core/info: %w", err)
	}
	if rel != r.stack.Release {
		return "", fmt.Errorf("cairn_core/info release %q, binary built as %q", rel, r.stack.Release)
	}
	var v struct {
		Version string `json:"version"`
	}
	if err := r.core.get(r.ctx, "/api/version", &v); err != nil {
		return "", err
	}
	return fmt.Sprintf("core %s (info key %s), worker stamped %s", v.Version, rel, workerRelease), nil
}

func (r *runner) stepLogin() (string, error) {
	return "session cookie for " + r.stack.AdminEmail, r.core.login(r.ctx, r.stack.AdminEmail, "e2e-bootstrap-password", r.stack.AdminPassword)
}

func (r *runner) stepMock() (string, error) {
	if err := r.prov.startMock(r.ctx); err != nil {
		return "", err
	}
	return "mock at " + r.prov.mockDirect + ", worker calls through " + r.prov.mockURL, nil
}

// stepWorker starts the worker and checks presence, release compatibility
// and that core lists the provider and its pool.
func (r *runner) stepWorker() (string, error) {
	hb, err := r.prov.startWorker(r.ctx, r.core)
	if err != nil {
		return "", err
	}
	r.hb = hb
	if hb.Release != workerRelease || hb.WorkerKey != r.spec.WorkerKey() {
		return "", fmt.Errorf("presence release %q key %q; want %s %s", hb.Release, hb.WorkerKey, workerRelease, r.spec.WorkerKey())
	}
	for _, c := range r.spec.Expect.Capabilities {
		if _, ok := hb.Capabilities[c]; !ok {
			return "", fmt.Errorf("manifest lacks capability %q (has %v)", c, keys(hb.Capabilities))
		}
	}
	if (r.spec.Steps.Webhook || r.spec.Steps.WebhookVerify) && !hb.Webhooks {
		return "", fmt.Errorf("presence does not advertise webhooks; the worker needs its verify secret")
	}
	return r.checkFleet()
}

// checkFleet asserts the admin fleet view and the connections page agree.
func (r *runner) checkFleet() (string, error) {
	workers, err := r.core.adminWorkers(r.ctx)
	if err != nil {
		return "", err
	}
	if !r.poolListed(workers) {
		return "", fmt.Errorf("/api/admin/workers: pool %s with a known, compatible release not found: %+v", r.spec.Worker.Name, workers)
	}
	conns, err := r.core.connections(r.ctx)
	if err != nil {
		return "", err
	}
	for _, p := range conns.Providers {
		if p.Provider != r.spec.Provider {
			continue
		}
		for _, pool := range p.WorkerPools {
			if pool.Name == r.spec.Worker.Name && pool.Online && pool.Compatible {
				return fmt.Sprintf("pool %s release %s, provider %q auth %s", pool.Name, pool.Release, p.DisplayName, p.AuthMethod), nil
			}
		}
	}
	return "", fmt.Errorf("/api/connections lists no online compatible pool %s for %s", r.spec.Worker.Name, r.spec.Provider)
}

// poolListed reports whether the fleet view shows the pool with a release
// core could parse and found compatible with its own.
func (r *runner) poolListed(workers []adminWorker) bool {
	for _, w := range workers {
		if w.WorkerName == r.spec.Worker.Name && w.Provider == r.spec.Provider && w.Compatible && w.Known {
			return true
		}
	}
	return false
}

// stepConnect links the account and parks its poll interval.
func (r *runner) stepConnect() (string, error) {
	acct, err := r.prov.connectAccount(r.ctx, r.core)
	if err != nil {
		return "", err
	}
	r.acct = acct
	if acct.AssignedWorker != r.spec.Worker.Name {
		return "", fmt.Errorf("account assigned to pool %q, want %s", acct.AssignedWorker, r.spec.Worker.Name)
	}
	if err := r.core.setPollInterval(r.ctx, acct.ID, pollNever); err != nil {
		return "", err
	}
	return fmt.Sprintf("account %s via %s (provider account %s)", acct.ID, r.spec.Auth.Method, acct.ProviderAccountID), nil
}

// stepReconcileOnConnect waits for the scheduler's first reconcile of the
// new account (last_sync_at is null, so the next tick is due) and its
// account-suffixed result; the seeded activities are older than the
// reconcile window, so nothing is imported yet.
func (r *runner) stepReconcileOnConnect() (string, error) {
	jobSubj := r.jobSubject("reconcile")
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "reconcile job", 0, func(m busMsg) bool {
		return m.Subject == jobSubj && m.Field("account_id") == r.acct.ID
	}); err != nil {
		return "", err
	}
	// The pool publishes cairn.results.reconcile.<provider>.<pool>.<account>.
	resPrefix, resSuffix := "cairn.results.reconcile."+r.spec.Provider+".", "."+r.acct.ID
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "reconcile result "+resPrefix+"*"+resSuffix, 0, func(m busMsg) bool {
		return strings.HasPrefix(m.Subject, resPrefix) && strings.HasSuffix(m.Subject, resSuffix)
	}); err != nil {
		return "", err
	}
	err := waitUntil(r.ctx, r.spec.Wait, "last_sync_at", func() (bool, error) {
		a, err := r.core.account(r.ctx, r.spec.Provider)
		return err == nil && a.LastSyncAt != nil, nil
	})
	if err != nil {
		return "", err
	}
	a, _ := r.core.account(r.ctx, r.spec.Provider)
	return fmt.Sprintf("last_sync_at %s, %d activities imported", *a.LastSyncAt, a.Imported), nil
}

// stepGear waits for the connect-time import_gear, whose GEAR events land
// as pending refs (docs/gear-v0.4.0.md: the user decides), decides "new
// gear" for each and checks the gear carries the provider's ref.
func (r *runner) stepGear() (string, error) {
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "import_gear job", 0, onSubject(r.jobSubject("import_gear"))); err != nil {
		return "", err
	}
	refs, err := r.pendingGearRefs()
	if err != nil {
		return "", err
	}
	var names []string
	for _, ref := range refs {
		g, err := r.core.createGearFromRef(r.ctx, ref)
		if err != nil {
			return "", fmt.Errorf("create gear from ref %s: %w", ref.ExternalID, err)
		}
		names = append(names, g.Name)
	}
	list, err := r.core.listGear(r.ctx)
	if err != nil {
		return "", err
	}
	if linked := r.gearWithProviderRef(list); linked < len(refs) {
		return "", fmt.Errorf("%d gear items carry a %s ref, want %d", linked, r.spec.Provider, len(refs))
	}
	return fmt.Sprintf("%d pending refs decided as new gear: %s", len(refs), strings.Join(names, ", ")), nil
}

// pendingGearRefs waits for the account's provider gear to await a decision.
func (r *runner) pendingGearRefs() ([]gearRef, error) {
	var refs []gearRef
	err := waitUntil(r.ctx, r.spec.Wait, "pending gear refs from "+r.spec.Provider, func() (bool, error) {
		all, err := r.core.listPendingGearRefs(r.ctx)
		if err != nil {
			return false, err
		}
		refs = refs[:0]
		for _, ref := range all {
			if ref.Provider == r.spec.Provider && ref.ExternalAccountID == r.acct.ID {
				refs = append(refs, ref)
			}
		}
		return len(refs) > 0, nil
	})
	return refs, err
}

// gearWithProviderRef counts gear items mapped from the provider.
func (r *runner) gearWithProviderRef(list []gear) int {
	n := 0
	for _, g := range list {
		for _, ref := range g.ExternalRefs {
			if ref.Provider == r.spec.Provider {
				n++
				break
			}
		}
	}
	return n
}

// stepDiscover asks core to preview the import (the discover request/reply).
func (r *runner) stepDiscover() (string, error) {
	p, err := r.core.syncPreview(r.ctx, r.acct.ID)
	if err != nil {
		return "", err
	}
	if p.Total != r.spec.Expect.Activities || !p.Complete {
		return "", fmt.Errorf("preview %+v; want total %d complete", p, r.spec.Expect.Activities)
	}
	return fmt.Sprintf("total %d, already present %d, new %d", p.Total, p.AlreadyPresent, p.New), nil
}

// stepFullSync runs the queue-driven full import and waits for every item.
func (r *runner) stepFullSync() (string, error) {
	want := r.spec.Expect.Activities
	mark := r.stack.bus.Mark()
	if err := r.core.syncStart(r.ctx, r.acct.ID, true); err != nil {
		return "", err
	}
	var a account
	err := waitUntil(r.ctx, r.spec.Wait, fmt.Sprintf("%d activities imported", want), func() (bool, error) {
		var err error
		a, err = r.core.account(r.ctx, r.spec.Provider)
		if err != nil {
			return false, err
		}
		if a.Queue["failed"] > 0 {
			return false, fmt.Errorf("queue has %d failed items: %+v%s", a.Queue["failed"], a.Queue, r.failureReasons(""))
		}
		return !a.Discovering && a.Imported >= want && a.Queue["pending"]+a.Queue["in_progress"] == 0, nil
	})
	if err != nil {
		return "", err
	}
	jobs := r.stack.bus.Count(mark, jobWithReason(r.jobSubject("fetch_source"), "queue"))
	return fmt.Sprintf("imported %d (provider total %d), queue %+v, %d fetch_source jobs", a.Imported, a.ProviderTotal, a.Queue, jobs), nil
}

// stepActivityData checks what the import produced: count, type, source
// stamps, laps, streams and gear on the activities.
func (r *runner) stepActivityData() (string, error) {
	acts, err := r.core.listActivities(r.ctx, r.spec.Provider)
	if err != nil {
		return "", err
	}
	if len(acts) < r.spec.Expect.Activities {
		return "", fmt.Errorf("%d activities listed, want %d", len(acts), r.spec.Expect.Activities)
	}
	for _, a := range acts {
		if err := r.checkActivity(a); err != nil {
			return "", fmt.Errorf("activity %s (%s): %w", a.ID, a.Title, err)
		}
	}
	msg := fmt.Sprintf("%d activities of type %s, sources stamped %s/%s", len(acts), acts[0].Type,
		acts[0].Sources[0].SourceWorkerName, acts[0].Sources[0].SourceWorkerVersion)
	if r.spec.Expect.Streams {
		msg += "; " + r.mergedChannels(acts[0].ID)
	}
	return msg, nil
}

func (r *runner) checkActivity(a activity) error {
	if want := r.spec.Expect.ActivityType; want != "" && a.Type != want {
		return fmt.Errorf("type %s, want %s", a.Type, want)
	}
	if err := checkTimezone(a.Timezone); err != nil {
		return err
	}
	if len(a.Sources) == 0 || a.Sources[0].Provider != r.spec.Provider || a.Sources[0].SourceWorkerName != r.spec.Worker.Name {
		return fmt.Errorf("sources %+v; want one from %s/%s", a.Sources, r.spec.Provider, r.spec.Worker.Name)
	}
	if r.spec.Expect.Archive && a.Sources[0].RawBlobID == "" {
		return fmt.Errorf("source has no raw_blob_id (archive expected)")
	}
	if r.spec.Expect.Gear && a.GearID == "" {
		return fmt.Errorf("no gear attached")
	}
	return r.checkActivityDetail(a)
}

// checkActivityDetail reads the laps and the source stream the spec expects.
func (r *runner) checkActivityDetail(a activity) error {
	if r.spec.Expect.Laps {
		laps, err := r.core.laps(r.ctx, a.ID)
		if err != nil {
			return fmt.Errorf("laps: %w", err)
		}
		if len(laps) == 0 {
			return fmt.Errorf("no laps")
		}
	}
	if r.spec.Expect.Streams {
		return r.waitSourceStream(a.Sources[0].ID)
	}
	return nil
}

// waitSourceStream waits for the samples the worker delivered for a source;
// core stores them a moment after the activity itself.
func (r *runner) waitSourceStream(sourceID string) error {
	var ch []string
	var n int
	err := waitUntil(r.ctx, r.spec.Wait, "source stream "+sourceID, func() (bool, error) {
		var err error
		ch, n, err = r.core.sourceStream(r.ctx, sourceID)
		if err != nil {
			return false, err
		}
		return n > 0 && len(ch) > 0, nil
	})
	if err != nil {
		return fmt.Errorf("%w (channels %v, %d samples)", err, ch, n)
	}
	return nil
}

// mergedChannels polls core's merged stream (a 5s continuous aggregate,
// refreshed after the import) for a short while and reports what it has.
func (r *runner) mergedChannels(activityID string) string {
	var ch []string
	_ = waitUntil(r.ctx, 30*time.Second, "merged stream", func() (bool, error) {
		var err error
		ch, err = r.core.mergedStream(r.ctx, activityID)
		return err == nil && len(ch) > 0, nil
	})
	if len(ch) == 0 {
		return "merged stream not materialised yet"
	}
	return fmt.Sprintf("merged stream channels %v", ch)
}

// stepReparse re-runs the worker's parser over the archived blob.
func (r *runner) stepReparse() (string, error) {
	acts, err := r.core.listActivities(r.ctx, r.spec.Provider)
	switch {
	case err != nil:
		return "", fmt.Errorf("list activities to re-parse: %w", err)
	case len(acts) == 0 || len(acts[0].Sources) == 0:
		return "", errors.New("no activity to re-parse")
	}
	act, src := acts[0], acts[0].Sources[0]
	before, err := r.storedView(act.ID, src.ID)
	if err != nil {
		return "", fmt.Errorf("before re-parse: %w", err)
	}
	mark := r.stack.bus.Mark()
	if err := r.core.reimportSource(r.ctx, act.ID, src.ID, "reparse"); err != nil {
		return "", err
	}
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "parse_blob job", mark, onSubject(r.jobSubject("parse_blob"))); err != nil {
		return "", err
	}
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "parse_blob result", mark, onSubject("cairn.results.parse_blob."+r.spec.Provider+".")); err != nil {
		return "", err
	}
	after, err := r.waitReparsed(act.ID, src)
	if err != nil {
		return "", err
	}
	if want := r.spec.Expect.ParserVersion; want != "" && after.SourceWorkerVersion != want {
		return "", fmt.Errorf("parser version %q after re-parse, want %q", after.SourceWorkerVersion, want)
	}
	// Core re-merges after it stamps the source; its activity.changed event marks the end.
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "activity.changed after re-parse", mark, func(m busMsg) bool {
		return m.Subject == activityChangedSubject && m.Field("activity_id") == act.ID
	}); err != nil {
		return "", err
	}
	again, err := r.parsedView(act.ID, src.ID)
	if err != nil {
		return "", fmt.Errorf("after re-parse: %w", err)
	}
	if diff := before.diff(again); len(diff) > 0 {
		return "", fmt.Errorf("re-parse of the same blob by parser %s changed the activity: %s", after.SourceWorkerVersion, strings.Join(diff, "; "))
	}
	return fmt.Sprintf("source %s re-parsed at %s, parser %s; activity, laps and stream unchanged", after.ID, after.LastReimportedAt, after.SourceWorkerVersion), nil
}

// activityChangedSubject is core's event after an ingest or re-parse was merged.
const activityChangedSubject = "cairn.events.activity.changed"

// parsedView is what one parse of a blob produced, as core shows it.
type parsedView struct {
	fields   map[string]any
	laps     int
	channels []string
	samples  int
}

// storedView is parsedView once core stored the source's stream, which
// lands a moment after the activity.
func (r *runner) storedView(activityID, sourceID string) (parsedView, error) {
	if r.spec.Expect.Streams {
		if err := r.waitSourceStream(sourceID); err != nil {
			return parsedView{}, err
		}
	}
	return r.parsedView(activityID, sourceID)
}

func (r *runner) parsedView(activityID, sourceID string) (parsedView, error) {
	var v parsedView
	var err error
	if v.fields, err = r.core.activityFields(r.ctx, r.spec.Provider, activityID); err != nil {
		return v, err
	}
	if r.spec.Expect.Laps {
		laps, err := r.core.laps(r.ctx, activityID)
		if err != nil {
			return v, fmt.Errorf("laps: %w", err)
		}
		v.laps = len(laps)
	}
	if r.spec.Expect.Streams {
		if v.channels, v.samples, err = r.core.sourceStream(r.ctx, sourceID); err != nil {
			return v, fmt.Errorf("stream: %w", err)
		}
		sort.Strings(v.channels)
	}
	return v, nil
}

func (v parsedView) diff(after parsedView) []string {
	out := fieldDiff(v.fields, after.fields, reparseFields)
	if v.laps != after.laps {
		out = append(out, fmt.Sprintf("laps: %d → %d", v.laps, after.laps))
	}
	if v.samples != after.samples || !slices.Equal(v.channels, after.channels) {
		out = append(out, fmt.Sprintf("stream: %d samples %v → %d samples %v", v.samples, v.channels, after.samples, after.channels))
	}
	return out
}

// waitReparsed waits until the source is current again with a new re-parse stamp.
func (r *runner) waitReparsed(activityID string, src activitySource) (activitySource, error) {
	var after activitySource
	err := waitUntil(r.ctx, r.spec.Wait, "source re-parsed", func() (bool, error) {
		srcs, err := r.core.listSources(r.ctx, activityID)
		if err != nil {
			return false, err
		}
		for _, s := range srcs {
			if s.ID == src.ID {
				after = s
			}
		}
		return after.ReimportStatus == "REIMPORT_STATUS_CURRENT" && after.LastReimportedAt != "" && after.LastReimportedAt != src.LastReimportedAt, nil
	})
	if err != nil {
		return after, fmt.Errorf("%w (source now %+v)", err, after)
	}
	return after, nil
}

// stepRateLimit injects one 429 on the mock; the fetch must back off
// (NakWithDelay) and complete on the redelivery.
func (r *runner) stepRateLimit() (string, error) {
	before, err := r.prov.ctl.stats(r.ctx)
	if err != nil {
		return "", err
	}
	extID, err := r.prov.ctl.addActivity(r.ctx, "e2e rate-limited import", time.Now().Add(-40*24*time.Hour))
	if err != nil {
		return "", err
	}
	armed, calls := time.Now(), r.prov.tap.mark()
	if err := r.prov.ctl.failNext(r.ctx, 429, 1, int(rateLimitRetryAfter/time.Second)); err != nil {
		return "", err
	}
	if err := r.core.importOne(r.ctx, r.acct.ID, extID); err != nil {
		return "", err
	}
	if err := r.waitActivity(extID); err != nil {
		return "", err
	}
	took := time.Since(armed)
	if err := earlyRetry(r.prov.tap.since(calls), rateLimitRetryAfter); err != nil {
		return "", err
	}
	after, err := r.prov.ctl.stats(r.ctx)
	if err != nil {
		return "", err
	}
	if after.RateLimitedServed != before.RateLimitedServed+1 {
		return "", fmt.Errorf("mock served %d 429s, want exactly one more than %d", after.RateLimitedServed, before.RateLimitedServed)
	}
	if !r.prov.worker.tail.Contains("rate") {
		return "", fmt.Errorf("worker log shows no rate-limit handling")
	}
	if err := r.prov.ctl.reset(r.ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("activity %s imported %s after one 429 (retry-after %s)", extID, took.Round(time.Millisecond), rateLimitRetryAfter), nil
}

// rateLimitRetryAfter is the Retry-After the mock sends with its 429.
const rateLimitRetryAfter = 2 * time.Second

// stepReconcile adds a recent activity and lowers the poll interval so the
// scheduler's next tick reconciles the account.
func (r *runner) stepReconcile() (string, error) {
	extID, err := r.prov.ctl.addActivity(r.ctx, "e2e reconcile", time.Now().Add(-2*time.Hour))
	if err != nil {
		return "", err
	}
	mark := r.stack.bus.Mark()
	if err := r.core.setPollInterval(r.ctx, r.acct.ID, 1); err != nil {
		return "", err
	}
	defer func() { _ = r.core.setPollInterval(r.ctx, r.acct.ID, pollNever) }()
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "scheduled reconcile job", mark, jobWithReason(r.jobSubject("reconcile"), "scheduled")); err != nil {
		return "", err
	}
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "fetch_source reason=reconcile", mark, func(m busMsg) bool {
		return jobWithReason(r.jobSubject("fetch_source"), "reconcile")(m) && m.Field("ext_id") == extID
	}); err != nil {
		return "", err
	}
	if err := r.waitActivity(extID); err != nil {
		return "", err
	}
	return fmt.Sprintf("activity %s found by reconcile and imported", extID), nil
}

// stepWebhook has the mock deliver an event to core's webhook route; the
// worker must look the owner up and fetch the activity.
func (r *runner) stepWebhook() (string, error) {
	extID, err := r.prov.ctl.addActivity(r.ctx, "e2e webhook", time.Now().Add(-time.Hour))
	if err != nil {
		return "", err
	}
	mark := r.stack.bus.Mark()
	url := r.stack.CoreURL + "/webhooks/" + r.spec.WorkerKey()
	status, body, err := r.prov.ctl.emitWebhook(r.ctx, url, r.spec.Webhook.Aspect, extID, false)
	if err != nil {
		return "", err
	}
	if status != 200 {
		return "", fmt.Errorf("core answered the webhook POST with %d: %s", status, body)
	}
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "forwarded webhook event", mark, onSubject("cairn.webhooks."+r.spec.WorkerKey()+".event")); err != nil {
		return "", err
	}
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "fetch_source reason=webhook", mark, func(m busMsg) bool {
		return jobWithReason(r.jobSubject("fetch_source"), "webhook")(m) && m.Field("ext_id") == extID
	}); err != nil {
		return "", err
	}
	if err := r.waitActivity(extID); err != nil {
		return "", err
	}
	return fmt.Sprintf("event for %s forwarded, fetched and imported", extID), nil
}

// stepWebhookForged has the mock deliver an event the provider never signed
// (wrong secret or token) for one activity, then a genuine event for another.
// Once the genuine one is imported the forged one, delivered first, must have
// been dropped: no fetch_source for it and no activity in core.
func (r *runner) stepWebhookForged() (string, error) {
	forged, err := r.prov.ctl.addActivity(r.ctx, "e2e forged webhook", time.Now().Add(-90*time.Minute))
	if err != nil {
		return "", err
	}
	genuine, err := r.prov.ctl.addActivity(r.ctx, "e2e webhook after a forged one", time.Now().Add(-80*time.Minute))
	if err != nil {
		return "", err
	}
	mark := r.stack.bus.Mark()
	url := r.stack.CoreURL + "/webhooks/" + r.spec.WorkerKey()
	fstatus, _, err := r.prov.ctl.emitWebhook(r.ctx, url, r.spec.Webhook.Aspect, forged, true)
	if err != nil {
		return "", fmt.Errorf("forged event: %w", err)
	}
	status, body, err := r.prov.ctl.emitWebhook(r.ctx, url, r.spec.Webhook.Aspect, genuine, false)
	switch {
	case err != nil:
		return "", fmt.Errorf("genuine event after the forged one: %w", err)
	case status != 200:
		return "", fmt.Errorf("genuine event after the forged one: core answered %d: %s", status, body)
	}
	if err := r.waitActivity(genuine); err != nil {
		return "", fmt.Errorf("genuine event after the forged one: %w", err)
	}
	fetches := r.stack.bus.Count(mark, func(m busMsg) bool {
		return jobWithReason(r.jobSubject("fetch_source"), "webhook")(m) && m.Field("ext_id") == forged
	})
	imported, err := r.activityListed(forged)
	switch {
	case err != nil:
		return "", err
	case fetches > 0 || imported:
		return "", fmt.Errorf("forged event for %s accepted: %d fetch_source jobs, in core %v; the worker does not verify the signature", forged, fetches, imported)
	}
	return fmt.Sprintf("forged event for %s (core answered %d) dropped; genuine event for %s imported", forged, fstatus, genuine), nil
}

// verifyKnownIssue names the core bug that breaks the GET handshake while
// core's checkout lacks the fix (the CAIRN_WEBHOOKS stream answers first).
func (r *runner) verifyKnownIssue() string {
	if r.stack.FixWebhookVerify {
		return ""
	}
	return "core " + r.stack.CoreCommit + " lacks " + fixWebhookID + ": CAIRN_WEBHOOKS captures cairn.webhooks.*.verify"
}

// stepWebhookVerify performs the provider's subscription handshake through core.
func (r *runner) stepWebhookVerify() (string, error) {
	challenge := randomToken()
	vars := map[string]string{"CHALLENGE": challenge}
	status, body, err := r.core.webhookVerify(r.ctx, r.spec.WorkerKey(), expandMap(r.spec.Webhook.VerifyQuery, vars))
	if err != nil {
		return "", err
	}
	subjects, _ := r.stack.bus.WebhookStreamSubjects(r.ctx)
	want := expand(r.spec.Webhook.VerifyExpect, vars)
	if status != 200 || !strings.Contains(body, want) {
		return "", fmt.Errorf("GET /webhooks/%s → %d %q, want 200 containing %q (CAIRN_WEBHOOKS subjects %v)", r.spec.WorkerKey(), status, strings.TrimSpace(body), want, subjects)
	}
	return fmt.Sprintf("challenge echoed (CAIRN_WEBHOOKS subjects %v)", subjects), nil
}

// stepImportMetrics seeds samples on the mock and triggers import_metrics.
func (r *runner) stepImportMetrics() (string, error) {
	for i, key := range r.spec.Metrics.Keys {
		if err := r.prov.ctl.addMetric(r.ctx, key, time.Now().Add(-time.Duration(i+1)*24*time.Hour), metricValue(i)); err != nil {
			return "", err
		}
	}
	mark := r.stack.bus.Mark()
	if err := r.core.importHealth(r.ctx, r.acct.ID, r.spec.Metrics.Days); err != nil {
		return "", err
	}
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "import_metrics job", mark, onSubject(r.jobSubject("import_metrics"))); err != nil {
		return "", err
	}
	got := map[string][]map[string]any{}
	err := waitUntil(r.ctx, r.spec.Wait, "seeded health samples stored", func() (bool, error) {
		for _, key := range r.spec.Metrics.Keys {
			rows, err := r.core.healthSeries(r.ctx, key, r.spec.Metrics.Days+1)
			if err != nil {
				return false, err
			}
			got[key] = rows
		}
		return len(r.missingSamples(got)) == 0, nil
	})
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.Join(r.missingSamples(got), "; "))
	}
	counts := map[string]int{}
	for key, rows := range got {
		counts[key] = len(rows)
	}
	return fmt.Sprintf("seeded values found per data type, daily samples %v", counts), nil
}

// missingSamples names each data type whose series lacks the seeded sample.
func (r *runner) missingSamples(got map[string][]map[string]any) []string {
	var out []string
	for i, key := range r.spec.Metrics.Keys {
		if !seededValue(got[key], r.spec.Provider, metricValue(i)) {
			out = append(out, fmt.Sprintf("%s %v not in %s", key, metricValue(i), describeRows(got[key])))
		}
	}
	return out
}

// metricValue is the value seeded for the i-th key: distinct per key, so a
// sample filed under the wrong data type is told apart.
func metricValue(i int) float64 { return 70 + 3*float64(i) }

// stepTokenRefresh: the mock's exchange token expires inside the SDK's
// refresh skew, so the first job refreshed it; exactly once for the whole
// run, and the stale access token was never sent to the provider.
func (r *runner) stepTokenRefresh() (string, error) {
	var s mockStats
	err := waitUntil(r.ctx, 30*time.Second, "a token refresh", func() (bool, error) {
		var err error
		s, err = r.prov.ctl.stats(r.ctx)
		return s.Refreshes >= 1, err
	})
	if err != nil {
		return "", err
	}
	if s.Refreshes != 1 || s.Unauthorized != 0 {
		return "", fmt.Errorf("mock stats %+v; want exactly one refresh and no 401", s)
	}
	if !r.prov.worker.tail.Contains("refreshing OAuth token") && !r.prov.worker.tail.Contains("refresh") {
		return "", fmt.Errorf("worker log shows no refresh")
	}
	return fmt.Sprintf("1 refresh, %d exchanges, %d API calls, 0 unauthorized", s.Exchanges, s.APICalls), nil
}

// stepConformance runs cairn-core's worker-conformance against the stack.
func (r *runner) stepConformance() (string, error) {
	args := []string{"go", "run", "./cmd/worker-conformance", "--nats", r.stack.NATSURL, "--provider", r.spec.Provider,
		"--worker-name", r.spec.Worker.Name, "--timeout", "30s"}
	args = append(args, r.spec.Conformance.Flags...)
	if r.spec.Conformance.Active {
		args = append(args, "--active", "--account-id", r.acct.ID)
	}
	out, err := run(r.ctx, r.stack.CoreRoot, nil, args...)
	_ = os.WriteFile(filepath.Join(r.prov.outDir(), "conformance.log"), []byte(out), 0o600)
	if err != nil {
		if !r.stack.FixConformanceActive && onlyReconcileRoundTripFailed(out) {
			return "", &knownIssue{issue: conformanceActiveIssue, err: fmt.Errorf("worker-conformance: %w", err)}
		}
		return "", fmt.Errorf("worker-conformance: %w", err)
	}
	return strings.TrimSpace(out), nil
}

// conformanceActiveIssue: before fix(conformance), cmd/worker-conformance
// --active awaited the result on the pre-ADR-0006 subject without the pool.
const conformanceActiveIssue = "cairn-core cmd/worker-conformance --active awaits cairn.results.reconcile.<provider>.<account>; " +
	"pools publish cairn.results.reconcile.<provider>.<pool>.<account> (ADR 0006)"

// onlyReconcileRoundTripFailed reports whether the tool's FAIL lines are all
// the --active round-trip check.
func onlyReconcileRoundTripFailed(out string) bool {
	fails := 0
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "FAIL") {
			continue
		}
		if !strings.Contains(line, "reconcile round-trip") {
			return false
		}
		fails++
	}
	return fails > 0
}

// waitActivity waits until an activity with the external id is listed; an
// import queue item for it that failed ends the wait with core's reason.
func (r *runner) waitActivity(extID string) error {
	return waitUntil(r.ctx, r.spec.Wait, "activity "+extID+" in core", func() (bool, error) {
		ok, err := r.activityListed(extID)
		if ok || err != nil {
			return ok, err
		}
		if why := r.failureReasons(extID); why != "" {
			return false, fmt.Errorf("import failed%s", why)
		}
		return false, nil
	})
}

// failureReasons renders core's last_error of the account's failed queue
// items (only extID's when set), or "" when none failed.
func (r *runner) failureReasons(extID string) string {
	items, err := r.core.failedQueueItems(r.ctx, r.acct.ID)
	if err != nil {
		return ""
	}
	var out []string
	for _, it := range items {
		if extID == "" || it.ExternalID == extID {
			out = append(out, fmt.Sprintf("%s after %d attempts: %s", it.ExternalID, it.Attempts, it.LastError))
		}
	}
	if len(out) == 0 {
		return ""
	}
	if len(out) > 3 {
		out = append(out[:3], fmt.Sprintf("%d more", len(out)-3))
	}
	return "; core: " + strings.Join(out, "; ")
}

// activityListed reports whether core lists an activity with the external id.
func (r *runner) activityListed(extID string) (bool, error) {
	acts, err := r.core.listActivities(r.ctx, r.spec.Provider)
	if err != nil {
		return false, err
	}
	for _, a := range acts {
		for _, s := range a.Sources {
			if s.ExternalID == extID {
				return true, nil
			}
		}
	}
	return false, nil
}

// jobSubject is the pool's subject for a job kind (ADR 0006).
func (r *runner) jobSubject(job string) string {
	return "cairn.jobs." + job + "." + r.spec.Provider + "." + r.spec.Worker.Name
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func randomToken() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

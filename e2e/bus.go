//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Stream and bucket names, as cairn-core bootstraps them
// (internal/adapter/secondary/nats/bootstrap_streams.go).
const (
	streamJobs     = "CAIRN_JOBS"
	streamResults  = "CAIRN_RESULTS"
	streamEvents   = "CAIRN_EVENTS"
	streamDLQ      = "CAIRN_DLQ"
	streamWebhooks = "CAIRN_WEBHOOKS"

	kvWorkerPresence  = "cairn_worker_presence"
	kvWorkerManifests = "cairn_worker_manifests"
	kvRateLimits      = "cairn_rate_limits"
	kvCoreInfo        = "cairn_core"
)

// busMsg is one observed NATS message.
type busMsg struct {
	Subject string
	Data    []byte
	At      time.Time
}

// JSON decodes the body into a generic map (nil when not JSON).
func (m busMsg) JSON() map[string]any {
	var out map[string]any
	if json.Unmarshal(m.Data, &out) != nil {
		return nil
	}
	return out
}

// Field returns a top-level string field of a JSON body.
func (m busMsg) Field(key string) string {
	v, _ := m.JSON()[key].(string)
	return v
}

// busObserver watches the job, result, event and webhook subjects with
// plain subscriptions (no JetStream consumer: nothing is taken from the
// workers) so steps can assert what core and the worker published.
type busObserver struct {
	nc *nats.Conn
	js jetstream.JetStream
	mu sync.Mutex
	// msgs grows for the run; a provider run publishes a few hundred messages.
	msgs []busMsg
	subs []*nats.Subscription
}

func newBusObserver(ctx context.Context, url string) (*busObserver, error) {
	nc, err := nats.Connect(url, nats.Name("cairn-e2e"), nats.Timeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("nats %s: %w", url, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	b := &busObserver{nc: nc, js: js}
	for _, subj := range []string{"cairn.jobs.>", "cairn.results.>", "cairn.events.>", "cairn.webhooks.>", "cairn.oauth.>", "cairn.tokens.>"} {
		sub, err := nc.Subscribe(subj, func(m *nats.Msg) {
			b.mu.Lock()
			b.msgs = append(b.msgs, busMsg{Subject: m.Subject, Data: m.Data, At: time.Now()})
			b.mu.Unlock()
		})
		if err != nil {
			b.close()
			return nil, err
		}
		b.subs = append(b.subs, sub)
	}
	_ = ctx
	return b, nil
}

func (b *busObserver) close() {
	for _, s := range b.subs {
		_ = s.Unsubscribe()
	}
	b.nc.Close()
}

// Mark returns a cursor: Since(mark) sees only later messages.
func (b *busObserver) Mark() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.msgs)
}

// Since returns the messages after a Mark that match pred.
func (b *busObserver) Since(mark int, pred func(busMsg) bool) []busMsg {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []busMsg
	for _, m := range b.msgs[mark:] {
		if pred(m) {
			out = append(out, m)
		}
	}
	return out
}

// Count is the number of messages after mark matching pred.
func (b *busObserver) Count(mark int, pred func(busMsg) bool) int { return len(b.Since(mark, pred)) }

// WaitFor blocks until a message after mark matches pred.
func (b *busObserver) WaitFor(ctx context.Context, d time.Duration, what string, mark int, pred func(busMsg) bool) (busMsg, error) {
	var found busMsg
	err := waitUntil(ctx, d, what, func() (bool, error) {
		ms := b.Since(mark, pred)
		if len(ms) == 0 {
			return false, nil
		}
		found = ms[0]
		return true, nil
	})
	return found, err
}

// onSubject matches a subject prefix.
func onSubject(prefix string) func(busMsg) bool {
	return func(m busMsg) bool { return strings.HasPrefix(m.Subject, prefix) }
}

// jobWithReason matches a job on subject whose JSON "reason" equals reason.
func jobWithReason(subject, reason string) func(busMsg) bool {
	return func(m busMsg) bool { return m.Subject == subject && m.Field("reason") == reason }
}

// purge drops the core streams and buckets left by earlier runs so stale
// jobs, consumers, duplicate-window entries, presence and rate-limit
// anchors cannot leak into this one; core bootstraps them again when it
// starts and the worker recreates its consumers.
func (b *busObserver) purge(ctx context.Context) error {
	for _, name := range []string{streamJobs, streamResults, streamEvents, streamDLQ, streamWebhooks} {
		if err := b.js.DeleteStream(ctx, name); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("delete stream %s: %w", name, err)
		}
	}
	for _, bucket := range []string{kvWorkerPresence, kvWorkerManifests, kvRateLimits} {
		if err := b.js.DeleteKeyValue(ctx, bucket); err != nil && !errors.Is(err, jetstream.ErrBucketNotFound) {
			return fmt.Errorf("delete kv %s: %w", bucket, err)
		}
	}
	return nil
}

// presence is the worker heartbeat core and the conformance tool read.
type presence struct {
	WorkerName   string          `json:"worker_name"`
	WorkerKey    string          `json:"worker_key"`
	InstanceID   string          `json:"instance_id"`
	Version      string          `json:"version"`
	Release      string          `json:"release"`
	Provider     string          `json:"provider"`
	Package      string          `json:"package"`
	Webhooks     bool            `json:"webhooks"`
	Capabilities map[string]any  `json:"capabilities"`
	LastSeen     time.Time       `json:"last_seen"`
	Raw          json.RawMessage `json:"-"`
}

// Presence reads the first heartbeat of the named worker.
func (b *busObserver) Presence(ctx context.Context, workerName string) (*presence, error) {
	kv, err := b.js.KeyValue(ctx, kvWorkerPresence)
	if err != nil {
		return nil, err
	}
	keys, err := kv.Keys(ctx)
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		if !strings.HasPrefix(k, workerName+".") {
			continue
		}
		e, err := kv.Get(ctx, k)
		if err != nil {
			continue
		}
		var p presence
		if err := json.Unmarshal(e.Value(), &p); err != nil {
			return nil, fmt.Errorf("presence %s: %w", k, err)
		}
		p.Raw = e.Value()
		return &p, nil
	}
	return nil, errors.New("no presence key")
}

// CoreRelease reads cairn_core/info, the release workers check against.
func (b *busObserver) CoreRelease(ctx context.Context) (string, error) {
	kv, err := b.js.KeyValue(ctx, kvCoreInfo)
	if err != nil {
		return "", err
	}
	e, err := kv.Get(ctx, "info")
	if err != nil {
		return "", err
	}
	var info struct {
		Release string `json:"release"`
	}
	if err := json.Unmarshal(e.Value(), &info); err != nil {
		return "", err
	}
	return info.Release, nil
}

// WebhookStreamSubjects returns the CAIRN_WEBHOOKS stream's subject filters.
func (b *busObserver) WebhookStreamSubjects(ctx context.Context) ([]string, error) {
	st, err := b.js.Stream(ctx, streamWebhooks)
	if err != nil {
		return nil, err
	}
	info, err := st.Info(ctx)
	if err != nil {
		return nil, err
	}
	return info.Config.Subjects, nil
}

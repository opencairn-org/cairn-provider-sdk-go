//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// consumerIngest is core's durable consumer of CAIRN_RESULTS (cmd/server/result_router.go).
const consumerIngest = "ingest-router"

// storeIssue: before fix(ingest), core dropped every METRIC key that is not a health data type.
const storeIssue = "cairn-core before fix(ingest) drops METRIC events whose key is not a health data type"

// storedMetric is one row of GET /api/metrics.
type storedMetric struct {
	ID               string          `json:"id"`
	Key              string          `json:"key"`
	Ts               string          `json:"ts"`
	ValueNumeric     *float64        `json:"value_numeric"`
	ValueStruct      json.RawMessage `json:"value_struct"`
	Provider         string          `json:"provider"`
	ExternalAccount  string          `json:"external_account_id"`
	ExternalID       string          `json:"external_id"`
	SourceWorkerName string          `json:"source_worker_name"`
}

func (m storedMetric) hasStruct() bool {
	s := strings.TrimSpace(string(m.ValueStruct))
	return s != "" && s != "null"
}

// storedMetrics pages every metrics-store row of one account.
func (c *coreClient) storedMetrics(ctx context.Context, accountID string) ([]storedMetric, error) {
	var all []storedMetric
	to := time.Now().Add(48 * time.Hour).UTC().Format(time.RFC3339)
	for {
		var page struct {
			Metrics []storedMetric `json:"metrics"`
			Total   int            `json:"total"`
		}
		q := url.Values{"external_account_id": {accountID}, "from": {"2000-01-01T00:00:00Z"}, "to": {to},
			"sort": {"asc"}, "limit": {"200"}, "offset": {strconv.Itoa(len(all))}}
		if err := c.get(ctx, "/api/metrics?"+q.Encode(), &page); err != nil {
			return nil, err
		}
		all = append(all, page.Metrics...)
		if len(page.Metrics) == 0 || len(all) >= page.Total {
			return all, nil
		}
	}
}

// resultsIngested waits until core's result consumer has acked every
// message CAIRN_RESULTS held when it was called.
func (b *busObserver) resultsIngested(ctx context.Context, d time.Duration) error {
	stream, err := b.js.Stream(ctx, streamResults)
	if err != nil {
		return err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	last := info.State.LastSeq
	return waitUntil(ctx, d, fmt.Sprintf("core to ingest results up to seq %d", last), func() (bool, error) {
		cons, err := stream.Consumer(ctx, consumerIngest)
		if err != nil {
			return false, err
		}
		ci, err := cons.Info(ctx)
		if err != nil {
			return false, err
		}
		return ci.Delivered.Stream >= last && ci.NumAckPending == 0, nil
	})
}

func (r *runner) storeKnownIssue() string {
	if r.stack.FixMetricsStore {
		return ""
	}
	return storeIssue
}

// seedStoreKeys seeds each store key on a day the seeded health keys leave
// alone; a key the mock answers 400 for comes from the mock's own data.
func (r *runner) seedStoreKeys() error {
	back := min(len(r.spec.Metrics.Keys)+1, r.spec.Metrics.Days)
	day := time.Now().Add(-time.Duration(back) * 24 * time.Hour)
	for _, key := range r.spec.Metrics.StoreKeys {
		err := r.prov.ctl.addMetric(r.ctx, key, day, storeSeedValue(key))
		var he *httpError
		switch {
		case err == nil:
			r.storeSeeded++
		case errors.As(err, &he) && he.Status == http.StatusBadRequest:
		default:
			return fmt.Errorf("seed %s: %w", key, err)
		}
	}
	return nil
}

// storeSeedValue is a value in range for the key's unit; the step checks presence, not value.
func storeSeedValue(key string) float64 {
	if strings.HasSuffix(key, "_pct") {
		return 94
	}
	return 42
}

// stepMetricsStore checks what import_metrics left in core's metrics store:
// every store key with the worker's provenance and a stable id, no metric
// refused, and a second import updating rows instead of adding them.
func (r *runner) stepMetricsStore() (string, error) {
	want := r.spec.Metrics.StoreKeys
	var rows []storedMetric
	err := waitUntil(r.ctx, r.spec.Wait, "store keys in /api/metrics", func() (bool, error) {
		var err error
		if rows, err = r.core.storedMetrics(r.ctx, r.acct.ID); err != nil {
			return false, err
		}
		// A refusal is final: no redelivery stores that metric.
		if refused := r.refusedMetrics(); len(refused) > 0 {
			return false, errors.New("core refused " + strings.Join(refused, "; "))
		}
		return len(missingStoreKeys(rows, want)) == 0, nil
	})
	if err != nil {
		return "", fmt.Errorf("%w (missing %v)", err, missingStoreKeys(rows, want))
	}
	if refused := r.refusedMetrics(); len(refused) > 0 {
		return "", errors.New("core refused " + strings.Join(refused, "; "))
	}
	if bad := badProvenance(rows, r.spec.Provider, r.acct.ID, r.spec.Worker.Name); len(bad) > 0 {
		return "", errors.New(strings.Join(bad, "; "))
	}

	mark := r.stack.bus.Mark()
	if err := r.core.importHealth(r.ctx, r.acct.ID, r.spec.Metrics.Days); err != nil {
		return "", err
	}
	if _, err := r.stack.bus.WaitFor(r.ctx, r.spec.Wait, "second import_metrics result", mark,
		onSubject("cairn.results.import_metrics."+r.spec.Provider+".")); err != nil {
		return "", err
	}
	if err := r.stack.bus.resultsIngested(r.ctx, r.spec.Wait); err != nil {
		return "", err
	}
	again, err := r.core.storedMetrics(r.ctx, r.acct.ID)
	if err != nil {
		return "", err
	}
	if added := addedRows(rows, again); len(added) > 0 {
		return "", fmt.Errorf("re-import added %d rows instead of updating: %s", len(added), strings.Join(added, "; "))
	}
	extra := unlistedKeys(rows, want)
	msg := fmt.Sprintf("%d store keys (%d seeded through the mock), %d rows; re-import updated in place",
		len(want), r.storeSeeded, len(rows))
	if len(extra) > 0 {
		msg += fmt.Sprintf("; also stored, not in spec: %v", extra)
	}
	return msg, nil
}

// missingStoreKeys names each wanted key without a row.
func missingStoreKeys(rows []storedMetric, want []string) []string {
	have := map[string]bool{}
	for _, m := range rows {
		have[m.Key] = true
	}
	var out []string
	for _, k := range want {
		if !have[k] {
			out = append(out, k)
		}
	}
	return out
}

// unlistedKeys are stored keys the spec does not name.
func unlistedKeys(rows []storedMetric, want []string) []string {
	var out []string
	for _, m := range rows {
		if !slices.Contains(want, m.Key) && !slices.Contains(out, m.Key) {
			out = append(out, m.Key)
		}
	}
	slices.Sort(out)
	return out
}

// badProvenance describes rows that lack the worker's provider, account,
// name, or an external_id of its own (core derives "derived:<key>@<ts>"),
// and sleep_stages rows without a structured value.
func badProvenance(rows []storedMetric, provider, account, worker string) []string {
	var out []string
	for _, m := range rows {
		var why []string
		if m.Provider != provider {
			why = append(why, "provider "+m.Provider)
		}
		if m.ExternalAccount != account {
			why = append(why, "account "+m.ExternalAccount)
		}
		if m.SourceWorkerName != worker {
			why = append(why, "worker "+m.SourceWorkerName)
		}
		if m.ExternalID == "" || strings.HasPrefix(m.ExternalID, "derived:") {
			why = append(why, "no external_id of its own ("+m.ExternalID+")")
		}
		if m.Key == "sleep_stages" && !m.hasStruct() {
			why = append(why, "no value_struct")
		}
		if len(why) > 0 {
			out = append(out, fmt.Sprintf("%s@%s: %s", m.Key, m.Ts, strings.Join(why, ", ")))
		}
	}
	return out
}

// addedRows names rows of after whose id is not in before.
func addedRows(before, after []storedMetric) []string {
	seen := map[string]bool{}
	for _, m := range before {
		seen[m.ID] = true
	}
	var out []string
	for _, m := range after {
		if !seen[m.ID] {
			out = append(out, fmt.Sprintf("%s@%s (%s)", m.Key, m.Ts, m.ExternalID))
		}
	}
	return out
}

// refusedRe reads key and reason off core's "worker metric refused" line (slog text).
var refusedRe = regexp.MustCompile(`\bkey=(\S+).*?\breason=(\S+)`)

// refusedMetrics lists "key: reason" for each metric core refused from this
// run's worker, read from the part of core.log written since the run began.
func (r *runner) refusedMetrics() []string {
	f, err := os.Open(r.stack.coreLog())
	if err != nil {
		return []string{"core.log: " + err.Error()}
	}
	defer func() { _ = f.Close() }() // read-only
	if _, err := f.Seek(r.coreLogFrom, io.SeekStart); err != nil {
		return []string{"core.log: " + err.Error()}
	}
	return refusalsIn(bufio.NewScanner(f), r.spec.Provider, r.spec.Worker.Name)
}

func refusalsIn(sc *bufio.Scanner, provider, worker string) []string {
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var out []string
	counts := map[string]int{}
	for sc.Scan() {
		line := sc.Text()
		if !strings.Contains(line, `msg="worker metric refused"`) ||
			!strings.Contains(line, " provider="+provider+" ") || !strings.Contains(line, " worker="+worker+" ") {
			continue
		}
		if m := refusedRe.FindStringSubmatch(line); m != nil {
			line = m[1] + ": " + m[2]
		}
		if counts[line] == 0 {
			out = append(out, line)
		}
		counts[line]++
	}
	for i, l := range out {
		if n := counts[l]; n > 1 {
			out[i] = fmt.Sprintf("%s (×%d)", l, n)
		}
	}
	return out
}

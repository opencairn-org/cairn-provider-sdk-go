//go:build e2e

package e2e

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCheckTimezone(t *testing.T) {
	for tz, ok := range map[string]bool{
		"Europe/Berlin": true, "Etc/GMT-2": true, "Australia/Adelaide": true, "UTC": true,
		"": false, "Local": false, "UTC+2": false, "+02:00": false, "Mars/Olympus": false,
	} {
		if err := checkTimezone(tz); (err == nil) != ok {
			t.Errorf("checkTimezone(%q) = %v; want ok %v", tz, err, ok)
		}
	}
}

func TestFieldDiff(t *testing.T) {
	before := map[string]any{
		"type": "ACTIVITY_TYPE_ROW", "title": "Row", "timezone": "Etc/GMT-1",
		"summary":   map[string]any{"distanceM": 2000.0, "avgHeartRateBpm": 150.0},
		"updatedAt": "2026-11-06T08:00:00Z",
	}
	same := map[string]any{
		"type": "ACTIVITY_TYPE_ROW", "title": "Row", "timezone": "Etc/GMT-1",
		"summary":   map[string]any{"distanceM": 2000.0, "avgHeartRateBpm": 150.0},
		"updatedAt": "2026-11-06T09:00:00Z", // audit stamps are not compared
	}
	if d := fieldDiff(before, same, reparseFields); len(d) != 0 {
		t.Errorf("identical parse reported %v", d)
	}
	drift := map[string]any{
		"type": "ACTIVITY_TYPE_ROW", "title": "Row", "timezone": "UTC",
		"summary": map[string]any{"distanceM": 2100.0},
	}
	d := fieldDiff(before, drift, reparseFields)
	want := []string{"timezone: Etc/GMT-1 → UTC", "summary.avgHeartRateBpm: 150 → <unset>", "summary.distanceM: 2000 → 2100"}
	if strings.Join(d, "|") != strings.Join(want, "|") {
		t.Errorf("diff = %v; want %v", d, want)
	}
	if d := fieldDiff(map[string]any{}, map[string]any{"summary": map[string]any{"maxPowerW": 300.0}}, reparseFields); len(d) != 1 {
		t.Errorf("a summary member appearing after the re-parse = %v", d)
	}
	// Training load and the course image key are core's, not the parser's.
	if d := fieldDiff(map[string]any{"mapImageEtag": "a"}, map[string]any{"mapImageEtag": "b", "summary": map[string]any{"tss": 18.6, "intensityFactor": 0.89}}, reparseFields); len(d) != 0 {
		t.Errorf("core-derived fields reported = %v", d)
	}
}

func TestParsedViewDiff(t *testing.T) {
	a := parsedView{fields: map[string]any{}, laps: 3, channels: []string{"hr", "power"}, samples: 100}
	if d := a.diff(a); len(d) != 0 {
		t.Errorf("same view = %v", d)
	}
	b := parsedView{fields: map[string]any{}, laps: 2, channels: []string{"hr"}, samples: 100}
	if d := a.diff(b); len(d) != 2 {
		t.Errorf("laps and stream changed = %v", d)
	}
}

func TestEarlyRetry(t *testing.T) {
	t0 := time.Date(2026, 11, 6, 9, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	failed := tapCall{Method: "GET", Path: "/v1/x", Status: 429, Start: at(0), End: at(20)}
	cases := []struct {
		name  string
		calls []tapCall
		early bool
	}{
		{"retry after Retry-After", []tapCall{failed, {Path: "/v1/x", Start: at(2100), End: at(2150)}}, false},
		{"retry inside Retry-After", []tapCall{failed, {Path: "/v1/x", Start: at(60), End: at(80)}}, true},
		// A call already in flight when the 429 came back is concurrent, not a retry.
		{"concurrent call", []tapCall{{Path: "/v1/y", Start: at(5), End: at(30)}, failed, {Path: "/v1/x", Start: at(2500), End: at(2600)}}, false},
		{"recorded out of order", []tapCall{{Path: "/v1/x", Start: at(40), End: at(50)}, failed}, true},
	}
	for _, c := range cases {
		err := earlyRetry(c.calls, 2*time.Second)
		if (err != nil) != c.early {
			t.Errorf("%s: %v; want early %v", c.name, err, c.early)
		}
	}
	if err := earlyRetry(nil, time.Second); err == nil {
		t.Error("no call after arming must fail")
	}
}

// The tap forwards to the mock and records every call.
func TestAPITap(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/limited" {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, "ok "+r.URL.Path)
	}))
	defer mock.Close()
	tap, err := startTap(mock.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer tap.stop()
	m := tap.mark()
	for _, p := range []string{"/limited", "/a"} {
		resp, err := http.Get(tap.url + p) //nolint:noctx // test against a local server
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if p == "/a" && string(body) != "ok /a" {
			t.Errorf("proxied body = %q", body)
		}
	}
	calls := tap.since(m)
	if len(calls) != 2 || calls[0].Status != 429 || calls[0].Path != "/limited" || calls[1].Status != 200 {
		t.Fatalf("calls = %+v", calls)
	}
	if err := earlyRetry(calls, 2*time.Second); err == nil {
		t.Error("an immediate second call after the 429 must count as early")
	}
}

func TestSeededValue(t *testing.T) {
	rows := []map[string]any{
		{"day": "2026-11-04", "value": 73.0, "unit": "ms", "provider": "whoop"},
		{"day": "2026-11-05", "value": 70.0, "unit": "ms", "provider": "oura"},
	}
	switch {
	case !seededValue(rows, "whoop", 73):
		t.Error("the seeded sample was not found")
	case seededValue(rows, "whoop", 70):
		t.Error("another provider's sample was taken for the seeded one")
	case seededValue(rows, "whoop", 76):
		t.Error("a sample of another value was taken for the seeded one")
	case seededValue(nil, "whoop", 73):
		t.Error("an empty series has no sample")
	}
	seen := map[float64]string{}
	for i, key := range []string{"Sleep", "HRV", "RestingHR", "Steps", "Weight", "SpO2"} {
		v := metricValue(key, i)
		if other, dup := seen[v]; dup {
			t.Errorf("%s and %s are seeded with the same value %v", key, other, v)
		}
		seen[v] = key
	}
	if int(metricValue("Sleep", 0))%60 != 0 {
		t.Error("sleep must be whole minutes: some providers record no seconds")
	}
	if s := describeRows(rows[:1]); s != "[2026-11-04 73ms from whoop]" {
		t.Errorf("describeRows = %s", s)
	}
}

func TestSpecValidation(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "x.yaml")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	base := "provider: x\nworker: {package: ./w}\nmock: {package: ./m}\n"
	if _, err := LoadSpecFile(write(base + "steps: {webhook_forged: true}\n")); err == nil || !strings.Contains(err.Error(), "webhook_forged needs steps.webhook") {
		t.Errorf("forged without webhook = %v", err)
	}
	if _, err := LoadSpecFile(write(base + "skip: {webhook forged: unsigned}\nsteps: {webhook: true}\n")); err != nil {
		t.Errorf("skip reason for the forged step = %v", err)
	}
	names, err := ListSpecs("providers")
	if err != nil || len(names) == 0 {
		t.Fatalf("in-tree specs = %v, %v", names, err)
	}
	for _, n := range names {
		if _, err := LoadSpec("providers", n); err != nil {
			t.Errorf("providers/%s.yaml: %v", n, err)
		}
	}
}

//go:build e2e

package e2e

import (
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

func TestRetriedEarly(t *testing.T) {
	if err := retriedEarly(300*time.Millisecond, 2*time.Second); err == nil || !strings.Contains(err.Error(), "retried without waiting") {
		t.Errorf("an import inside Retry-After = %v", err)
	}
	for _, took := range []time.Duration{2 * time.Second, 11 * time.Second} {
		if err := retriedEarly(took, 2*time.Second); err != nil {
			t.Errorf("import after %v = %v", took, err)
		}
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
	if metricValue(0) == metricValue(1) {
		t.Error("seeded values must differ per data type")
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

//go:build e2e

package e2e

import (
	"bufio"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestMissingAndUnlistedStoreKeys(t *testing.T) {
	rows := []storedMetric{{Key: "spo2_pct"}, {Key: "acme.x"}, {Key: "spo2_pct"}, {Key: "acme.a"}}
	if got := missingStoreKeys(rows, []string{"spo2_pct", "sleep_score"}); !reflect.DeepEqual(got, []string{"sleep_score"}) {
		t.Fatalf("missing = %v", got)
	}
	if got := unlistedKeys(rows, []string{"spo2_pct"}); !reflect.DeepEqual(got, []string{"acme.a", "acme.x"}) {
		t.Fatalf("unlisted = %v", got)
	}
}

func TestBadProvenance(t *testing.T) {
	ok := storedMetric{Key: "spo2_pct", Provider: "acme", ExternalAccount: "a1", SourceWorkerName: "w", ExternalID: "spo2_pct:2026-05-01"}
	stages := ok
	stages.Key, stages.ValueStruct = "sleep_stages", json.RawMessage(`{"deep":60}`)
	if bad := badProvenance([]storedMetric{ok, stages}, "acme", "a1", "w"); len(bad) != 0 {
		t.Fatalf("valid rows flagged: %v", bad)
	}
	cases := map[string]func(*storedMetric){
		"provider other": func(m *storedMetric) { m.Provider = "other" },
		"account a2":     func(m *storedMetric) { m.ExternalAccount = "a2" },
		"worker x":       func(m *storedMetric) { m.SourceWorkerName = "x" },
		"external_id":    func(m *storedMetric) { m.ExternalID = "derived:spo2_pct@2026-05-01T00:00:00Z" },
		"value_struct":   func(m *storedMetric) { m.Key, m.ValueStruct = "sleep_stages", json.RawMessage("null") },
	}
	for want, mutate := range cases {
		m := ok
		mutate(&m)
		bad := badProvenance([]storedMetric{m}, "acme", "a1", "w")
		if len(bad) != 1 || !strings.Contains(bad[0], want) {
			t.Errorf("%s: got %v", want, bad)
		}
	}
	empty := ok
	empty.ExternalID = ""
	if bad := badProvenance([]storedMetric{empty}, "acme", "a1", "w"); len(bad) != 1 {
		t.Errorf("empty external_id not flagged")
	}
}

func TestAddedRows(t *testing.T) {
	before := []storedMetric{{ID: "1"}, {ID: "2"}}
	if got := addedRows(before, []storedMetric{{ID: "2"}, {ID: "1"}}); len(got) != 0 {
		t.Fatalf("same ids reported added: %v", got)
	}
	got := addedRows(before, []storedMetric{{ID: "1"}, {ID: "2"}, {ID: "3", Key: "spo2_pct", Ts: "2026-05-01T00:00:00Z", ExternalID: "e"}})
	if !reflect.DeepEqual(got, []string{"spo2_pct@2026-05-01T00:00:00Z (e)"}) {
		t.Fatalf("added = %v", got)
	}
}

func TestRefusalsIn(t *testing.T) {
	log := strings.Join([]string{
		`time=2026-11-06T10:00:00Z level=WARN msg="worker metric refused" provider=acme worker=acme-e2e key=Spo2 external_id=x store=metrics reason=invalid_key error="metric key invalid"`,
		`time=2026-11-06T10:00:01Z level=WARN msg="worker metric refused" provider=other worker=other-e2e key=a external_id=x store=metrics reason=invalid_key error=x`,
		`time=2026-11-06T10:00:02Z level=WARN msg="worker metric refused" provider=acme worker=acme-v2 key=b external_id=x store=metrics reason=bad_value error=x`,
		`time=2026-11-06T10:00:03Z level=INFO msg="ingested" provider=acme worker=acme-e2e key=c reason=none`,
		`time=2026-11-06T10:00:04Z level=WARN msg="worker metric refused" provider=acme worker=acme-e2e key=sleep_stages external_id=y store=metrics reason=shape_mismatch error="key=z reason=q"`,
		`time=2026-11-06T10:00:05Z level=WARN msg="worker metric refused" provider=acme worker=acme-e2e key=Spo2 external_id=z store=metrics reason=invalid_key error=x`,
	}, "\n")
	got := refusalsIn(bufio.NewScanner(strings.NewReader(log)), "acme", "acme-e2e")
	if want := []string{"Spo2: invalid_key (×2)", "sleep_stages: shape_mismatch"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("refusals = %v; want %v", got, want)
	}
}

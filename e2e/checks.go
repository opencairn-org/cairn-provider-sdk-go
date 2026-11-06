//go:build e2e

package e2e

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
	_ "time/tzdata" // the zone database core embeds
)

// checkTimezone accepts what core can render in local time: a loadable IANA
// zone name (core itself only refuses an empty one).
func checkTimezone(tz string) error {
	switch tz {
	case "":
		return errors.New("no timezone (core requires an IANA zone on every activity)")
	case "Local":
		return errors.New(`timezone "Local" is the host's zone, not an IANA name`)
	}
	if _, err := time.LoadLocation(tz); err != nil {
		return fmt.Errorf("timezone %q is not an IANA zone: %w", tz, err)
	}
	return nil
}

// reparseFields are the activity fields a re-parse of the same blob by the
// same parser must reproduce (protojson names); audit stamps, the merge
// time, the sources, the geocoded place and the course image's cache key
// (it changes with every merge) are left out.
var reparseFields = []string{
	"type", "discipline", "isVirtual", "isEbike", "isCommute", "isRace", "customSubtype",
	"title", "description", "startTime", "endTime", "elapsedDuration", "movingDuration",
	"timezone", "summary", "tags", "startLat", "startLng",
}

// coreDerived are summary members core computes itself after an import
// (training load), possibly only after the snapshot before the re-parse.
var coreDerived = map[string]bool{"tss": true, "intensityFactor": true}

// fieldDiff lists "field: before → after" for every key whose value differs;
// an object (summary) is compared per member, core-derived members aside.
func fieldDiff(before, after map[string]any, keys []string) []string {
	var out []string
	for _, k := range keys {
		b, a := before[k], after[k]
		bm, bObj := b.(map[string]any)
		am, aObj := a.(map[string]any)
		if bObj || aObj {
			for _, sub := range unionKeys(bm, am) {
				if !coreDerived[sub] && !reflect.DeepEqual(bm[sub], am[sub]) {
					out = append(out, fmt.Sprintf("%s.%s: %s → %s", k, sub, show(bm[sub]), show(am[sub])))
				}
			}
			continue
		}
		if !reflect.DeepEqual(b, a) {
			out = append(out, fmt.Sprintf("%s: %s → %s", k, show(b), show(a)))
		}
	}
	return out
}

func unionKeys(a, b map[string]any) []string {
	seen := map[string]bool{}
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func show(v any) string {
	if v == nil {
		return "<unset>"
	}
	return fmt.Sprint(v)
}

// retriedEarly reports a worker that fetched again before the provider's
// Retry-After had passed. took runs from arming the 429 to seeing the
// activity in core, so an honouring worker can never be faster.
func retriedEarly(took, retryAfter time.Duration) error {
	if took < retryAfter {
		return fmt.Errorf("activity imported %s after the 429 was armed, inside its Retry-After of %s: the worker retried without waiting",
			took.Round(time.Millisecond), retryAfter)
	}
	return nil
}

// seededValue finds the sample the harness seeded for a data type in core's
// daily series: a row from the provider whose value is the seeded one. A
// worker that files a sample under another data type (or converts its unit)
// leaves the series without it.
func seededValue(rows []map[string]any, provider string, want float64) bool {
	for _, row := range rows {
		v, ok := row["value"].(float64)
		if ok && row["provider"] == provider && math.Abs(v-want) <= 1e-6*math.Max(1, math.Abs(want)) {
			return true
		}
	}
	return false
}

// describeRows renders a series for a failure message.
func describeRows(rows []map[string]any) string {
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		parts = append(parts, fmt.Sprintf("%v %v%v from %v", row["day"], row["value"], row["unit"], row["provider"]))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

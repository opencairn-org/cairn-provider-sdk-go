package tz

import (
	"testing"
	"time"
	_ "time/tzdata" // core embeds the same database
)

// probes are instants in both hemispheres' summer, so a zone with DST shows
// each of its offsets.
var probes = []time.Time{
	time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
	time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC),
}

func observes(t *testing.T, zone string, offset time.Duration) bool {
	t.Helper()
	loc, err := time.LoadLocation(zone)
	if err != nil {
		t.Fatalf("%s: %v", zone, err)
	}
	for _, p := range probes {
		if _, sec := p.In(loc).Zone(); time.Duration(sec)*time.Second == offset {
			return true
		}
	}
	return false
}

func TestFromOffsetWholeHours(t *testing.T) {
	for h := -12; h <= 14; h++ {
		off := time.Duration(h) * time.Hour
		z := FromOffset(off)
		if !observes(t, z, off) {
			t.Errorf("FromOffset(%v) = %s, which does not observe it", off, z)
		}
	}
}

func TestFromOffsetFractional(t *testing.T) {
	for m, want := range fractional {
		off := time.Duration(m) * time.Minute
		if got := FromOffset(off); got != want {
			t.Errorf("FromOffset(%v) = %s; want %s", off, got, want)
		}
		if !observes(t, want, off) {
			t.Errorf("%s does not observe %v", want, off)
		}
	}
}

func TestFromOffsetFallback(t *testing.T) {
	for _, off := range []time.Duration{
		0, 20 * time.Minute, 15 * time.Hour, -13 * time.Hour,
		5*time.Hour + 30*time.Minute + 20*time.Second, // rounds to +05:30
	} {
		z := FromOffset(off)
		if _, err := time.LoadLocation(z); err != nil {
			t.Errorf("FromOffset(%v) = %s: %v", off, z, err)
		}
	}
	if z := FromOffset(20 * time.Minute); z != "UTC" {
		t.Errorf("unknown fractional offset = %s; want UTC", z)
	}
	if z := FromOffset(5*time.Hour + 30*time.Minute + 20*time.Second); z != "Asia/Kolkata" {
		t.Errorf("rounded +05:30 = %s; want Asia/Kolkata", z)
	}
}

func TestFromLocal(t *testing.T) {
	utc := time.Date(2026, 3, 1, 6, 0, 0, 0, time.UTC)
	cases := []struct {
		local time.Time
		want  string
	}{
		{time.Date(2026, 3, 1, 7, 0, 0, 0, time.UTC), "Etc/GMT-1"},
		{time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC), "Etc/GMT+5"},
		{time.Date(2026, 3, 1, 11, 30, 0, 0, time.UTC), "Asia/Kolkata"},
		// A local time carrying a zone is read as wall clock only.
		{time.Date(2026, 3, 1, 8, 0, 0, 0, time.FixedZone("x", 3600)), "Etc/GMT-2"},
	}
	for _, c := range cases {
		if got := FromLocal(c.local, utc); got != c.want {
			t.Errorf("FromLocal(%v) = %s; want %s", c.local, got, c.want)
		}
	}
}

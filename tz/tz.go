// Package tz names an IANA zone for a bare UTC offset. Core requires a zone
// on every activity, and several providers report only an offset.
package tz

import (
	"fmt"
	"time"
)

// fractional maps the non-whole-hour offsets in use (minutes east of UTC) to a
// zone that observes them.
var fractional = map[int]string{
	-570: "Pacific/Marquesas",   // -09:30
	-210: "America/St_Johns",    // -03:30
	-150: "America/St_Johns",    // -02:30, Newfoundland summer
	210:  "Asia/Tehran",         // +03:30
	270:  "Asia/Kabul",          // +04:30
	330:  "Asia/Kolkata",        // +05:30
	345:  "Asia/Kathmandu",      // +05:45
	390:  "Asia/Yangon",         // +06:30
	525:  "Australia/Eucla",     // +08:45
	570:  "Australia/Darwin",    // +09:30
	630:  "Australia/Lord_Howe", // +10:30
	765:  "Pacific/Chatham",     // +12:45
	825:  "Pacific/Chatham",     // +13:45, Chatham summer
}

// FromOffset names a zone for an offset east of UTC, rounded to the minute:
// Etc/GMT∓h for whole hours (POSIX inverts the sign), a zone observing the
// offset for the fractional ones in use, and "UTC" for anything else.
func FromOffset(offset time.Duration) string {
	m := int(offset.Round(time.Minute) / time.Minute)
	if m%60 != 0 {
		if z, ok := fractional[m]; ok {
			return z
		}
		return "UTC"
	}
	h := m / 60
	if h == 0 || h < -12 || h > 14 {
		return "UTC"
	}
	return fmt.Sprintf("Etc/GMT%+d", -h)
}

// FromLocal names the zone of a provider's local wall-clock time recorded
// next to the same instant in UTC (FIT local_timestamp, "start_date_local").
func FromLocal(local, utc time.Time) string {
	l := time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), local.Minute(), local.Second(), 0, time.UTC)
	u := utc.UTC().Truncate(time.Second)
	return FromOffset(l.Sub(u))
}

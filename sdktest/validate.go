package sdktest

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // core embeds the same database

	cairnv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/v1"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
)

// ValidateActivity runs the checks of core's Activity.Validate that an
// imported activity decides: a known type, end not before start, durations
// not negative, moving time within elapsed time, and a loadable IANA zone.
// Core rejects the import when one fails. Discipline/type compatibility is
// core's merge logic and is not repeated here.
func ValidateActivity(ia *workerv1.ImportedActivity) error {
	p := ia.GetPayload()
	if p == nil {
		return errors.New("payload is missing")
	}
	if _, known := cairnv1.ActivityType_name[int32(p.GetType())]; !known || p.GetType() == cairnv1.ActivityType_ACTIVITY_TYPE_UNSPECIFIED {
		return fmt.Errorf("type %v is invalid", p.GetType())
	}
	if p.GetStartTime() != nil && p.GetEndTime() != nil && p.GetEndTime().AsTime().Before(p.GetStartTime().AsTime()) {
		return errors.New("end_time precedes start_time")
	}
	elapsed, moving := p.GetElapsedDuration().AsDuration(), p.GetMovingDuration().AsDuration()
	if elapsed < 0 || moving < 0 {
		return errors.New("duration is negative")
	}
	if moving > elapsed {
		return fmt.Errorf("moving_duration %v exceeds elapsed_duration %v", moving, elapsed)
	}
	if p.GetTimezone() == "" {
		return errors.New("timezone is required")
	}
	if _, err := time.LoadLocation(p.GetTimezone()); err != nil {
		return fmt.Errorf("timezone %q is not an IANA zone: %w", p.GetTimezone(), err)
	}
	return nil
}

// checkActivities fails the test for every activity core would reject.
func checkActivities(t testing.TB, jr *workerv1.JobResult) {
	t.Helper()
	for _, ev := range jr.GetEvents() {
		ia := ev.GetActivity()
		if ia == nil {
			continue
		}
		if err := ValidateActivity(ia); err != nil {
			t.Errorf("sdktest: core would reject activity %q: %v", ia.GetRef().GetExternalId(), err)
		}
	}
}

// healthTypes are the data types core files in its daily health series,
// with the reserved store key a worker must not send alongside each.
var healthTypes = map[string]string{
	"Steps": "steps", "HRV": "hrv_rmssd_ms", "RestingHR": "resting_hr_bpm",
	"Sleep": "sleep_total_min", "Weight": "weight_kg", "WaterIntake": "",
}

// metricKeyRe and maxMetricKeyLen mirror core's domain.ValidateMetricKey.
var metricKeyRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_.]*$`)

const maxMetricKeyLen = 64

// structMetricKeys and reservedNumericMetricKeys mirror core's
// internal/domain/metric_query.go: the shape each reserved key must carry.
var (
	structMetricKeys          = map[string]bool{"sleep_stages": true}
	reservedNumericMetricKeys = map[string]bool{
		"weight_kg": true, "body_fat_pct": true, "lean_mass_kg": true, "bone_mass_kg": true, "hydration_pct": true, "muscle_mass_kg": true,
		"resting_hr_bpm": true, "max_hr_bpm": true, "hrv_rmssd_ms": true, "hrv_sdnn_ms": true,
		"blood_pressure_systolic_mmhg": true, "blood_pressure_diastolic_mmhg": true, "spo2_pct": true,
		"steps": true, "floors_climbed": true, "active_minutes": true, "calories_burned_kcal": true,
		"sleep_total_min": true, "sleep_deep_min": true, "sleep_rem_min": true, "sleep_light_min": true, "sleep_awake_min": true, "sleep_score": true,
		"vo2max_ml_kg_min": true, "ftp_cycling_w": true, "lthr_running_bpm": true, "lthr_cycling_bpm": true,
		"threshold_pace_running_s_per_km": true, "css_swimming_s_per_100m": true, "critical_power_w": true, "w_prime_kj": true,
		"tss": true, "ctl": true, "atl": true, "tsb": true,
		"mood": true, "energy": true, "soreness": true, "stress": true, "motivation": true,
	}
)

// ValidateMetric runs the checks core's metrics.Store.Import applies to a
// METRIC event: user and account set, a timestamp, exactly one value, a key
// that is a health data type or valid store key, the shape a reserved key
// dictates, and valid JSON. It also requires the external_id core would
// otherwise derive from key and timestamp, so a re-import upserts.
func ValidateMetric(im *workerv1.ImportedMetric) error {
	ref := im.GetRef()
	if ref.GetUserId() == "" {
		return errors.New("ref.user_id is required")
	}
	if ref.GetExternalAccountId() == "" {
		return errors.New("ref.external_account_id is required")
	}
	if ref.GetExternalId() == "" {
		return errors.New("ref.external_id is required for a stable upsert")
	}
	if im.GetTs() == nil || im.GetTs().AsTime().IsZero() {
		return errors.New("ts is required")
	}
	hasNum, hasJSON := im.ValueNumeric != nil, strings.TrimSpace(im.GetValueJson()) != ""
	if hasNum == hasJSON {
		return errors.New("exactly one of value_numeric and value_json is required")
	}
	if hasJSON && !json.Valid([]byte(im.GetValueJson())) {
		return errors.New("value_json is not valid JSON")
	}
	key := im.GetKey()
	if _, ok := healthTypes[key]; ok {
		return nil
	}
	if len(key) > maxMetricKeyLen || !metricKeyRe.MatchString(key) {
		return fmt.Errorf("key %q is neither a health data type nor a valid store key", key)
	}
	if (structMetricKeys[key] && hasNum) || (reservedNumericMetricKeys[key] && hasJSON) {
		return fmt.Errorf("key %q carries the wrong value shape", key)
	}
	return nil
}

// checkMetrics fails the test for every metric core would refuse or
// duplicate, and for a value sent both as a health type and its store twin.
func checkMetrics(t testing.TB, jr *workerv1.JobResult) {
	t.Helper()
	keys := map[string]bool{}
	for _, ev := range jr.GetEvents() {
		im := ev.GetMetric()
		if im == nil {
			continue
		}
		keys[im.GetKey()] = true
		if err := ValidateMetric(im); err != nil {
			t.Errorf("sdktest: metric %q at %v breaks core's import contract: %v", im.GetKey(), im.GetTs().AsTime(), err)
		}
	}
	for health, twin := range healthTypes {
		if twin != "" && keys[health] && keys[twin] {
			t.Errorf("sdktest: metric %s is sent both as %s and as %s; send the data type only", twin, health, twin)
		}
	}
}

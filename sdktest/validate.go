package sdktest

import (
	"errors"
	"fmt"
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

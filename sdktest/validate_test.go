package sdktest

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cairnv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/v1"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
)

func TestValidateActivity(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 7, 0, 0, 0, time.UTC)
	valid := func() *cairnv1.ActivitySourcePayload {
		return &cairnv1.ActivitySourcePayload{
			Type:            cairnv1.ActivityType_ACTIVITY_TYPE_RUN,
			StartTime:       timestamppb.New(t0),
			EndTime:         timestamppb.New(t0.Add(time.Hour)),
			ElapsedDuration: durationpb.New(time.Hour),
			MovingDuration:  durationpb.New(50 * time.Minute),
			Timezone:        "Europe/Berlin",
		}
	}
	cases := []struct {
		name   string
		mutate func(*cairnv1.ActivitySourcePayload)
		want   string
	}{
		{"valid", func(*cairnv1.ActivitySourcePayload) {}, ""},
		{"etc zone", func(p *cairnv1.ActivitySourcePayload) { p.Timezone = "Etc/GMT-2" }, ""},
		{"no timezone", func(p *cairnv1.ActivitySourcePayload) { p.Timezone = "" }, "timezone is required"},
		{"bad timezone", func(p *cairnv1.ActivitySourcePayload) { p.Timezone = "UTC+2" }, "not an IANA zone"},
		{"unspecified type", func(p *cairnv1.ActivitySourcePayload) { p.Type = cairnv1.ActivityType_ACTIVITY_TYPE_UNSPECIFIED }, "type"},
		{"unknown type", func(p *cairnv1.ActivitySourcePayload) { p.Type = 9999 }, "type"},
		{"end before start", func(p *cairnv1.ActivitySourcePayload) { p.EndTime = timestamppb.New(t0.Add(-time.Minute)) }, "precedes"},
		{"negative", func(p *cairnv1.ActivitySourcePayload) { p.ElapsedDuration = durationpb.New(-time.Second) }, "negative"},
		{"moving > elapsed", func(p *cairnv1.ActivitySourcePayload) { p.MovingDuration = durationpb.New(2 * time.Hour) }, "exceeds"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := valid()
			c.mutate(p)
			err := ValidateActivity(&workerv1.ImportedActivity{Payload: p})
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
				t.Fatalf("error %v; want it to mention %q", err, c.want)
			}
		})
	}
	if err := ValidateActivity(&workerv1.ImportedActivity{}); err == nil {
		t.Fatal("missing payload accepted")
	}
}

// recTB records the errors a check reports instead of failing the test.
type recTB struct {
	testing.TB
	errs []string
}

func (r *recTB) Helper() {}
func (r *recTB) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

// checkActivities flags each activity core would reject and only those.
func TestCheckActivities(t *testing.T) {
	ok := &cairnv1.ActivitySourcePayload{Type: cairnv1.ActivityType_ACTIVITY_TYPE_RUN, Timezone: "Etc/GMT-1"}
	bad := &cairnv1.ActivitySourcePayload{Type: cairnv1.ActivityType_ACTIVITY_TYPE_RUN}
	activity := func(id string, p *cairnv1.ActivitySourcePayload) *workerv1.WorkerEvent {
		return &workerv1.WorkerEvent{Entity: &workerv1.WorkerEvent_Activity{Activity: &workerv1.ImportedActivity{Ref: &workerv1.ExternalRef{ExternalId: id}, Payload: p}}}
	}
	rec := &recTB{TB: t}
	checkActivities(rec, &workerv1.JobResult{Events: []*workerv1.WorkerEvent{activity("good", ok), {}, activity("no-zone", bad)}})
	if len(rec.errs) != 1 || !strings.Contains(rec.errs[0], `"no-zone"`) || !strings.Contains(rec.errs[0], "timezone is required") {
		t.Errorf("reported %q; want the activity without a timezone only", rec.errs)
	}
}

func TestValidateMetric(t *testing.T) {
	ts := timestamppb.New(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
	num := func(v float64) *float64 { return &v }
	valid := func(key string) *workerv1.ImportedMetric {
		return &workerv1.ImportedMetric{
			Ref: &workerv1.ExternalRef{UserId: "u", Provider: "acme", ExternalAccountId: "a", ExternalId: key + ":2026-05-01"},
			Key: key, Ts: ts, ValueNumeric: num(1),
		}
	}
	cases := []struct {
		name   string
		metric func() *workerv1.ImportedMetric
		want   string
	}{
		{"health type", func() *workerv1.ImportedMetric { return valid("HRV") }, ""},
		{"reserved key", func() *workerv1.ImportedMetric { return valid("spo2_pct") }, ""},
		{"namespaced key", func() *workerv1.ImportedMetric { return valid("acme.recovery_score") }, ""},
		{"struct key", func() *workerv1.ImportedMetric {
			m := valid("sleep_stages")
			m.ValueNumeric, m.ValueJson = nil, `{"deep":60}`
			return m
		}, ""},
		{"no user", func() *workerv1.ImportedMetric { m := valid("HRV"); m.Ref.UserId = ""; return m }, "user_id"},
		{"no account", func() *workerv1.ImportedMetric { m := valid("HRV"); m.Ref.ExternalAccountId = ""; return m }, "external_account_id"},
		{"no external id", func() *workerv1.ImportedMetric { m := valid("HRV"); m.Ref.ExternalId = ""; return m }, "external_id"},
		{"no ts", func() *workerv1.ImportedMetric { m := valid("HRV"); m.Ts = nil; return m }, "ts"},
		{"no value", func() *workerv1.ImportedMetric { m := valid("HRV"); m.ValueNumeric = nil; return m }, "exactly one"},
		{"both values", func() *workerv1.ImportedMetric { m := valid("acme.x"); m.ValueJson = `{}`; return m }, "exactly one"},
		{"bad json", func() *workerv1.ImportedMetric {
			m := valid("acme.x")
			m.ValueNumeric, m.ValueJson = nil, `{`
			return m
		}, "not valid JSON"},
		{"upper-case key", func() *workerv1.ImportedMetric { return valid("Spo2") }, "valid store key"},
		{"long key", func() *workerv1.ImportedMetric { return valid("acme." + strings.Repeat("x", 60)) }, "valid store key"},
		{"number for struct key", func() *workerv1.ImportedMetric { return valid("sleep_stages") }, "shape"},
		{"json for reserved key", func() *workerv1.ImportedMetric {
			m := valid("sleep_score")
			m.ValueNumeric, m.ValueJson = nil, `80`
			return m
		}, "shape"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateMetric(c.metric())
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
				t.Fatalf("error %v; want it to mention %q", err, c.want)
			}
		})
	}
}

// checkMetrics flags refused metrics and a value sent under both homes.
func TestCheckMetrics(t *testing.T) {
	ts := timestamppb.New(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
	metric := func(key, extID string) *workerv1.WorkerEvent {
		v := 1.0
		return &workerv1.WorkerEvent{Entity: &workerv1.WorkerEvent_Metric{Metric: &workerv1.ImportedMetric{
			Ref: &workerv1.ExternalRef{UserId: "u", ExternalAccountId: "a", ExternalId: extID}, Key: key, Ts: ts, ValueNumeric: &v,
		}}}
	}
	rec := &recTB{TB: t}
	checkMetrics(rec, &workerv1.JobResult{Events: []*workerv1.WorkerEvent{metric("HRV", "h"), metric("spo2_pct", "s"), {}}})
	if len(rec.errs) != 0 {
		t.Fatalf("valid metrics flagged: %v", rec.errs)
	}
	checkMetrics(rec, &workerv1.JobResult{Events: []*workerv1.WorkerEvent{metric("Weight", "w"), metric("weight_kg", "k"), metric("x", "")}})
	if len(rec.errs) != 2 || !strings.Contains(strings.Join(rec.errs, "\n"), "both as Weight and as weight_kg") {
		t.Fatalf("errors %q; want the missing external_id and the twin", rec.errs)
	}
}

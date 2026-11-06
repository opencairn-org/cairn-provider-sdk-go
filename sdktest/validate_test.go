package sdktest

import (
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

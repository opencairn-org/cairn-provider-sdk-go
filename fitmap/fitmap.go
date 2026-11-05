// Package fitmap turns activity files into cairn.worker.v1.ImportedActivity:
// FIT (the native format of Garmin, Wahoo, Hammerhead, COROS, Polar and
// most file exports) through github.com/muktihari/fit, and GPX through
// github.com/tkrajina/gpxgo.
//
// The mapper fills Payload (type and discipline from the FIT sport, title,
// start and end, elapsed and moving durations, summary, laps, stream
// summary) and Stream (time, GPS, altitude, distance, speed, heart rate,
// power, cadence, temperature, grade as column arrays of equal length).
// Ref, raw_blob_id and attachments are the worker's: it knows the account
// and the provider id, and it archived the bytes it is about to map.
//
//	res, err := fitmap.FromFIT(bytes.NewReader(raw))
//	act := res.Activity
//	act.Ref = job.Ref()
//	act.RawBlobId, act.RawContentType, act.RawSizeBytes = blobID, fitmap.ContentTypeFIT, int64(len(raw))
//
// Columns are forward-filled over samples that lack a value (a GPS gap, a
// dropped heart-rate sensor) and back-filled before the first value, so
// every present channel has sample_count entries, as the proto requires.
// A multisport FIT file maps its first session; Info.Sessions tells the
// caller when there are more. Time zones: FIT carries no zone, only an
// optional local-time offset, which lands in Payload.Extra["utc_offset"].
package fitmap

import (
	"math"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cairnv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/v1"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
)

// Content types of the archived raw file.
const (
	ContentTypeFIT = "application/vnd.ant.fit"
	ContentTypeGPX = "application/gpx+xml"
)

// Result is a mapped file.
type Result struct {
	// Activity has Payload and Stream set; Ref and the raw-blob fields are
	// left for the caller.
	Activity *workerv1.ImportedActivity
	Info     Info
}

// Info is what the caller may want for ids and logging.
type Info struct {
	// Format is "fit" or "gpx".
	Format string
	// StartTime is the activity start (UTC).
	StartTime time.Time
	// NativeType is the file's own sport label ("cycling/gravel_cycling",
	// a GPX track type), also stored as Payload.ProviderNativeType.
	NativeType string
	// Sessions counts FIT sessions (1 for single-sport); 0 for GPX.
	Sessions int
	// Samples is the stream length.
	Samples int
	// Device identity from the FIT file_id (zero for GPX): with
	// TimeCreated it makes a stable external id when the provider has none.
	Manufacturer string
	Product      string
	SerialNumber uint32
	TimeCreated  time.Time
}

// columns accumulates the stream channels sample by sample.
type columns struct {
	n        int
	timeS    []float64
	lat, lon column[float64]
	alt      column[float64]
	dist     column[float64]
	speed    column[float64]
	hr       column[int32]
	power    column[int32]
	cadence  column[int32]
	temp     column[float64]
	grade    column[float64]
}

// column is one channel with per-sample presence.
type column[T int32 | float64] struct {
	vals  []T
	valid []bool
	any   bool
}

func (c *column[T]) add(v T, ok bool) {
	c.vals = append(c.vals, v)
	c.valid = append(c.valid, ok)
	c.any = c.any || ok
}

// filled returns the values with gaps forward-filled and the leading gap
// back-filled from the first value; nil when the channel is empty.
func (c *column[T]) filled() []T {
	if !c.any {
		return nil
	}
	out := make([]T, len(c.vals))
	first := -1
	for i, ok := range c.valid {
		if ok {
			first = i
			break
		}
	}
	last := c.vals[first]
	for i := range c.vals {
		if c.valid[i] {
			last = c.vals[i]
		}
		out[i] = last
	}
	return out
}

// pair adds a lat/lon sample; both must be present to count.
func (c *columns) pair(lat, lon float64, ok bool) {
	c.lat.add(lat, ok)
	c.lon.add(lon, ok)
}

// stream builds the proto stream; nil without samples.
func (c *columns) stream() *cairnv1.ActivityStream {
	if c.n == 0 {
		return nil
	}
	st := &cairnv1.ActivityStream{SampleCount: clampI32(c.n), ResolutionHz: resolution(c.timeS), TimeS: c.timeS}
	add := func(ch cairnv1.StreamChannel, present bool) {
		if present {
			st.Channels = append(st.Channels, ch)
		}
	}
	add(cairnv1.StreamChannel_STREAM_CHANNEL_TIME, true)
	if st.Latitude, st.Longitude = c.lat.filled(), c.lon.filled(); st.Latitude != nil {
		add(cairnv1.StreamChannel_STREAM_CHANNEL_LATITUDE, true)
		add(cairnv1.StreamChannel_STREAM_CHANNEL_LONGITUDE, true)
	}
	st.AltitudeM = c.alt.filled()
	add(cairnv1.StreamChannel_STREAM_CHANNEL_ALTITUDE, st.AltitudeM != nil)
	st.DistanceM = c.dist.filled()
	add(cairnv1.StreamChannel_STREAM_CHANNEL_DISTANCE, st.DistanceM != nil)
	st.SpeedMps = c.speed.filled()
	add(cairnv1.StreamChannel_STREAM_CHANNEL_SPEED, st.SpeedMps != nil)
	st.HeartRateBpm = c.hr.filled()
	add(cairnv1.StreamChannel_STREAM_CHANNEL_HEART_RATE, st.HeartRateBpm != nil)
	st.PowerW = c.power.filled()
	add(cairnv1.StreamChannel_STREAM_CHANNEL_POWER, st.PowerW != nil)
	st.Cadence = c.cadence.filled()
	add(cairnv1.StreamChannel_STREAM_CHANNEL_CADENCE, st.Cadence != nil)
	st.TemperatureC = c.temp.filled()
	add(cairnv1.StreamChannel_STREAM_CHANNEL_TEMPERATURE, st.TemperatureC != nil)
	st.Grade = c.grade.filled()
	add(cairnv1.StreamChannel_STREAM_CHANNEL_GRADE, st.Grade != nil)
	return st
}

// resolution is the mean sample rate in Hz.
func resolution(timeS []float64) float64 {
	if len(timeS) < 2 {
		return 1
	}
	span := timeS[len(timeS)-1] - timeS[0]
	if span <= 0 {
		return 1
	}
	return math.Round(float64(len(timeS)-1)/span*1000) / 1000
}

// streamSummary describes the stream on the payload.
func streamSummary(st *cairnv1.ActivityStream) *cairnv1.StreamSummary {
	if st == nil {
		return nil
	}
	return &cairnv1.StreamSummary{
		Channels:     st.GetChannels(),
		SampleCount:  st.GetSampleCount(),
		SampleRateHz: st.GetResolutionHz(),
		HasGps:       st.GetLatitude() != nil,
	}
}

// fillFromStream completes summary fields the file does not state:
// elevation and temperature extremes, max values of heart rate, power,
// cadence and speed.
func fillFromStream(s *cairnv1.ActivitySummary, st *cairnv1.ActivityStream) {
	if st == nil {
		return
	}
	if s.MinElevationM == nil && st.GetAltitudeM() != nil {
		lo, hi := minMaxF(st.GetAltitudeM())
		s.MinElevationM, s.MaxElevationM = &lo, &hi
	}
	if s.MaxSpeedMps == nil && st.GetSpeedMps() != nil {
		_, hi := minMaxF(st.GetSpeedMps())
		s.MaxSpeedMps = &hi
	}
	if s.MinTemperatureC == nil && st.GetTemperatureC() != nil {
		lo, hi := minMaxF(st.GetTemperatureC())
		s.MinTemperatureC, s.MaxTemperatureC = &lo, &hi
	}
	if s.MaxHeartRateBpm == nil && st.GetHeartRateBpm() != nil {
		s.MaxHeartRateBpm = maxI(st.GetHeartRateBpm())
	}
	if s.MaxPowerW == nil && st.GetPowerW() != nil {
		s.MaxPowerW = maxI(st.GetPowerW())
	}
	if s.MaxCadence == nil && st.GetCadence() != nil {
		s.MaxCadence = maxI(st.GetCadence())
	}
}

func minMaxF(v []float64) (lo, hi float64) {
	lo, hi = v[0], v[0]
	for _, x := range v[1:] {
		lo, hi = min(lo, x), max(hi, x)
	}
	return lo, hi
}

func maxI(v []int32) *int32 {
	hi := v[0]
	for _, x := range v[1:] {
		hi = max(hi, x)
	}
	return &hi
}

// meanI is the rounded mean of the channel.
func meanI(v []int32) *int32 {
	if len(v) == 0 {
		return nil
	}
	var sum int64
	for _, x := range v {
		sum += int64(x)
	}
	m := clampI32(int(math.Round(float64(sum) / float64(len(v)))))
	return &m
}

func meanF(v []float64) *float64 {
	if len(v) == 0 {
		return nil
	}
	var sum float64
	for _, x := range v {
		sum += x
	}
	m := round3(sum / float64(len(v)))
	return &m
}

func round3(v float64) float64 { return math.Round(v*1000) / 1000 }

func clampI32(n int) int32 {
	return int32(min(max(n, math.MinInt32), math.MaxInt32))
}

// optF returns a pointer unless v is NaN/invalid.
func optF(v float64, ok bool) *float64 {
	if !ok || math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	v = round3(v)
	return &v
}

func optI(v int, ok bool) *int32 {
	if !ok {
		return nil
	}
	i := clampI32(v)
	return &i
}

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t.UTC())
}

func dur(d time.Duration) *durationpb.Duration {
	if d <= 0 {
		return nil
	}
	return durationpb.New(d)
}

func seconds(s float64) time.Duration {
	if math.IsNaN(s) || s <= 0 {
		return 0
	}
	return time.Duration(s * float64(time.Second))
}

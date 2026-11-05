package fitmap

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/muktihari/fit/decoder"
	"github.com/muktihari/fit/kit/datetime"
	"github.com/muktihari/fit/profile/basetype"
	"github.com/muktihari/fit/profile/filedef"
	"github.com/muktihari/fit/profile/mesgdef"
	"github.com/muktihari/fit/profile/typedef"

	cairnv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/v1"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
)

// ErrNotActivity is returned for FIT files of another type (a course, a
// workout, a settings file).
var ErrNotActivity = errors.New("fitmap: not an activity FIT file")

// ErrNoData is returned for a file without sessions or records.
var ErrNoData = errors.New("fitmap: no session or record data")

// FromFIT decodes the first FIT file in r (chained files are rare and
// ignored) and maps it. A corrupt or truncated file is an error; checksum
// errors are not tolerated.
func FromFIT(r io.Reader) (*Result, error) {
	fit, err := decoder.New(r).Decode()
	if err != nil {
		return nil, fmt.Errorf("fitmap: decode FIT: %w", err)
	}
	act := filedef.NewActivity(fit.Messages...)
	if act.FileId.Type != typedef.FileActivity {
		return nil, ErrNotActivity
	}
	if len(act.Sessions) == 0 && len(act.Records) == 0 {
		return nil, ErrNoData
	}
	sort.Slice(act.Records, func(i, j int) bool { return act.Records[i].Timestamp.Before(act.Records[j].Timestamp) })
	session := sessionOf(act)
	start := session.StartTime
	if start.IsZero() && len(act.Records) > 0 {
		start = act.Records[0].Timestamp
	}

	cols := recordColumns(act.Records, start)
	stream := cols.stream()
	payload := sessionPayload(session, act, start)
	payload.Summary = sessionSummary(session, stream)
	payload.Laps = laps(act.Laps, start)
	payload.StreamSummary = streamSummary(stream)

	info := fileInfo(act)
	info.StartTime, info.NativeType, info.Sessions, info.Samples = start.UTC(), payload.ProviderNativeType, len(act.Sessions), cols.n
	return &Result{Activity: &workerv1.ImportedActivity{Payload: payload, Stream: stream}, Info: info}, nil
}

// sessionOf is the first session, or one synthesised from the records.
func sessionOf(act *filedef.Activity) *mesgdef.Session {
	if len(act.Sessions) > 0 {
		return act.Sessions[0]
	}
	s := mesgdef.NewSession(nil)
	first, last := act.Records[0], act.Records[len(act.Records)-1]
	s.StartTime, s.Timestamp = first.Timestamp, last.Timestamp
	s.SetTotalElapsedTime(uint32(last.Timestamp.Sub(first.Timestamp).Milliseconds())) //nolint:gosec // G115: activity durations fit
	if d := last.DistanceScaled(); !math.IsNaN(d) {
		s.SetTotalDistance(uint32(d * 100)) //nolint:gosec // G115: metres ×100 fit
	}
	return s
}

func fileInfo(act *filedef.Activity) Info {
	info := Info{Format: "fit", SerialNumber: act.FileId.SerialNumber, TimeCreated: act.FileId.TimeCreated.UTC()}
	if act.FileId.Manufacturer != typedef.ManufacturerInvalid {
		info.Manufacturer = act.FileId.Manufacturer.String()
	}
	if _, value := act.FileId.GetProduct(); act.FileId.Product != basetype.Uint16Invalid {
		info.Product = productName(value, act.FileId)
	}
	if act.FileId.SerialNumber == basetype.Uint32zInvalid {
		info.SerialNumber = 0
	}
	return info
}

// productName prefers the file's product_name, then the profile's name of
// a known device (typedef.GarminProduct), else the numeric product id.
func productName(value any, id mesgdef.FileId) string {
	if id.ProductName != "" {
		return id.ProductName
	}
	if s, ok := value.(fmt.Stringer); ok && !strings.Contains(s.String(), "(") {
		return s.String()
	}
	return strconv.Itoa(int(id.Product))
}

// sessionPayload maps the session header: type, times, durations.
func sessionPayload(s *mesgdef.Session, act *filedef.Activity, start time.Time) *cairnv1.ActivitySourcePayload {
	kind := mapSport(s.Sport, s.SubSport)
	elapsed := seconds(s.TotalElapsedTimeScaled())
	moving := seconds(s.TotalTimerTimeScaled())
	if mv := seconds(s.TotalMovingTimeScaled()); mv > 0 {
		moving = mv
	}
	p := &cairnv1.ActivitySourcePayload{
		Type:               kind.typ,
		Discipline:         kind.discipline,
		IsVirtual:          kind.virtual,
		IsEbike:            kind.ebike,
		IsCommute:          s.SubSport == typedef.SubSportCommuting,
		ProviderNativeType: nativeType(s.Sport, s.SubSport),
		Title:              s.SportProfileName,
		StartTime:          ts(start),
		ElapsedDuration:    dur(elapsed),
		MovingDuration:     dur(moving),
		Extra:              map[string]string{},
	}
	if elapsed > 0 {
		p.EndTime = ts(start.Add(elapsed))
	} else if !s.Timestamp.IsZero() {
		p.EndTime = ts(s.Timestamp)
	}
	if kind.typ == cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT {
		p.CustomSubtype = p.ProviderNativeType
	}
	if act.Activity != nil && !act.Activity.LocalTimestamp.IsZero() && !act.Activity.Timestamp.IsZero() {
		p.Extra["utc_offset"] = strconv.Itoa(datetime.TzOffsetHours(act.Activity.LocalTimestamp, act.Activity.Timestamp))
	}
	if len(p.Extra) == 0 {
		p.Extra = nil
	}
	return p
}

// nativeType renders "sport" or "sport/sub_sport".
func nativeType(sport typedef.Sport, sub typedef.SubSport) string {
	if sport == typedef.SportInvalid {
		return ""
	}
	if sub == typedef.SubSportInvalid || sub == typedef.SubSportGeneric {
		return sport.String()
	}
	return sport.String() + "/" + sub.String()
}

// sessionSummary maps the session totals and completes them from the stream.
func sessionSummary(s *mesgdef.Session, stream *cairnv1.ActivityStream) *cairnv1.ActivitySummary {
	sum := &cairnv1.ActivitySummary{
		DistanceM:        optF(s.TotalDistanceScaled(), true),
		ElevationGainM:   optF(float64(s.TotalAscent), s.TotalAscent != basetype.Uint16Invalid),
		ElevationLossM:   optF(float64(s.TotalDescent), s.TotalDescent != basetype.Uint16Invalid),
		AvgSpeedMps:      optF(firstValid(s.EnhancedAvgSpeedScaled(), s.AvgSpeedScaled()), true),
		MaxSpeedMps:      optF(firstValid(s.EnhancedMaxSpeedScaled(), s.MaxSpeedScaled()), true),
		AvgHeartRateBpm:  optI(int(s.AvgHeartRate), s.AvgHeartRate != basetype.Uint8Invalid),
		MaxHeartRateBpm:  optI(int(s.MaxHeartRate), s.MaxHeartRate != basetype.Uint8Invalid),
		AvgPowerW:        optI(int(s.AvgPower), s.AvgPower != basetype.Uint16Invalid),
		MaxPowerW:        optI(int(s.MaxPower), s.MaxPower != basetype.Uint16Invalid),
		NormalizedPowerW: optI(int(s.NormalizedPower), s.NormalizedPower != basetype.Uint16Invalid),
		AvgCadence:       optI(int(s.AvgCadence), s.AvgCadence != basetype.Uint8Invalid),
		MaxCadence:       optI(int(s.MaxCadence), s.MaxCadence != basetype.Uint8Invalid),
		AvgTemperatureC:  optF(float64(s.AvgTemperature), s.AvgTemperature != basetype.Sint8Invalid),
		MaxTemperatureC:  optF(float64(s.MaxTemperature), s.MaxTemperature != basetype.Sint8Invalid),
		CaloriesKcal:     optI(int(s.TotalCalories), s.TotalCalories != basetype.Uint16Invalid),
		Tss:              optF(s.TrainingStressScoreScaled(), true),
		IntensityFactor:  optF(s.IntensityFactorScaled(), true),
		MinElevationM:    optF(firstValid(s.EnhancedMinAltitudeScaled(), s.MinAltitudeScaled()), true),
		MaxElevationM:    optF(firstValid(s.EnhancedMaxAltitudeScaled(), s.MaxAltitudeScaled()), true),
	}
	if s.Sport == typedef.SportSwimming {
		sum.PoolLengthM = optI(int(math.Round(s.PoolLengthScaled())), !math.IsNaN(s.PoolLengthScaled()))
		sum.TotalStrokes = optI(int(s.TotalCycles), s.TotalCycles != basetype.Uint32Invalid)
	}
	fillFromStream(sum, stream)
	return sum
}

// firstValid picks the enhanced value when present, else the legacy one.
func firstValid(enhanced, legacy float64) float64 {
	if !math.IsNaN(enhanced) {
		return enhanced
	}
	return legacy
}

// laps maps the lap messages in order.
func laps(in []*mesgdef.Lap, start time.Time) []*cairnv1.ActivityLap {
	if len(in) == 0 {
		return nil
	}
	sort.SliceStable(in, func(i, j int) bool { return in[i].StartTime.Before(in[j].StartTime) })
	out := make([]*cairnv1.ActivityLap, 0, len(in))
	for i, l := range in {
		lap := &cairnv1.ActivityLap{
			Index:           clampI32(i + 1),
			StartTime:       ts(l.StartTime),
			ElapsedDuration: dur(seconds(l.TotalElapsedTimeScaled())),
			MovingDuration:  dur(seconds(l.TotalTimerTimeScaled())),
			DistanceM:       optF(l.TotalDistanceScaled(), true),
			AvgHeartRateBpm: optI(int(l.AvgHeartRate), l.AvgHeartRate != basetype.Uint8Invalid),
			MaxHeartRateBpm: optI(int(l.MaxHeartRate), l.MaxHeartRate != basetype.Uint8Invalid),
			AvgPowerW:       optI(int(l.AvgPower), l.AvgPower != basetype.Uint16Invalid),
			MaxPowerW:       optI(int(l.MaxPower), l.MaxPower != basetype.Uint16Invalid),
			AvgSpeedMps:     optF(firstValid(l.EnhancedAvgSpeedScaled(), l.AvgSpeedScaled()), true),
			AvgCadence:      optI(int(l.AvgCadence), l.AvgCadence != basetype.Uint8Invalid),
			ElevationGainM:  optF(float64(l.TotalAscent), l.TotalAscent != basetype.Uint16Invalid),
			ElevationLossM:  optF(float64(l.TotalDescent), l.TotalDescent != basetype.Uint16Invalid),
		}
		if !l.StartTime.IsZero() && !start.IsZero() {
			lap.StartOffsetS = int64(l.StartTime.Sub(start).Seconds())
		}
		out = append(out, lap)
	}
	return out
}

// recordColumns pivots the records into channels.
func recordColumns(records []*mesgdef.Record, start time.Time) *columns {
	c := &columns{}
	for _, r := range records {
		if r.Timestamp.IsZero() {
			continue
		}
		c.n++
		c.timeS = append(c.timeS, round3(r.Timestamp.Sub(start).Seconds()))
		lat, lon := r.PositionLatDegrees(), r.PositionLongDegrees()
		c.pair(lat, lon, !math.IsNaN(lat) && !math.IsNaN(lon))
		addF(&c.alt, firstValid(r.EnhancedAltitudeScaled(), r.AltitudeScaled()))
		addF(&c.dist, r.DistanceScaled())
		addF(&c.speed, firstValid(r.EnhancedSpeedScaled(), r.SpeedScaled()))
		c.hr.add(int32(r.HeartRate), r.HeartRate != basetype.Uint8Invalid)
		c.power.add(int32(r.Power), r.Power != basetype.Uint16Invalid)
		c.cadence.add(int32(r.Cadence), r.Cadence != basetype.Uint8Invalid)
		c.temp.add(float64(r.Temperature), r.Temperature != basetype.Sint8Invalid)
		addF(&c.grade, r.GradeScaled())
	}
	return c
}

// addF adds a scaled value, absent when NaN.
func addF(c *column[float64], v float64) {
	ok := !math.IsNaN(v)
	if ok {
		v = round3(v)
	}
	c.add(v, ok)
}

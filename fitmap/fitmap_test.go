package fitmap

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/muktihari/fit/encoder"
	"github.com/muktihari/fit/profile/filedef"
	"github.com/muktihari/fit/profile/mesgdef"
	"github.com/muktihari/fit/profile/typedef"
	"github.com/muktihari/fit/proto"
	"github.com/tkrajina/gpxgo/gpx"
	"google.golang.org/protobuf/encoding/protojson"

	cairnv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/v1"
)

// -update regenerates testdata/*.fit, *.gpx (encoded with the libraries
// themselves) and testdata/golden/*.json. Review the diff before committing.
var update = flag.Bool("update", false, "rewrite fixtures and goldens")

var t0 = time.Date(2026, 5, 1, 7, 0, 0, 0, time.UTC)

// rideFIT is a gravel ride: GPS with a gap, heart rate arriving late,
// power, cadence, temperature, grade, two laps and a local-time offset.
func rideFIT() *filedef.Activity {
	a := filedef.NewActivity()
	a.FileId = *mesgdef.NewFileId(nil).SetType(typedef.FileActivity).SetManufacturer(typedef.ManufacturerGarmin).
		SetProduct(3121).SetSerialNumber(123456).SetTimeCreated(t0)
	a.Activity = mesgdef.NewActivity(nil).SetTimestamp(t0.Add(20 * time.Second)).SetLocalTimestamp(t0.Add(2*time.Hour + 20*time.Second)).
		SetNumSessions(1).SetEvent(typedef.EventActivity).SetEventType(typedef.EventTypeStop)
	s := mesgdef.NewSession(nil).SetTimestamp(t0.Add(20 * time.Second)).SetStartTime(t0).SetSport(typedef.SportCycling).SetSubSport(typedef.SubSportGravelCycling).
		SetEvent(typedef.EventSession).SetEventType(typedef.EventTypeStop).
		SetTotalElapsedTime(20_000).SetTotalTimerTime(18_000).SetTotalDistance(500_00).SetTotalAscent(50).SetTotalDescent(40).
		SetAvgHeartRate(140).SetMaxHeartRate(171).SetAvgPower(210).SetMaxPower(560).SetNormalizedPower(230).
		SetAvgCadence(85).SetMaxCadence(110).SetAvgTemperature(21).SetMaxTemperature(24).SetTotalCalories(420).
		SetTrainingStressScore(456).SetIntensityFactor(850).SetEnhancedAvgSpeed(27_777).SetEnhancedMaxSpeed(50_000)
	s.SportProfileName = "Gravel"
	a.Sessions = []*mesgdef.Session{s}
	a.Laps = []*mesgdef.Lap{
		mesgdef.NewLap(nil).SetTimestamp(t0.Add(10 * time.Second)).SetStartTime(t0).SetTotalElapsedTime(10_000).SetTotalTimerTime(9_500).SetTotalDistance(250_00).
			SetAvgHeartRate(130).SetMaxHeartRate(150).SetAvgPower(200).SetMaxPower(400).SetEnhancedAvgSpeed(25_000).SetAvgCadence(80).SetTotalAscent(30).SetTotalDescent(10),
		mesgdef.NewLap(nil).SetTimestamp(t0.Add(20 * time.Second)).SetStartTime(t0.Add(10 * time.Second)).SetTotalElapsedTime(10_000).SetTotalTimerTime(8_500).SetTotalDistance(250_00),
	}
	for i := 0; i <= 20; i++ {
		r := mesgdef.NewRecord(nil).SetTimestamp(t0.Add(time.Duration(i) * time.Second)).
			SetDistance(uint32(i * 25_00)).SetEnhancedSpeed(uint32(25_000 + i*100)).SetEnhancedAltitude(uint32((500 + 100 + float64(i)) * 5)). //nolint:gosec // G115: small fixture values
			SetPower(uint16(200 + i*5)).SetCadence(uint8(80 + i)).SetTemperature(int8(20 + i/10)).SetGrade(int16(i*10 - 50))                   //nolint:gosec // G115: small fixture values
		if i < 5 || i > 7 { // GPS gap on samples 5..7
			r.SetPositionLatDegrees(47.5 + float64(i)*0.0001).SetPositionLongDegrees(9.3 + float64(i)*0.0001)
		}
		if i >= 2 { // the strap connected late
			r.SetHeartRate(uint8(120 + i)) //nolint:gosec // G115: small fixture values
		}
		a.Records = append(a.Records, r)
	}
	return a
}

// swimFIT is a pool swim: no GPS, no sport profile, no local time, laps
// out of order, one record without a timestamp.
func swimFIT() *filedef.Activity {
	a := filedef.NewActivity()
	a.FileId = *mesgdef.NewFileId(nil).SetType(typedef.FileActivity).SetManufacturer(typedef.ManufacturerDevelopment).SetProduct(1).SetProductName("Pool-O-Matic").SetTimeCreated(t0)
	a.Sessions = []*mesgdef.Session{mesgdef.NewSession(nil).SetStartTime(t0).SetTimestamp(t0.Add(time.Hour)).SetSport(typedef.SportSwimming).SetSubSport(typedef.SubSportLapSwimming).
		SetTotalElapsedTime(3_600_000).SetTotalTimerTime(3_000_000).SetTotalDistance(2000_00).SetPoolLength(25_00).SetTotalCycles(880).SetAvgHeartRate(135)}
	a.Laps = []*mesgdef.Lap{
		mesgdef.NewLap(nil).SetStartTime(t0.Add(30 * time.Minute)).SetTimestamp(t0.Add(time.Hour)).SetTotalElapsedTime(1_800_000).SetTotalDistance(1000_00),
		mesgdef.NewLap(nil).SetStartTime(t0).SetTimestamp(t0.Add(30 * time.Minute)).SetTotalElapsedTime(1_800_000).SetTotalDistance(1000_00),
	}
	for i := 0; i < 5; i++ {
		a.Records = append(a.Records, mesgdef.NewRecord(nil).SetTimestamp(t0.Add(time.Duration(i)*10*time.Minute)).SetHeartRate(uint8(130+i))) //nolint:gosec // G115: fixture
	}
	a.Records = append(a.Records, mesgdef.NewRecord(nil).SetHeartRate(200)) // no timestamp: skipped
	return a
}

// recordsOnlyFIT has no session: the mapper synthesises one.
func recordsOnlyFIT() *filedef.Activity {
	a := filedef.NewActivity()
	a.FileId = *mesgdef.NewFileId(nil).SetType(typedef.FileActivity).SetManufacturer(typedef.ManufacturerWahooFitness).SetProduct(2).SetTimeCreated(t0)
	for i := 0; i < 3; i++ {
		a.Records = append(a.Records, mesgdef.NewRecord(nil).SetTimestamp(t0.Add(time.Duration(i)*time.Second)).SetDistance(uint32(i*100))) //nolint:gosec // G115: fixture
	}
	return a
}

func encodeFIT(t *testing.T, a *filedef.Activity) []byte {
	t.Helper()
	fit := a.ToFIT(nil)
	var buf bytes.Buffer
	if err := encoder.New(&buf).Encode(&fit); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

const tpxNS = "http://www.garmin.com/xmlschemas/TrackPointExtension/v1"

// runGPX is a Strava-style run export with TrackPointExtension and power.
func runGPX(t *testing.T) []byte {
	t.Helper()
	g := &gpx.GPX{Version: "1.1", Creator: "fitmap test", Name: "Morning Run", Description: "easy"}
	g.RegisterNamespace("gpxtpx", tpxNS)
	trk := gpx.GPXTrack{Name: "Morning Run", Type: "9"}
	var seg gpx.GPXTrackSegment
	for i := 0; i < 12; i++ {
		p := gpx.GPXPoint{Point: gpx.Point{Latitude: 47.5 + float64(i)*0.0002, Longitude: 9.3 + float64(i)*0.0002}, Timestamp: t0.Add(time.Duration(i) * 5 * time.Second)}
		if i != 4 {
			p.Elevation = *gpx.NewNullableFloat64(400 + float64(i*8%30))
		}
		if i%2 == 0 {
			tpx := p.Extensions.GetOrCreateNode(tpxNS, "TrackPointExtension")
			tpx.GetOrCreateNode("hr").Data = "150"
			tpx.GetOrCreateNode("cad").Data = "88"
			tpx.GetOrCreateNode("atemp").Data = "18.5"
		}
		p.Extensions.GetOrCreateNode(gpx.NoNamespace, "power").Data = "250"
		seg.AppendPoint(&p)
	}
	trk.AppendSegment(&seg)
	g.AppendTrack(&trk)
	return toXML(t, g)
}

// toXML renders a document with a trailing newline, as editors and hooks expect of text fixtures.
func toXML(t *testing.T, g *gpx.GPX) []byte {
	t.Helper()
	out, err := g.ToXml(gpx.ToXmlParams{Version: "1.1", Indent: true})
	if err != nil {
		t.Fatal(err)
	}
	return append(out, '\n')
}

// timelessGPX has no timestamps, elevations or type.
func timelessGPX(t *testing.T) []byte {
	t.Helper()
	g := &gpx.GPX{Version: "1.1", Creator: "fitmap test"}
	trk := gpx.GPXTrack{Type: "space walk"}
	var seg gpx.GPXTrackSegment
	for i := 0; i < 3; i++ {
		seg.AppendPoint(&gpx.GPXPoint{Point: gpx.Point{Latitude: 47.5, Longitude: 9.3 + float64(i)*0.001}})
	}
	trk.AppendSegment(&seg)
	g.AppendTrack(&trk)
	return toXML(t, g)
}

// fixtures lists every fixture with the code that regenerates it.
func fixtures(t *testing.T) map[string]func() []byte {
	t.Helper()
	return map[string]func() []byte{
		"ride.fit":         func() []byte { return encodeFIT(t, rideFIT()) },
		"swim.fit":         func() []byte { return encodeFIT(t, swimFIT()) },
		"records-only.fit": func() []byte { return encodeFIT(t, recordsOnlyFIT()) },
		"run.gpx":          func() []byte { return runGPX(t) },
		"timeless.gpx":     func() []byte { return timelessGPX(t) },
	}
}

func mapBytes(t *testing.T, name string, raw []byte) *Result {
	t.Helper()
	var (
		res *Result
		err error
	)
	if strings.HasSuffix(name, ".gpx") {
		res, err = FromGPX(bytes.NewReader(raw))
	} else {
		res, err = FromFIT(bytes.NewReader(raw))
	}
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return res
}

// canonical renders the activity as stable, indented JSON.
func canonical(t *testing.T, res *Result) []byte {
	t.Helper()
	b, err := protojson.Marshal(res.Activity)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	out, err := json.MarshalIndent(map[string]any{"activity": v, "info": res.Info}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(out, '\n')
}

func TestGolden(t *testing.T) {
	for name, build := range fixtures(t) {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("testdata", name)
			golden := filepath.Join("testdata", "golden", name+".json")
			if *update {
				if err := os.MkdirAll(filepath.Dir(golden), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, build(), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(golden, canonical(t, mapBytes(t, name, build())), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("missing fixture (run with -update): %v", err)
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("missing golden (run with -update): %v", err)
			}
			// The committed bytes and a fresh encoding of the same messages map alike.
			for label, b := range map[string][]byte{"committed": raw, "re-encoded": build()} {
				if got := canonical(t, mapBytes(t, name, b)); !bytes.Equal(got, want) {
					t.Errorf("%s %s differs from golden:\n%s", name, label, got)
				}
			}
		})
	}
}

func TestRideDetails(t *testing.T) {
	res := mapBytes(t, "ride.fit", encodeFIT(t, rideFIT()))
	p, st := res.Activity.GetPayload(), res.Activity.GetStream()
	if p.GetType() != cairnv1.ActivityType_ACTIVITY_TYPE_RIDE || p.GetDiscipline() != cairnv1.Discipline_DISCIPLINE_RIDE_GRAVEL || p.GetTitle() != "Gravel" ||
		p.GetProviderNativeType() != "cycling/gravel_cycling" || p.GetExtra()["utc_offset"] != "2" {
		t.Errorf("payload = %v", p)
	}
	if p.GetElapsedDuration().AsDuration() != 20*time.Second || p.GetMovingDuration().AsDuration() != 18*time.Second || p.GetEndTime().AsTime() != t0.Add(20*time.Second) {
		t.Errorf("durations = %v %v %v", p.GetElapsedDuration(), p.GetMovingDuration(), p.GetEndTime())
	}
	s := p.GetSummary()
	if s.GetDistanceM() != 500 || s.GetElevationGainM() != 50 || s.GetAvgSpeedMps() != 27.777 || s.GetTss() != 45.6 || s.GetIntensityFactor() != 0.85 ||
		s.GetNormalizedPowerW() != 230 || s.GetCaloriesKcal() != 420 || s.GetMinElevationM() != 100 || s.GetMaxElevationM() != 120 || s.GetMinTemperatureC() != 20 {
		t.Errorf("summary = %v", s)
	}
	if n := st.GetSampleCount(); n != 21 || len(st.GetLatitude()) != 21 || len(st.GetHeartRateBpm()) != 21 || st.GetResolutionHz() != 1 {
		t.Errorf("stream shape = %d lat %d hr %d hz %v", n, len(st.GetLatitude()), len(st.GetHeartRateBpm()), st.GetResolutionHz())
	}
	// Gaps: GPS forward-filled over 5..7, heart rate back-filled before 2.
	if st.GetLatitude()[6] != st.GetLatitude()[4] || st.GetLatitude()[8] == st.GetLatitude()[4] || st.GetHeartRateBpm()[0] != 122 || st.GetHeartRateBpm()[20] != 140 {
		t.Errorf("gap filling: lat %v hr %v", st.GetLatitude(), st.GetHeartRateBpm())
	}
	if st.GetGrade()[0] != -0.5 || st.GetPowerW()[20] != 300 || st.GetDistanceM()[20] != 500 {
		t.Errorf("columns: grade %v power %v dist %v", st.GetGrade()[0], st.GetPowerW()[20], st.GetDistanceM()[20])
	}
	laps := p.GetLaps()
	if len(laps) != 2 || laps[1].GetIndex() != 2 || laps[1].GetStartOffsetS() != 10 || laps[0].GetAvgSpeedMps() != 25 || laps[0].GetElevationGainM() != 30 || laps[1].AvgHeartRateBpm != nil {
		t.Errorf("laps = %v", laps)
	}
	if p.GetStreamSummary().GetSampleCount() != 21 || !p.GetStreamSummary().GetHasGps() || len(p.GetStreamSummary().GetChannels()) != 11 {
		t.Errorf("stream summary = %v", p.GetStreamSummary())
	}
	if res.Info.Manufacturer != "garmin" || res.Info.Product != "edge_530" || res.Info.SerialNumber != 123456 || res.Info.Sessions != 1 || res.Info.StartTime != t0 {
		t.Errorf("info = %+v", res.Info)
	}
}

func TestSwimAndRecordsOnly(t *testing.T) {
	swim := mapBytes(t, "swim.fit", encodeFIT(t, swimFIT()))
	p := swim.Activity.GetPayload()
	if p.GetType() != cairnv1.ActivityType_ACTIVITY_TYPE_SWIM || p.GetDiscipline() != cairnv1.Discipline_DISCIPLINE_SWIM_POOL || p.GetSummary().GetPoolLengthM() != 25 ||
		p.GetSummary().GetTotalStrokes() != 880 || p.GetExtra() != nil || p.GetTitle() != "" {
		t.Errorf("swim payload = %v", p)
	}
	st := swim.Activity.GetStream()
	if st.GetSampleCount() != 5 || st.GetLatitude() != nil || len(st.GetHeartRateBpm()) != 5 || p.GetStreamSummary().GetHasGps() {
		t.Errorf("swim stream = %v", st)
	}
	if laps := p.GetLaps(); len(laps) != 2 || laps[0].GetStartOffsetS() != 0 || laps[1].GetStartOffsetS() != 1800 {
		t.Errorf("laps must be sorted by start: %v", laps)
	}
	if swim.Info.Product != "Pool-O-Matic" || swim.Info.Manufacturer != "development" || swim.Info.SerialNumber != 0 {
		t.Errorf("swim info = %+v", swim.Info)
	}

	ro := mapBytes(t, "records-only.fit", encodeFIT(t, recordsOnlyFIT()))
	p = ro.Activity.GetPayload()
	if p.GetType() != cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT || p.GetElapsedDuration().AsDuration() != 2*time.Second || p.GetSummary().GetDistanceM() != 2 ||
		p.GetStartTime().AsTime() != t0 || ro.Info.Sessions != 0 || ro.Info.Product != "2" {
		t.Errorf("records-only = %v info %+v", p, ro.Info)
	}
}

func TestGPXDetails(t *testing.T) {
	run := mapBytes(t, "run.gpx", runGPX(t))
	p, st := run.Activity.GetPayload(), run.Activity.GetStream()
	if p.GetType() != cairnv1.ActivityType_ACTIVITY_TYPE_RUN || p.GetTitle() != "Morning Run" || p.GetDescription() != "easy" || p.GetProviderNativeType() != "9" ||
		p.GetElapsedDuration().AsDuration() != 55*time.Second || p.GetEndTime().AsTime() != t0.Add(55*time.Second) {
		t.Errorf("payload = %v", p)
	}
	if st.GetSampleCount() != 12 || st.GetTimeS()[11] != 55 || len(st.GetAltitudeM()) != 12 || st.GetAltitudeM()[4] != st.GetAltitudeM()[3] ||
		st.GetHeartRateBpm()[1] != 150 || st.GetPowerW()[3] != 250 || st.GetCadence()[0] != 88 || st.GetTemperatureC()[5] != 18.5 || st.GetGrade() != nil {
		t.Errorf("stream = %v", st)
	}
	s := p.GetSummary()
	if s.GetDistanceM() <= 0 || s.GetAvgSpeedMps() <= 0 || s.GetAvgHeartRateBpm() != 150 || s.GetAvgPowerW() != 250 || s.GetMaxSpeedMps() <= 0 ||
		s.GetElevationGainM() <= 0 || p.GetMovingDuration().AsDuration() <= 0 {
		t.Errorf("summary = %v moving %v", s, p.GetMovingDuration())
	}
	if run.Info.Format != "gpx" || run.Info.StartTime != t0 || run.Info.Samples != 12 || run.Info.NativeType != "9" {
		t.Errorf("info = %+v", run.Info)
	}

	tl := mapBytes(t, "timeless.gpx", timelessGPX(t))
	p, st = tl.Activity.GetPayload(), tl.Activity.GetStream()
	if p.GetType() != cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT || p.GetCustomSubtype() != "space walk" || p.StartTime != nil || p.ElapsedDuration != nil ||
		st.GetTimeS()[2] != 2 || st.GetAltitudeM() != nil || st.GetSpeedMps() != nil || p.GetSummary().AvgSpeedMps != nil || p.GetSummary().GetDistanceM() <= 0 {
		t.Errorf("timeless = %v %v", p, st)
	}
}

func TestErrors(t *testing.T) {
	if _, err := FromFIT(bytes.NewReader([]byte("not a fit file"))); err == nil {
		t.Error("garbage FIT: want error")
	}
	course := filedef.NewActivity()
	course.FileId = *mesgdef.NewFileId(nil).SetType(typedef.FileCourse).SetManufacturer(typedef.ManufacturerGarmin).SetProduct(1).SetTimeCreated(t0)
	if _, err := FromFIT(bytes.NewReader(encodeFIT(t, course))); !errors.Is(err, ErrNotActivity) {
		t.Errorf("course = %v", err)
	}
	empty := filedef.NewActivity()
	empty.FileId = *mesgdef.NewFileId(nil).SetType(typedef.FileActivity).SetManufacturer(typedef.ManufacturerGarmin).SetProduct(1).SetTimeCreated(t0)
	if _, err := FromFIT(bytes.NewReader(encodeFIT(t, empty))); !errors.Is(err, ErrNoData) {
		t.Errorf("empty = %v", err)
	}
	if _, err := FromGPX(strings.NewReader("<gpx")); err == nil {
		t.Error("garbage GPX: want error")
	}
	if _, err := FromGPX(strings.NewReader(`<gpx version="1.1" creator="x"></gpx>`)); !errors.Is(err, ErrNoData) {
		t.Errorf("no points = %v", err)
	}
}

func TestMapSportTable(t *testing.T) {
	cases := []struct {
		sport typedef.Sport
		sub   typedef.SubSport
		want  kind
	}{
		{typedef.SportCycling, typedef.SubSportIndoorCycling, kind{typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, virtual: true}},
		{typedef.SportCycling, typedef.SubSportEBikeMountain, kind{typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_MTB, ebike: true}},
		{typedef.SportCycling, typedef.SubSportGeneric, k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED)},
		{typedef.SportEBiking, typedef.SubSportGeneric, kind{typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, ebike: true}},
		{typedef.SportRunning, typedef.SubSportTrail, k(cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_RUN_TRAIL)},
		{typedef.SportRunning, typedef.SubSportTreadmill, kind{typ: cairnv1.ActivityType_ACTIVITY_TYPE_RUN, virtual: true}},
		{typedef.SportRunning, typedef.SubSportInvalid, k(cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED)},
		{typedef.SportSwimming, typedef.SubSportOpenWater, k(cairnv1.ActivityType_ACTIVITY_TYPE_SWIM, cairnv1.Discipline_DISCIPLINE_SWIM_OPEN_WATER)},
		{typedef.SportAlpineSkiing, typedef.SubSportBackcountry, k(cairnv1.ActivityType_ACTIVITY_TYPE_SKI, cairnv1.Discipline_DISCIPLINE_SKI_BACKCOUNTRY)},
		{typedef.SportAlpineSkiing, typedef.SubSportResort, k(cairnv1.ActivityType_ACTIVITY_TYPE_SKI, cairnv1.Discipline_DISCIPLINE_SKI_ALPINE)},
		{typedef.SportFitnessEquipment, typedef.SubSportElliptical, k(cairnv1.ActivityType_ACTIVITY_TYPE_ELLIPTICAL, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED)},
		{typedef.SportFitnessEquipment, typedef.SubSportStairClimbing, kind{typ: cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT}},
		{typedef.SportTennis, typedef.SubSportMatch, k(cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED)},
		{typedef.SportInvalid, typedef.SubSportInvalid, kind{typ: cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT}},
	}
	for _, c := range cases {
		if got := mapSport(c.sport, c.sub); got != c.want {
			t.Errorf("mapSport(%v, %v) = %+v; want %+v", c.sport, c.sub, got, c.want)
		}
	}
	if nativeType(typedef.SportInvalid, typedef.SubSportInvalid) != "" || nativeType(typedef.SportRunning, typedef.SubSportInvalid) != "running" {
		t.Error("nativeType")
	}
	if mapGPXType(" Cycling ") != cairnv1.ActivityType_ACTIVITY_TYPE_RIDE || mapGPXType("zumba") != cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT {
		t.Error("mapGPXType")
	}
}

func TestHelpers(t *testing.T) {
	var c column[int32]
	if c.filled() != nil {
		t.Error("empty column must be nil")
	}
	if resolution([]float64{0}) != 1 || resolution([]float64{0, 0}) != 1 || resolution([]float64{0, 2, 4}) != 0.5 {
		t.Error("resolution")
	}
	if meanI(nil) != nil || meanF(nil) != nil || *meanI([]int32{1, 2}) != 2 || *meanF([]float64{1, 2}) != 1.5 {
		t.Error("means")
	}
	if optF(1, false) != nil || optI(1, false) != nil || dur(0) != nil || ts(time.Time{}) != nil || seconds(-1) != 0 {
		t.Error("optionals")
	}
	gain, loss := elevationGainLoss([]float64{100, 102, 110, 104, 120, 100})
	if gain != 26 || loss != 26 {
		t.Errorf("gain/loss = %v %v", gain, loss)
	}
	var fit proto.FIT
	if len(fit.Messages) != 0 {
		t.Error("unused")
	}
}

// Elevation counts a climb or descent once it reaches 5 m from the last
// anchor; smaller wobbles are noise.
func TestElevationGainLoss(t *testing.T) {
	for _, c := range []struct {
		alt        []float64
		gain, loss float64
	}{
		{[]float64{100, 104, 100, 104}, 0, 0},
		{[]float64{100, 105, 110, 105, 100}, 10, 10},
		{[]float64{100, 103, 106, 101, 95}, 6, 11},
		{[]float64{100}, 0, 0},
		{[]float64{100, 105}, 5, 0}, // exactly the threshold counts
		{[]float64{100, 95}, 0, 5},
	} {
		if gain, loss := elevationGainLoss(c.alt); gain != c.gain || loss != c.loss {
			t.Errorf("elevationGainLoss(%v) = %v, %v; want %v, %v", c.alt, gain, loss, c.gain, c.loss)
		}
	}
}

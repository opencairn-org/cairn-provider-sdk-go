package fitmap

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/tkrajina/gpxgo/gpx"

	cairnv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/v1"
	workerv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/worker/v1"
)

// FromGPX parses a GPX document and maps its tracks (all of them, in
// order, as one activity). Points need timestamps for a time axis; without
// any, time_s counts samples. Garmin TrackPointExtension values (hr, cad,
// atemp, speed) and a power extension are read when present; speed is
// derived from distance and time otherwise.
func FromGPX(r io.Reader) (*Result, error) {
	g, err := gpx.Parse(r)
	if err != nil {
		return nil, fmt.Errorf("fitmap: parse GPX: %w", err)
	}
	var points []gpx.GPXPoint
	for _, trk := range g.Tracks {
		for _, seg := range trk.Segments {
			points = append(points, seg.Points...)
		}
	}
	if len(points) == 0 {
		return nil, ErrNoData
	}
	start := firstTime(points)
	cols, moving := gpxColumns(points, start)
	stream := cols.stream()
	payload := gpxPayload(g, start, cols, moving)
	payload.Summary = gpxSummary(stream, moving, payload.GetElapsedDuration().AsDuration())
	payload.StreamSummary = streamSummary(stream)

	info := Info{Format: "gpx", StartTime: start.UTC(), NativeType: payload.ProviderNativeType, Samples: cols.n}
	return &Result{Activity: &workerv1.ImportedActivity{Payload: payload, Stream: stream}, Info: info}, nil
}

func firstTime(points []gpx.GPXPoint) time.Time {
	for _, p := range points {
		if !p.Timestamp.IsZero() {
			return p.Timestamp
		}
	}
	return time.Time{}
}

// gpxPayload maps the document header and the time span.
func gpxPayload(g *gpx.GPX, start time.Time, cols *columns, moving time.Duration) *cairnv1.ActivitySourcePayload {
	title, desc, label := g.Name, g.Description, ""
	if len(g.Tracks) > 0 {
		trk := g.Tracks[0]
		if trk.Name != "" {
			title = trk.Name
		}
		if trk.Description != "" {
			desc = trk.Description
		}
		label = trk.Type
	}
	typ := mapGPXType(label)
	p := &cairnv1.ActivitySourcePayload{
		Type:               typ,
		ProviderNativeType: label,
		Title:              title,
		Description:        desc,
		StartTime:          ts(start),
	}
	if typ == cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT && label != "" {
		p.CustomSubtype = label
	}
	if n := len(cols.timeS); n > 0 && !start.IsZero() {
		elapsed := seconds(cols.timeS[n-1])
		p.ElapsedDuration, p.MovingDuration = dur(elapsed), dur(moving)
		p.EndTime = ts(start.Add(elapsed))
	}
	return p
}

// gpxSummary derives the totals from the stream.
func gpxSummary(st *cairnv1.ActivityStream, moving, elapsed time.Duration) *cairnv1.ActivitySummary {
	sum := &cairnv1.ActivitySummary{}
	if st == nil {
		return sum
	}
	if d := st.GetDistanceM(); d != nil {
		total := d[len(d)-1]
		sum.DistanceM = optF(total, true)
		if base := moving; base > 0 || elapsed > 0 {
			if base <= 0 {
				base = elapsed
			}
			sum.AvgSpeedMps = optF(total/base.Seconds(), true)
		}
	}
	if alt := st.GetAltitudeM(); alt != nil {
		gain, loss := elevationGainLoss(alt)
		sum.ElevationGainM, sum.ElevationLossM = optF(gain, true), optF(loss, true)
	}
	sum.AvgHeartRateBpm = meanI(st.GetHeartRateBpm())
	sum.AvgPowerW = meanI(st.GetPowerW())
	sum.AvgCadence = meanI(st.GetCadence())
	sum.AvgTemperatureC = meanF(st.GetTemperatureC())
	fillFromStream(sum, st)
	return sum
}

// elevationGainLoss sums climbs and descents over a 5 m hysteresis, like
// gpxgo's uphill/downhill but on the mapped (filled) column.
func elevationGainLoss(alt []float64) (gain, loss float64) {
	const threshold = 5
	anchor := alt[0]
	for _, a := range alt[1:] {
		switch d := a - anchor; {
		case d >= threshold:
			gain += d
			anchor = a
		case d <= -threshold:
			loss -= d
			anchor = a
		}
	}
	return round3(gain), round3(loss)
}

// gpxColumns pivots the points into channels; moving time counts gaps
// between consecutive points at or above walking pace (0.5 m/s), as the
// GPX file states no pauses itself.
func gpxColumns(points []gpx.GPXPoint, start time.Time) (*columns, time.Duration) {
	c := &columns{}
	var moving time.Duration
	var dist float64
	var prev *gpx.GPXPoint
	for i := range points {
		p := &points[i]
		c.n++
		t := float64(i)
		if !start.IsZero() && !p.Timestamp.IsZero() {
			t = round3(p.Timestamp.Sub(start).Seconds())
		}
		c.timeS = append(c.timeS, t)
		c.pair(p.Latitude, p.Longitude, true)
		c.alt.add(round3(p.Elevation.Value()), p.Elevation.NotNull())
		var step float64
		if prev != nil {
			step = gpx.HaversineDistance(prev.Latitude, prev.Longitude, p.Latitude, p.Longitude)
			dist += step
		}
		c.dist.add(round3(dist), true)
		speed, hasSpeed := extensionF(p, "speed")
		var dt float64
		if timed := !start.IsZero() && prev != nil && !prev.Timestamp.IsZero() && !p.Timestamp.IsZero(); timed {
			dt = c.timeS[len(c.timeS)-1] - c.timeS[len(c.timeS)-2]
		}
		if !hasSpeed && dt > 0 {
			speed, hasSpeed = step/dt, true
		}
		c.speed.add(round3(speed), hasSpeed)
		if hasSpeed && speed >= 0.5 && dt > 0 {
			moving += seconds(dt)
		}
		hr, okHR := extensionF(p, "hr")
		c.hr.add(int32(hr), okHR)
		pw, okPW := extensionF(p, "power")
		c.power.add(int32(pw), okPW)
		cad, okCad := extensionF(p, "cad")
		c.cadence.add(int32(cad), okCad)
		temp, okTemp := extensionF(p, "atemp")
		c.temp.add(round3(temp), okTemp)
		c.grade.add(0, false)
		prev = p
	}
	return c, moving
}

// extensionF finds a numeric extension by local name (any namespace, any
// nesting), e.g. gpxtpx:hr inside gpxtpx:TrackPointExtension.
func extensionF(p *gpx.GPXPoint, name string) (float64, bool) {
	for i := range p.Extensions.Nodes {
		if v, ok := findNode(&p.Extensions.Nodes[i], name); ok {
			return v, true
		}
	}
	return 0, false
}

func findNode(n *gpx.ExtensionNode, name string) (float64, bool) {
	if strings.EqualFold(n.LocalName(), name) {
		v, err := strconv.ParseFloat(strings.TrimSpace(n.Data), 64)
		return v, err == nil
	}
	for i := range n.Nodes {
		if v, ok := findNode(&n.Nodes[i], name); ok {
			return v, true
		}
	}
	return 0, false
}

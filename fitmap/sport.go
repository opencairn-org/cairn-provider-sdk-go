package fitmap

import (
	"strings"

	"github.com/muktihari/fit/profile/typedef"

	cairnv1 "github.com/opencairn-org/cairn-provider-sdk-go/proto/cairn/v1"
)

// kind is the mapped activity classification.
type kind struct {
	typ        cairnv1.ActivityType
	discipline cairnv1.Discipline
	ebike      bool
	virtual    bool
}

func k(t cairnv1.ActivityType, d cairnv1.Discipline) kind { return kind{typ: t, discipline: d} }

// Sport/sub-sport tables. Unknown sports fall back to WORKOUT, keeping the
// native label in custom_subtype so nothing is lost.
var (
	sportKinds = map[typedef.Sport]kind{
		typedef.SportCycling:               k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportEBiking:               {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, ebike: true},
		typedef.SportRunning:               k(cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportSwimming:              k(cairnv1.ActivityType_ACTIVITY_TYPE_SWIM, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportHiking:                k(cairnv1.ActivityType_ACTIVITY_TYPE_HIKE, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportMountaineering:        k(cairnv1.ActivityType_ACTIVITY_TYPE_HIKE, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportSnowshoeing:           k(cairnv1.ActivityType_ACTIVITY_TYPE_HIKE, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportWalking:               k(cairnv1.ActivityType_ACTIVITY_TYPE_WALK, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportRowing:                k(cairnv1.ActivityType_ACTIVITY_TYPE_ROW, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportAlpineSkiing:          k(cairnv1.ActivityType_ACTIVITY_TYPE_SKI, cairnv1.Discipline_DISCIPLINE_SKI_ALPINE),
		typedef.SportCrossCountrySkiing:    k(cairnv1.ActivityType_ACTIVITY_TYPE_SKI, cairnv1.Discipline_DISCIPLINE_SKI_NORDIC),
		typedef.SportSnowboarding:          k(cairnv1.ActivityType_ACTIVITY_TYPE_SNOWBOARD, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportInlineSkating:         k(cairnv1.ActivityType_ACTIVITY_TYPE_SKATE, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportIceSkating:            k(cairnv1.ActivityType_ACTIVITY_TYPE_SKATE, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportKayaking:              k(cairnv1.ActivityType_ACTIVITY_TYPE_KAYAK, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportCanoeing:              k(cairnv1.ActivityType_ACTIVITY_TYPE_KAYAK, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportPaddling:              k(cairnv1.ActivityType_ACTIVITY_TYPE_KAYAK, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportRafting:               k(cairnv1.ActivityType_ACTIVITY_TYPE_KAYAK, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportStandUpPaddleboarding: k(cairnv1.ActivityType_ACTIVITY_TYPE_SUP, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportSurfing:               k(cairnv1.ActivityType_ACTIVITY_TYPE_SURF, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportWindsurfing:           k(cairnv1.ActivityType_ACTIVITY_TYPE_SURF, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportKitesurfing:           k(cairnv1.ActivityType_ACTIVITY_TYPE_SURF, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportWakesurfing:           k(cairnv1.ActivityType_ACTIVITY_TYPE_SURF, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportGolf:                  k(cairnv1.ActivityType_ACTIVITY_TYPE_GOLF, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportRockClimbing:          k(cairnv1.ActivityType_ACTIVITY_TYPE_CLIMB, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportTennis:                k(cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportRacket:                k(cairnv1.ActivityType_ACTIVITY_TYPE_TENNIS, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportWheelchairPushWalk:    k(cairnv1.ActivityType_ACTIVITY_TYPE_WHEELCHAIR, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
		typedef.SportWheelchairPushRun:     k(cairnv1.ActivityType_ACTIVITY_TYPE_WHEELCHAIR, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
	}
	cyclingSubs = map[typedef.SubSport]kind{
		typedef.SubSportRoad:            k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_ROAD),
		typedef.SubSportCommuting:       k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_ROAD),
		typedef.SubSportRecumbent:       k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_ROAD),
		typedef.SubSportMountain:        k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_MTB),
		typedef.SubSportDownhill:        k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_MTB),
		typedef.SubSportGravelCycling:   k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_GRAVEL),
		typedef.SubSportMixedSurface:    k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_GRAVEL),
		typedef.SubSportCyclocross:      k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_CYCLOCROSS),
		typedef.SubSportTrackCycling:    k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_TRACK),
		typedef.SubSportBmx:             k(cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, cairnv1.Discipline_DISCIPLINE_RIDE_BMX),
		typedef.SubSportIndoorCycling:   {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, virtual: true},
		typedef.SubSportSpin:            {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, virtual: true},
		typedef.SubSportVirtualActivity: {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, virtual: true},
		typedef.SubSportEBikeFitness:    {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_ROAD, ebike: true},
		typedef.SubSportEBikeMountain:   {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, discipline: cairnv1.Discipline_DISCIPLINE_RIDE_MTB, ebike: true},
		typedef.SubSportHandCycling:     k(cairnv1.ActivityType_ACTIVITY_TYPE_WHEELCHAIR, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
	}
	runningSubs = map[typedef.SubSport]kind{
		typedef.SubSportStreet:          k(cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_RUN_ROAD),
		typedef.SubSportTrail:           k(cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_RUN_TRAIL),
		typedef.SubSportUltra:           k(cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_RUN_TRAIL),
		typedef.SubSportTrack:           k(cairnv1.ActivityType_ACTIVITY_TYPE_RUN, cairnv1.Discipline_DISCIPLINE_RUN_TRACK),
		typedef.SubSportTreadmill:       {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RUN, virtual: true},
		typedef.SubSportIndoorRunning:   {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RUN, virtual: true},
		typedef.SubSportVirtualActivity: {typ: cairnv1.ActivityType_ACTIVITY_TYPE_RUN, discipline: cairnv1.Discipline_DISCIPLINE_RUN_ROAD, virtual: true},
	}
	otherSubs = map[typedef.SubSport]kind{
		typedef.SubSportLapSwimming: k(cairnv1.ActivityType_ACTIVITY_TYPE_SWIM, cairnv1.Discipline_DISCIPLINE_SWIM_POOL),
		typedef.SubSportOpenWater:   k(cairnv1.ActivityType_ACTIVITY_TYPE_SWIM, cairnv1.Discipline_DISCIPLINE_SWIM_OPEN_WATER),
		typedef.SubSportBackcountry: k(cairnv1.ActivityType_ACTIVITY_TYPE_SKI, cairnv1.Discipline_DISCIPLINE_SKI_BACKCOUNTRY),
		typedef.SubSportSkateSkiing: k(cairnv1.ActivityType_ACTIVITY_TYPE_SKI, cairnv1.Discipline_DISCIPLINE_SKI_NORDIC),
		typedef.SubSportElliptical:  k(cairnv1.ActivityType_ACTIVITY_TYPE_ELLIPTICAL, cairnv1.Discipline_DISCIPLINE_UNSPECIFIED),
	}
)

// mapSport classifies a FIT sport/sub-sport pair.
func mapSport(sport typedef.Sport, sub typedef.SubSport) kind {
	switch sport {
	case typedef.SportCycling:
		if k, ok := cyclingSubs[sub]; ok {
			return k
		}
	case typedef.SportRunning:
		if k, ok := runningSubs[sub]; ok {
			return k
		}
	case typedef.SportSwimming, typedef.SportAlpineSkiing, typedef.SportCrossCountrySkiing, typedef.SportFitnessEquipment:
		if k, ok := otherSubs[sub]; ok {
			return k
		}
	}
	if k, ok := sportKinds[sport]; ok {
		return k
	}
	return kind{typ: cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT}
}

// gpxKinds classifies GPX track types: words from common exporters and the
// numeric codes Strava writes.
var gpxKinds = map[string]cairnv1.ActivityType{
	"cycling": cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, "biking": cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, "ride": cairnv1.ActivityType_ACTIVITY_TYPE_RIDE, "1": cairnv1.ActivityType_ACTIVITY_TYPE_RIDE,
	"running": cairnv1.ActivityType_ACTIVITY_TYPE_RUN, "run": cairnv1.ActivityType_ACTIVITY_TYPE_RUN, "9": cairnv1.ActivityType_ACTIVITY_TYPE_RUN,
	"hiking": cairnv1.ActivityType_ACTIVITY_TYPE_HIKE, "hike": cairnv1.ActivityType_ACTIVITY_TYPE_HIKE, "4": cairnv1.ActivityType_ACTIVITY_TYPE_HIKE,
	"walking": cairnv1.ActivityType_ACTIVITY_TYPE_WALK, "walk": cairnv1.ActivityType_ACTIVITY_TYPE_WALK, "10": cairnv1.ActivityType_ACTIVITY_TYPE_WALK,
	"swimming": cairnv1.ActivityType_ACTIVITY_TYPE_SWIM, "swim": cairnv1.ActivityType_ACTIVITY_TYPE_SWIM, "15": cairnv1.ActivityType_ACTIVITY_TYPE_SWIM,
	"skiing": cairnv1.ActivityType_ACTIVITY_TYPE_SKI, "2": cairnv1.ActivityType_ACTIVITY_TYPE_SKI, "3": cairnv1.ActivityType_ACTIVITY_TYPE_SKI,
	"snowboarding": cairnv1.ActivityType_ACTIVITY_TYPE_SNOWBOARD, "12": cairnv1.ActivityType_ACTIVITY_TYPE_SNOWBOARD,
	"kayaking": cairnv1.ActivityType_ACTIVITY_TYPE_KAYAK, "14": cairnv1.ActivityType_ACTIVITY_TYPE_KAYAK,
	"rowing": cairnv1.ActivityType_ACTIVITY_TYPE_ROW,
}

// mapGPXType classifies a GPX <type>; unknown labels are WORKOUT.
func mapGPXType(label string) cairnv1.ActivityType {
	if t, ok := gpxKinds[strings.ToLower(strings.TrimSpace(label))]; ok {
		return t
	}
	return cairnv1.ActivityType_ACTIVITY_TYPE_WORKOUT
}

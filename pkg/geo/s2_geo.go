package geo

import (
	"math"

	"github.com/golang/geo/s2"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/util"
)

func ProjectPointToLineCoord(nearestStPoint datastructure.Coordinate, secondNearestStPoint datastructure.Coordinate,
	snap datastructure.Coordinate) datastructure.Coordinate {
	nearestStPoint = MakeSixDigitsAfterComa2(nearestStPoint, 6)
	secondNearestStPoint = MakeSixDigitsAfterComa2(secondNearestStPoint, 6)
	snapLat := snap.GetLat()
	snapLon := snap.GetLon()
	MakeSixDigitsAfterComaLatLon(&snapLat, &snapLon, 6)

	nearestStS2 := s2.PointFromLatLng(s2.LatLngFromDegrees(nearestStPoint.GetLat(), nearestStPoint.GetLon()))
	secondNearestStS2 := s2.PointFromLatLng(s2.LatLngFromDegrees(secondNearestStPoint.GetLat(), secondNearestStPoint.GetLon()))
	snapS2 := s2.PointFromLatLng(s2.LatLngFromDegrees(snapLat, snapLon))
	projection := s2.Project(snapS2, nearestStS2, secondNearestStS2)
	projectLatLng := s2.LatLngFromPoint(projection)
	return datastructure.NewCoordinate(projectLatLng.Lat.Degrees(), projectLatLng.Lng.Degrees())
}

// return in meter
func PointLinePerpendicularDistance(nearestStPoint datastructure.Coordinate, secondNearestStPoint datastructure.Coordinate,
	snap datastructure.Coordinate) float64 {
	projectionPoint := ProjectPointToLineCoord(nearestStPoint, secondNearestStPoint, snap)

	dist := CalculateHaversineDistance(snap.GetLat(), snap.GetLon(), projectionPoint.GetLat(), projectionPoint.GetLon())

	return dist * 1000
}

const (
	tolerancePointInLine = 1e-3
)

// IsPointBetweenLine checks if a point is between two points
// lat,lon is the projection of query point to the line
// lats, lons are the line points
func PointPositionBetweenLinePoints(lat, lon float64, linePoints []datastructure.Coordinate) int {
	minDiff := math.MaxFloat64
	var pos int
	for i := 0; i < len(linePoints)-1; i++ {

		currQueryDist := s2.LatLngFromDegrees(lat, lon).Distance(s2.LatLngFromDegrees(linePoints[i].GetLat(), linePoints[i].GetLon())).Radians()
		nextQueryDist := s2.LatLngFromDegrees(lat, lon).Distance(s2.LatLngFromDegrees(linePoints[i+1].GetLat(), linePoints[i+1].GetLon())).Radians()

		currNextDist := s2.LatLngFromDegrees(linePoints[i].GetLat(), linePoints[i].GetLon()).Distance(s2.LatLngFromDegrees(linePoints[i+1].GetLat(), linePoints[i+1].GetLon())).Radians()

		diff := math.Abs(currQueryDist + nextQueryDist - currNextDist)
		if diff < tolerancePointInLine && diff < minDiff {
			minDiff = diff
			pos = i + 1
		}
	}
	return pos
}

func MakeSixDigitsAfterComa2(n datastructure.Coordinate, precision int) datastructure.Coordinate {

	lat := n.GetLat()
	lon := n.GetLon()
	if util.CountDecimalPlacesF64(n.GetLat()) != precision {
		lat = util.RoundFloat(n.GetLat()+0.000001, 6)
	}
	if util.CountDecimalPlacesF64(n.GetLon()) != precision {
		lon = util.RoundFloat(n.GetLon()+0.000001, 6)
	}
	return datastructure.NewCoordinate(lat, lon)
}
func MakeSixDigitsAfterComaLatLon(lat, lon *float64, precision int) {

	if util.CountDecimalPlacesF64(*lat) != precision {
		*lat = util.RoundFloat(*lat+0.000001, 6)
	}
	if util.CountDecimalPlacesF64(*lon) != precision {
		*lon = util.RoundFloat(*lon+0.000001, 6)
	}
}

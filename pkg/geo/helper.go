package geo

import (
	"container/list"

	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"
	"github.com/twpayne/go-polyline"
)

const (
	DOUGLAS_PEUCKER_THRESHOLDS = 1.0 // 1.0 meter
)

// https://cartography-playground.gitlab.io/playgrounds/douglas-peucker-algorithm/

func RamerDouglasPeucker(coords []datastructure.Coordinate) []datastructure.Coordinate {
	size := len(coords)
	if size < 2 {
		return coords
	}

	projected := make([]datastructure.Coordinate, size)
	copy(projected, coords)

	kepts := make([]bool, size)
	kepts[0] = true
	kepts[size-1] = true

	stack := list.New()
	stack.PushBack([2]int{0, size - 1})

	threshold := DOUGLAS_PEUCKER_THRESHOLDS
	for stack.Len() > 0 {
		pair := stack.Remove(stack.Back()).([2]int)
		left, right := pair[0], pair[1]
		var maxDist float64
		farthestIndex := left

		// swep over range to find the farthest point from the segment (left,right)
		for i := left + 1; i < right; i++ {
			dist := PointLinePerpendicularDistance(projected[left], projected[right], projected[i])
			if dist > maxDist && dist > threshold {
				maxDist = dist
				farthestIndex = i
			}
		}

		if maxDist > threshold {
			// if the perpendicular distance of the farthestIndex point is greater than the threshold
			// we kept this point
			// recursively calls itself with the first point and the farthest point and then with the farthest point and the last point,
			kepts[farthestIndex] = true
			if left < farthestIndex {
				stack.PushBack([2]int{left, farthestIndex})
			}
			if farthestIndex < right {
				stack.PushBack([2]int{farthestIndex, right})
			}
		}
	}

	simplifiedGeometry := make([]datastructure.Coordinate, 0)
	for i, necessary := range kepts {
		if necessary {
			simplifiedGeometry = append(simplifiedGeometry, coords[i])
		}
	}
	return simplifiedGeometry
}

func PoylineFromCoords(path []datastructure.Coordinate) string {
	s := ""
	coords := make([][]float64, 0)
	for _, p := range path {
		pT := p
		coords = append(coords, []float64{pT.Lat(), pT.Lon()})
	}
	s = string(polyline.EncodeCoords(coords))
	return s
}

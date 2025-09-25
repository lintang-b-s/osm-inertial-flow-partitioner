package partitioner

import (
	"math"
	"sort"

	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"
)

func (dmf *DinicMaxFlow) computeInertialFlow(graph *datastructure.PartitionGraph,
	sourceSinkRate float64) *MinCut {
	var (
		best                    *MinCut = &MinCut{}
		bestNumberOfMinCutEdges         = math.MaxInt
	)

	balanceDelta := func(numPartTwoNodes int) int {

		diff := graph.NumberOfVertices()/2 - numPartTwoNodes
		if diff < 0 {
			diff = -diff
		}
		return diff
	}

	for i := 0; i < pkg.INERTIAL_FLOW_ITERATION; i++ {
		slope := -1 + float64(i)*(2.0/pkg.INERTIAL_FLOW_ITERATION)
		sources, sinks := dmf.sortVerticesByLineProjection(graph, slope, sourceSinkRate)
		minCut := dmf.ComputeMinCut(graph, sources, sinks)

		if minCut.GetNumberOfMinCutEdges() < bestNumberOfMinCutEdges ||
			(best.GetNumberOfMinCutEdges() == minCut.GetNumberOfMinCutEdges() &&
				balanceDelta(minCut.GetNumNodesInPartitionTwo()) < balanceDelta(best.GetNumNodesInPartitionTwo())) {
			best = minCut
			bestNumberOfMinCutEdges = minCut.GetNumberOfMinCutEdges()
		}
	}

	return best
}

func (dmf *DinicMaxFlow) sortVerticesByLineProjection(graph *datastructure.PartitionGraph, slope, ratio float64) ([]datastructure.Index, []datastructure.Index) {

	vertices := graph.GetVertices()

	sort.Slice(vertices, func(i, j int) bool {
		latI, lonI := vertices[i].GetVertexCoordinate()
		latJ, lonJ := vertices[j].GetVertexCoordinate()

		projI := slope*lonI + (1-slope)*latI
		projJ := slope*lonJ + (1-slope)*latJ

		return projI < projJ
	})

	endpointsLength := int(float64(len(vertices)) * ratio)
	sourceNodes := make([]datastructure.Index, 0, endpointsLength)
	sinkNodes := make([]datastructure.Index, 0, endpointsLength)

	for i := 0; i < endpointsLength; i++ {
		sourceNodes = append(sourceNodes, vertices[i].GetID())
		sinkNodes = append(sinkNodes, vertices[len(vertices)-1-i].GetID())
	}

	return sourceNodes, sinkNodes
}

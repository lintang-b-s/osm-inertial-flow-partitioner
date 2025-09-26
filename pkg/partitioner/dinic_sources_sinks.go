package partitioner

import (
	"container/list"
	"fmt"
	"math"

	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/util"
)

func (dmf *DinicMaxFlow) bfsLevelGraph(
	source, target datastructure.Index) bool {

	dmf.graph.ForEachVertices(func(v datastructure.PartitionVertex) {
		dmf.graph.SetVertexLevel(v.GetID(), INVALID_LEVEL)
	})

	levelQueue := list.New()
	levelQueue.PushBack(source)
	dmf.graph.SetVertexLevel(source, 0)

	for levelQueue.Len() > 0 {
		u := levelQueue.Front().Value.(datastructure.Index)
		levelQueue.Remove(levelQueue.Front())

		uLevel := dmf.graph.GetVertexLevel(u)
		level := uLevel + 1
		if u == target {
			break
		}

		dmf.graph.ForEachVertexEdges(u, func(edge *datastructure.MaxFlowEdge) {
			target := edge.GetTo()

			residual := edge.GetCapacity() - edge.GetFlow()
			if residual > 0 && dmf.graph.GetVertexLevel(target) == INVALID_LEVEL {
				dmf.graph.SetVertexLevel(target, level)
				levelQueue.PushBack(target)
			}
		})

	}
	return dmf.graph.GetVertexLevel(target) != INVALID_LEVEL
}

func (dmf *DinicMaxFlow) dfsAugmentPath(nodeId datastructure.Index, t datastructure.Index, maxFlow int) int {
	// termination

	if nodeId == t || maxFlow == 0 {
		return maxFlow
	}

	for ; dmf.graph.GetLastEdgeIndex(nodeId) < dmf.graph.GetVertexEdgesSize(nodeId); dmf.graph.IncrementLastEdgeIndex(nodeId) {
		j := dmf.graph.GetLastEdgeIndex(nodeId)
		edge := dmf.graph.GetEdgeOfVertex(nodeId, j)
		v := edge.GetTo()
		residual := edge.GetCapacity() - edge.GetFlow()
		if dmf.graph.GetVertexLevel(v) != dmf.graph.GetVertexLevel(nodeId)+1 {
			continue
		}

		if flow := dmf.dfsAugmentPath(v, t, util.MinInt(residual, maxFlow)); flow > 0 {
			edge.AddFlow(flow)
			revEdge := dmf.graph.GetReversedEdgeOfVertex(nodeId, j)
			revEdge.AddFlow(-flow)
			return flow
		}
	}
	dmf.graph.SetVertexLevel(nodeId, INVALID_LEVEL)

	return 0.0
}

func (dmf *DinicMaxFlow) computeMinCutSuperSourceSink(s datastructure.Index, t datastructure.Index, sources, sinks []datastructure.Index) *MinCut {
	var (
		minCut = NewMinCut(dmf.graph.NumberOfVertices() - 2) // exclude artificial source and sink
	)
	maxFlow := 0
	for {

		dmf.resetCurrentEdges()
		if dmf.bfsLevelGraph(s, t) {
			for {
				flow := dmf.dfsAugmentPath(s, t, math.MaxInt)
				if flow == 0 {
					break
				}
				maxFlow += flow
			}

		} else {

			if dmf.debug && !dmf.validateResultOne(minCut, s, t, sources, sinks) {
				fmt.Printf("invalid min cut result: violating capacity constraint, flow conservation, and max-flow min-cut theorem!!\n")
			}
			dmf.makeMinCutFlags(minCut, maxFlow)

			return minCut
		}
	}
}

func (dmf *DinicMaxFlow) validateResultOne(minCut *MinCut,
	source, sink datastructure.Index, sources, sinks []datastructure.Index) bool {
	// for every vertex that not in source and sink, check if incoming flow equal to outgoing flow
	// see CLRS section 26.1 & 26.2
	incomingFlow := make([]int, dmf.graph.NumberOfVertices())
	outgoingFlow := make([]int, dmf.graph.NumberOfVertices())
	cutEdgesCount := 0
	sourceOutgoingFlow := 0
	sinkIncomingFlow := 0
	sourceSet := makeNodeSet(sources)
	sinkSet := makeNodeSet(sinks)
	for u := datastructure.Index(0); u < datastructure.Index(dmf.graph.NumberOfVertices()-2); u++ {
		for i := 0; i < dmf.graph.GetVertexEdgesSize(u); i++ {
			edge := dmf.graph.GetEdgeOfVertex(u, i)
			v := edge.GetTo()
			flow := edge.GetFlow()

			if flow > 0 {
				outgoingFlow[u] += flow
				incomingFlow[v] += flow
			}

			if flow > edge.GetCapacity() {
				// Capacity constraint for all edges, flow(u,v) <= c(u,v)
				return false
			}

			if v != sink && v != source &&
				minCut.GetFlag(u) && !minCut.GetFlag(v) {
				cutEdgesCount++
			}

			if v == sink {
				sinkIncomingFlow += flow
			}
		}
	}

	for i := 0; i < dmf.graph.GetVertexEdgesSize(source); i++ {
		edge := dmf.graph.GetEdgeOfVertex(source, i)
		flow := edge.GetFlow()
		sourceOutgoingFlow += flow
	}

	for u := datastructure.Index(0); u < datastructure.Index(dmf.graph.NumberOfVertices()-2); u++ {
		// Flow conservation, sum over incoming flow of a vertex not in source or sink must equal to sum over outgoing flow
		if !nodeInSet(u, sourceSet) && !nodeInSet(u, sinkSet) && incomingFlow[u] != outgoingFlow[u] {
			return false
		}
	}

	if minCut.GetNumberOfMinCutEdges() != cutEdgesCount {
		// max-flow min-cut theorem, the value of the maximum flow is equal to the capacity of the minimum cut
		return false
	}

	if sourceOutgoingFlow != sinkIncomingFlow {
		// outgoing flow from source must equal to incoming flow to sink
		return false
	}

	return true
}

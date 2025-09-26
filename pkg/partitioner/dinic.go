package partitioner

import (
	"container/list"
	"fmt"
	"math"

	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/util"
)

type DinicMaxFlow struct {
	graph *datastructure.PartitionGraph
	debug bool
}

func NewDinicMaxFlow(graph *datastructure.PartitionGraph, debug bool) *DinicMaxFlow {
	return &DinicMaxFlow{graph: graph, debug: debug}
}

func (dmf *DinicMaxFlow) bfsComputeLevelGraph(
	borderSourceNodes, sourceNodes, sinkNodes []datastructure.Index) {
	sourceNodeSet := makeNodeSet(sourceNodes)
	sinkNodeSet := makeNodeSet(sinkNodes)

	dmf.graph.ForEachVertices(func(v datastructure.PartitionVertex) {
		dmf.graph.SetVertexLevel(v.GetID(), INVALID_LEVEL)
	})

	levelQueue := list.New()

	for _, nodeId := range borderSourceNodes {
		dmf.graph.SetVertexLevel(nodeId, 0)
		levelQueue.PushBack(nodeId)
		dmf.graph.ForEachVertexEdges(nodeId, func(edge *datastructure.MaxFlowEdge) {

			target := edge.GetTo()

			// if target is in source set, set level to 0
			if nodeInSet(target, sourceNodeSet) {
				dmf.graph.SetVertexLevel(target, 0)
			}
		})
	}

	for levelQueue.Len() > 0 {
		u := levelQueue.Front().Value.(datastructure.Index)

		if nodeInSet(u, sinkNodeSet) {
			// dont relax sink nodes
			levelQueue.Remove(levelQueue.Front())
			continue
		}

		uLevel := dmf.graph.GetVertexLevel(u)
		level := uLevel + 1

		dmf.graph.ForEachVertexEdges(u, func(edge *datastructure.MaxFlowEdge) {
			target := edge.GetTo()

			residual := edge.GetCapacity() - edge.GetFlow()
			if residual > 0 && dmf.graph.GetVertexLevel(target) > level {
				dmf.graph.SetVertexLevel(target, level)
				levelQueue.PushBack(target)
			}
		})

		levelQueue.Remove(levelQueue.Front())
	}
}

// dfsGetAugmentingPath. perform dfs from nodeId to find augmenting path
func (dmf *DinicMaxFlow) dfsAugmentingPath(nodeId datastructure.Index,
	sinkNodeSet map[datastructure.Index]struct{}, maxFlow int) int {
	// termination

	if _, exists := sinkNodeSet[nodeId]; exists || maxFlow == 0 {
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

		if flow := dmf.dfsAugmentingPath(v, sinkNodeSet, util.MinInt(residual, maxFlow)); flow > 0 {
			edge.AddFlow(flow)
			revEdge := dmf.graph.GetReversedEdgeOfVertex(nodeId, j)
			revEdge.AddFlow(-flow)
			return flow
		}
	}
	dmf.graph.SetVertexLevel(nodeId, INVALID_LEVEL)

	return 0.0
}

func (dmf *DinicMaxFlow) blockingFlow(
	sinkNodes, borderSourceNodes []datastructure.Index) int {
	// [On Balanced Separators in Road Networks, Schild, et al.] maximum flow using Dinic’s algorithm (augmenting paths, in the unit-capacity case computed by breadth-first search)
	// number of augmenting paths in the end will equal the the number of cut edges
	flowIncrease := 0
	dmf.resetCurrentEdges()
	sinkNodesSet := makeNodeSet(sinkNodes)
	for _, borderSourceNodeId := range borderSourceNodes {
		if dmf.graph.GetVertexLevel(borderSourceNodeId) == INVALID_LEVEL {
			continue
		}

		for {
			flow := dmf.dfsAugmentingPath(borderSourceNodeId, sinkNodesSet, math.MaxInt)
			if flow == 0 {
				break
			}
			flowIncrease++
		}
	}

	return flowIncrease
}

func (dmf *DinicMaxFlow) resetCurrentEdges() {
	for i := 0; i < dmf.graph.NumberOfVertices(); i++ {
		dmf.graph.SetLastEdgeIndex(datastructure.Index(i), 0)
	}
}

func (dmf *DinicMaxFlow) resetGraph() {
	dmf.graph.ResetGraph()
}

func (dmf *DinicMaxFlow) ComputeMinCut(
	sourceNodes, sinkNodes []datastructure.Index) *MinCut {

	minCut := NewMinCut(dmf.graph.NumberOfVertices())

	borderSourceNodes := dmf.buildBorderNodes(sourceNodes)

	borderSinkNodes := dmf.buildBorderNodes(sinkNodes)

	sourceSet := makeNodeSet(sourceNodes)
	sinkSet := makeNodeSet(sinkNodes)

	numberOfMinCutEdges := 0
	for {
		dmf.bfsComputeLevelGraph(borderSourceNodes, sourceNodes, sinkNodes)

		if !dmf.isSeparated(borderSinkNodes) {
			numberOfMinCutEdges += dmf.blockingFlow(sinkNodes, borderSourceNodes)
		} else {
			for _, s := range sourceNodes {
				dmf.graph.SetVertexLevel(s, 0)
			}

			dmf.makeMinCutFlags(minCut, numberOfMinCutEdges)

			if dmf.debug && !dmf.validateResult(minCut, sourceSet, sinkSet) {
				fmt.Printf("incorrect max flow result!!!, number of min-cut not the same as max flow, also flow conservation violated\n")
			}
			return minCut
		}
	}
}

func (dmf *DinicMaxFlow) makeMinCutFlags(minCut *MinCut, numberOfMinCutEdges int) {
	for u := datastructure.Index(0); u < datastructure.Index(dmf.graph.NumberOfVertices()-2); u++ {
		if dmf.graph.GetVertexLevel(u) != INVALID_LEVEL {
			minCut.SetFlag(u, true)
		} else {
			minCut.incrementNumNodesInPartitionTwo()
		}
	}
	minCut.setNumberofMinCutEdges(numberOfMinCutEdges)
}

func (dmf *DinicMaxFlow) isSeparated(borderSinkNodes []datastructure.Index) bool {
	for _, borderSinkNodeId := range borderSinkNodes {
		if dmf.graph.GetVertexLevel(borderSinkNodeId) != INVALID_LEVEL {
			return false
		}
	}

	return true
}

func (dmf *DinicMaxFlow) validateResult(minCut *MinCut,
	sourceSet, sinkSet map[datastructure.Index]struct{}) bool {
	// for every vertex that not in source and sink, check if incoming flow equal to outgoing flow
	// see CLRS section 26.1 & 26.2
	incomingFlow := make([]int, dmf.graph.NumberOfVertices())
	outgoingFlow := make([]int, dmf.graph.NumberOfVertices())
	cutEdgesCount := 0

	sourceOutgoingFlow := 0
	sinkIncomingFlow := 0
	for u := datastructure.Index(0); u < datastructure.Index(dmf.graph.NumberOfVertices()); u++ {
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

			if minCut.GetFlag(u) && !minCut.GetFlag(v) {
				cutEdgesCount++
			}

			if nodeInSet(u, sourceSet) {
				sourceOutgoingFlow += flow
			}
			if nodeInSet(v, sinkSet) {
				sinkIncomingFlow += flow
			}
		}
	}
	for u := datastructure.Index(0); u < datastructure.Index(dmf.graph.NumberOfVertices()); u++ {
		// Flow conservation, sum over incoming flow of a vertex not in source or sink must equal to sum over outgoing flow
		inSourceSet := nodeInSet(u, sourceSet)
		inSinkSet := nodeInSet(u, sinkSet)
		if !inSourceSet && !inSinkSet && incomingFlow[u] != outgoingFlow[u] {
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

func (dmf *DinicMaxFlow) buildBorderNodes(nodeIds []datastructure.Index) []datastructure.Index {
	borderNodes := make([]datastructure.Index, 0, int(float64(len(nodeIds))*0.1))

	set := make(map[datastructure.Index]struct{}, len(nodeIds))
	for _, nodeId := range nodeIds {
		set[nodeId] = struct{}{}
	}
	for _, sourceNodeId := range nodeIds {
		if dmf.hasNeighborNotInSet(sourceNodeId, set) {
			borderNodes = append(borderNodes, sourceNodeId)
		}
	}
	return borderNodes
}

func (dmf *DinicMaxFlow) hasNeighborNotInSet(u datastructure.Index, set map[datastructure.Index]struct{},
) bool {

	for i := 0; i < dmf.graph.GetVertexEdgesSize(u); i++ {
		edge := dmf.graph.GetEdgeOfVertex(u, i)
		v := edge.GetTo()

		if _, exists := set[v]; !exists {
			return true
		}
	}

	return false
}

func nodeInSet(u datastructure.Index, nodeSet map[datastructure.Index]struct{}) bool {
	_, exists := nodeSet[u]
	return exists
}

func makeNodeSet(nodeIds []datastructure.Index) map[datastructure.Index]struct{} {
	set := make(map[datastructure.Index]struct{}, len(nodeIds))
	for _, nodeId := range nodeIds {
		set[nodeId] = struct{}{}
	}

	return set
}

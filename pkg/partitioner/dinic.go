package partitioner

import (
	"container/list"
	"fmt"
	"math"

	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"
)

type DinicMaxFlow struct {
}

func NewDinicMaxFlow() *DinicMaxFlow {
	return &DinicMaxFlow{}
}

func (dmf *DinicMaxFlow) bfsComputeLevelGraph(graph *datastructure.PartitionGraph,
	borderSourceNodes, sourceNodes, sinkNodes []datastructure.Index) {
	sourceNodeSet := makeNodeSet(sourceNodes)
	sinkNodeSet := makeNodeSet(sinkNodes)

	graph.ForEachVertices(func(v datastructure.PartitionVertex) {
		graph.SetVertexLevel(v.GetID(), INVALID_LEVEL)
	})

	levelQueue := list.New()

	for _, nodeId := range borderSourceNodes {
		graph.SetVertexLevel(nodeId, 0)
		levelQueue.PushBack(nodeId)
		graph.ForEachVertexEdges(nodeId, func(edge *datastructure.MaxFlowEdge) {

			target := edge.GetTo()

			// if target is in source set, set level to 0
			if nodeInSet(target, sourceNodeSet) {
				graph.SetVertexLevel(target, 0)
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

		uLevel := graph.GetVertexLevel(u)
		level := uLevel + 1

		graph.ForEachVertexEdges(u, func(edge *datastructure.MaxFlowEdge) {
			target := edge.GetTo()

			residual := edge.GetCapacity() - edge.GetFlow()
			if residual > 0 && graph.GetVertexLevel(target) > level {
				graph.SetVertexLevel(target, level)
				levelQueue.PushBack(target)
			}
		})

		levelQueue.Remove(levelQueue.Front())
	}
}

// dfsGetAugmentingPath. perform dfs from nodeId to find augmenting path
func (dmf *DinicMaxFlow) dfsAugmentingPath(graph *datastructure.PartitionGraph, nodeId datastructure.Index,
	sinkNodeSet map[datastructure.Index]struct{}, maxFlow float64) float64 {
	// termination

	if _, exists := sinkNodeSet[nodeId]; exists || maxFlow == 0 {
		return maxFlow
	}

	for ; graph.GetLastEdgeIndex(nodeId) < graph.GetVertexEdgesSize(nodeId); graph.IncrementLastEdgeIndex(nodeId) {
		j := graph.GetLastEdgeIndex(nodeId)
		edge := graph.GetEdgeOfVertex(nodeId, j)
		v := edge.GetTo()
		residual := edge.GetCapacity() - edge.GetFlow()
		if graph.GetVertexLevel(v) != graph.GetVertexLevel(nodeId)+1 {
			continue
		}

		if flow := dmf.dfsAugmentingPath(graph, v, sinkNodeSet, math.Min(residual, maxFlow)); flow > 0 {
			edge.AddFlow(flow)
			revEdge := graph.GetReversedEdgeOfVertex(nodeId, j)
			revEdge.AddFlow(-flow)
			return flow
		}
	}
	graph.SetVertexLevel(nodeId, INVALID_LEVEL)

	return 0.0
}

func (dmf *DinicMaxFlow) blockingFlow(graph *datastructure.PartitionGraph,
	sinkNodes, borderSourceNodes []datastructure.Index) int {
	// [On Balanced Separators in Road Networks, Schild, et al.] maximum flow using Dinic’s algorithm (augmenting paths, in the unit-capacity case computed by breadth-first search)
	// number of augmenting paths in the end will equal the the number of cut edges
	flowIncrease := 0
	dmf.resetCurrentEdges(graph)
	sinkNodesSet := makeNodeSet(sinkNodes)
	for _, borderSourceNodeId := range borderSourceNodes {
		if graph.GetVertexLevel(borderSourceNodeId) == INVALID_LEVEL {
			continue
		}

		for {
			flow := dmf.dfsAugmentingPath(graph, borderSourceNodeId, sinkNodesSet, math.MaxFloat64)
			if flow == 0 {
				break
			}
			flowIncrease++
		}
	}

	return flowIncrease
}

func (dmf *DinicMaxFlow) resetCurrentEdges(graph *datastructure.PartitionGraph) {
	for i := 0; i < graph.NumberOfVertices(); i++ {
		graph.SetLastEdgeIndex(datastructure.Index(i), 0)
	}
}

func (dmf *DinicMaxFlow) resetGraph(graph *datastructure.PartitionGraph) {
	graph.ResetGraph()
}

func (dmf *DinicMaxFlow) ComputeMinCut(graph *datastructure.PartitionGraph,
	sourceNodes, sinkNodes []datastructure.Index) *MinCut {
	dmf.resetGraph(graph)
	minCut := NewMinCut(graph.NumberOfVertices())

	borderSourceNodes := buildBorderNodes(sourceNodes, graph)

	borderSinkNodes := buildBorderNodes(sinkNodes, graph)

	sourceSet := makeNodeSet(sourceNodes)
	sinkSet := makeNodeSet(sinkNodes)

	numberOfMinCutEdges := 0
	for {
		dmf.bfsComputeLevelGraph(graph, borderSourceNodes, sourceNodes, sinkNodes)

		if !dmf.isSeparated(borderSinkNodes, graph) {
			numberOfMinCutEdges += dmf.blockingFlow(graph, sinkNodes, borderSourceNodes)
		} else {
			for _, s := range sourceNodes {
				graph.SetVertexLevel(s, 0)
			}

			dmf.makeMinCutFlags(graph, minCut, numberOfMinCutEdges)

			if !dmf.validateResult(graph, minCut, sourceSet, sinkSet) {
				fmt.Printf("incorrect max flow result!!!, number of min-cut not the same as max flow, also flow conservation violated\n")
			}
			return minCut
		}
	}
}

func (dmf *DinicMaxFlow) makeMinCutFlags(graph *datastructure.PartitionGraph, minCut *MinCut, numberOfMinCutEdges int) {
	for u := datastructure.Index(0); u < datastructure.Index(graph.NumberOfVertices()); u++ {
		if graph.GetVertexLevel(u) != INVALID_LEVEL {
			minCut.SetFlag(u, true)
		} else {
			minCut.incrementNumNodesInPartitionTwo()
		}
	}
	minCut.setNumberofMinCutEdges(numberOfMinCutEdges)
}

func (dmf *DinicMaxFlow) isSeparated(borderSinkNodes []datastructure.Index, graph *datastructure.PartitionGraph) bool {
	for _, borderSinkNodeId := range borderSinkNodes {
		if graph.GetVertexLevel(borderSinkNodeId) != INVALID_LEVEL {
			return false
		}
	}

	return true
}

func (dmf *DinicMaxFlow) validateResult(graph *datastructure.PartitionGraph, minCut *MinCut,
	sourceSet, sinkSet map[datastructure.Index]struct{}) bool {
	// for every vertex that not in source and sink, check if incoming flow equal to outgoing flow
	incomingFlow := make([]float64, graph.NumberOfVertices())
	outgoingFlow := make([]float64, graph.NumberOfVertices())
	cutEdgesCount := 0
	for u := datastructure.Index(0); u < datastructure.Index(graph.NumberOfVertices()); u++ {
		for i := 0; i < graph.GetVertexEdgesSize(u); i++ {
			edge := graph.GetEdgeOfVertex(u, i)
			v := edge.GetTo()
			flow := edge.GetFlow()

			if flow > 0 {
				outgoingFlow[u] += flow
				incomingFlow[v] += flow
			}

			if flow > edge.GetCapacity() {
				return false
			}

			if minCut.GetFlag(u) && !minCut.GetFlag(v) {
				cutEdgesCount++
			}
		}
	}
	for u := datastructure.Index(0); u < datastructure.Index(graph.NumberOfVertices()); u++ {
		inSourceSet := nodeInSet(u, sourceSet)
		inSinkSet := nodeInSet(u, sinkSet)
		if !inSourceSet && !inSinkSet && incomingFlow[u] != outgoingFlow[u] {
			return false
		}
	}

	if minCut.GetNumberOfMinCutEdges() != cutEdgesCount {
		return false
	}

	return true
}

func buildBorderNodes(nodeIds []datastructure.Index, graph *datastructure.PartitionGraph) []datastructure.Index {
	borderNodes := make([]datastructure.Index, 0, int(float64(len(nodeIds))*0.1))

	set := make(map[datastructure.Index]struct{}, len(nodeIds))
	for _, nodeId := range nodeIds {
		set[nodeId] = struct{}{}
	}
	for _, sourceNodeId := range nodeIds {
		if hasNeighborNotInSet(sourceNodeId, set, graph) {
			borderNodes = append(borderNodes, sourceNodeId)
		}
	}
	return borderNodes
}

func hasNeighborNotInSet(u datastructure.Index, set map[datastructure.Index]struct{},
	graph *datastructure.PartitionGraph) bool {

	for i := 0; i < graph.GetVertexEdgesSize(u); i++ {
		edge := graph.GetEdgeOfVertex(u, i)
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

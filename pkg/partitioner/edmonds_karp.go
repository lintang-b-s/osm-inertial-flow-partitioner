package partitioner

import (
	"container/list"
	"fmt"

	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/util"
)

type EdmondsKarp struct {
	graph *datastructure.PartitionGraph
	debug bool
}

func NewEdmondsKarp(graph *datastructure.PartitionGraph, debug bool) *EdmondsKarp {
	return &EdmondsKarp{graph: graph, debug: debug}
}

func (ek *EdmondsKarp) bfsAugmentingPath(source, sink datastructure.Index) int {
	prev := make([]*datastructure.MaxFlowEdge, ek.graph.NumberOfVertices())
	queue := list.New()

	queue.PushBack(source)
	ek.graph.SetVisited(source, true)

	for queue.Len() > 0 {
		u := queue.Remove(queue.Front()).(datastructure.Index)

		if u == sink {
			break
		}

		ek.graph.ForEachVertexEdges(u, func(e *datastructure.MaxFlowEdge) {
			if !ek.graph.IsVisited(e.GetTo()) && e.GetCapacity()-e.GetFlow() > 0 {
				ek.graph.SetVisited(e.GetTo(), true)
				prev[e.GetTo()] = e
				queue.PushBack(e.GetTo())
			}
		})
	}

	if prev[sink] == nil {
		return 0
	}

	bottleneck := pkg.INF_WEIGHT

	for e := prev[sink]; e != nil; e = prev[e.GetFrom()] {
		residualCapacity := e.GetCapacity() - e.GetFlow()
		bottleneck = util.MinInt(bottleneck, residualCapacity)
	}

	for e := prev[sink]; e != nil; e = prev[e.GetFrom()] {
		e.AddFlow(bottleneck)
		ek.graph.GetEdgeById(e.GetID() ^ 1).AddFlow(-bottleneck)
	}

	return bottleneck
}

func (ek *EdmondsKarp) resetGraph() {
	ek.graph.ResetGraph()
}
func (ek *EdmondsKarp) computeMinCut(source, sink datastructure.Index, sources, sinks []datastructure.Index) *MinCut {

	var (
		minCut = NewMinCut(ek.graph.NumberOfVertices() - 2) // exclude artificial source and sink
	)
	maxFlow := 0
	for {
		ek.graph.HandleVisited(func(u datastructure.Index, visited bool) {
			ek.graph.SetVisited(u, false)
		})

		flow := ek.bfsAugmentingPath(source, sink)
		if flow == 0 {
			if ek.debug && !ek.validateResultOne(minCut, source, sink, sources, sinks) {
				fmt.Printf("Edmonds Karp: invalid min cut result")
			}
			ek.makeMinCutFlags(minCut, maxFlow)
			break
		}
		maxFlow += flow

	}

	return minCut
}

func (ek *EdmondsKarp) makeMinCutFlags(minCut *MinCut, numberOfMinCutEdges int) {
	for u := datastructure.Index(0); u < datastructure.Index(ek.graph.NumberOfVertices()-2); u++ { // exclude artificial source and sink
		if ek.graph.IsVisited(u) {
			minCut.SetFlag(u, true)
		} else {
			minCut.incrementNumNodesInPartitionTwo()
		}
	}
	minCut.setNumberofMinCutEdges(numberOfMinCutEdges)
}

func (ek *EdmondsKarp) validateResultOne(minCut *MinCut,
	source, target datastructure.Index, sources, sinks []datastructure.Index) bool {
	sourceSet := makeNodeSet(sources)
	sinkNodesSet := makeNodeSet(sinks)
	incomingFlow := make([]int, ek.graph.NumberOfVertices())
	outgoingFlow := make([]int, ek.graph.NumberOfVertices())
	cutEdgesCount := 0
	for u := datastructure.Index(0); u < datastructure.Index(ek.graph.NumberOfVertices()-2); u++ {
		for i := 0; i < ek.graph.GetVertexEdgesSize(u); i++ {
			edge := ek.graph.GetEdgeOfVertex(u, i)
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

	for u := datastructure.Index(0); u < datastructure.Index(ek.graph.NumberOfVertices()-2); u++ {
		if !nodeInSet(u, sourceSet) && !nodeInSet(u, sinkNodesSet) && incomingFlow[u] != outgoingFlow[u] {
			return false
		}
	}
	if minCut.GetNumberOfMinCutEdges() != cutEdgesCount {
		return false
	}

	return true
}

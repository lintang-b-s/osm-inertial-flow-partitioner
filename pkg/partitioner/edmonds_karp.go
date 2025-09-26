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
	queue := list.New()

	queue.PushBack(source)

	prevSource := &datastructure.MaxFlowEdge{}
	ek.graph.SetPrev(source, prevSource)

	for queue.Len() > 0 {
		u := queue.Remove(queue.Front()).(datastructure.Index)

		if u == sink {
			break
		}

		ek.graph.ForEachVertexEdges(u, func(e *datastructure.MaxFlowEdge) {
			if ek.graph.GetPrev(e.GetTo()) == nil && e.GetCapacity()-e.GetFlow() > 0 {
				ek.graph.SetPrev(e.GetTo(), e)
				queue.PushBack(e.GetTo())
			}
		})
	}

	if ek.graph.GetPrev(sink) == nil {
		return 0
	}

	bottleneck := pkg.INF_WEIGHT

	for e := ek.graph.GetPrev(sink); e != prevSource; e = ek.graph.GetPrev(e.GetFrom()) {
		residualCapacity := e.GetCapacity() - e.GetFlow()
		bottleneck = util.MinInt(bottleneck, residualCapacity)
	}

	for e := ek.graph.GetPrev(sink); e != prevSource; e = ek.graph.GetPrev(e.GetFrom()) {
		e.AddFlow(bottleneck)
		ek.graph.GetEdgeById(e.GetID() ^ 1).AddFlow(-bottleneck)
	}

	return bottleneck
}

func (ek *EdmondsKarp) computeMinCut(source, sink datastructure.Index, sources, sinks []datastructure.Index) *MinCut {

	var (
		minCut = NewMinCut(ek.graph.NumberOfVertices() - 2) // exclude artificial source and sink
	)
	maxFlow := 0
	for {
		ek.graph.ResetPrev()

		flow := ek.bfsAugmentingPath(source, sink)
		if flow == 0 {
			if ek.debug && !ek.validateResultOne(minCut, source, sink, sources, sinks) {
				fmt.Printf("invalid min cut result: violating capacity constraint, flow conservation, and max-flow min-cut theorem!!\n")
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
		if ek.graph.GetPrev(u) != nil {
			minCut.SetFlag(u, true)
		} else {
			minCut.incrementNumNodesInPartitionTwo()
		}
	}
	minCut.setNumberofMinCutEdges(numberOfMinCutEdges)
}

func (ek *EdmondsKarp) validateResultOne(minCut *MinCut,
	source, sink datastructure.Index, sources, sinks []datastructure.Index) bool {
	// see CLRS section 26.1 & 26.2
	sourceSet := makeNodeSet(sources)
	sinkSet := makeNodeSet(sinks)
	incomingFlow := make([]int, ek.graph.NumberOfVertices())
	outgoingFlow := make([]int, ek.graph.NumberOfVertices())
	cutEdgesCount := 0
	sourceOutgoingFlow := 0
	sinkIncomingFlow := 0
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

			if v != sink && v != source &&
				minCut.GetFlag(u) && !minCut.GetFlag(v) {
				cutEdgesCount++
			}

			if v == sink {
				sinkIncomingFlow += flow
			}
		}
	}

	for i := 0; i < ek.graph.GetVertexEdgesSize(source); i++ {
		edge := ek.graph.GetEdgeOfVertex(source, i)
		flow := edge.GetFlow()
		sourceOutgoingFlow += flow
	}

	for u := datastructure.Index(0); u < datastructure.Index(ek.graph.NumberOfVertices()-2); u++ {
		if !nodeInSet(u, sourceSet) && !nodeInSet(u, sinkSet) && incomingFlow[u] != outgoingFlow[u] {
			// Flow conservation, sum over incoming flow of a vertex not in source or sink must equal to sum over outgoing flow
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

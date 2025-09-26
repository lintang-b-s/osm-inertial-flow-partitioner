package partitioner

import (
	"math"

	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/concurrent"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"
)

type minCutJob struct {
	slope    float64
	diagonal bool
	line     []float64
}

func newMinCutJob(slope float64, diagonal bool, line []float64) minCutJob {
	return minCutJob{slope: slope, diagonal: diagonal, line: line}
}
func (mj minCutJob) GetSlope() float64 {
	return mj.slope
}

func (mj minCutJob) isDiagonal() bool {
	return mj.diagonal
}

func (mj minCutJob) getLine() []float64 {
	return mj.line
}

type inertialFlow struct {
	graph *datastructure.PartitionGraph
}

func NewInertialFlow(graph *datastructure.PartitionGraph) *inertialFlow {
	return &inertialFlow{graph: graph}
}

func (inf *inertialFlow) getPartitionGraph() *datastructure.PartitionGraph {
	return inf.graph
}

func (inf *inertialFlow) computeInertialFlowEdmondsKarp(sourceSinkRate float64) *MinCut {
	var (
		best                    *MinCut = &MinCut{}
		bestNumberOfMinCutEdges         = math.MaxInt
	)

	wpInertialFlow := concurrent.NewWorkerPool[minCutJob, *MinCut](5, 20)

	balanceDelta := func(numPartTwoNodes int, numberOfVertices int) int {
		diff := numberOfVertices/2 - numPartTwoNodes
		if diff < 0 {
			diff = -diff
		}
		return diff
	}

	for i := 0; i < pkg.INERTIAL_FLOW_ITERATION; i++ {
		slope := -1 + float64(i)*(2.0/pkg.INERTIAL_FLOW_ITERATION)
		wpInertialFlow.AddJob(newMinCutJob(slope, false, []float64{}))
	}

	wpInertialFlow.AddJob(newMinCutJob(0, true, []float64{1, 0}))
	wpInertialFlow.AddJob(newMinCutJob(0, true, []float64{0, 1}))
	wpInertialFlow.AddJob(newMinCutJob(0, true, []float64{1, 1}))
	wpInertialFlow.AddJob(newMinCutJob(0, true, []float64{1, -1}))
	wpInertialFlow.AddJob(newMinCutJob(0, true, []float64{-1, 1}))

	computeMinCut := func(input minCutJob) *MinCut {
		slope := input.GetSlope()
		ek := NewEdmondsKarp(inf.getPartitionGraph().Clone(), false)
		var (
			sources []datastructure.Index
			sinks   []datastructure.Index
		)
		if !input.isDiagonal() {
			sources, sinks = ek.sortVerticesByLineProjection(slope, sourceSinkRate)
		} else {
			sources, sinks = ek.sortVerticesByLineDiagonalProjection(input.getLine(), sourceSinkRate)
		}

		s, t := ek.createArtificialSourceSink(sources, sinks)
		return ek.computeMinCut(s, t, sources, sinks)
	}

	wpInertialFlow.Close()
	wpInertialFlow.Start(computeMinCut)
	wpInertialFlow.Wait()

	numberOfVertices := inf.graph.NumberOfVertices()
	for minCut := range wpInertialFlow.CollectResults() {
		if minCut.GetNumberOfMinCutEdges() < bestNumberOfMinCutEdges ||
			(best.GetNumberOfMinCutEdges() == minCut.GetNumberOfMinCutEdges() &&
				balanceDelta(minCut.GetNumNodesInPartitionTwo(),
					numberOfVertices) < balanceDelta(best.GetNumNodesInPartitionTwo(),
					numberOfVertices)) {
			best = minCut
			bestNumberOfMinCutEdges = minCut.GetNumberOfMinCutEdges()
		}
	}

	return best
}

func (inf *inertialFlow) computeInertialFlowDinic(sourceSinkRate float64) *MinCut {
	var (
		best                    *MinCut = &MinCut{}
		bestNumberOfMinCutEdges         = math.MaxInt
	)

	wpInertialFlow := concurrent.NewWorkerPool[minCutJob, *MinCut](5, 20)

	balanceDelta := func(numPartTwoNodes int, numberOfVertices int) int {
		diff := numberOfVertices/2 - numPartTwoNodes
		if diff < 0 {
			diff = -diff
		}
		return diff
	}

	for i := 0; i < pkg.INERTIAL_FLOW_ITERATION; i++ {
		slope := -1 + float64(i)*(2.0/pkg.INERTIAL_FLOW_ITERATION)
		wpInertialFlow.AddJob(newMinCutJob(slope, false, []float64{}))
	}

	wpInertialFlow.AddJob(newMinCutJob(0, true, []float64{1, 0}))
	wpInertialFlow.AddJob(newMinCutJob(0, true, []float64{0, 1}))
	wpInertialFlow.AddJob(newMinCutJob(0, true, []float64{1, 1}))
	wpInertialFlow.AddJob(newMinCutJob(0, true, []float64{1, -1}))
	wpInertialFlow.AddJob(newMinCutJob(0, true, []float64{-1, 1}))

	computeMinCut := func(input minCutJob) *MinCut {
		slope := input.GetSlope()
		dn := NewDinicMaxFlow(inf.getPartitionGraph().Clone(), false)
		var (
			sources []datastructure.Index
			sinks   []datastructure.Index
		)
		if !input.isDiagonal() {
			sources, sinks = dn.sortVerticesByLineProjection(slope, sourceSinkRate)
		} else {
			sources, sinks = dn.sortVerticesByLineDiagonalProjection(input.getLine(), sourceSinkRate)
		}

		return dn.ComputeMinCut(sources, sinks)
	}

	wpInertialFlow.Close()
	wpInertialFlow.Start(computeMinCut)
	wpInertialFlow.Wait()

	numberOfVertices := inf.graph.NumberOfVertices()
	for minCut := range wpInertialFlow.CollectResults() {
		if minCut.GetNumberOfMinCutEdges() < bestNumberOfMinCutEdges ||
			(best.GetNumberOfMinCutEdges() == minCut.GetNumberOfMinCutEdges() &&
				balanceDelta(minCut.GetNumNodesInPartitionTwo(),
					numberOfVertices) < balanceDelta(best.GetNumNodesInPartitionTwo(),
					numberOfVertices)) {
			best = minCut
			bestNumberOfMinCutEdges = minCut.GetNumberOfMinCutEdges()
		}
	}

	return best
}

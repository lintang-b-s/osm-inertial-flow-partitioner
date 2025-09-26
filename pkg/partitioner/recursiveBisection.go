package partitioner

import (
	"container/list"
	"sync"

	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"
	"go.uber.org/zap"
)

type RecursiveBisection struct {
	originalGraph   *datastructure.Graph
	maximumCellSize int
	finalPartition  []int // map from vertex id to partition id
	partitionCount  int
	logger          *zap.Logger
	mu              sync.Mutex
}

func NewRecursiveBisection(graph *datastructure.Graph, maximumCellSize int, logger *zap.Logger,
) *RecursiveBisection {
	finalPartitions := make([]int, graph.NumberOfVertices())
	for i := range finalPartitions {
		finalPartitions[i] = pkg.INVALID_PARTITION_ID
	}
	return &RecursiveBisection{
		originalGraph:   graph,
		maximumCellSize: maximumCellSize,
		finalPartition:  finalPartitions,
		partitionCount:  0,
		logger:          logger,
	}
}

// [On Balanced Separators in Road Networks, Schild, et al.] https://aschild.github.io/papers/roadseparator.pdf
func (rb *RecursiveBisection) Partition(initialNodeIds []datastructure.Index) {
	pg := rb.buildInitialPartitionGraph(initialNodeIds)
	queue := list.New()
	queue.PushBack(pg)

	for queue.Len() > 0 {

		curPartitionGraph := queue.Remove(queue.Front()).(*datastructure.PartitionGraph)

		iflow := NewInertialFlow(curPartitionGraph)
		cut := iflow.computeInertialFlowDinic(pkg.SOURCE_SINK_RATE)
		tooSmall := func(partitionSize int) bool {
			return partitionSize < rb.maximumCellSize
		}

		partOne, partTwo := rb.applyBisection(cut, curPartitionGraph)

		if !tooSmall(partOne.NumberOfVertices()) {
			queue.PushBack(partOne)
		} else {
			rb.assignFinalPartition(partOne)
		}
		if !tooSmall(partTwo.NumberOfVertices()) {
			queue.PushBack(partTwo)
		} else {
			rb.assignFinalPartition(partTwo)
		}
	}

}

func (rb *RecursiveBisection) applyBisection(cut *MinCut, graph *datastructure.PartitionGraph) (*datastructure.PartitionGraph, *datastructure.PartitionGraph) {
	var (
		partitionOne = datastructure.NewPartitionGraph(graph.NumberOfVertices() - cut.GetNumNodesInPartitionTwo())
		partitionTwo = datastructure.NewPartitionGraph(cut.GetNumNodesInPartitionTwo())
	)

	// remap id
	partOneId := datastructure.Index(0)
	partTwoId := datastructure.Index(0)

	partOneMap := make(map[datastructure.Index]datastructure.Index)
	partTwoMap := make(map[datastructure.Index]datastructure.Index)
	origGraphVertIdMap := make(map[datastructure.Index]datastructure.Index) // map from original graph vertex id to partition graph vertex id
	graph.ForEachVertices(func(v datastructure.PartitionVertex) {
		if v.GetOriginalVertexID() == datastructure.Index(pkg.ARTIFICIAL_SOURCE_ID) ||
			v.GetOriginalVertexID() == datastructure.Index(pkg.ARTIFICIAL_SINK_ID) {
			// skip artificial source and sink
			return
		}
		origGraphVertIdMap[v.GetOriginalVertexID()] = v.GetID()

		lat, lon := v.GetVertexCoordinate()
		if cut.GetFlag(v.GetID()) {
			newVertex := datastructure.NewPartitionVertex(partOneId, v.GetOriginalVertexID(),
				lat, lon)
			partitionOne.AddVertex(newVertex)
			partOneMap[v.GetID()] = partOneId
			partOneId++
		} else {
			newVertex := datastructure.NewPartitionVertex(partTwoId, v.GetOriginalVertexID(),
				lat, lon)
			partitionTwo.AddVertex(newVertex)
			partTwoMap[v.GetID()] = partTwoId
			partTwoId++
		}
	})

	for _, uVertex := range graph.GetVertices() {
		rb.originalGraph.ForOutEdgesOfVertex(uVertex.GetOriginalVertexID(), func(e *datastructure.OutEdge, exitPoint datastructure.Index) {
			v, ok := origGraphVertIdMap[e.GetHead()]
			if !ok {
				return
			}
			u := uVertex.GetID()
			if cut.GetFlag(u) && cut.GetFlag(v) {
				uPartOne := partOneMap[u]
				vPartOne := partOneMap[v]
				partitionOne.AddEdge(uPartOne, vPartOne)
			} else if !cut.GetFlag(u) && !cut.GetFlag(v) {
				uPartTwo := partTwoMap[u]
				vPartTwo := partTwoMap[v]
				partitionTwo.AddEdge(uPartTwo, vPartTwo)
			}
		})
	}

	return partitionOne, partitionTwo
}

func (rb *RecursiveBisection) assignFinalPartition(graph *datastructure.PartitionGraph) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for i := 0; i < graph.NumberOfVertices(); i++ {
		v := graph.GetVertex(datastructure.Index(i))
		originalId := v.GetOriginalVertexID()
		rb.finalPartition[originalId] = rb.partitionCount
	}
	rb.partitionCount++
}

func (rb *RecursiveBisection) buildInitialPartitionGraph(initialVerticeIds []datastructure.Index) *datastructure.PartitionGraph {
	pg := datastructure.NewPartitionGraph(len(initialVerticeIds))
	// initialVerticeIds = stil original vertex id

	initialVerticeIdSet := makeNodeSet(initialVerticeIds)

	newVid := datastructure.Index(0)
	newMapVid := make(map[datastructure.Index]datastructure.Index)
	for _, vId := range initialVerticeIds {
		lat, lon := rb.originalGraph.GetVertexCoordinates(vId)
		vertex := datastructure.NewPartitionVertex(newVid, vId, lat, lon)
		newMapVid[vId] = newVid
		pg.AddVertex(vertex)
		newVid++
	}

	for _, vId := range initialVerticeIds {
		rb.originalGraph.ForOutEdgesOfVertex(vId, func(e *datastructure.OutEdge, exitPoint datastructure.Index) {
			if _, adjVertexInSet := initialVerticeIdSet[exitPoint]; !adjVertexInSet {
				// skip arc that have endpoint outside current cell
				return
			}
			pg.AddEdge(newMapVid[vId], newMapVid[e.GetHead()])
		})
	}

	return pg
}

func (rb *RecursiveBisection) GetFinalPartition() []int {
	return rb.finalPartition
}

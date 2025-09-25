package partitioner

import (
	"container/list"
	"fmt"
	"log"

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
}

func NewRecursiveBisection(graph *datastructure.Graph, maximumCellSize int, logger *zap.Logger) *RecursiveBisection {
	return &RecursiveBisection{
		originalGraph:   graph,
		maximumCellSize: maximumCellSize,
		finalPartition:  make([]int, graph.NumberOfVertices()),
		partitionCount:  0,
		logger:          logger,
	}
}

// [On Balanced Separators in Road Networks, Schild, et al.] https://aschild.github.io/papers/roadseparator.pdf
func (rb *RecursiveBisection) Partition() {
	pg := rb.buildInitialPartitionGraph()
	queue := list.New()
	queue.PushBack(pg)

	for queue.Len() > 0 {

		curPartitionGraph := queue.Remove(queue.Front()).(*datastructure.PartitionGraph)

		cut := NewDinicMaxFlow().computeInertialFlow(curPartitionGraph, pkg.SOURCE_SINK_RATE)

		tooSmall := func(partitionSize int) bool {
			return partitionSize < rb.maximumCellSize
		}

		if len(cut.flags) == 0 {
			fmt.Printf("debug")
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
	graph.ForEachVertices(func(v datastructure.PartitionVertex) {
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

	graph.ForEachVertices(func(v datastructure.PartitionVertex) {
		graph.ForEachVertexEdges(v.GetID(), func(e *datastructure.MaxFlowEdge) {
			u := e.GetFrom()
			v := e.GetTo()

			// exclude cut edges (edges that connect partition one and two)
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
	})

	return partitionOne, partitionTwo
}

func (rb *RecursiveBisection) assignFinalPartition(graph *datastructure.PartitionGraph) {
	log.Printf("Created partition %d with %d vertices", rb.partitionCount, graph.NumberOfVertices())
	for i := 0; i < graph.NumberOfVertices(); i++ {
		v := graph.GetVertex(datastructure.Index(i))
		originalId := v.GetOriginalVertexID()
		rb.finalPartition[originalId] = rb.partitionCount
	}
	rb.partitionCount++
}

func (rb *RecursiveBisection) buildInitialPartitionGraph() *datastructure.PartitionGraph {
	pg := datastructure.NewPartitionGraph(rb.originalGraph.NumberOfVertices())
	rb.originalGraph.ForEachVertices(func(v *datastructure.Vertex, vId datastructure.Index) {
		lat, lon := rb.originalGraph.GetVertexCoordinates(vId)
		vertex := datastructure.NewPartitionVertex(vId, vId, lat, lon)

		pg.AddVertex(vertex)

		rb.originalGraph.ForOutEdgesOfVertex(vId, func(e *datastructure.OutEdge, exitPoint datastructure.Index) {
			pg.AddEdge(vId, e.GetHead())
		})
	})
	return pg
}

func (rb *RecursiveBisection) GetFinalPartition() []int {
	return rb.finalPartition
}

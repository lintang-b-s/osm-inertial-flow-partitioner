package partitioner

import "github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"

type MinCut struct {
	flags                  []bool // true if the vertex is reachable from source in residual graph, or partition one, else partition two
	numNodesInPartitionTwo int    // number of nodes in source partition (partition 2)
	numberOfMinCutEdges    int    // number of edges in the min cut
}

func NewMinCut(numberOfVertices int) *MinCut {
	return &MinCut{
		flags: make([]bool, numberOfVertices),
	}
}

func (mc *MinCut) SetFlag(u datastructure.Index, flag bool) {
	mc.flags[u] = flag
}

func (mc *MinCut) GetFlag(u datastructure.Index) bool {
	return mc.flags[u]
}

func (mc *MinCut) GetNumNodesInPartitionTwo() int {
	return mc.numNodesInPartitionTwo
}

func (mc *MinCut) incrementNumNodesInPartitionTwo() {
	mc.numNodesInPartitionTwo++
}

func (mc *MinCut) GetNumberOfMinCutEdges() int {
	return mc.numberOfMinCutEdges
}

func (mc *MinCut) setNumberofMinCutEdges(n int) {
	mc.numberOfMinCutEdges = n
}

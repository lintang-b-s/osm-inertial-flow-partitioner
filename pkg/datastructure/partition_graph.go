package datastructure

// we use capacity 1 for each edge in graph (see On Balanced Separators in Road Networks, Schild, et al.)
// maximum flow using Dinic’s algorithm (augmenting paths, in the unit-capacity case computed by breadth-first search)
type MaxFlowEdge struct {
	u        Index
	v        Index
	capacity float64
	flow     float64
}

func NewMaxFlowEdge(u, v Index, capacity float64) *MaxFlowEdge {
	return &MaxFlowEdge{
		u:        u,
		v:        v,
		capacity: capacity,
		flow:     0,
	}
}

func (e *MaxFlowEdge) GetCapacity() float64 {
	return e.capacity
}

func (e *MaxFlowEdge) GetFlow() float64 {
	return e.flow
}

func (e *MaxFlowEdge) GetFrom() Index {
	return e.u
}

func (e *MaxFlowEdge) GetTo() Index {
	return e.v
}

func (e *MaxFlowEdge) AddFlow(f float64) {
	e.flow += f
}

type PartitionVertex struct {
	id               Index
	originalVertexId Index
	lat, lon         float64
}

func NewPartitionVertex(id, originalVertexId Index, lat, lon float64) PartitionVertex {
	return PartitionVertex{
		id:               id,
		originalVertexId: originalVertexId,
		lat:              lat,
		lon:              lon,
	}
}

func (v *PartitionVertex) GetID() Index {
	return v.id
}

func (v *PartitionVertex) SetId(id Index) {
	v.id = id
}

func (v *PartitionVertex) GetOriginalVertexID() Index {
	return v.originalVertexId
}

func (v *PartitionVertex) GetVertexCoordinate() (float64, float64) {
	return v.lat, v.lon
}

type PartitionGraph struct {
	vertices      []PartitionVertex
	adjacencyList [][]int
	edgeList      []*MaxFlowEdge
	level         []int
	last          []int
}

func NewPartitionGraph(numberOfVertices int) *PartitionGraph {
	adjacencyList := make([][]int, numberOfVertices)
	for i := range adjacencyList {
		adjacencyList[i] = make([]int, 0)
	}
	return &PartitionGraph{
		vertices:      make([]PartitionVertex, 0),
		adjacencyList: adjacencyList,
		edgeList:      make([]*MaxFlowEdge, 0),
		level:         make([]int, numberOfVertices),
		last:          make([]int, numberOfVertices),
	}
}

func (g *PartitionGraph) GetVertices() []PartitionVertex {
	return g.vertices
}
func (g *PartitionGraph) NumberOfVertices() int {
	return len(g.vertices)
}

func (g *PartitionGraph) AddVertex(v PartitionVertex) {
	g.vertices = append(g.vertices, v)
}

func (g *PartitionGraph) GetVertex(u Index) PartitionVertex {
	return g.vertices[u]
}

func (g *PartitionGraph) AddEdge(u, v Index) {
	if u == v {
		return
	}

	// undirected graph
	edge := NewMaxFlowEdge(u, v, 1)
	g.edgeList = append(g.edgeList, edge)
	g.adjacencyList[u] = append(g.adjacencyList[u], len(g.edgeList)-1)

	reverseEdge := NewMaxFlowEdge(v, u, 1)
	g.edgeList = append(g.edgeList, reverseEdge)
	g.adjacencyList[v] = append(g.adjacencyList[v], len(g.edgeList)-1)
}

func (g *PartitionGraph) ResetGraph() {
	for _, edge := range g.edgeList {
		edge.flow = 0
		edge.capacity = 1
	}
}

func (g *PartitionGraph) GetVertexLevel(u Index) int {
	return g.level[u]
}
func (g *PartitionGraph) SetVertexLevel(u Index, level int) {
	g.level[u] = level
}

func (g *PartitionGraph) GetLastEdgeIndex(u Index) int {
	return g.last[u]
}

func (g *PartitionGraph) SetLastEdgeIndex(u Index, idx int) {
	g.last[u] = idx
}

func (g *PartitionGraph) IncrementLastEdgeIndex(u Index) {
	g.last[u]++
}

func (g *PartitionGraph) GetVertexEdgesSize(u Index) int {
	return len(g.adjacencyList[u])
}

func (g *PartitionGraph) GetEdgeOfVertex(u Index, idx int) *MaxFlowEdge {
	edgeIndex := g.adjacencyList[u][idx]
	return g.edgeList[edgeIndex]
}

func (g *PartitionGraph) GetReversedEdgeOfVertex(u Index, idx int) *MaxFlowEdge {
	edgeIndex := g.adjacencyList[u][idx] ^ 1
	return g.edgeList[edgeIndex]
}

func (g *PartitionGraph) ForEachVertexEdges(u Index, handle func(e *MaxFlowEdge)) {
	for _, edgeIdx := range g.adjacencyList[u] {
		handle(g.edgeList[edgeIdx])
	}
}

func (g *PartitionGraph) ForEachVertices(handle func(v PartitionVertex)) {
	for _, v := range g.vertices {
		handle(v)
	}
}

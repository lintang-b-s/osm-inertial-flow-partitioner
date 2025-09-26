package partitioner

import (
	"math"
	"sort"

	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"
)

// [see clrs section 26.1]   We add a supersource s and add a directed edge (s,si) with c(s,si)=inf for each i=1,2,3,..,m We also create a new supersink t and add a directed edge (ti,t) with capacity c(ti,t) = inf for each i=1,2,...,n
func (ek *EdmondsKarp) createArtificialSourceSink(sourceNodes, sinkNodes []datastructure.Index) (datastructure.Index, datastructure.Index) {
	artificialSource := datastructure.Index(ek.graph.NumberOfVertices())
	artificialSink := datastructure.Index(ek.graph.NumberOfVertices() + 1)

	ek.graph.AddVertex(datastructure.NewPartitionVertex(artificialSource, datastructure.Index(pkg.ARTIFICIAL_SOURCE_ID), 0.0, 0.0))
	ek.graph.AddVertex(datastructure.NewPartitionVertex(artificialSink, datastructure.Index(pkg.ARTIFICIAL_SINK_ID), 0.0, 0.0))

	for _, s := range sourceNodes {
		ek.graph.AddInfEdge(artificialSource, s)
	}

	for _, t := range sinkNodes {
		ek.graph.AddInfEdge(t, artificialSink)
	}
	return artificialSource, artificialSink
}

func (ek *EdmondsKarp) sortVerticesByLineProjection(slope, ratio float64) ([]datastructure.Index, []datastructure.Index) {

	vertices := ek.graph.GetVertices()

	type item struct {
		idx        int
		projection float64
	}
	n := len(vertices)

	items := make([]item, n)
	for i, v := range vertices {
		lat, lon := v.GetVertexCoordinate()
		proj := slope*lon + (1.0-math.Abs(slope))*lat
		items[i] = item{idx: i, projection: proj}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].projection < items[j].projection
	})

	endpointsLength := int(float64(n) * ratio)
	sourceNodes := make([]datastructure.Index, 0, endpointsLength)
	sinkNodes := make([]datastructure.Index, 0, endpointsLength)

	for i := 0; i < endpointsLength; i++ {
		sourceNodes = append(sourceNodes, vertices[items[i].idx].GetID())
		sinkNodes = append(sinkNodes, vertices[items[n-1-i].idx].GetID())
	}
	return sourceNodes, sinkNodes
}

func (ek *EdmondsKarp) sortVerticesByLineDiagonalProjection(line []float64, ratio float64) ([]datastructure.Index, []datastructure.Index) {

	vertices := ek.graph.GetVertices()

	type item struct {
		idx        int
		projection float64
	}
	n := len(vertices)

	items := make([]item, n)
	for i, v := range vertices {
		lat, lon := v.GetVertexCoordinate()
		proj := line[0]*lon + line[1]*lat
		items[i] = item{idx: i, projection: proj}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].projection < items[j].projection
	})

	endpointsLength := int(float64(n) * ratio)
	sourceNodes := make([]datastructure.Index, 0, endpointsLength)
	sinkNodes := make([]datastructure.Index, 0, endpointsLength)

	for i := 0; i < endpointsLength; i++ {
		sourceNodes = append(sourceNodes, vertices[items[i].idx].GetID())
		sinkNodes = append(sinkNodes, vertices[items[n-1-i].idx].GetID())
	}
	return sourceNodes, sinkNodes
}

func (dn *DinicMaxFlow) sortVerticesByLineProjection(slope, ratio float64) ([]datastructure.Index, []datastructure.Index) {

	vertices := dn.graph.GetVertices()

	type item struct {
		idx        int
		projection float64
	}
	n := len(vertices)

	items := make([]item, n)
	for i, v := range vertices {
		lat, lon := v.GetVertexCoordinate()
		proj := slope*lon + (1.0-math.Abs(slope))*lat
		items[i] = item{idx: i, projection: proj}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].projection < items[j].projection
	})

	endpointsLength := int(float64(n) * ratio)
	sourceNodes := make([]datastructure.Index, 0, endpointsLength)
	sinkNodes := make([]datastructure.Index, 0, endpointsLength)

	for i := 0; i < endpointsLength; i++ {
		sourceNodes = append(sourceNodes, vertices[items[i].idx].GetID())
		sinkNodes = append(sinkNodes, vertices[items[n-1-i].idx].GetID())
	}
	return sourceNodes, sinkNodes
}

func (dn *DinicMaxFlow) sortVerticesByLineDiagonalProjection(line []float64, ratio float64) ([]datastructure.Index, []datastructure.Index) {

	vertices := dn.graph.GetVertices()

	type item struct {
		idx        int
		projection float64
	}
	n := len(vertices)

	items := make([]item, n)
	for i, v := range vertices {
		lat, lon := v.GetVertexCoordinate()
		proj := line[0]*lon + line[1]*lat
		items[i] = item{idx: i, projection: proj}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].projection < items[j].projection
	})

	endpointsLength := int(float64(n) * ratio)
	sourceNodes := make([]datastructure.Index, 0, endpointsLength)
	sinkNodes := make([]datastructure.Index, 0, endpointsLength)

	for i := 0; i < endpointsLength; i++ {
		sourceNodes = append(sourceNodes, vertices[items[i].idx].GetID())
		sinkNodes = append(sinkNodes, vertices[items[n-1-i].idx].GetID())
	}
	return sourceNodes, sinkNodes
}

package partitioner

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"
	"golang.org/x/exp/rand"
)

func (mp *MulitlevelPartitioner) savePartitionsToFile(partitions [][]datastructure.Index, graph *datastructure.Graph,
	name string, level int) error {
	type partitionType struct {
		Nodes []datastructure.Coordinate `json:"nodes"`
	}
	rand.Seed(uint64(time.Now().UnixNano()))

	parts := []partitionType{}
	for _, partition := range partitions {
		rand.Shuffle(len(partition), func(i, j int) { partition[i], partition[j] = partition[j], partition[i] })
		partitionNodes := make([]datastructure.Coordinate, 0)

		for i := 0; i < int(float64(len(partition))*0.3); i++ {
			node := graph.GetVertex(partition[i])
			partitionNodes = append(partitionNodes, datastructure.NewCoordinate(
				node.GetLat(), node.GetLon(),
			))
		}
		parts = append(parts, partitionType{
			Nodes: partitionNodes,
		})
	}
	buf, err := json.MarshalIndent(parts, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(fmt.Sprintf("nodePerPartitions_%s_level_%v.json", name, level), buf, 0644); err != nil {
		return err
	}
	return nil
}

func (mp *MulitlevelPartitioner) writeMLPToMLPFile(filename string) error {

	numCells := make([]int, mp.l)
	for i := 0; i < mp.l; i++ {
		numCells[i] = len(mp.overlayNodes[i])
	}

	pvOffset := make([]int, mp.l+1)
	for i := 0; i < mp.l; i++ {
		pvOffset[i+1] = pvOffset[i] + int(math.Ceil(math.Log2(float64(numCells[i])))) // ceil(log2(numCells[i])) = number of bits needed to represent cell id in level-i
	}

	cellNumbers := make([]uint64, mp.graph.NumberOfVertices()) // 64 bit integer. rightmost contain level 0 cellId, leftmost contain level l-1 cellId

	for l := 0; l < mp.l; l++ {
		for cellId, vertexIds := range mp.overlayNodes[l] {
			for _, vertexId := range vertexIds {
				cellNumbers[vertexId] |= uint64(cellId) << uint64(pvOffset[l])
			}
		}
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}

	defer f.Close()

	_, err = f.WriteString(fmt.Sprintf("%d\n", len(numCells)))
	if err != nil {
		return err
	}

	for i := 0; i < len(numCells); i++ {
		_, err := f.WriteString(fmt.Sprintf("%d\n", numCells[i]))
		if err != nil {
			return err
		}
	}

	_, err = f.WriteString(fmt.Sprintf("%d\n", mp.graph.NumberOfVertices()))
	if err != nil {
		return err
	}

	for _, vertexID := range mp.graph.GetVerticeIds() {
		_, err := f.WriteString(fmt.Sprintf("%d\n", cellNumbers[vertexID]))
		if err != nil {
			return err
		}
	}
	return nil
}

package main

import (
	"math"

	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/logger"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/osmparser"
	"github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/partitioner"
)

func main() {
	logger, err := logger.New()
	if err != nil {
		panic(err)
	}
	osmParser := osmparser.NewOSMParserV2()

	graph := osmParser.Parse("./data/solo_jogja.osm.pbf", logger)

	partitioner := partitioner.NewRecursiveBisection(graph, int(math.Pow(2, 17)), logger)
	partitioner.Partition()

	finalPartition := partitioner.GetFinalPartition()
	_ = finalPartition
}

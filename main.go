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

	mlp := partitioner.NewMultilevelPartitioner(
		[]int{int(math.Pow(2, 8)), int(math.Pow(2, 11)), int(math.Pow(2, 14)), int(math.Pow(2, 17)), int(math.Pow(2, 20))},
		5,
		graph, logger,
	)
	mlp.RunMultilevelPartitioning("solo_jogja")
}

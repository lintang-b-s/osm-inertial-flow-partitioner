package osmparser

import "github.com/lintang-b-s/osm-inertial-flow-partitioner/pkg/datastructure"

type Edge struct {
	from          uint32
	to            uint32
	weight        float64
	distance      float64
	edgeID        uint32
	bidirectional bool
}

func (e *Edge) GetFrom() datastructure.Index {
	return datastructure.Index(e.from)
}

func (e *Edge) GetTo() datastructure.Index {
	return datastructure.Index(e.to)
}

func NewEdge(from, to uint32, weight, distance float64, edgeID uint32, bidirectional bool) Edge {
	return Edge{
		from:          from,
		to:            to,
		weight:        weight,
		distance:      distance,
		edgeID:        edgeID,
		bidirectional: bidirectional,
	}
}

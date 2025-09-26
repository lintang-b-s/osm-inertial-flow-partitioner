package pkg

// enum of turn_type
type TurnType uint8

const (
	LEFT_TURN TurnType = iota
	RIGHT_TURN
	STRAIGHT_ON
	U_TURN
	NO_ENTRY
	NONE
)

const (
	INF_WEIGHT              int = 1e9
	MAX_FLOW                    = 2e9
	INERTIAL_FLOW_ITERATION     = 5
	INVALID_PARTITION_ID        = -1
	SOURCE_SINK_RATE            = 0.25
	ARTIFICIAL_SOURCE_ID        = int32(2147483646)
	ARTIFICIAL_SINK_ID          = int32(2147483647)
)

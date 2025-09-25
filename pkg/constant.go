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
	INF_WEIGHT              = 1e9
	MAX_FLOW                = 2e9
	INERTIAL_FLOW_ITERATION = 3
	SOURCE_SINK_RATE        = 0.25
)

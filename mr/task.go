package mr

import "time"

type Task struct {
	Id string
	// 0:stop 1:map 2:reduce
	Type    int
	Inputs  []string
	OutPuts []string
	// start running time
	startTime time.Time
}

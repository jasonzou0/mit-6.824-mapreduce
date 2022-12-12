package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type MapTask struct {
	TaskId    int
	InputFile string
}

type ReduceTask struct {
}

type TaskType int

const (
	Mapper = iota
	Reducer
)

type WorkerTask struct {
	Type       TaskType
	MapTask    MapTask
	ReduceTask ReduceTask
}

// Add your RPC definitions here.
type GetTaskRequest struct {
	WorkerId string
}

type GetTaskResponse struct {
	Task WorkerTask
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

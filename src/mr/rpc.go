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
	NReduce   int
}

type ReduceTask struct {
	// Reducer task id is also the reducer shard number.
	TaskId int
	// InputFiles[i] stores the tempfile coming from Mapper task i
	InputFiles map[int]string
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

//
// The GetTask RPC interface
//
type GetTaskRequest struct {
	WorkerId string
}

type GetTaskResponse struct {
	Task WorkerTask
}

//
// The TaskDone RPC interface
//
type TaskDoneRequest struct {
	WorkerId string
	TaskDone WorkerTask
	// This stores the temp file output from the mapper as a map from reduce shard to filename
	TempFiles map[int]string
}

type TaskDoneResponse struct {
	Ok bool
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

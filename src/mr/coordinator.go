package mr

import "errors"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Status int

const (
	Unassigned = iota
	Assigned
	Abandoned
)

type Task struct {
	map_task  MapTask
	worker_id string
	status    Status
}

type Coordinator struct {
	// Your definitions here.
	input_files []string
	n_reduce    int
	map_tasks   []Task
}

// Your code here -- RPC handlers for the worker to call.

//
// Task assignment RPC
func (c *Coordinator) GetTask(request *GetTaskRequest, reply *GetTaskResponse) error {
	// TODO: lock this method or the underlying data structure
	map_task_i := -1
	for i, map_task := range c.map_tasks {
		if map_task.status == Unassigned {
			map_task_i = i
			break
		}
	}
	if map_task_i == -1 {
		return errors.New("No more tasks")
	}
	// Update internal status
	c.map_tasks[map_task_i].worker_id = request.WorkerId
	c.map_tasks[map_task_i].status = Assigned
	// Handle response
	reply.Task.Type = Mapper
	reply.Task.MapTask = c.map_tasks[map_task_i].map_task
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Make len(files) number of map tasks.
	c := Coordinator{files, nReduce, make([]Task, len(files))}
	for i := 0; i < len(files); i++ {
		c.map_tasks[i] =
			Task{MapTask{i, files[i], c.n_reduce}, "", Unassigned}
	}

	// Your code here.

	// Design:
	// 1. iterate over the files
	// 2. store status in a map.
	// 2.5 figure out representation for map and reduce tasks
	// 3. implement "get available task" rpc method - for both map and reduce tasks
	//    each map & reduce task keeps track of the workers and their status.
	// 4. implement "task done" rpc method - for communicating to coordinate that work is done

	// Q: where is the worker pool maintained??? ans: in the coordinator
	// Q: how should the communication model work between coordinator and worker?
	// -  get_task: worker->coordinator
	// -  task_done: worker->coordinator
	// -  health_check: coordinator->worker but simplified to just waiting for 10s
	// Q: how do i know if the intermediate file is done generated for a single reduce task?
	// - all using files: mr-X-Y is the output of map task X for reduce task Y.
	// - coordinator will periodically check if all map tasks for a given reduce shard is complete.
	// Q: where does the sorting of immediate data happen?
	// - should happen in the worker. ideally there is a shuffling worker whose job is sorting.

	c.server()
	return &c
}

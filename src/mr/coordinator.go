package mr

import "errors"
import "fmt"
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
	Done
)

// This is the coordinator's internal representation of a task. Wraps around the
// the WorkTask construct from the RPC interface.
type InternalTask struct {
	task      WorkerTask
	worker_id string
	status    Status
}

type Coordinator struct {
	// Your definitions here.
	input_files  []string
	map_tasks    []InternalTask
	reduce_tasks []InternalTask
}

// Your code here -- RPC handlers for the worker to call.

// Get an map task that is available to be assigned
func (c *Coordinator) GetAvailableMapTask() *InternalTask {
	var map_task_found *InternalTask
	for i := 0; i < len(c.map_tasks); i++ {
		if c.map_tasks[i].status == Unassigned {
			map_task_found = &c.map_tasks[i]
			break
		}
	}
	return map_task_found
}

// Get a reduce task that is available to be assigned
func (c *Coordinator) GetAvailableReduceTask() *InternalTask {
	var reduce_task_found *InternalTask
	n_map := len(c.map_tasks)
	
	for i := 0; i < len(c.reduce_tasks); i++ {
		if c.reduce_tasks[i].status == Unassigned && n_map == len(c.reduce_tasks[i].task.ReduceTask.InputFiles) {
			reduce_task_found = &c.reduce_tasks[i]
			break
		}
	}
	return reduce_task_found
}


//
// Task assignment RPC
func (c *Coordinator) GetTask(request *GetTaskRequest, reply *GetTaskResponse) error {
	// TODO: lock this method or the underlying data structure
	task_found := c.GetAvailableMapTask()
	if task_found == nil {
		log.Printf("No more Map tasks. Pinging reduce tasks..")
		task_found = c.GetAvailableReduceTask()
	}
	if task_found == nil {
		return errors.New("No more tasks!")
	}
	// Update internal status
	task_found.worker_id = request.WorkerId
	task_found.status = Assigned
	// Handle response
	reply.Task = task_found.task
	return nil
}

func (c *Coordinator) SendMapOutputToReduceTasks(map_task_id int, temp_files map[int]string) error {
	for r_shard, fname := range temp_files {
		if !(r_shard >= 0 && r_shard <= len(c.reduce_tasks)) {
			return errors.New(fmt.Sprintf("Invalid reduce shard number %d", r_shard))
		}

		reduce_task := &c.reduce_tasks[r_shard].task.ReduceTask
		old_fname, exists := reduce_task.InputFiles[map_task_id]
		if exists {
			log.Printf("Overwriting existing input for reduce task %d from map task %d: %s", r_shard, map_task_id, old_fname)
		}
		reduce_task.InputFiles[map_task_id] = fname
	}
	return nil
}

func task_type_to_str(task_type TaskType) string {
	if task_type == Mapper {
		return "Map"
	} else {
		return "Reduce"
	}
}

//
// Marking task as done RPC
func (c *Coordinator) TaskDone(request *TaskDoneRequest, reply *TaskDoneResponse) error {
	task_done := &request.TaskDone
	var task_id int
	if task_done.Type == Mapper {
		task_id = task_done.MapTask.TaskId
		if !(task_id >= 0 && task_id < len(c.map_tasks)) {
			return errors.New("Invalid map task id")
		}
		internal_task := &c.map_tasks[task_id]
		if internal_task.worker_id != request.WorkerId {
			return errors.New("Worker id is not supposed to be working on Map task")
		}
		internal_task.status = Done
		err := c.SendMapOutputToReduceTasks(task_id, request.TempFiles)
		if err != nil {
			reply.Ok = false
			return err
		}
	} else {
		// Type must be reducer
		task_id = task_done.ReduceTask.TaskId
		if !(task_id >= 0 && task_id < len(c.reduce_tasks)) {
			return errors.New("Invalid reduce task id")
		}
		internal_task := &c.reduce_tasks[task_id]
		if internal_task.worker_id != request.WorkerId {
			return errors.New("Worker id is not supposed to be working on Reduce task")
		}
		internal_task.status = Done
	}

	log.Printf("%s task %d done", task_type_to_str(task_done.Type), task_id)
	reply.Ok = true
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

// Creates a new map task
func NewMapTask(task_id int, input_file string, n_reduce int) WorkerTask {
	return WorkerTask{
		Mapper,
		MapTask{
			task_id,
			input_file,
			n_reduce,
		},
		ReduceTask{},
	}
}

// Creates a new reduce task with InputFiles initialized
func NewReduceTask(task_id int, n_mapper int) WorkerTask {
	return WorkerTask{
		Reducer,
		MapTask{},
		ReduceTask{task_id, make(map[int]string)},
	}

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Make len(files) number of map tasks and nReduce number of reduce tasks
	c := Coordinator{files, make([]InternalTask, len(files)), make([]InternalTask, nReduce)}
	for i := 0; i < len(files); i++ {
		c.map_tasks[i] =
			InternalTask{NewMapTask(i, files[i], nReduce), "", Unassigned}
	}
	for i := 0; i < nReduce; i++ {
		c.reduce_tasks[i] =
			InternalTask{NewReduceTask(i, len(files)), "", Unassigned}
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

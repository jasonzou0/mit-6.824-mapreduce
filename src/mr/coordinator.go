package mr

import "errors"
import "fmt"
import "log"
import "net"
import "os"
import "sync"
import "time"
import "net/rpc"
import "net/http"

type Status int

var DEBUG = false

const (
	Unassigned = iota
	Assigned
	Abandoned
	Done
)

// This is the coordinator's internal representation of a task. Wraps around the
// the WorkTask construct from the RPC interface.
type InternalTask struct {
	task          WorkerTask
	worker_id     string
	status        Status
	assigned_time time.Time
}

type Coordinator struct {
	// Synchronizes access to all the tasks below
	tasks_mu sync.Mutex
	// They are laid out so that the first n_map tasks must be map tasas and remaining ones are reducer tasks.
	// Reducer task with task_id i is guaranteed to be in all_tasks[n_map+i]
	all_tasks []InternalTask
	n_map     int
	n_reduce  int
}

// Your code here -- RPC handlers for the worker to call.

// Get an map task that is available to be assigned
func (c *Coordinator) GetAvailableMapTask() *InternalTask {
	var map_task_found *InternalTask
	for i := 0; i < c.n_map; i++ {
		if c.all_tasks[i].task.Type != Mapper {
			log.Fatalf("Expect Mapper task at index %d but actually saw %v", i, c.all_tasks[i])
		}
		if c.all_tasks[i].status == Unassigned {
			map_task_found = &c.all_tasks[i]
			break
		}
	}
	return map_task_found
}

// Get a reduce task that is available to be assigned
func (c *Coordinator) GetAvailableReduceTask() *InternalTask {
	var reduce_task_found *InternalTask
	for i := c.n_map; i < len(c.all_tasks); i++ {
		// The 2nd condition checks that the reduce task has received all the input from all the mappers.
		if c.all_tasks[i].task.Type != Reducer {
			log.Fatalf("Expect reducer task at index %d but actually saw %v", i, c.all_tasks[i])
		}
		if c.all_tasks[i].status == Unassigned && c.n_map == len(c.all_tasks[i].task.ReduceTask.InputFiles) {
			reduce_task_found = &c.all_tasks[i]
			break
		}
	}
	return reduce_task_found
}

//
// Task assignment RPC
func (c *Coordinator) GetTask(request *GetTaskRequest, reply *GetTaskResponse) error {
	// TODO: lock this method or the underlying data structure
	c.tasks_mu.Lock()
	defer c.tasks_mu.Unlock()
	task_found := c.GetAvailableMapTask()
	if task_found == nil {
		task_found = c.GetAvailableReduceTask()
	}
	if task_found == nil {
		return errors.New("No more tasks!")
	}
	// Update internal status
	task_found.worker_id = request.WorkerId
	task_found.status = Assigned
	task_found.assigned_time = time.Now()
	// Handle response
	reply.Task = task_found.task
	return nil
}

func (c *Coordinator) SendMapOutputToReduceTasks(map_task_id int, temp_files map[int]string) error {
	for r_shard, fname := range temp_files {
		if !(r_shard >= 0 && r_shard <= c.n_reduce) {
			return errors.New(fmt.Sprintf("Invalid reduce shard number %d", r_shard))
		}

		reduce_task := &c.all_tasks[c.n_map + r_shard].task.ReduceTask
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
	c.tasks_mu.Lock()
	defer c.tasks_mu.Unlock()

	task_done := &request.TaskDone
	// The shard id of the task
	var task_id int
	// The internal index into the c.all_tasks list
	var task_index int
	
	if task_done.Type == Mapper {
		task_id = task_done.MapTask.TaskId
		task_index = task_id
		if !(task_id >= 0 && task_id < c.n_map) {
			return errors.New(fmt.Sprintf("Invalid map task id %d", task_id))
		}
	} else {
		task_id = task_done.ReduceTask.TaskId
		task_index = task_id + c.n_map
		if !(task_id >= 0 && task_id < c.n_reduce) {
			return errors.New(fmt.Sprintf("Invalid reduce task id %d", task_id))
		}			
	}


	internal_task := &c.all_tasks[task_index]
	if internal_task.worker_id != request.WorkerId {
		return errors.New("Worker id is not supposed to be working on task")
	}
	internal_task.status = Done
	if task_done.Type == Mapper {
		err := c.SendMapOutputToReduceTasks(task_id, request.TempFiles)
		if err != nil {
			reply.Ok = false
			return err
		}
	}

	if DEBUG {
		log.Printf("%s task %d done", task_type_to_str(task_done.Type), task_id)
	}
	reply.Ok = true
	return nil
}

// Find and unassign any orphaned tasks, for which we implement using a proxy of "10s has passed since task assignment"
func (c *Coordinator) CleanUpOrphanTasks() {
	MAX_TASK_WAIT_TIME := time.Second * 10

	for {
		c.tasks_mu.Lock()
		for i := 0; i < len(c.all_tasks); i++ {
			if c.all_tasks[i].status == Assigned && c.all_tasks[i].assigned_time.Before(time.Now().Add(-MAX_TASK_WAIT_TIME)) {
				c.all_tasks[i].status = Unassigned
				c.all_tasks[i].worker_id = ""
				c.all_tasks[i].assigned_time = time.Time{}
			}
		}
		c.tasks_mu.Unlock()
		time.Sleep(time.Second * 1)
	}
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
	go c.CleanUpOrphanTasks()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	done := true

	c.tasks_mu.Lock()
	defer c.tasks_mu.Unlock()
	for _, itask := range c.all_tasks {
		if itask.status != Done {
			done = false
			break
		}
	}
	return done
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
	all_tasks := make([]InternalTask, len(files)+nReduce)
	for i := 0; i < len(files); i++ {
		all_tasks[i] =
			InternalTask{
				task:   NewMapTask(i, files[i], nReduce),
				status: Unassigned,
			}
	}
	for i := len(files); i < len(files)+nReduce; i++ {
		all_tasks[i] =
			InternalTask{
				task:   NewReduceTask(i - len(files), len(files)),
				status: Unassigned,
			}
	}
	c := Coordinator{
		all_tasks: all_tasks,
		n_map:     len(files),
		n_reduce:  nReduce,
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

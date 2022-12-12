package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c := Coordinator{}

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

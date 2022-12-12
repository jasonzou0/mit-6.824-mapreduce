package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "strconv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	hostname,_ := os.Hostname()
	worker_id := hostname + ":" + strconv.Itoa(os.Getpid())

	// Your worker implementation here.
	GetTask(worker_id)
}

func GetTask(worker_id string) {
	request := GetTaskRequest{worker_id}
	reply := GetTaskResponse{}

	ok := call("Coordinator.GetTask", &request, &reply)
	if ok {
		if reply.Task.Type == Mapper {
			fmt.Printf("Get back Map task with file: %s\n", reply.Task.MapTask.InputFile)
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "os"
import "io/ioutil"
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

// Get the reducer shard number
func get_reducer_shard(key string, n_reduce int) int {
	return ihash(key) % n_reduce
}

// Write the output of mapper to tempfiles. n_reduce is the # of reducer shards
func write_kvs(kvs []KeyValue, map_task_id int, n_reduce int) {
	// encoders[i] stores the json encoder for reducer shard i 
	encoders := make([]*json.Encoder, n_reduce)
	for i := 0; i < n_reduce; i++ {
		fname := fmt.Sprintf("mr-tmp/mr-temp-%d-%d", map_task_id, i)
		file, err := os.Create(fname)
		if err != nil {
			log.Fatalf("cannot open %v", fname)
		}
		defer file.Close()
		encoders[i] = json.NewEncoder(file)		
	}
	
	for _, kv := range kvs {
		err := encoders[get_reducer_shard(kv.Key, n_reduce)].Encode(&kv)
		if err != nil {
			log.Fatalf("json encoding error on %v", kv)
		}
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	hostname, _ := os.Hostname()
	worker_id := hostname + ":" + strconv.Itoa(os.Getpid())

	// Your worker implementation here.
	worker_task, _ := GetTask(worker_id)
	// TODO: properly handle error

	if worker_task.Type == Mapper {
		map_task := worker_task.MapTask
		filename := map_task.InputFile
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		write_kvs(kva, map_task.TaskId, map_task.NReduce)
	}
}

func GetTask(worker_id string) (WorkerTask, error) {
	request := GetTaskRequest{worker_id}
	reply := GetTaskResponse{}

	ok, err := call("Coordinator.GetTask", &request, &reply)
	if ok {
		if reply.Task.Type == Mapper {
			fmt.Printf("Get back Map task with file: %s\n", reply.Task.MapTask.InputFile)
		}
		return reply.Task, nil
	}
	return WorkerTask{}, err
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) (bool, error) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true, nil
	}

	fmt.Println(err)
	return false, err
}

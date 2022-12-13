package mr

import "errors"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "os"
import "io/ioutil"
import "sort"
import "strconv"
import "strings"
import "time"

var ServerConnectionErrorStr string = "Connection Error"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func write_kvs(kvs []KeyValue, map_task_id int, n_reduce int) map[int]string {
	temp_files := make(map[int]string)
	// encoders[i] stores the json encoder for reducer shard i
	encoders := make([]*json.Encoder, n_reduce)
	for i := 0; i < n_reduce; i++ {
		fname := fmt.Sprintf("mr-temp-%d-%d", map_task_id, i)
		temp_files[i] = fname
		file, err := os.Create(fname)
		if err != nil {
			log.Fatalf("Cannot open %s for write", fname)
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
	return temp_files
}

func read_kvs(fname string) []KeyValue {
	file, err := os.Open(fname)
	if err != nil {
		log.Fatalf("Cannot open %s to read KVs from", fname)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err2 := dec.Decode(&kv); err2 != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

//
// main/mrworker.go calls this function.
//

func handle_map_task(worker_id string, mapf func(string, string) []KeyValue, map_task *MapTask) {
	filename := map_task.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open %s for map task", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	temp_files := write_kvs(kva, map_task.TaskId, map_task.NReduce)
	// Calls RPC to communicate Map task done.
	TaskDone(worker_id, WorkerTask{Mapper, *map_task, ReduceTask{}}, temp_files)
}

func handle_reduce_task(worker_id string, reducef func(string, []string) string, reduce_task *ReduceTask) {
	intermediate := []KeyValue{}
	for _, filename := range reduce_task.InputFiles {
		kva := read_kvs(filename)
		intermediate = append(intermediate, kva...)
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduce_task.TaskId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-i.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	// Calls RPC to communicate Reduce task done.
	TaskDone(worker_id, WorkerTask{Reducer, MapTask{}, *reduce_task}, map[int]string{})
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	hostname, _ := os.Hostname()
	worker_id := hostname + ":" + strconv.Itoa(os.Getpid())

	for {
		worker_task, err := GetTask(worker_id)

		if err != nil {
			if strings.HasPrefix(err.Error(), ServerConnectionErrorStr) {
				return
			}
			// Other reason must be RPC level failure such as server running out of tasks.
			// Sleep a bit before fetching again.
			time.Sleep(1 * time.Second)
			continue
		}
		if worker_task.Type == Mapper {
			handle_map_task(worker_id, mapf, &worker_task.MapTask)
		} else {
			handle_reduce_task(worker_id, reducef, &worker_task.ReduceTask)
		}
	}
}

// Calls the GetTask RPC
func GetTask(worker_id string) (WorkerTask, error) {
	request := GetTaskRequest{worker_id}
	reply := GetTaskResponse{}

	ok, err := call("Coordinator.GetTask", &request, &reply)
	if ok {
		if DEBUG {
			if reply.Task.Type == Mapper {
				fmt.Printf("Get back Map task with file: %s\n", reply.Task.MapTask.InputFile)
			} else {
				fmt.Printf("Get back Reduce task with file: %v\n", reply.Task.ReduceTask.InputFiles)
			}
		}
		return reply.Task, nil
	}
	return WorkerTask{}, err
}

// Calls the TaskDone RPC
func TaskDone(worker_id string, task_done WorkerTask, temp_files map[int]string) {
	request := TaskDoneRequest{worker_id, task_done, temp_files}
	reply := TaskDoneResponse{}

	call("Coordinator.TaskDone", &request, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong, such as a server level error like connection refused,
// or rpc level error.
func call(rpcname string, args interface{}, reply interface{}) (bool, error) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false, errors.New(ServerConnectionErrorStr)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true, nil
	}
	if DEBUG {
		fmt.Println(err)
	}
	return false, err
}

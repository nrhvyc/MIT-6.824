package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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

//
// main/mrworker.go calls this function.
// - ask the coordinator for a task
// - read the task's input from one or more files
// - execute the task
// - write the task's output to one or more files.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Map Phase
	for {
		// Get a task
		taskReply := CallAssignMapTask()
		if taskReply.Phase == ReducePhase {
			break
		}
		// No task to run
		if taskReply.InputFileName == nil {
			fmt.Println("no map task to run. sleeping...")
			time.Sleep(time.Second * 1)
			continue
		}
		// Report to the coordinator about task
		intermediateFileNames, err := mapTask(mapf, taskReply)
		if err != nil {
			fmt.Printf("mapTask failed with err: %s", err)
			CallMapTaskStatus(MapTaskStatusArgs{Status: Idle})
		} else {
			CallMapTaskStatus(MapTaskStatusArgs{
				Status:                Completed,
				IntermediateFileNames: intermediateFileNames,
			})
		}
	}

	for {
		// Get a task
		taskReply := CallAssignReduceTask()
		if taskReply.Phase == DonePhase {
			break
		}

		fmt.Printf("taskReply.TaskNumber: %s", taskReply.TaskNumber)

		reduceTaskNumber := taskReply.TaskNumber

		var intermediate []KeyValue

		// Read from intermediate files and read into []KeyValue
		for m := 0; m < taskReply.NumMapTasks; m++ {
			intermediateFileName := fmt.Sprintf("mr-%d-%d", m, reduceTaskNumber)

			intermediateFile, err := os.Open(intermediateFileName)
			if err != nil {
				fmt.Printf("error opening intermediate file err: %s", err)
			}
			dec := json.NewDecoder(intermediateFile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}

		outputFileName := fmt.Sprintf("mr-out-%d", reduceTaskNumber)
		outputFile, _ := os.Create(outputFileName)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
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
			fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		outputFile.Close()

		CallReduceTaskStatus(ReduceTaskStatusArgs{
			Status:         Completed,
			OutputFileName: outputFileName,
			TaskNumber:     reduceTaskNumber,
		})
	}
	return
}

func mapTask(mapf func(string, string) []KeyValue, task AssignMapTaskReply) ([]string, error) {
	fmt.Printf("mapTask()->task: %+v", task)

	// Read input file
	file, err := os.Open(*task.InputFileName)
	if err != nil {
		log.Fatalf("cannot open %v", *task.InputFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.InputFileName)
	}
	file.Close()

	// Run user map function
	intermediate := mapf(*task.InputFileName, string(content))

	sort.Sort(ByKey(intermediate))

	// Keys are the intermediate file names
	var buckets [][]KeyValue

	// Init keys
	for i := 0; i < task.NumReduce; i++ {
		buckets[i] = []KeyValue{}
	}

	// Partition into buckets for reduce
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		reduceTaskNumber := ihash(intermediate[i].Key) % task.NumReduce
		buckets[reduceTaskNumber] = append(buckets[reduceTaskNumber], intermediate[i:j]...)

		i = j
	}

	var intermediateFileNames []string // to be returned

	// Write each bucket to a reduce task file
	for reduceTaskNumber, bucket := range buckets {
		// mr-{Map Task Number}-{Reduce Task Number}
		intermediateFileName := fmt.Sprintf("mr-%d-%d",
			task.InputFileIndex, reduceTaskNumber)
		intermediateFileNames = append(intermediateFileNames, intermediateFileName)

		intermediatefile, err := os.Create(intermediateFileName)
		if err != nil {
			return []string{}, err
		}

		enc := json.NewEncoder(intermediatefile)
		for _, kv := range bucket {
			err = enc.Encode(&kv)
			if err != nil {
				return []string{}, err
			}
		}
		intermediatefile.Close()
	}

	return intermediateFileNames, nil
}

func CallAssignMapTask() (reply AssignMapTaskReply) {
	assignTask := AssignMapTaskArgs{}
	call("Coordinator.AssignMapTask", &assignTask, &reply)
	return
}

func CallMapTaskStatus(args MapTaskStatusArgs) (reply MapTaskStatusReply) {
	call("Coordinator.MapTaskStatus", &args, &reply)
	return
}

func CallAssignReduceTask() (reply AssignReduceTaskReply) {
	assignTask := AssignMapTaskArgs{}
	call("Coordinator.AssignReduceTask", &assignTask, &reply)
	return
}

func CallReduceTaskStatus(args ReduceTaskStatusArgs) (reply ReduceTaskStatusReply) {
	call("Coordinator.ReduceTaskStatus", &args, &reply)
	return
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

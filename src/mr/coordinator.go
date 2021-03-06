package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type (
	Coordinator struct {
		sync.Mutex
		MapTasks        map[string]*MapTask // Key is InputFileName
		ReduceTasks     []ReduceTask
		NumReduce       int   // How many output files to produce from reduce tasks
		Phase           Phase // Map, Reduce, or Done?
		MapTasksLeft    int   // Counts down from Map tasks completed
		ReduceTasksLeft int   // Counts down from Reduce tasks completed
	}

	MapTask struct {
		TaskStatus           Status
		IntermediateFileName string
		TaskStartTime        time.Time
		InputFileIndex       int
	}

	ReduceTask struct {
		TaskStatus Status
		// IntermediateFileName string
		// OutputFileName string
		TaskStartTime time.Time
	}

	Phase  int // enum
	Status int // enum
)

const (
	Idle    Status = iota
	Running        // could be assigned to more than one worker later
	Completed
)
const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

func (c *Coordinator) AssignMapTask(args *AssignMapTaskArgs, reply *AssignMapTaskReply) (err error) {
	c.Lock()
	if c.Phase != MapPhase {
		reply.Phase = ReducePhase
		return
	}

	for inputFileName, mapTask := range c.MapTasks {
		// Skip any currently assigned tasks,
		// but in future if task time above threshold then reassign
		if mapTask.TaskStatus == Idle {
			reply.InputFileName = &inputFileName
			reply.InputFileIndex = mapTask.InputFileIndex
			reply.Phase = MapPhase
			reply.NumReduce = c.NumReduce

			c.MapTasks[inputFileName].TaskStatus = Running
			break
		}
	}
	c.Unlock()

	return
}

func (c *Coordinator) AssignReduceTask(args *AssignReduceTaskArgs, reply *AssignReduceTaskReply) (err error) {
	c.Lock()
	if c.Phase != ReducePhase {
		return
	}

	for i := 0; i < c.NumReduce; i++ {
		// Skip any currently assigned tasks, but in future if task above threshold then reassign
		if c.ReduceTasks[i].TaskStatus == Idle {
			reply.TaskNumber = i
			reply.Phase = c.Phase
			reply.NumMapTasks = len(c.MapTasks)
			c.ReduceTasks[i].TaskStatus = Running
			break
		}
	}
	c.Unlock()
	return
}

// MapTaskStatus - if ran distributed, information about the intermediate keys would be passed to coordinator
func (c *Coordinator) MapTaskStatus(args *MapTaskStatusArgs, reply *MapTaskStatusReply) (err error) {
	c.Lock()
	c.MapTasks[args.InputFileName].TaskStatus = args.Status

	if args.Status == Completed {
		c.MapTasksLeft -= 1
	}
	if c.MapTasksLeft <= 0 {
		c.Phase = ReducePhase
	}
	c.Unlock()
	return
}

// ReduceTaskStatus
func (c *Coordinator) ReduceTaskStatus(args *ReduceTaskStatusArgs, reply *ReduceTaskStatusReply) (err error) {
	c.Lock()
	c.ReduceTasks[args.TaskNumber].TaskStatus = args.Status

	if args.Status == Completed {
		c.ReduceTasksLeft -= 1
	}
	if c.ReduceTasksLeft <= 0 {
		c.Phase = DonePhase
	}
	c.Unlock()
	return
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1235")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
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
	c.Lock()
	if c.Phase == DonePhase {
		return true
	}
	c.Unlock()

	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// fmt.Println("Initialize coordinator...")
	// fileNames := filepath.Glob(files)

	c.Lock()
	c.MapTasks = make(map[string]*MapTask)
	for idx, fileName := range files {
		c.MapTasks[fileName] = &MapTask{InputFileIndex: idx}
	}
	c.NumReduce = nReduce
	c.MapTasksLeft = len(files)
	c.ReduceTasksLeft = nReduce
	c.ReduceTasks = make([]ReduceTask, nReduce)

	// fmt.Println("Initialized.")

	// fmt.Println("Listening for workers...")
	c.Unlock()
	c.server()
	return &c
}

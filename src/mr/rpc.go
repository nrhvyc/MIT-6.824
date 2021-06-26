package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type AssignMapTaskArgs struct {
	// X int
}

type AssignMapTaskReply struct {
	InputFileName  *string // File name of an as-yet-unstarted map task, nil if no task to assign
	InputFileIndex int
	Phase          Phase // Map or Reduce Phase. Once in Reduce phase, worker stops making this request
	NumReduce      int   // how many reduce tasks are there?
}

type AssignReduceTaskArgs struct {
	// X int
}

type AssignReduceTaskReply struct {
	IntermediateFileName *string // File name of an as-yet-unstarted map task, nil if no task to assign
	TaskNumber           int
}

type UpdateMapTaskStatusArgs struct {
	Status                Status
	IntermediateFileNames []string
	InputFileName         string
	TaskNumber            int
}

type UpdateMapTaskStatusReply struct {
}

type UpdateReduceTaskStatusArgs struct {
	Status          Status
	OutputFileNames []string
	TaskNumber      int
}

type UpdateReduceTaskStatusReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

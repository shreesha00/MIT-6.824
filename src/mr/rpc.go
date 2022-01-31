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

const MAPTASK = 0
const REDUCETASK = 1
const EXITTASK = 2

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AskReply struct {
	TaskType          int      // 0 for Map, 1 for Reduce, 2 for pseudo exit-task
	MapTaskNumber     int      // If value is X, files mr-X-* are used to store intermediate results. If not a Map task, value is -1
	ReduceTaskNumber  int      // If value is Y, files mr-*-Y are used for the Reduce task. If not a Reduce task, value is -1
	NReduce           int      // Total number of Reduce tasks. Passed irrespective of Map or Reduce task
	MapFilename       string   // Filename to read from in case of Map task. If not a Map task, value is empty string
	NMaps             int      // Total number of Map tasks. Passed irrespective of Map or Reduce Task
	IntermediateFiles []string // List of intermediate files to read from. Empty in case of Map Task
}

type AskArgs struct {
}

type CompletedArgs struct {
	TaskType          int      // 0 for Map, 1 for Reduce
	MapTaskNumber     int      // If value is X, Xth Map task was completed. If not a Map task, value is -1
	ReduceTaskNumber  int      // If value is Y, Yth Reduce task was completed. If not a Reduce task, value is -1
	IntermediateFiles []string // List of intermediate files generated. Empty in case of Reduce task
}

type CompletedReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

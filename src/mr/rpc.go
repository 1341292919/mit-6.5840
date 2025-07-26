package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type TaskArgs struct {
}
type TaskReply struct {
	AssginedTask *Task
	Finished     bool
	NReduce      int
}
type ReportArgs struct {
	TaskId   int
	TaskType TaskType
}
type ReportReply struct {
}

type HealthArgs struct {
	TaskId   int
	TaskType TaskType
}

type HealthReply struct {
}

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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

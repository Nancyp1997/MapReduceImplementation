package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"sync"
	"time"
)

// Struct for task (map or reduce)
type Task struct {
	Type int
	// type=0 not yet started, 1 processing/waiting to complete, 2 complete
	Status    int
	ID        int
	WorkerID  int
	FileName  string
	mu        sync.Mutex
	timestamp time.Time
}

// Struct for worker request

type WorkerRequestArgs struct {
	TaskAsk int
	// 0 = not asking, 1=asking map task, 2=asking reduce task
}

// Struct for master's reply
type MasterReplyArgs struct {
	AssignedWork    Task
	IDGivenToWorker int
	NReduce         int
	TaskType        string
}

type ACKArgs struct {
	Type      string
	WorkerID  int
	FileInUse string
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

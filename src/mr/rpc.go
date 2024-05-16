package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"os"
	"strconv"
)

// Add your RPC definitions here.

// Coordinator never sends messages to workers
// All communication between workers and the coordinator is initiated by the worker
type MessageType int 

const (
	RequestTask MessageType = iota // Worker requests a task from the coordinator
	AssignMapTask 				   // Coordinator assigns a map task to the worker
	AssignReduceTask 			   // Coordinator assigns a reduce task to the worker
	SucceedMapTask 				   // Worker reports a successful map task
	FailMapTask 				   // Worker reports a failed map task
	SucceedReduceTask 			   // Worker reports a successful reduce task
	FailReduceTask 				   // Worker reports a failed reduce task
	ShutdownWorker 				   // Coordinator tells the worker to shut down
	HoldWorker 					   // Coordinator tells the worker to hold on
)

type MessageSent struct {
	Type MessageType
	TaskID int
}

type MessageReplied struct {
	Type MessageType
	TaskID int
	NReduce int
	FileName string 				// Each txt file is a map task 
}

var (
	MessageTypeError = errors.New("Message type error")
	TasksRunningOutError = errors.New("Tasks running out")
)


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

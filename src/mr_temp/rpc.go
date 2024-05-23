package main 

import (
	"strconv"
	"os"
	"errors"
)

type MessageType int

// Define all message types 
// Worker asks for tasks and report task status; Coordinator assign tasks to workers and exit workers 
// if tasks are complete or they need to be idle 

const (
	// Worker messages 
	RequestTask MessageType=iota  
	ReportMapSucceed
	ReportMapFail 
	ReportReduceSucceed 
	ReportReduceFail
	// Coordinator messages 
	AssignMapTask 
	AssignReduceTask
	AskWorkerToBeIdle
	AskWorkerToExit
)

// define error messages 
var (
	InvalidMessageType = errors.New("Message type is not valid.")
)


// Worker is allowed for task request, reporting(4)
type MessageFromWorker struct {
	Type MessageType
	TaskID int 
}

// Coordinator is allowed for tasks(2) assigment and changing the states of workers
type MessageFromCoordinator struct {
	Type MessageType
	TaskID int
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
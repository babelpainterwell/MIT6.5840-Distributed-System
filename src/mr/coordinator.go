package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type taskStatus int 

const (
	idle taskStatus = iota
	inProgress
	completed
	failed
)

type MapTaskState struct {
	TaskID int
	Status taskStatus
	StartTime int64
}

type ReduceTaskState struct {
	// no need to store the task ID
	Status taskStatus
	StartTime int64
}


type Coordinator struct {
	NReduce int                              // number of reduce tasks	
	MapTasks map[string]*MapTaskState        // the coordinator only needs to know the status of each map task, with file name as the key
	MapSuccess bool                          // if all map tasks are completed
	muMap sync.Mutex 					     // mutex for map tasks
	ReduceTasks []*ReduceTaskState           // the coordinator only needs to know the status of each reduce task
	ReduceSuccess bool                      // if all reduce tasks are completed
	muReduce sync.Mutex 					 // mutex for reduce tasks

}

func (c *Coordinator) StartMapReduce(files []string) {
	// the coordinator record both map and reduce tasks
	for i, fileName := range files {
		c.MapTasks[fileName] = &MapTaskState{
			TaskID: i,
			Status: idle,
		}
	}
	for i := range c.ReduceTasks {
		c.ReduceTasks[i] = &ReduceTaskState{
			Status: idle,
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) ReplyTaskRequest(req *MessageSent, reply *MessageReplied) error {
	if req.Type != RequestTask {
		return MessageTypeError
	}
	if !c.MapSuccess {
		// assign a map task
		c.muMap.Lock()

		map_success_count := 0
		for fileName, state := range c.MapTasks {
			assign := false

			// assign the task if it is idle or failed
			if state.Status == idle || state.Status == failed {
				assign = true
			} else if state.Status == inProgress {
				// if the task is in progress, check if it is timeout
				// if it is timeout, assign the task to another worker
				TimeNow := time.Now().Unix()
				if TimeNow - state.StartTime > 10 {
					state.StartTime = TimeNow
					assign = true
				}
			} else {
				// if the task is completed, increase the count
				map_success_count++
			}

			if assign {
				reply.Type = AssignMapTask
				reply.TaskID = state.TaskID
				reply.FileName = fileName
				reply.NReduce = c.NReduce

				state.Status = inProgress
				state.StartTime = time.Now().Unix()
				c.muMap.Unlock()
				return nil
			}
		}
		c.muMap.Unlock()

		if map_success_count < len(c.MapTasks) {
			// if there are still map tasks running, hold the worker
			reply.Type = HoldWorker
			return nil
		} else {
			c.MapSuccess = true
		}
	}

	if !c.ReduceSuccess {
		// assign a reduce task
		c.muReduce.Lock()

		reduce_success_count := 0
		for i, state := range c.ReduceTasks {
			assign := false 

			// assign the task if it is idle or failed
			if state.Status == idle || state.Status == failed {
				assign = true
			} else if state.Status == inProgress {
				// if the task is in progress, check if it is timeout
				// if it is timeout, assign the task to another worker
				TimeNow := time.Now().Unix()
				if TimeNow - state.StartTime > 10 {
					state.StartTime = TimeNow
					assign = true
				}
			} else {
				// if the task is completed, increase the count
				reduce_success_count++
			}

			if assign {
				reply.Type = AssignReduceTask
				reply.TaskID = i
				reply.NReduce = c.NReduce

				state.Status = inProgress
				state.StartTime = time.Now().Unix()
				c.muReduce.Unlock()
				return nil
			}
		}

		c.muReduce.Unlock()

		if reduce_success_count < len(c.ReduceTasks) {
			// if there are still reduce tasks running, hold the worker
			reply.Type = HoldWorker
			return nil
		} else {
			c.ReduceSuccess = true
		}
	}

	// if all tasks are completed, shut down the worker
	reply.Type = ShutdownWorker

	return nil
}

func (c *Coordinator) ReplyTaskStatusReport (req *MessageSent, reply *MessageReplied) error {
	if req.Type == SucceedMapTask {
		c.muMap.Lock()
		for _, state := range c.MapTasks {
			if state.TaskID == req.TaskID {
				state.Status = completed
				c.muMap.Unlock()
				return nil
			}
		}
		c.muMap.Unlock()
	} else if req.Type == SucceedReduceTask {
		c.muReduce.Lock() 
		c.ReduceTasks[req.TaskID].Status = completed // no need to check the task ID
		c.muReduce.Unlock()
		return nil
	} else if req.Type == FailMapTask {
		c.muMap.Lock()
		for _, state := range c.MapTasks {
			if state.TaskID == req.TaskID && state.Status == inProgress {
				state.Status = failed
				c.muMap.Unlock()
				return nil
			}
		}
		c.muMap.Unlock()
	} else if req.Type == FailReduceTask {
		c.muReduce.Lock()
		if c.ReduceTasks[req.TaskID].Status == inProgress {
			c.ReduceTasks[req.TaskID].Status = failed
		}
		c.muReduce.Unlock()
		return nil
	} else {
		return MessageTypeError
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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

	// make sure the map tasks and reduce tasks are all completed
	for _, state := range c.MapTasks {
		if state.Status != completed {
			return false
		}
	}
	for _, state := range c.ReduceTasks {
		if state.Status != completed {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce: nReduce,
		MapTasks: make(map[string]*MapTaskState),
		ReduceTasks: make([]*ReduceTaskState, nReduce),
	}

	// Initialize the coordinator
	c.StartMapReduce(files)

	// start the server
	c.server()
	return &c
}

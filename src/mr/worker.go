package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// The main responsibility of a worker is to:
//    1. request for tasks from the coordinator continuously
//    2. execute the task
//    3. report the task completion to the coordinator

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// Implementing Sort Interface:
// For more complex sorting needs, we define our own type, which requires three methods: Len(), Less(i, j int) bool, and Swap(i, j int).
type SortedKeyValueArray []KeyValue

func (a SortedKeyValueArray) Len() int {
	return len(a)
}
func (a SortedKeyValueArray) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}
func (a SortedKeyValueArray) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}


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
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := SendRequestTaskMessage()
		if reply == nil {
			break
		}
		// execute the task
		switch reply.Type {
		case AssignMapTask:
			err := ExecuteMapTask(reply, mapf)
			if err != nil {
				_ = ReportTaskStatus(FailMapTask, reply.TaskID)
			} else {
				_ = ReportTaskStatus(SucceedMapTask, reply.TaskID)
			}
		case AssignReduceTask:
			err := ExecuteReduceTask(reply, reducef)
			if err != nil {
				_ = ReportTaskStatus(FailReduceTask, reply.TaskID)
			} else {
				_ = ReportTaskStatus(SucceedReduceTask, reply.TaskID)
			}
		case HoldWorker:
			time.Sleep(10 * time.Second)
		case ShutdownWorker:
			os.Exit(0)
		}
		time.Sleep(1 * time.Second)
	}
}

func ExecuteMapTask (reply *MessageReplied, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.FileName)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	keyValues := mapf(reply.FileName, string(content))
	// sort the keyValues by key
	sort.Sort(SortedKeyValueArray(keyValues))

	// create nReduce intermediate files
	intermediateFiles := make([]*os.File, reply.NReduce)
	// encoders is a slice of json.Encoder pointers used to encode key-value pairs into intermediate files
	encoders := make([]*json.Encoder, reply.NReduce)

	// Distribute key-value pairs into intermediate files based on the hash of the key
	for _, kv := range keyValues {
		reduceTaskID := ihash(kv.Key) % reply.NReduce
		if encoders[reduceTaskID] == nil {
			tempFile, err := ioutil.TempFile("", fmt.Sprintf("mr-map-tmp-%d", reduceTaskID))
			if err != nil {
				return err
			}
			defer tempFile.Close()
			intermediateFiles[reduceTaskID] = tempFile
			encoders[reduceTaskID] = json.NewEncoder(tempFile)
		}
		// encode the key-value pair into the intermediate file
		err := encoders[reduceTaskID].Encode(&kv)
		if err != nil {
			return err
		
		}
	}

	// Rename the temporary files to final intermediate file names
	for i, file := range intermediateFiles {
		if file != nil {
			fileName := file.Name()
			file.Close()
			newFileName := fmt.Sprintf("mr-out-%d-%d", reply.TaskID, i)
			if err := os.Rename(fileName, newFileName); err != nil {
				return err
			}
		}
	}
	return nil
}

func ExecuteReduceTask(reply *MessageReplied, reducef func(string, []string) string) error {
	keyID := reply.TaskID

	// Initialize a map to hold the aggregated values by key
	kvs := make(map[string][]string)

	// Read intermediate files for the given reduce task ID
	fileList, err := ReadSpecificFile(keyID, "./")
	if err != nil {
		return err
	}

	// Decode the JSON-encoded key-value pairs from each file
	for _, file := range fileList {
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// Check specifically for io.EOF to break the loop. If another error occurs, return it.
				if err == io.EOF {
					break
				}
				return err
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
		file.Close()
	}

	// Collect all unique keys and sort them
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create the output file for the reduce task results
	outputFileName := "mr-out-" + strconv.Itoa(reply.TaskID)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	// Apply the reduce function to each key and write the results to the output file
	for _, key := range keys {
		output := reducef(key, kvs[key])
		_, err := fmt.Fprintf(outputFile, "%v %v\n", key, output)
		if err != nil {
			return err
		}
	}

	// Clean up intermediate files for the reduce task
	err = DelFileByReduceId(reply.TaskID, "./")
	if err != nil {
		return err
	}

	return nil
}

func SendRequestTaskMessage() *MessageReplied {
	// send a request task message to the coordinator
	args := MessageSent{
		Type: RequestTask,
	}

	reply := MessageReplied{} // empty reply

	// send the RPC request, wait for the reply.
	err := call("Coordinator.RequestTask", &args, &reply)
	if err != nil {
		return &reply // return the pointer to the reply, in the worker node, we can directly use the reply?
	} else {
		return nil
	}

}

// There are four message types that the worker can send to the coordinator
// 1. SucceedMapTask
// 2. FailMapTask
// 3. SucceedReduceTask
// 4. FailReduceTask
func ReportTaskStatus(messageType MessageType, taskID int) error {
	
	if messageType != SucceedMapTask && messageType != FailMapTask && messageType != SucceedReduceTask && messageType != FailReduceTask {
		return MessageTypeError
	}
	args := MessageSent{
		Type: messageType,
		TaskID: taskID,
	}
	err := call("Coordinator.ReportTaskStatus", &args, nil)
	return err
}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// We return the error message from the RPC call.
//
func call(rpcname string, args interface{}, reply interface{}) error { // interface{} is an empty interface, can be any type
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply) // directly write the reply to the reply interface
	// if err == nil {
	// 	return true
	// }

	// fmt.Println(err)
	// return false
	return err
}

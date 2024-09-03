package mr

import (
    "encoding/json"
    "fmt"
    "hash/fnv"
    "io/ioutil"
    "log"
    "net/rpc"
    "os"
    "path/filepath"
    "sort"
    "time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
func Worker(
	mapf func(string, string) []KeyValue, 
	reducef func(string, []string) string) {

	// Your worker implementation here.
	hasMapTask := true
	for {
		if hasMapTask {
			taskIdx, fileName, message, reduceNumber := GetMapTask()
			if message == "error" {
					time.Sleep(2 * time.Second)
					continue
			}
			if message == "No tasks" {
				hasMapTask = false
				continue
			}
			if message == "No not-started tasks" {
				continue
			}
			if taskIdx >= 0 {
				mapResult := Map(taskIdx, fileName, mapf, reduceNumber)
				if mapResult == "succeed" {
					CompleteMapTask(fileName)
				}
			} else {
				time.Sleep(2 * time.Second)
				continue
			}
		} else {
			taskIdx,message := GetReduceTask()
			if message == "error" {
				time.Sleep(2 * time.Second)
				continue
			}
			if message == "No tasks" {
				break
			}
			if taskIdx >= 0 {
				reducepResult := Reduce(taskIdx, reducef)
				if reducepResult == "succeed" {
					CompleteReduceTask(taskIdx)
				}
			} else {
				time.Sleep(2 * time.Second)
				continue
			}
		}
	}
}

func GetMapTask() (int,string,string,int) {

	request := GetMapTaskRequest{}
	response := GetMapTaskResponse{}

	ok := call("Coordinator.GetMapTask", &request, &response)
	if ok {
		return response.TaskIdx,response.Filename,response.Message,response.ReduceNumber
	}
	return -1,"","error",-1
}

func Map(taskIdx int, fileName string, mapf func(string, string) []KeyValue, reduceNumber int) string {
	// log.Printf("start map work for file: %s, idx: %d", fileName, taskIdx)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
		return "failed"
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
		return "failed"
	}
	
	kva := mapf(fileName, string(content))
	reduceIdx2kvMap := make(map[int][]KeyValue)

	for _, keyValue := range kva {
		key := keyValue.Key
		reduceTaskIdx := ihash(key) % reduceNumber
		reduceIdx2kvMap[reduceTaskIdx] = append(reduceIdx2kvMap[reduceTaskIdx], keyValue)
	}

	for reduceIdx, kvList := range reduceIdx2kvMap {
		outputFileName := fmt.Sprintf("mr-%d-%d", taskIdx, reduceIdx)
		file, err = os.OpenFile(outputFileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return "failed"
		}
		defer file.Close()

		enc := json.NewEncoder(file)
		for _, kv := range kvList {
			if err := enc.Encode(&kv); err != nil {
                log.Fatalf("cannot encode to %v", outputFileName)
                return "failed"
            }
		}
	}
	return "succeed"
}

func CompleteMapTask(fileName string) {

	request := CompleteMapTaskRequest{Filename: fileName}

	response := CompleteMapTaskResponse{}

	for {
		ok := call("Coordinator.CompleteMapTask", &request, &response)
		if ok {
			break
		}
		time.Sleep(2 * time.Second)
	}
}

func GetReduceTask() (int,string) {

	request := GetReduceTaskRequest{}
	response := GetReduceTaskResponse{}

	ok := call("Coordinator.GetReduceTask", &request, &response)
	if ok {
		return response.TaskNumber,response.Message
	}
	return -1,"error"
}

func Reduce(taskIdx int, reducef func(string, []string) string) string {
	// log.Printf("start reduce work taskIdx: %d", taskIdx)
	key2valueList := make(map[string][]string)
	pattern := fmt.Sprintf("mr-*-%d", taskIdx)
	files, err := filepath.Glob(pattern)
	// log.Printf("found %d files under pattern: %s", len(files), pattern)
    if err != nil {
		log.Fatalf("failed to find files matching pattern %s: %v", pattern, err)
        return "failed"
    }

	for _, fileName := range files {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v: %v", fileName, err)
			return "failed"
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
                break
            }
            key2valueList[kv.Key] = append(key2valueList[kv.Key], kv.Value)
		}
	}
	
	var intermediate []KeyValue
	for key, list := range key2valueList {
		value := reducef(key, list)
		intermediate = append(intermediate, KeyValue{Key: key, Value: value})
	}

	sort.Slice(intermediate, func(i, j int) bool {
        return intermediate[i].Key < intermediate[j].Key
    })

	oname := fmt.Sprintf("mr-out-%d", taskIdx)
	ofile, err := os.Create(oname)
	if err != nil {
        log.Fatalf("cannot create %v", oname)
        return "failed"
    }
    defer ofile.Close()

	for _, kv := range intermediate {
		fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
	}

	return "succeed"
}

func CompleteReduceTask(taskIdx int) {
    request := CompleteReduceTaskRequest{TaskNumber: taskIdx}
    response := CompleteReduceTaskResponse{}

    for {
        ok := call("Coordinator.CompleteReduceTask", &request, &response)
        if ok {
            break
        }
        time.Sleep(2 * time.Second)
    }
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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

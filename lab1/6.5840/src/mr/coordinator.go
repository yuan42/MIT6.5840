package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"


type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	StartTime time.Time
	ReduceNumber int
	FileName2IdxMap map[string]int
	FileIdx2NameMap map[int]string
	NotStartedMapTasks chan int
	NotStartedReduceTasks chan int
	PendingMapTasks [][]int
	PendingReduceTasks [][]int
	PendingMapTaskIndex int
	PendingReduceTaskIndex int
	CompletedMapTasks map[int]struct{}
	CompletedReduceTasks map[int]struct{}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Reschedule() {
    elapsed := int(time.Since(c.StartTime).Seconds())
	// log.Printf("start reschedule with elapsed: %d", elapsed)

    c.mu.Lock()
    defer c.mu.Unlock()

    // Reschedule pending map tasks that are overdue or completed
    for c.PendingMapTaskIndex < len(c.PendingMapTasks) {
        taskIdx := c.PendingMapTasks[c.PendingMapTaskIndex][1]
        if _, exists := c.CompletedMapTasks[taskIdx]; !exists {
			if c.PendingMapTasks[c.PendingMapTaskIndex][0] <= elapsed-10 {
				select {
				case c.NotStartedMapTasks <- taskIdx:
					// Task successfully rescheduled
				default:
					// log.Printf("NotStartedMapTasks channel is full, could not reschedule task: %d", taskIdx)
				}
			} else {
				break
			}
        }
		c.PendingMapTaskIndex++
    }

    // Reschedule pending reduce tasks that are overdue
    for c.PendingReduceTaskIndex < len(c.PendingReduceTasks) {
        taskIdx := c.PendingReduceTasks[c.PendingReduceTaskIndex][1]
        if _, exists := c.CompletedReduceTasks[taskIdx]; !exists {
            if c.PendingReduceTasks[c.PendingReduceTaskIndex][0] <= elapsed-10 {
				select {
				case c.NotStartedReduceTasks <- taskIdx:
					// Task successfully rescheduled
				default:
					// log.Printf("NotStartedReduceTasks channel is full, could not reschedule task: %d", taskIdx)
				}
			} else {
				break
			}
        }
        c.PendingReduceTaskIndex++
    }
}

func (c *Coordinator) StartRescheduler() {
    go func() {
        for {
            c.Reschedule()
            time.Sleep(2 * time.Second) // Adjust the sleep duration as necessary
        }
    }()
}

func (c *Coordinator) GetMapTask(request *GetMapTaskRequest, response *GetMapTaskResponse) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	// log.Printf("not started tasks:%d, pending: %d", len(c.NotStartedMapTasks), c.PendingMapTaskIndex - len(c.PendingMapTasks))
	if len(c.NotStartedMapTasks) == 0 && c.PendingMapTaskIndex == len(c.PendingMapTasks) {
		response.Message = "No tasks"
	} else if len(c.NotStartedMapTasks) == 0 {
		response.Message = "No not-started tasks"
	} else {
		taskIdx := -1
		for len(c.NotStartedMapTasks) > 0 {
			// log.Printf("current not started work length is %d", len(c.NotStartedMapTasks))
			temp := <-c.NotStartedMapTasks
			_, exists := c.CompletedMapTasks[temp]
			if exists {
				continue
			} else {
				taskIdx = temp
				// log.Printf("task: %d is not in completed", temp)
				break
			}
		}
		response.TaskIdx = taskIdx
		if taskIdx >= 0 {
			response.Filename = c.FileIdx2NameMap[taskIdx]
			response.ReduceNumber = c.ReduceNumber
			elapsed := int(time.Since(c.StartTime).Seconds())
			c.PendingMapTasks = append(c.PendingMapTasks, []int{elapsed, taskIdx})
		} else {
			response.Message = "No not-started tasks"
		}
	}
	return nil
}

func (c *Coordinator) CompleteMapTask(request *CompleteMapTaskRequest, response *CompleteMapTaskResponse) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	c.CompletedMapTasks[c.FileName2IdxMap[request.Filename]] = struct{}{}
	return nil
}

func (c *Coordinator) GetReduceTask(request *GetReduceTaskRequest, response *GetReduceTaskResponse) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	if len(c.NotStartedReduceTasks) == 0 && c.PendingReduceTaskIndex == len(c.PendingReduceTasks) {
		response.Message = "No tasks"
	} else if len(c.NotStartedReduceTasks) == 0 {
		response.Message = "No not-started tasks"
	} else {
		taskIdx := -1
		for len(c.NotStartedReduceTasks) > 0 {
			temp := <-c.NotStartedReduceTasks
			_, exists := c.CompletedReduceTasks[temp]
			if exists {
				continue
			} else {
				taskIdx = temp
				break
			}
		}
		if taskIdx >= 0 {
			response.TaskNumber = taskIdx
			elapsed := int(time.Since(c.StartTime).Seconds())
			c.PendingReduceTasks = append(c.PendingReduceTasks, []int{elapsed, taskIdx})
		} else {
			response.Message = "No not-started tasks"
		}
	}
	return nil
}

func (c *Coordinator) CompleteReduceTask(request *CompleteReduceTaskRequest, response *CompleteReduceTaskResponse) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	c.CompletedReduceTasks[request.TaskNumber] = struct{}{}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.
	if len(c.NotStartedReduceTasks) == 0 && c.PendingReduceTaskIndex == len(c.PendingReduceTasks) {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.StartTime = time.Now()
	c.ReduceNumber = nReduce
	c.NotStartedReduceTasks = make(chan int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.NotStartedReduceTasks <- i
	}

	c.FileName2IdxMap = make(map[string]int)
	c.FileIdx2NameMap = make(map[int]string)
	c.NotStartedMapTasks = make(chan int, len(files))
	for index, fileName := range files {
		c.FileName2IdxMap[fileName] = index
		c.FileIdx2NameMap[index] = fileName
		c.NotStartedMapTasks <- index
	}

	c.PendingMapTaskIndex = 0
	c.PendingReduceTaskIndex = 0

	c.CompletedMapTasks = make(map[int]struct{})
	c.CompletedReduceTasks = make(map[int]struct{})

	go c.server()
	c.StartRescheduler()
	return &c
}

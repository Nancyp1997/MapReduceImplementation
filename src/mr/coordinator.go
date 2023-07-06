package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

/*
queue := make([]int, 0)
	// Push to the queue
	queue = append(queue, 1)
	queue = append(queue, 2)
	queue = append(queue, 3)
	// Top (just get next element, don't remove it)
	x := queue[0]
	fmt.Println(x)
	// Discard top element
	queue = queue[1:]
	// Is empty ?
	if len(queue) == 0 {
		fmt.Println("Queue is empty !")
	} else {
		fmt.Println(queue[1])
	}
*/

type Coordinator struct {
	// Separate queues for map not yet started, processing & complete tasks
	// Separate queues for reduce not yet started, processing & complete tasks
	map0          []Task
	map1          []Task
	map2          []Task
	reduce0       []Task
	reduce1       []Task
	reduce2       []Task
	nReduce       int
	workerCounter int
	mu            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {

	reply.Y = c.nReduce
	// args.X + 1
	return nil
}

func (c *Coordinator) ReceiveACK(args *ACKArgs, reply *ACKArgs) error {

	if args.Type == "Map" {
		// Remove the file name from c.map1 and place it in c.map2 array
		for i := 0; i < len(c.map1); i++ {
			if c.map1[i].FileName == args.FileInUse {

				c.map2 = append(c.map2, c.map1[i])
				c.map1 = append(c.map1[:i], c.map1[i+1:]...)
			}
		}
		// Once the file is transfered to c.map2 for being complete
		// add all the corresponding intermediate files (nReduce in count)
		// to c.reduce0
		for i := 0; i < c.nReduce; i++ {
			var task1 Task
			task1.FileName = "mr-" + strconv.Itoa(args.WorkerID) + "-" + strconv.Itoa(i)
			task1.ID = len(c.reduce0)
			task1.Status = -1
			task1.Type = 0
			c.reduce0 = append(c.reduce0, task1)
		}
	} else if args.Type == "Reduce" {
		for i := 0; i < len(c.reduce1); i++ {
			if c.reduce1[i].FileName == args.FileInUse {
				c.reduce2 = append(c.reduce2, c.reduce1[i])
				c.reduce1 = append(c.reduce1[:i], c.reduce1[i+1:]...)
			}
		}

	} else {
		log.Println("Invalid operation ACK")
	}
	c.mu.Unlock()
	return nil
}

// RPC handler to assign task to worker
func (c *Coordinator) AssignTaskToWorker(args *WorkerRequestArgs, reply *MasterReplyArgs) error {

	// Assigning worker ID to worker
	reply.IDGivenToWorker = c.workerCounter + 1
	c.workerCounter = c.workerCounter + 1
	reply.NReduce = c.nReduce
	c.mu.Lock()

	if args.TaskAsk == 0 {
		return nil
	}
	if args.TaskAsk == 1 {
		// If any incomplete map tasks, assign them first, else reduce tasks
		reply.NReduce = c.nReduce
		if len(c.map0) > 0 {
			// log.Print("Master sending worker map Task")
			currTask := c.map0[0]
			currTask.mu.Lock()
			reply.AssignedWork = currTask
			reply.TaskType = "Map"
			c.map1 = append(c.map1, currTask)
			c.map0 = c.map0[1:]
			currTask.mu.Unlock()

			// Coordinator waits for 10 secs to check if status of the task changes from 0 to 1
			time.Sleep(10 * time.Second)
			currTask.mu.Lock()

			if reply.AssignedWork.Status != 1 {
				c.map0 = append(c.map0, currTask)
			}

			currTask.mu.Unlock()

		} else if len(c.reduce0) > 0 {
			// log.Println("Master sending worker reduce Task")
			reply.TaskType = "Reduce"
			reply.AssignedWork = c.reduce0[0]
			c.reduce1 = append(c.reduce1, c.reduce0[0])
			c.reduce0 = c.reduce0[1:]
		} else {
			log.Println("No pending tasks at coordinator for now")
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	if len(c.map0) == 0 && len(c.reduce0) == 0 {
		ret = true
	}
	c.mu.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce1 int) *Coordinator {
	c := Coordinator{}
	// log.Print("Coordinator.MakeCoordinator()")
	// log.Print(nReduce1)

	// Your code here.
	for i := 0; i < len(files); i++ {
		var task1 Task
		task1.FileName = files[i]
		task1.ID = i
		task1.Status = -1
		task1.Type = 0
		c.map0 = append(c.map0, task1)
	}

	c.nReduce = nReduce1
	c.workerCounter = 0
	c.server()
	return &c
}

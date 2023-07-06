package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	log.Print("Worker.init()")
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	AskForTask(mapf, reducef)
}

// func getMapReduceFxn(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
// 	p, err := plugin.Open(filename)
// 	if err != nil {
// 		log.Fatalf("cannot load plugin %v", filename)
// 	}
// 	xmapf, err := p.Lookup("Map")
// 	if err != nil {
// 		log.Fatalf("cannot find Map in %v", filename)
// 	}
// 	mapf := xmapf.(func(string, string) []KeyValue)
// 	xreducef, err := p.Lookup("Reduce")
// 	if err != nil {
// 		log.Fatalf("cannot find Reduce in %v", filename)
// 	}
// 	reducef := xreducef.(func(string, []string) string)

// 	return mapf, reducef
// }

// Function to ask master for task
func AskForTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// log.Println("Worker.AskForTask()")

	args := WorkerRequestArgs{}
	args.TaskAsk = 1 // 1 for asking task

	// Reply structure
	reply := MasterReplyArgs{}

	// Send RPC request, wait for reply
	ok := call("Coordinator.AssignTaskToWorker", &args, &reply)
	if ok {
		currentFile := reply.AssignedWork.FileName
		// log.Println("Worker received task")
		// log.Println(reply)
		if reply.TaskType == "Map" {
			// Map task
			intermediate := []KeyValue{}
			// fmt.Printf("Filename assigned : %v\n", currentFile)
			//Call map fxn as in mrsequential.go

			// mapf, _ := getMapReduceFxn("wc.so")
			file, err := os.Open(currentFile)
			if err != nil {
				log.Fatalf("cannot open %v", currentFile)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", currentFile)
			}
			file.Close()
			kva := mapf(currentFile, string(content))
			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))

			// Each worker has to create n intermediate files for consumption
			// by the reducer by name "mr-out-<workerID>-<nReduceNumber>"
			// Divide by the keys to nReduce num of buckets
			for i := 0; i < reply.NReduce; i++ {
				intermediateFileName := "mr-" + strconv.Itoa(reply.IDGivenToWorker) + "-" + strconv.Itoa(i)
				oFile, _ := os.Create(intermediateFileName)
				oFile.Close()
			}

			// intermediate keys are to be divided for n reduce tasks.
			for i := 0; i < len(intermediate); i++ {
				keyString := intermediate[i].Key
				hashValue1 := ihash(keyString) % reply.NReduce
				fileToWriteTo := "mr-" + strconv.Itoa(reply.IDGivenToWorker) + "-" + strconv.Itoa(hashValue1)
				f, err := os.OpenFile(fileToWriteTo,
					os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Println(err)
				}

				if _, err := f.WriteString(intermediate[i].Key + "," + intermediate[i].Value + "\n"); err != nil {
					log.Println(err)
				}
				// fmt.Printf(fileToWriteTo, intermediate[i])
				f.Close()
			}
			reply.AssignedWork.Status = 1
			// Once map task is finished, send ACK to coordinator
			SendMapACK(reply.IDGivenToWorker, currentFile)

		} else if reply.TaskType == "Reduce" {
			// Read the intermediate file passed

			intermediate := []KeyValue{}
			// _, reducef := getMapReduceFxn("wc.so")
			file, err := os.Open(currentFile)
			if err != nil {
				log.Fatalf("cannot open %v", currentFile)
			}
			// log.Println(currentFile)
			fscanner := bufio.NewScanner(file)
			for fscanner.Scan() {
				line1 := fscanner.Text()
				strs := strings.Split(line1, ",")
				var kv KeyValue
				kv.Key = strs[0]
				kv.Value = strs[1]
				// From the content form KeyValue struct and append to intermediate
				intermediate = append(intermediate, kv)
			}
			file.Close()
			// log.Println(intermediate)
			oname := "mr-out-" + strconv.Itoa(reply.IDGivenToWorker)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()
		}

	} else {
		fmt.Printf("call failed!\n")
	}

}

func SendMapACK(workerID int, fileName string) {
	args := ACKArgs{}
	args.Type = "Map"
	args.WorkerID = workerID
	args.FileInUse = fileName
	ok := call("Coordinator.ReceiveACK", &args, &args)
	if ok {
		log.Println("ACK sent to master")
	} else {
		log.Println("ACK failed")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}
	log.Print("Worker.CallExample()")
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	// log.Print("Worker.call()")
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

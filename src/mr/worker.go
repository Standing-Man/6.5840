package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Node struct {
	Id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	nReduce int
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (node *Node) ProcessMap(mapFileName string) []string {
	// generate the intermdiate key/value from mapFile
	intermediate := []KeyValue{}
	file, err := os.Open(mapFileName)
	if err != nil {
		log.Fatalf("WordId: %d: ProcessMap cannot open %s", node.Id, mapFileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("WordId: %d: ProcessMap cannot read %s", node.Id, mapFileName)
	}
	file.Close()
	kva := node.mapf(mapFileName, string(content))
	intermediate = append(intermediate, kva...)

	return node.handleInter(intermediate)
}

func (node *Node) handleInter(intermediate []KeyValue) []string {
	res := []string{}
	// current directory
	// create the temp file to store the intermediate key/value
	files := []*os.File{}
	for i := 0; i < node.nReduce; i++ {
		tempFile, err := os.CreateTemp("./", "tempfile-*.txt")
		if err != nil {
			log.Fatalf("Worker %d: failed to create temp file for map", node.Id)
			return res
		}
		defer tempFile.Close()
		files = append(files, tempFile)
	}

	for i := 0; i < len(intermediate); i++ {
		index := ihash(intermediate[i].Key) % node.nReduce
		enc := json.NewEncoder(files[index])
		err := enc.Encode(&intermediate[i])
		if err != nil {
			log.Fatalln("failed to store the intermediate key/value")
			return res
		}
	}
	for i := 0; i < node.nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d.txt", node.Id, i)
		os.Rename(files[i].Name(), filename)
		res = append(res, filename)
	}
	return res
}

func (node *Node) progressReduce(reduceFiles []string, reduceId int) string {
	var intermediate []KeyValue
	res := ""

	// create a temp file to store Key/value for reduce
	tempFile, err := os.CreateTemp("./", "tempfile-*.txt")
	if err != nil {
		log.Fatalf("Worker %d: failed to create temp file for reduce", node.Id)
		return res
	}

	for i := 0; i < len(reduceFiles); i++ {
		file, err := os.Open(reduceFiles[i])
		if err != nil {
			log.Fatalf("progressReduce: cannot open %v", reduceFiles[i])
		}
		// read the
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

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
		output := node.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	filename := fmt.Sprintf("mr-out-%d", reduceId)
	os.Rename(tempFile.Name(), filename)
	return tempFile.Name()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	node := Node{
		Id:      -1,
		mapf:    mapf,
		reducef: reducef,
	}

	// Your worker implementation here.
	node.RequestTask()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func (node *Node) RequestTask() {
	for {
		assignReply, ok := node.AssignTask()
		if !ok {
			continue
		}
		if assignReply.WaitFlag {
			time.Sleep(10 * time.Second)
			continue
		}
		if assignReply.IsDone {
			break
		}
		node.Id = assignReply.WorkerId
		node.nReduce = assignReply.NReduce
		if assignReply.Tasks.TaskType == MAP {
			MapFile := assignReply.Tasks.MapFile
			interFiles := node.ProcessMap(MapFile)
			// transfer to Master
			TransInterReply, _ := node.TransferInterFiles(interFiles, MapFile)
			if TransInterReply.Finished {
				log.Printf("Successfully transfer the intermediate files %s to master\n", interFiles)
			}
		}

		if assignReply.Tasks.TaskType == REDUCE {
			result := node.progressReduce(assignReply.Tasks.ReduceFiles, assignReply.Tasks.ReduceId)
			deleteFiles := assignReply.Tasks.ReduceFiles
			// transfer to Master
			TransResReply, ok := node.TransferResult(result, assignReply.Tasks.ReduceId)
			if TransResReply.Finished && ok {
				// delete the intermediate files
				log.Printf("Delete the intermediate files %s from workerId %d", deleteFiles, assignReply.WorkerId)
				removeInterFiles(deleteFiles)
			}
		}
	}
}

func removeInterFiles(filenames []string) {
	for i := 0; i < len(filenames); i++ {
		err := os.Remove(filenames[i])
		if err != nil {
			log.Panicf("failed to delete the intermediate file %s\n", filenames[i])
		}
	}
}

func (node *Node) TransferResult(result string, reduceId int) (*TransferResultReply, bool) {
	args := TransferResultArgs{}
	args.Result = result
	args.ReduceId = reduceId
	args.WordId = node.Id

	reply := TransferResultReply{}

	ok := call("Coordinator.AccpetResult", &args, &reply)

	return &reply, ok
}

func (node *Node) TransferInterFiles(files []string, mapFile string) (*TransferInterFilesReply, bool) {
	args := TransferInterFilesArgs{}
	args.InterFiles = files
	args.MapFile = mapFile
	args.WorkId = node.Id

	reply := TransferInterFilesReply{}

	ok := call("Coordinator.AccpetInterFiles", &args, &reply)

	return &reply, ok
}

func (node *Node) AssignTask() (*AssignReply, bool) {
	args := AssignArgs{}

	reply := AssignReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)

	return &reply, ok
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

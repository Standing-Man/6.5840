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

type STATE int

const (
	IDLE STATE = iota
	IN_PROGRESS
	COMPLETED
)

type Bucket struct {
	state       STATE
	reduceFiles []string
}

func (b *Bucket) insertFile(filename string) {
	b.reduceFiles = append(b.reduceFiles, filename)
}

func (b *Bucket) setState(state STATE) {
	b.state = state
}

type Coordinator struct {
	// Your definitions here.
	mu            sync.Mutex
	workerId      int
	nReduce       int
	mapFiles      map[string]STATE
	reduceBuckets map[int](*Bucket)
}

type TASKTYPE int

const (
	MAP TASKTYPE = iota
	REDUCE
)

type Task struct {
	TaskType    TASKTYPE
	MapFile     string
	ReduceFiles []string
	ReduceId    int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Monitor(task Task) {
	time.Sleep(time.Duration(10))
	c.mu.Lock()
	if task.TaskType == MAP {
		mapFile := task.MapFile
		if c.mapFiles[mapFile] == IN_PROGRESS {
			c.mapFiles[mapFile] = IDLE
		}
	}
	if task.TaskType == REDUCE {
		reduceId := task.ReduceId
		if c.reduceBuckets[reduceId].state == IN_PROGRESS {
			c.reduceBuckets[reduceId].setState(IDLE)
		}
	}
	c.mu.Unlock()
}

func (c *Coordinator) AssignTask(args *AssignArgs, reply *AssignReply) error {
	c.mu.Lock()

	if c.Done() {
		reply.IsDone = true
		c.mu.Unlock()
		return nil
	}

	reply.WorkerId = c.workerId
	c.workerId++

	reply.NReduce = c.nReduce

	// Get the Map Task
	task := Task{}
	mapFile, ok := c.GetMapTask()
	if ok {
		c.mapFiles[mapFile] = IN_PROGRESS
		// log.Printf("Coordinator: Assign Map file: %s\n", mapFile)
		task.MapFile = mapFile
		task.TaskType = MAP
		go c.Monitor(task)
		reply.Tasks = task
		c.mu.Unlock()
		return nil
	}

	reduceFiles, reduceId, ok := c.GetReduceTask()

	if ok {
		// log.Printf("Coordinator: Assign Reduce files: %s\n", reduceFiles)
		task.TaskType = REDUCE
		task.ReduceFiles = reduceFiles
		task.ReduceId = reduceId
		reply.Tasks = task
		go c.Monitor(task)
		c.mu.Unlock()
		return nil
	}

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) AccpetInterFiles(args *TransferInterFilesArgs, reply *TransferInterFilesReply) error {
	c.mu.Lock()
	// Set the mapFile is finished
	if c.mapFiles[args.MapFile] == COMPLETED {
		c.mu.Unlock()
		return nil
	}
	c.mapFiles[args.MapFile] = COMPLETED
	for i := 0; i < c.nReduce; i++ {
		// log.Printf("Successfuly to append the map file %s into bucket %d\n", args.InterFiles[i], i)
		c.reduceBuckets[i].insertFile(args.InterFiles[i])
	}
	reply.Finished = true
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) AccpetResult(args *TransferResultArgs, reply *TransferResultReply) error {
	c.mu.Lock()
	if c.reduceBuckets[args.ReduceId].state == COMPLETED {
		c.mu.Unlock()
		return nil
	}
	c.reduceBuckets[args.ReduceId].setState(COMPLETED)
	reply.Finished = true
	c.mu.Unlock()
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
	ret := true

	// Your code here.
	for i := 0; i < len(c.reduceBuckets); i++ {
		if c.reduceBuckets[i].state != COMPLETED {
			return !ret
		}
	}

	return ret
}

func (c *Coordinator) GetMapTask() (string, bool) {
	for file, state := range c.mapFiles {
		if state == IDLE {
			return file, true
		}
	}
	return "", false
}

func (c *Coordinator) GetReduceTask() ([]string, int, bool) {
	for i := 0; i < c.nReduce; i++ {
		if c.reduceBuckets[i].state == IDLE {
			return c.reduceBuckets[i].reduceFiles, i, true
		}
	}
	return []string{}, -1, false
}

func (c *Coordinator) initization(files []string, nReduce int) {
	c.mapFiles = make(map[string]STATE, 0)
	for _, file := range files {
		c.mapFiles[file] = IDLE
	}

	c.reduceBuckets = make(map[int]*Bucket)
	for i := 0; i < nReduce; i++ {
		c.reduceBuckets[i] = &Bucket{
			state:       IDLE,
			reduceFiles: []string{},
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.workerId = 0
	c.nReduce = nReduce
	c.initization(files, nReduce)
	// Your code here.

	c.server()
	return &c
}

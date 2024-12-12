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
	time.Sleep(time.Second * 10)
	c.mu.Lock()
	if task.TaskType == MAP {
		if c.mapFiles[task.MapFile] == IN_PROGRESS {
			c.mapFiles[task.MapFile] = IDLE
		}
	}

	if task.TaskType == REDUCE {
		if c.reduceBuckets[task.ReduceId].state == IN_PROGRESS {
			c.reduceBuckets[task.ReduceId].setState(IDLE)
		}
	}
	c.mu.Unlock()
}

func (c *Coordinator) shouldWait() bool {
	for _, state := range c.mapFiles {
		if state != COMPLETED {
			return true
		}
	}
	return false
}

func (c *Coordinator) AssignTask(args *AssignArgs, reply *AssignReply) error {
	if c.Done() {
		reply.IsDone = true
		return nil
	}
	c.mu.Lock()

	reply.WorkerId = c.workerId
	c.workerId++

	reply.NReduce = c.nReduce

	// Get the Map Task
	task := Task{}
	mapFile, ok := c.GetMapTask()
	if ok {
		c.mapFiles[mapFile] = IN_PROGRESS
		log.Printf("Coordinator: Assign Map file to workerId %d: %s\n", reply.WorkerId, mapFile)
		task.MapFile = mapFile
		task.TaskType = MAP
		reply.Tasks = task
		go c.Monitor(task)
		c.mu.Unlock()
		return nil
	}

	if c.shouldWait() {
		reply.WaitFlag = true
		reply.WorkerId = -1
		c.workerId -= 1
		c.mu.Unlock()
		return nil
	}

	reduceFiles, reduceId, ok := c.GetReduceTask()

	if ok {
		log.Printf("Coordinator: Assign Reduce files to WorkerId %d: %s\n", reply.WorkerId, reduceFiles)
		c.reduceBuckets[reduceId].setState(IN_PROGRESS)
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
		reply.Finished = false
		c.mu.Unlock()
		return nil
	}
	// set the mapFile state is Complete
	log.Printf("Coordinator: accept intermediate files %s from WorkerId %d\n", args.InterFiles, args.WorkId)
	c.mapFiles[args.MapFile] = COMPLETED
	for i := 0; i < c.nReduce; i++ {
		c.reduceBuckets[i].insertFile(args.InterFiles[i])
		log.Printf("Bucket %d: %s\n",i, c.reduceBuckets[i].reduceFiles)
	}
	reply.Finished = true
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) AccpetResult(args *TransferResultArgs, reply *TransferResultReply) error {
	c.mu.Lock()
	if c.reduceBuckets[args.ReduceId].state == COMPLETED {
		reply.Finished = false
		c.mu.Unlock()
		return nil
	}
	log.Printf("Coordinator: accept result files %s from WorkerId %d\n", args.Result, args.ReduceId)
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
	c.mu.Lock()
	ret := true
	// Your code here.
	for i := 0; i < len(c.reduceBuckets); i++ {
		if c.reduceBuckets[i].state != COMPLETED {
			c.mu.Unlock()
			return !ret
		}
	}
	c.mu.Unlock()
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

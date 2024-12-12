package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type AssignArgs struct {
	WorkerId int
	State    STATE
}

type AssignReply struct {
	WorkerId int
	IsDone   bool
	Tasks    Task
	NReduce  int
}

type TransferInterFilesArgs struct {
	WorkId     int
	MapFile    string
	InterFiles []string
}

type TransferInterFilesReply struct {
	Finished bool
	MapFile  string
}

type TransferResultArgs struct {
	Result   string
	ReduceId int
}

type TransferResultReply struct {
	Finished bool
	Result   string
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

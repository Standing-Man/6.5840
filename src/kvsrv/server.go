package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	memory map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	// Your code here.
	key := args.Key
	if value, ok := kv.memory[key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	key := args.Key
	oldValue, ok := kv.memory[key]
	kv.memory[key] = args.Value
	if ok {
		reply.Value = oldValue
	} else {
		reply.Value = ""
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	key := args.Key
	oldValue, ok := kv.memory[key]
	kv.memory[key] += args.Value
	if ok {
		reply.Value = oldValue
	} else {
		reply.Value = ""
	}
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.memory = make(map[string]string)
	

	return kv
}

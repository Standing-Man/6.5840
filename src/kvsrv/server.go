package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	table  map[int64](Entry)
	memory map[string]string
}

type Entry struct {
	SeqNo      int64
	ReplyValue string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.memory[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.table[args.ClientId]
	if ok && entry.SeqNo == args.SeqNo {
		reply.Value = entry.ReplyValue
		return
	}
	kv.memory[args.Key] = args.Value
	kv.table[args.ClientId] = Entry{
		SeqNo:      args.SeqNo,
		ReplyValue: reply.Value,
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.table[args.ClientId]
	if ok && entry.SeqNo == args.SeqNo {
		reply.Value = entry.ReplyValue
		return
	}
	reply.Value = kv.memory[args.Key]
	kv.memory[args.Key] += args.Value
	kv.table[args.ClientId] = Entry{
		SeqNo:      args.SeqNo,
		ReplyValue: reply.Value,
	}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.table = make(map[int64]Entry)
	kv.memory = make(map[string]string)
	return kv
}

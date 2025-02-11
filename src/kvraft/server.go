package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type OpType int

const (
	G OpType = iota
	P
	A
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqNo    int64
	Key      string
	Value    string
	Type     OpType
	Term     int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Table    map[int64]Record
	Memory   map[string]string
	notifyCh map[int]chan Op

	persister      *raft.Persister
	committedIndex int
}

type Record struct {
	SeqNo int64
	Reply Reply
}

type Reply struct {
	Err        Err
	ReplyValue string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	term, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if entry, ok := kv.Table[args.ClientId]; ok {
		if entry.SeqNo == args.SeqNo {
			reply.Err = entry.Reply.Err
			reply.Value = entry.Reply.ReplyValue
			kv.mu.Unlock()
			return
		}
	}

	kv.mu.Unlock()

	command := Op{
		Type:     G,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
		Term:     term,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notifyCh := make(chan Op, 1)
	kv.mu.Lock()
	kv.notifyCh[index] = notifyCh
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()

	select {
	case committedCommand := <-notifyCh:
		if committedCommand.ClientId == args.ClientId && committedCommand.SeqNo == args.SeqNo {
			kv.mu.Lock()
			if value, existing := kv.Memory[committedCommand.Key]; existing {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
			kv.Table[committedCommand.ClientId] = Record{
				SeqNo: committedCommand.SeqNo,
				Reply: Reply{
					Err:        reply.Err,
					ReplyValue: reply.Value,
				},
			}
			kv.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	term, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if entry, ok := kv.Table[args.ClientId]; ok {
		if entry.SeqNo >= args.SeqNo {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	command := Op{
		Type:     P,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
		Term:     term,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notifyCh := make(chan Op, 1)
	kv.mu.Lock()
	kv.notifyCh[index] = notifyCh
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()

	select {
	case committedCommand := <-notifyCh:
		if committedCommand.ClientId == args.ClientId && committedCommand.SeqNo == args.SeqNo {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	term, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if entry, ok := kv.Table[args.ClientId]; ok {
		if entry.SeqNo >= args.SeqNo {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}

	kv.mu.Unlock()
	command := Op{
		Type:     A,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
		Term:     term,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	notifyCh := make(chan Op, 1)
	kv.mu.Lock()
	kv.notifyCh[index] = notifyCh
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()

	select {
	case committedCommand := <-notifyCh:
		if committedCommand.ClientId == args.ClientId && committedCommand.SeqNo == args.SeqNo {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) applyHandlerLoop() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		if msg.CommandValid {
			if msg.CommandIndex <= kv.committedIndex {
				continue
			}
			committedCommand := msg.Command.(Op)
			kv.committedIndex = msg.CommandIndex
			if entry, ok := kv.Table[committedCommand.ClientId]; !ok || entry.SeqNo < committedCommand.SeqNo {
				if committedCommand.Type == P {
					kv.Memory[committedCommand.Key] = committedCommand.Value
				} else if committedCommand.Type == A {
					kv.Memory[committedCommand.Key] += committedCommand.Value
				}
				if committedCommand.Type == P || committedCommand.Type == A {
					kv.Table[committedCommand.ClientId] = Record{
						SeqNo: committedCommand.SeqNo,
					}
				}
			}

			term, isLeader := kv.rf.GetState()

			if ch, ok := kv.notifyCh[msg.CommandIndex]; ok && isLeader && term == committedCommand.Term {
				ch <- committedCommand
				close(ch)
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				snapshot := kv.createSnapshot()
				kv.rf.Snapshot(kv.committedIndex, snapshot)
			}
		}
		if msg.SnapshotValid {
			if msg.SnapshotIndex <= kv.committedIndex {
				continue
			}
			kv.readSnapshot(msg.Snapshot)
			kv.committedIndex = msg.SnapshotIndex
		}
		kv.mu.Unlock()

		if kv.killed() {
			return
		}
	}
}

func (kv *KVServer) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Table)
	e.Encode(kv.Memory)
	return w.Bytes()
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var table map[int64]Record
	var memory map[string]string
	if d.Decode(&table) == nil &&
		d.Decode(&memory) == nil {
		kv.Table = table
		kv.Memory = memory
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.Memory = make(map[string]string)
	kv.Table = make(map[int64]Record)
	kv.notifyCh = make(map[int]chan Op)
	kv.persister = persister
	kv.committedIndex = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapshot(kv.persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.applyHandlerLoop()

	return kv
}

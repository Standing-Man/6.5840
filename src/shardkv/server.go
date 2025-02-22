package shardkv

import (
	"bytes"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	table    map[int64]Record
	memory   map[string]string
	notifyCh map[int]chan Op

	persister      *raft.Persister
	committedIndex int

	mck    *shardctrler.Clerk
	config shardctrler.Config
}

type Record struct {
	SeqNo int64
	Reply Reply
}

type Reply struct {
	Err        Err
	ReplyValue string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shardID := key2shard(args.Key)
	config := kv.getConfig()
	if config.Shards[shardID] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	term, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if entry, ok := kv.table[args.ClientId]; ok {
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
			if value, existing := kv.memory[committedCommand.Key]; existing {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
			kv.table[committedCommand.ClientId] = Record{
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
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shardID := key2shard(args.Key)
	config := kv.getConfig()
	if config.Shards[shardID] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	
	term, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if entry, ok := kv.table[args.ClientId]; ok {
		if entry.SeqNo >= args.SeqNo {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	command := Op{
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
		Term:     term,
	}
	if args.Op == "Put" {
		command.Type = P
	} else if args.Op == "Append" {
		command.Type = A
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
	}
}

func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		if msg.CommandValid {
			if msg.CommandIndex <= kv.committedIndex {
				continue
			}
			committedCommand := msg.Command.(Op)
			kv.committedIndex = msg.CommandIndex
			if entry, ok := kv.table[committedCommand.ClientId]; !ok || entry.SeqNo < committedCommand.SeqNo {
				if committedCommand.Type == P {
					kv.memory[committedCommand.Key] = committedCommand.Value
				} else if committedCommand.Type == A {
					kv.memory[committedCommand.Key] += committedCommand.Value
				}
				if committedCommand.Type == P || committedCommand.Type == A {
					kv.table[committedCommand.ClientId] = Record{
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
	}
}

func (kv *ShardKV) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.table)
	e.Encode(kv.memory)
	return w.Bytes()
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var table map[int64]Record
	var memory map[string]string
	if d.Decode(&table) == nil &&
		d.Decode(&memory) == nil {
		kv.table = table
		kv.memory = memory
	}
}

func (kv *ShardKV) getConfig() shardctrler.Config {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config
}

func (kv *ShardKV) queryConfig() {
	for {
		kv.mu.Lock()
		kv.config = kv.mck.Query(-1)
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.memory = make(map[string]string)
	kv.table = make(map[int64]Record)
	kv.notifyCh = make(map[int]chan Op)
	kv.persister = persister
	kv.committedIndex = 0

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.applier()
	go kv.queryConfig()
	return kv
}

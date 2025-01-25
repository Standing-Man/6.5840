package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId int64
	SeqNo    int64
	LeaderId int
	mu       sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.SeqNo = 0
	ck.LeaderId = -1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{
		Key:      key,
		SeqNo:    ck.SeqNo,
		ClientId: ck.ClientId,
	}
	for {
		if ck.LeaderId == -1 {
			for i := 0; i < len(ck.servers); i++ {
				var reply GetReply
				ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.SeqNo = args.SeqNo + 1
					ck.LeaderId = i
					return reply.Value
				}
			}
		} else {
			var reply GetReply
			ok := ck.servers[ck.LeaderId].Call("KVServer.Get", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				ck.SeqNo = args.SeqNo + 1
				return reply.Value
			} else {
				ck.LeaderId = -1
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		SeqNo:    ck.SeqNo,
		ClientId: ck.ClientId,
	}
	for {
		if ck.LeaderId == -1 {
			for i := 0; i < len(ck.servers); i++ {
				var reply PutAppendReply
				ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
				if ok && reply.Err == OK {
					ck.SeqNo = args.SeqNo + 1
					ck.LeaderId = i
					return
				}
			}
		} else {
			var reply PutAppendReply
			ok := ck.servers[ck.LeaderId].Call("KVServer."+op, &args, &reply)
			if ok && reply.Err == OK {
				ck.SeqNo = args.SeqNo + 1
				return
			} else {
				ck.LeaderId = -1
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

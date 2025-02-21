package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs        []Config // indexed by config num
	table          map[int64]Record
	notifyCh       map[int]chan Op
	committedIndex int
}

type Record struct {
	SeqNo  int64
	Config Config
}

type OpType int

const (
	J OpType = iota
	L
	M
	Q
)

type Op struct {
	// Your data here.
	ClientId int64
	SeqNo    int64
	Type     OpType
	Args     Args
	Term     int
}

type Args struct {
	JArgs *JoinArgs
	LArgs *LeaveArgs
	MArgs *MoveArgs
	QArgs *QueryArgs
}

/*
*
creating a new configuration that includes the new replica groups.
*
*/
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	if entry, ok := sc.table[args.ClientId]; ok {
		if entry.SeqNo == args.SeqNo {
			reply.Err = OK
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	command := Op{
		Type:     J,
		Args:     Args{JArgs: args},
		Term:     term,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	notifyCh := make(chan Op, 1)
	sc.mu.Lock()
	sc.notifyCh[index] = notifyCh
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.notifyCh, index)
		sc.mu.Unlock()
	}()

	select {
	case committedCommand := <-notifyCh:
		if committedCommand.ClientId == args.ClientId && committedCommand.SeqNo == args.SeqNo {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeout
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	if entry, ok := sc.table[args.ClientId]; ok {
		if entry.SeqNo == args.SeqNo {
			reply.Err = OK
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	command := Op{
		Type:     L,
		Args:     Args{LArgs: args},
		Term:     term,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	notifyCh := make(chan Op, 1)
	sc.mu.Lock()
	sc.notifyCh[index] = notifyCh
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.notifyCh, index)
		sc.mu.Unlock()
	}()

	select {
	case committedCommand := <-notifyCh:
		if committedCommand.ClientId == args.ClientId && committedCommand.SeqNo == args.SeqNo {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	if entry, ok := sc.table[args.ClientId]; ok {
		if entry.SeqNo == args.SeqNo {
			reply.Err = OK
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	command := Op{
		Type:     M,
		Args:     Args{MArgs: args},
		Term:     term,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	notifyCh := make(chan Op, 1)
	sc.mu.Lock()
	sc.notifyCh[index] = notifyCh
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.notifyCh, index)
		sc.mu.Unlock()
	}()

	select {
	case committedCommand := <-notifyCh:
		if committedCommand.ClientId == args.ClientId && committedCommand.SeqNo == args.SeqNo {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	term, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	if entry, ok := sc.table[args.ClientId]; ok {
		if entry.SeqNo == args.SeqNo {
			reply.Err = OK
			reply.Config = entry.Config
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	command := Op{
		Type:     Q,
		Args:     Args{QArgs: args},
		Term:     term,
		ClientId: args.ClientId,
		SeqNo:    args.SeqNo,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	notifyCh := make(chan Op, 1)
	sc.mu.Lock()
	sc.notifyCh[index] = notifyCh
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.notifyCh, index)
		sc.mu.Unlock()
	}()

	select {
	case committedCommand := <-notifyCh:
		if committedCommand.ClientId == args.ClientId && committedCommand.SeqNo == args.SeqNo {
			sc.mu.Lock()
			CommittedArgs := committedCommand.Args.QArgs
			sz := len(sc.configs)
			if CommittedArgs.Num == -1 || CommittedArgs.Num >= sz {
				reply.Config = sc.configs[sz-1]
			} else {
				reply.Config = sc.configs[CommittedArgs.Num]
			}
			reply.Err = OK
			sc.table[committedCommand.ClientId] = Record{
				SeqNo:  committedCommand.SeqNo,
				Config: reply.Config,
			}
			sc.mu.Unlock()
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(100 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func copyGroups(src map[int][]string) map[int][]string {
	dst := make(map[int][]string)
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func copyShards(src [NShards]int) [NShards]int {
	var dst [NShards]int
	for i := 0; i < NShards; i++ {
		dst[i] = src[i]
	}
	return dst
}

func SortedByGID(servers map[int]([]string)) []int {
	gids := make([]int, 0, len(servers))
	for k := range servers {
		gids = append(gids, k)
	}
	sort.Ints(gids)
	return gids
}

func (sc *ShardCtrler) joinServers(config *Config, servers map[int][]string) {
	gids := SortedByGID(servers)
	for _, gid := range gids {
		sc.reBalanceForJoin(config, gid)
		config.Groups[gid] = servers[gid]
	}
}

/*
*

	10                               2
	5  5                             5
	4  3  3                          3
	3  3  2  2                       2
	2  2  2  2  2                    2
	2  2  2  2  1  1                 1
	2  2  2  1  1  1  1              1
	2  2  1  1  1  1  1  1           1
	2  1  1  1  1  1  1  1  1        1
	1  1  1  1  1  1  1  1  1  1     1
*/
func (sc *ShardCtrler) reBalanceForJoin(config *Config, gid int) {
	numGroups := countNumG(config)
	if numGroups >= NShards {
		return
	}
	if numGroups == 0 {
		// the shards doesn't have any replica groups to responsible.
		for i := 0; i < NShards; i++ {
			config.Shards[i] = gid
		}
		return
	}
	moveShards := NShards / (numGroups + 1)
	gidShardsCount := make(map[int]([]int))
	for i := 0; i < NShards; i++ {
		gidShardsCount[config.Shards[i]] = append(gidShardsCount[config.Shards[i]], i)
	}
	sortedGIDs := SortKeysDes(gidShardsCount)
	index, length := 0, len(sortedGIDs)
	for moveShards != 0 {
		shardsId := gidShardsCount[sortedGIDs[index]]
		id := shardsId[0]
		shardsId = shardsId[1:]
		config.Shards[id] = gid
		moveShards -= 1
		gidShardsCount[sortedGIDs[index]] = shardsId
		index = (index + 1) % length
	}
}

// Sort by the number of shards managed by replica groups. (descending order)
// [1:2,2:4,3:1] => [2,1,3]
func SortKeysDes(m map[int][]int) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.SliceStable(keys, func(i, j int) bool {
		if len(m[keys[i]]) != len(m[keys[j]]) {
			return len(m[keys[i]]) > len(m[keys[j]]) // 长度大的在前
		}
		return keys[i] > keys[j]
	})

	return keys
}

func checkUnUsedGroups(config *Config) []int {
	used := make(map[int]int)
	unUsed := []int{}
	for _, gid := range config.Shards {
		used[gid] += 1
	}
	gids := SortedByGID(config.Groups)
	for _, gid := range gids {
		if _, ok := used[gid]; !ok {
			unUsed = append(unUsed, gid)
		}
	}
	return unUsed
}

func (sc *ShardCtrler) LeaveGroups(config *Config, GIDs []int) {
	for _, gid := range GIDs {
		delete(config.Groups, gid)
		sc.reBalanceForLeave(config, gid)
		if gids := checkUnUsedGroups(config); countNumG(config) < NShards && len(gids) > 0 {
			for _, id := range gids {
				sc.reBalanceForJoin(config, id)
			}
		}
	}
}

func countNumG(config *Config) int {
	m := make(map[int]int)
	for _, gid := range config.Shards {
		if gid != 0 {
			m[gid] += 1
		}
	}
	return len(m)
}

func (sc *ShardCtrler) reBalanceForLeave(config *Config, gid int) {
	numGroups := countNumG(config)
	if numGroups == 1 && config.Shards[0] == gid {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}
	// the index array that move to another replica groups
	moveShards := []int{}
	for i := 0; i < NShards; i++ {
		if config.Shards[i] == gid {
			moveShards = append(moveShards, i)
		}
	}
	if len(moveShards) == 0 {
		return
	}
	gidShardsCount := make(map[int]([]int))
	for i := 0; i < NShards; i++ {
		gidShardsCount[config.Shards[i]] = append(gidShardsCount[config.Shards[i]], i)
	}
	delete(gidShardsCount, gid)
	sortedGIDs := SortKeysDes(gidShardsCount)
	index, length := len(sortedGIDs)-1, len(sortedGIDs)
	for len(moveShards) != 0 {
		sortedGID := sortedGIDs[index]
		shardId := moveShards[0]
		moveShards = moveShards[1:]
		config.Shards[shardId] = sortedGID
		if index == 0 {
			index = length - 1
		} else {
			index = (index - 1) % length
		}
	}

}

func (sc *ShardCtrler) moveGToS(config *Config, Shard int, GID int) {
	if _, ok := config.Groups[GID]; !ok {
		panic("there don't have this groups")
	}
	config.Shards[Shard] = GID
}

func (sc *ShardCtrler) applyHandlerLoop() {
	for msg := range sc.applyCh {
		sc.mu.Lock()
		if msg.CommandValid {
			if msg.CommandIndex <= sc.committedIndex {
				continue
			}
			sc.committedIndex = msg.CommandIndex
			committedCommand := msg.Command.(Op)
			if entry, ok := sc.table[committedCommand.ClientId]; !ok || entry.SeqNo < committedCommand.SeqNo {
				sz := len(sc.configs)
				lastConfig := sc.configs[sz-1]
				if committedCommand.Type == J {
					args := committedCommand.Args.JArgs
					config := Config{
						Num:    sz,
						Groups: copyGroups(lastConfig.Groups),
						Shards: copyShards(lastConfig.Shards),
					}
					sc.joinServers(&config, args.Servers)
					sc.configs = append(sc.configs, config)
				} else if committedCommand.Type == L {
					args := committedCommand.Args.LArgs
					config := Config{
						Num:    sz,
						Groups: copyGroups(lastConfig.Groups),
						Shards: copyShards(lastConfig.Shards),
					}
					sc.LeaveGroups(&config, args.GIDs)
					sc.configs = append(sc.configs, config)
				} else if committedCommand.Type == M {
					args := committedCommand.Args.MArgs
					config := Config{
						Num:    sz,
						Groups: copyGroups(lastConfig.Groups),
						Shards: copyShards(lastConfig.Shards),
					}
					sc.moveGToS(&config, args.Shard, args.GID)
					sc.configs = append(sc.configs, config)
				}

				if committedCommand.Type == J || committedCommand.Type == L || committedCommand.Type == M {
					sc.table[committedCommand.ClientId] = Record{
						SeqNo: committedCommand.SeqNo,
					}
				}
			}

			term, isLeader := sc.rf.GetState()

			if ch, ok := sc.notifyCh[msg.CommandIndex]; ok && isLeader && term == committedCommand.Term {
				ch <- committedCommand
				close(ch)
			}
		}
		sc.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.committedIndex = 0

	// Your code here.
	sc.table = make(map[int64]Record)
	sc.notifyCh = make(map[int]chan Op)

	go sc.applyHandlerLoop()

	return sc
}

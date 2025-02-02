package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const ElectionInterval = 1 * time.Second

type State int

const (
	Leader State = iota
	Candidate
	Follower
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State
	currentTerm int
	votedFor    int
	logs        Logs

	// For Election, time of starting election
	electionTime time.Time

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg

	// SnapShot
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs Logs
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil {
	} else {
		rf.currentTerm = currentTerm
	}
	if d.Decode(&votedFor) != nil {
	} else {
		rf.votedFor = votedFor
	}
	if d.Decode(&logs) != nil {
	} else {
		rf.logs = logs
	}
	if d.Decode(&lastIncludedIndex) != nil {
	} else {
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
	}
	if d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.lastIncludedTerm = lastIncludedTerm
	}

	rf.snapshot = rf.persister.ReadSnapshot()
}

type InstallSnapshotRPCArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Offset            int
	Done              bool
}

type InstallSnapshotRPCReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotRPCArgs, reply *InstallSnapshotRPCReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	defer rf.persist()

	if rf.currentTerm < args.Term {
		rf.CovertToFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		// the snapshot that Leader send to follower is outdated
		return
	}

	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if args.LastIncludedIndex >= rf.logs.Len() {
		// the follower is slow
		// clear the logs and set the begin of logs
		rf.logs.Entries = []Entry{}
		rf.logs.StartIndex = rf.lastIncludedIndex + 1
	}

	if args.LastIncludedIndex >= rf.logs.StartIndex && args.LastIncludedIndex < rf.logs.Len() {
		rf.logs.CutStart(args.LastIncludedIndex + 1)
	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.commitIndex {
		return
	}

	defer rf.persist()

	// generate the snapshot and save
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs.GetTermByIndex(index)
	rf.snapshot = snapshot

	// delete the log including the index
	rf.logs.CutStart(index + 1)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// optimizate that backs up nextIndex
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) FindFirstIndex(term int, endIndex int) int {
	for i := endIndex; i > rf.logs.StartIndex; i-- {
		if rf.logs.GetTermByIndex(i) == term && rf.logs.GetTermByIndex(i-1) != term {
			return i
		}
	}
	/** conditions:
	1. i == rf.logs.StartIndex
	2. The term is same form start to end.
	**/
	return rf.logs.StartIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	defer rf.persist()

	// args.Term >= rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.CovertToFollower(args.Term)
	} else if args.Term == rf.currentTerm {
		if rf.state == Candidate {
			rf.state = Follower
		}
	}
	rf.setElectionTime()

	reply.Term = rf.currentTerm

	if args.PrevLogIndex >= rf.logs.Len() {
		// Leader's logs are longer than follower's
		reply.Success = false
		reply.XLen = rf.logs.Len()
		return
	} else if args.PrevLogIndex < rf.logs.StartIndex {
		if args.PrevLogIndex == rf.lastIncludedIndex {
			if args.PrevLogTerm != rf.lastIncludedTerm {
				panic("fails")
			}
		} else {
			if args.PrevLogIndex+len(args.Entries) > rf.lastIncludedIndex {
				// cut the prefix entries before rf.logs.StartIndex
				args.Entries = args.Entries[rf.logs.StartIndex-args.PrevLogIndex-1:]
				args.PrevLogIndex = rf.lastIncludedIndex
				args.PrevLogTerm = rf.lastIncludedTerm
			} else {
				// panic("Log entries fully covered by the snapshot")
				reply.Success = true
				return
			}
		}
	} else {
		if rf.logs.GetTermByIndex(args.PrevLogIndex) != args.PrevLogTerm {
			reply.XTerm = rf.logs.GetTermByIndex(args.PrevLogIndex)
			reply.XIndex = rf.FindFirstIndex(reply.XTerm, args.PrevLogIndex)
			reply.Success = false
			return
		}
	}

	// rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm && rf.currentTerm >= args.Term

	reply.Success = true

	// append entry into rf.logs
	// follow AppendEntry rule 3, 4
	/***
	>> rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
	this is wrong, because if the leader send a outdated appendEntry RPC
	**/
	index := 1 + args.PrevLogIndex
	for index < rf.logs.Len() && index-args.PrevLogIndex-1 < len(args.Entries) {
		if rf.logs.GetTermByIndex(index) != args.Entries[index-args.PrevLogIndex-1].Term {
			// delete the existing entry and all that follow it
			rf.logs.CutEnd(index)
			break
		}
		index += 1
	}
	/**
	1. index >= len(rf.logs)
		=> index-args.PrevLogIndex-1 < len(args.Entries) => ok
		=> index-args.PrevLogIndex-1 == len(args.Entries) => ok
	2. index-args.PrevLogIndex-1 >= len(args.Entries) => indicate all logs match
	3. find an entry can't match Leader's => find entry can't match and delete that all follow it
	*/
	// append new entries not already in the log
	if index-args.PrevLogIndex-1 < len(args.Entries) {
		rf.logs.AppendEntries(args.Entries[index-args.PrevLogIndex-1:])
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logs.Len()-1)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// args.Term < rf.currentTerm
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// rf.currentTerm == args.Term
	if rf.currentTerm == args.Term {
		if rf.state == Leader {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
	}

	// rf.currentTerm < args.Term
	rf.CovertToFollower(args.Term)
	// check the candidate log is more update to date
	updateToDate := rf.checkUpdate(args)
	reply.Term = rf.currentTerm

	if !updateToDate {
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
	rf.setElectionTime()
}

func (rf *Raft) checkUpdate(args *RequestVoteArgs) bool {
	var lastLogIndex, lastLogTerm int

	lastLogIndex = rf.logs.Len() - 1
	if lastLogIndex == rf.lastIncludedIndex {
		// if the logs is empty after snapshot
		lastLogTerm = rf.lastIncludedTerm
	} else {
		lastLogTerm = rf.logs.GetTermByIndex(lastLogIndex)
	}

	if args.LastLogTerm > lastLogTerm {
		return true
	}

	if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		return true
	}
	return false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapshotRPCArgs, reply *InstallSnapshotRPCReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.state == Leader)
	if !isLeader {
		return index, term, isLeader
	}
	term = rf.currentTerm
	rf.logs.AppendEntry(Entry{
		Term:    term,
		Command: command,
	})
	rf.persist()
	index = rf.logs.Len() - 1

	rf.sendAppendRPCs(false)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) CovertToFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) CovertToLeader() {
	rf.state = Leader
	rf.persist()
	// reinitializaed after election
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.logs.Len()
		rf.matchIndex[i] = 0
	}
	rf.sendAppendRPCs(true)
}

func (rf *Raft) sendAppend(server int) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex == rf.lastIncludedIndex {
		args.PrevLogTerm = rf.lastIncludedTerm
	} else {
		args.PrevLogTerm = rf.logs.GetTermByIndex(args.PrevLogIndex)
	}

	lastLogIndex := rf.logs.Len() - 1
	if lastLogIndex >= rf.nextIndex[server] {
		args.Entries = append(args.Entries, rf.logs.GetSuffix(rf.nextIndex[server])...)
	}
	go func() {
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(server, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.CovertToFollower(reply.Term)
				rf.setElectionTime()
				return
			}
			// reply.Term <= rf.currentTerm
			if rf.currentTerm != args.Term || rf.state != Leader {
				// compare the current term with the term you sent in your appendEntry RPC
				return
			}
			if !reply.Success {
				if args.PrevLogIndex < rf.logs.StartIndex {
					// the log that send to follower is discarded by snapshot
					rf.sendSnapShot(server)
				} else {
					// Condition: log inconsistency
					rf.nextIndex[server] = rf.pickupNext(reply)
					rf.sendAppendRPCs(false)
				}
			} else {
				//Successfully append entry

				oldMatch := rf.matchIndex[server]
				rf.nextIndex[server] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])
				rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
				if oldMatch != rf.matchIndex[server] {
					rf.checkCommit()
				}
			}

		}
	}()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) checkLog(term int) (int, bool) {
	for i := rf.logs.Len() - 1; i >= rf.logs.StartIndex; i-- {
		if rf.logs.GetTermByIndex(i) == term {
			// find the last entry for term
			return i, true
		}
	}
	if term == rf.lastIncludedTerm {
		// the last included Index in the snapshot
		return rf.lastIncludedIndex, true
	}
	return 1, false
}

func (rf *Raft) pickupNext(reply AppendEntriesReply) int {
	if reply.XLen != 0 {
		// follower's log is too short
		return reply.XLen
	} else {
		if index, existed := rf.checkLog(reply.XTerm); existed {
			return index
		} else {
			return reply.XIndex
		}
	}
}

func (rf *Raft) sendAppendRPCs(heartbeat bool) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		lastLogIndex := rf.logs.Len() - 1
		if lastLogIndex >= rf.nextIndex[i] || heartbeat {
			if rf.nextIndex[i] < rf.logs.StartIndex {
				rf.sendSnapShot(i)
			} else {
				// rf.nextIndex[i] >= rf.logs.StartIndex
				rf.sendAppend(i)
			}
		}
	}
}

func (rf *Raft) sendSnapShot(server int) {
	args := InstallSnapshotRPCArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
		Offset:            0,
		Done:              true,
	}
	go func() {
		var reply InstallSnapshotRPCReply
		ok := rf.sendInstallSnapShot(server, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.CovertToFollower(reply.Term)
				rf.startElection()
			}

			if reply.Term == rf.currentTerm {
				// the follower receive the snaphsot
				rf.nextIndex[server] = rf.lastIncludedIndex + 1
				rf.matchIndex[server] = rf.lastIncludedIndex
			}
		}
	}()
}

func (rf *Raft) requestVote(server int, arg *RequestVoteArgs, votes *int) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(server, arg, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.CovertToFollower(reply.Term)
			rf.setElectionTime()
			return
		}
		if rf.state != Candidate || rf.currentTerm != arg.Term {
			return
		}

		if reply.VoteGranted {
			*votes += 1
			if *votes >= (len(rf.peers)/2)+1 {
				rf.CovertToLeader()
			}
		}
	}
}

func (rf *Raft) requestVoteRPCs() {
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	args.LastLogIndex = rf.logs.Len() - 1
	if args.LastLogIndex == rf.lastIncludedIndex {
		// if the los is empty
		args.LastLogTerm = rf.lastIncludedTerm
	} else {
		args.LastLogTerm = rf.logs.GetTermByIndex(args.LastLogIndex)
	}

	votes := 1
	n := len(rf.peers)
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		go rf.requestVote(i, args, &votes)
	}
}

func (rf *Raft) checkCommit() {
	for N := rf.commitIndex + 1; N < rf.logs.Len(); N++ {
		if N <= rf.lastIncludedIndex || rf.logs.GetTermByIndex(N) != rf.currentTerm {
			continue
		}
		count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= N {
				count++
				if count >= (len(rf.peers)/2)+1 {
					rf.commitIndex = N
					break
				}
			}
		}
	}
}

func (rf *Raft) startElection() {
	// Convert to Candidate
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.setElectionTime()

	rf.requestVoteRPCs()
}

func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		// Send heartBeat to followers
		rf.sendAppendRPCs(true)
	} else {
		if time.Now().After(rf.electionTime) {
			rf.startElection()
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.tick()
		ms := 100
		// sleep 100 ms to check the server's state
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

/*
*
methods:
 1. use dedicated “applier” to detect the Raft node need to send entry into applyCh
 2. use mutex to lock around these applies, so that some other routine doesn’t also detect
    that entries need to be applied and also tries to apply
*/
func (rf *Raft) apply() {
	for {
		rf.mu.Lock()
		msgs := []ApplyMsg{}
		if rf.lastIncludedIndex > rf.lastApplied {
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotIndex: rf.lastIncludedIndex,
				SnapshotTerm:  rf.lastIncludedTerm,
			}
			rf.lastApplied = rf.lastIncludedIndex
			rf.commitIndex = rf.lastIncludedIndex
			msgs = append(msgs, msg)
		} else {
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs.GetCommandByIndex(rf.lastApplied),
					CommandIndex: rf.lastApplied,
				}
				msgs = append(msgs, msg)
			}
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyCh <- msg
		}

		time.Sleep(10 * time.Millisecond)
	}
}

/*
* only three scenarios:
 1. you get an AppendEntries RPC from the current leader
    (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer)
 2. you are starting an election;
 3. you grant a vote to another peer
*/
func (rf *Raft) setElectionTime() {
	t := time.Now()
	// t = t.Add(ElectionInterval)
	ms := 150 + (rand.Int63() % 450)
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = New(0, rf.currentTerm)
	rf.commitIndex = 0
	rf.lastApplied = 0
	n := len(rf.peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = rf.logs.Len()
		rf.matchIndex[i] = 0
	}
	// init the snapshot
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = rf.logs.GetTermByIndex(rf.lastIncludedIndex)
	rf.applyCh = applyCh
	// Set the election timer
	rf.setElectionTime()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// initialize from snapshot persisted before a crash
	// rf.snapshot = rf.persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.apply()

	return rf
}

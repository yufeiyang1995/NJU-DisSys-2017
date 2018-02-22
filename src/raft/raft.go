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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Log entry contains command for state machine
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	votedCount  int
	logs        []LogEntry
	state       string

	commitIndex int
	lastApplied int

	voteResultChan chan bool
	voteGrantChan  chan bool
	heartBeatChan  chan bool
	commitChan     chan bool
	applyChan      chan ApplyMsg

	// volatile state on leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = (rf.state == "LEADER")
	// println("state", rf.State)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.logs)

	// e.Encode(rf.yyy)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.logs)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// Append Entry RPC arguments structure
type AppendEntryArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
	Index   int
	// yyf wrong：加Index
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// println("RequestVote")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.CandidateID == rf.me {
		return
	}

	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.reduceToFollower()
		reply.Term = rf.currentTerm
		rf.persist()
	}

	lastTerm := rf.logs[len(rf.logs)-1].Term
	lastIndex := len(rf.logs) - 1
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		// 判断最后一个Term
		if args.LastLogTerm < lastTerm {
			return
		}

		// 判断最后一个Index
		// wrong
		if args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex {
			return
		}
		// println("test")
		reply.VoteGranted = true
		rf.state = "FOLLOWER"
		rf.votedFor = args.CandidateID
		rf.votedCount = 0
		rf.voteGrantChan <- true
		//println("test:", reply.VoteGranted, ",", reply.Term)
		return
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		//println("test0")
		return ok
	}
	//println("reply:", reply.Term, ",", reply.VoteGranted)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "CANDIDATE" || rf.currentTerm != args.Term {
		//println("test1")
	} else if rf.currentTerm < reply.Term {
		//println("test2")
		rf.currentTerm = reply.Term
		rf.reduceToFollower()
		rf.persist()
	} else if reply.VoteGranted {
		rf.votedCount++
		if rf.votedCount > (len(rf.peers) / 2) {
			rf.state = "LEADER"
			rf.voteResultChan <- true
		}
	}

	return ok
}

func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false

	leaderTerm := args.Term
	if leaderTerm < rf.currentTerm {
		return
	}

	if leaderTerm > rf.currentTerm {
		rf.reduceToFollower()
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.persist()
	}
	if args.LeaderID != rf.me {
		rf.heartBeatChan <- true
	}

	lastIndex := len(rf.logs) - 1
	//println(rf.me, "index:", args.PrevLogIndex, ",lastIndex:", lastIndex)
	//wrong

	if args.PrevLogIndex < 0 {
		args.PrevLogIndex = -1
	} else {
		if lastIndex < args.PrevLogIndex {
			reply.Index = lastIndex + 1
			return
		} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				if rf.logs[i].Term == args.PrevLogTerm {
					reply.Index = i + 1
					break
				}
			}
			return
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// yyf wrong
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		// yyf-wrong
		rf.commitChan <- true
	}

	rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	reply.Index = lastIndex + 1
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)

	if !ok {
		return ok
	}

	if rf.state != "LEADER" || rf.currentTerm != args.Term {
		return ok
	} else if rf.currentTerm < reply.Term {
		rf.reduceToFollower()
		rf.currentTerm = reply.Term
		rf.persist()
		return ok
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			//wrong
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		rf.nextIndex[server] = reply.Index
	}
	return ok
}

func (rf *Raft) PrevLog(server int) LogEntry {
	index := rf.nextIndex[server] - 1
	if index >= 0 {
		return rf.logs[index]
	} else {
		return LogEntry{Term: 0}
	}
}

func (rf *Raft) LeaderCommitIndex() int {
	for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
		if rf.logs[i].Term != rf.currentTerm {
			break
		}
		count := 0
		for j := range rf.peers {
			if rf.matchIndex[j] >= i {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			temp := i - rf.commitIndex
			rf.commitIndex = i
			return temp
		}
	}
	return 0

}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	commits := rf.LeaderCommitIndex()
	if commits > 0 {
		rf.commitChan <- true
	}

	term := rf.currentTerm
	leaderID := rf.me
	leaderCommit := rf.commitIndex
	entries := make([]LogEntry, 0)
	prevLogIndex := len(rf.logs) - 1
	prevLogTerm := rf.logs[prevLogIndex].Term

	for i := range rf.peers {
		reply := new(AppendEntryReply)
		prevLogIndex = rf.nextIndex[i] - 1
		prevLogTerm = rf.PrevLog(i).Term
		//index wrong
		entries = make([]LogEntry, len(rf.logs[prevLogIndex+1:]))
		copy(entries, rf.logs[prevLogIndex+1:])
		args := AppendEntryArgs{term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit}
		go func(index int, arg AppendEntryArgs) {
			//init wrong
			rf.sendAppendEntry(index, arg, reply)
		}(i, args)
	}
}

func (rf *Raft) elect() {
	candidateID := rf.me
	term := rf.currentTerm
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	args := RequestVoteArgs{term, candidateID, lastLogIndex, lastLogTerm}

	for i := range rf.peers {
		go func(index int) {
			reply := new(RequestVoteReply)
			rf.sendRequestVote(index, args, reply)
		}(i)
	}
}

// 候选人给自己投票
func (rf *Raft) candidateVoteSelf() {
	// println("can vote:", rf.State)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votedCount = 1
}

// 退回Follower
func (rf *Raft) reduceToFollower() {
	rf.state = "FOLLOWER"
	rf.votedCount = 0
	rf.votedFor = -1
}

func (rf *Raft) initLeader() {
	rf.state = "LEADER"
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) loop() {
	a := 0
	for {
		switch rf.state {
		case "FOLLOWER":
			// println("i:", a, "follewer")
			select {
			case <-rf.voteGrantChan:
			case <-rf.heartBeatChan:
			case <-time.After(time.Duration(150+rand.Int63n(150)) * time.Millisecond):
				rf.state = "CANDIDATE"
			}
		case "CANDIDATE":
			// println("i:", a, "candidate")
			rf.candidateVoteSelf()
			rf.persist()
			go rf.elect()

			select {
			case <-rf.voteResultChan:
				//println("success:")
				rf.initLeader()
				rf.persist()
			case <-rf.heartBeatChan:
			case <-rf.voteGrantChan:
			case <-time.After(time.Duration(150+rand.Int63n(150)) * time.Millisecond):
			}

		case "LEADER":
			//println("i:", a, "leader")
			rf.sendHeartbeat()
			time.Sleep(50 * time.Millisecond)
		}
		a++
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := false

	if rf.state == "LEADER" {
		//println(rf.me, " log leader:", command.(int))
		isLeader = true
		index = len(rf.logs)
		rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
	}
	return index, term, isLeader
}

func (rf *Raft) Commit() {
	for {
		select {
		case <-rf.commitChan:
			rf.mu.Lock()
			// Commit Log
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				msg := ApplyMsg{Index: rf.lastApplied, Command: rf.logs[rf.lastApplied].Command}
				rf.applyChan <- msg
			}
			rf.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.votedCount = 0
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	// println("log len:", len(rf.logs))
	rf.state = "FOLLOWER"
	rf.heartBeatChan = make(chan bool, 100)
	rf.voteGrantChan = make(chan bool, 100)
	rf.voteResultChan = make(chan bool, 100)
	rf.commitChan = make(chan bool, 100)
	rf.applyChan = applyCh
	// initialize from state persisted before a crash

	go rf.loop()
	go rf.Commit()
	rf.readPersist(persister.ReadRaftState())

	return rf
}

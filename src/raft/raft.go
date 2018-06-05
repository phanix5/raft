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
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// current state
	// 0 - follower
	// 1 - candidate
	// 2 - leader
	status int

	// persistant state
	currentTerm int
	votedFor    int
	log         []*Log

	//volatile state for servers
	commitIndex int
	lastApplied int

	//volatile state on leaders
	//reinitialized after election
	nextIndex  []int
	matchIndex []int

	//persistance
	lastIncludedIndex int
	lastIncludedTerm  int

	// util
	// timer index
	timerIndex int
	// votecount
	voteCount int
	// last log entry index
	lastLogIndex int

	//apply channel
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	term = rf.currentTerm
	if rf.status == 2 {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.log) != nil {
		//fmt.Println("FATAL: error during state read.")
	} else {
		rf.lastLogIndex = len(rf.log) - 1
		//fmt.Printf("State for %v persisted succesfully\n", rf.me)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateTerm int
	CandidateID   int
	LastLogIndex  int
	LastLogTerm   int
}

//
// append entries struct
//
type AppendEntriesArgs struct {
	LeadersTerm  int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []*Log
	LeaderCommit int
}

//
// append entries reply
//
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// Install Snapshot Struct
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

// Install Snapshot Reply
type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("%v voter Status:Last log index:%v\nLast log term: %v\n", rf.me, len(rf.log), rf.log[len(rf.log)-1].Term)
	if args.CandidateTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//fmt.Println("Sending False because of outdated term")
	} else if args.CandidateTerm == rf.currentTerm {
		reply.Term = rf.currentTerm
		//check if self is available to vote in the current term and log status
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && rf.CheckCandidateLogStatus(args) == true {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.status = 0
			go rf.CreateTimer(0)
		} else {
			reply.VoteGranted = false
			//fmt.Printf("Sending false because of outdated log/already voted, votedTo: %v\n", rf.votedFor)
		}
	} else {
		// candidate has higher term, check for log status and vote for it
		rf.currentTerm = args.CandidateTerm
		rf.votedFor = -1
		if rf.CheckCandidateLogStatus(args) == true {
			reply.VoteGranted = true
			rf.status = 0
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateID
			// In this case, this has to become a follower
			go rf.CreateTimer(0)
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			//fmt.Println("Sending false because outdated logs")
		}
	}
}

//
// Check candidate's log status with self
//
func (rf *Raft) CheckCandidateLogStatus(args *RequestVoteArgs) bool {

	var term = rf.log[len(rf.log)-1].Term
	//fmt.Printf("Voter's term: %v, Candidate's term: %v\n", term, args.CandidateTerm)
	if args.LastLogTerm > term {
		return true
	} else if args.LastLogTerm < term {
		return false
	}
	if args.LastLogIndex >= rf.lastLogIndex {
		return true
	}
	return false
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// check if voters term is higher than self
		// Also check if server is still on term given in args
		//fmt.Printf("%v recieved %v vote from %v\n", rf.me, reply.VoteGranted, server)
		if rf.currentTerm == args.CandidateTerm {
			if reply.Term > rf.currentTerm {
				// drop self from election
				//fmt.Printf("voter %v's term (%v) is higher than %v's term(%v)\n", server, reply.Term, rf.me, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.status = 0
				// create a follower timer
				go rf.CreateTimer(0)
			} else if reply.VoteGranted == true {
				rf.voteCount++
				// check if got majority
				if rf.voteCount > len(rf.peers)/2 {
					// server became leader!!
					// reintialize volatile state
					//fmt.Printf("%v became the leader!\n", rf.me)
					//fmt.Printf("Last log index: %v\nLast Log term: %v\n", len(rf.log), rf.log[len(rf.log)-1].Term)
					rf.status = 2
					rf.nextIndex = nil
					rf.matchIndex = nil
					for range rf.peers {
						rf.nextIndex = append(rf.nextIndex, rf.lastLogIndex+1)
						rf.matchIndex = append(rf.matchIndex, 0)
					}
					rf.voteCount = 0
					rf.timerIndex++
					//rf.DebugPrintRaftStatus()
					go rf.HeartBeat()
					go rf.CheckMajorityCommit()
				}
			}
		}
	}
	//fmt.Printf("%v\n", ok)
	return ok
}

// DebugPrintRaftStatus
func (rf *Raft) DebugPrintRaftStatus() {
	fmt.Printf("Server %v status:\n", rf.me)
	fmt.Printf("currentTerm: %v | votedFor: %v | commitIndex: %v | lastApplied: %v\n", rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied)
	fmt.Printf("Log: [ ")
	for _, v := range rf.log {
		fmt.Printf(" %v-%v ", v.Command, v.Term)
	}
	fmt.Println("")
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if rf.status == 2 {
		term = rf.currentTerm
		rf.log = append(rf.log, &Log{command, rf.currentTerm})
		rf.lastLogIndex++
		index = rf.lastLogIndex
		//fmt.Printf("Server %v got new log entry %v at %v\n", rf.me, command, index)
	} else {
		isLeader = false
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timerIndex = -1
	// Your code here, if desired.
}

//
// timer utility function. to be launched as a go routine
//
func (rf *Raft) Timer(index int) {
	time.Sleep((400 + time.Duration(rand.Intn(400))) * time.Millisecond)
	rf.mu.Lock()
	if index == rf.timerIndex {
		//fmt.Printf("%v Timed out!! Starting an election.\n", rf.me)
		go rf.StartElection()
	}
	rf.mu.Unlock()
}

//
// Timer to restart election on timeout
//
func (rf *Raft) ElectionTimer(index int) {
	time.Sleep((400 + time.Duration(rand.Intn(400))) * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index == rf.timerIndex {
		// timer still relevant => server has not got majority. restart election
		//fmt.Printf("%v's election timed out, restarting election\n", rf.me)
		go rf.StartElection()
	}
}

//
// Timer to restart heartbeats
//
func (rf *Raft) HeartBeatTimer(index int) {
	time.Sleep(100 * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index == rf.timerIndex {
		//fmt.Printf("%v creating next set of hearbeats\n", rf.me)
		go rf.HeartBeat()
	}
}

//
// function that creates a new timer and increments the timerIndex
//
func (rf *Raft) CreateTimer(timerType int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.timerIndex == -1 {
		return
	}
	//fmt.Printf("Creating timer of type %v for %v\n", timerType, rf.me)
	rf.timerIndex++
	switch timerType {
	case 0:
		go rf.Timer(rf.timerIndex)
	case 1:
		go rf.ElectionTimer(rf.timerIndex)
	case 2:
		go rf.HeartBeatTimer(rf.timerIndex)
	}
}

//
// function begins election
//
func (rf *Raft) StartElection() {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status = 1
	rf.voteCount = 1
	rf.votedFor = rf.me
	rf.currentTerm++
	//fmt.Printf("%v candidate's status:\nLastLogIndex: %v\nLastLogTerm: %v\n", rf.me, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
	for i := range rf.peers {
		if i != rf.me {
			request := &RequestVoteArgs{CandidateTerm: rf.currentTerm, CandidateID: rf.me}
			if len(rf.log) == 0 {
				request.LastLogIndex = rf.lastIncludedIndex
				request.LastLogTerm = rf.lastIncludedTerm
			} else {
				request.LastLogIndex = rf.lastLogIndex
				request.LastLogTerm = rf.log[len(rf.log)-1].Term
			}
			reply := &RequestVoteReply{}
			go rf.sendRequestVote(i, request, reply)
		}
	}
	// start an election timer
	go rf.CreateTimer(1)

}

//
// Start a heartbeat (for leaders)
//
func (rf *Raft) HeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			currentIndex := rf.nextIndex[i] - rf.lastIncludedIndex - 1
			if currentIndex >= 0 {
				args := &AppendEntriesArgs{LeaderID: rf.me, LeadersTerm: rf.currentTerm, LeaderCommit: rf.commitIndex, PrevLogIndex: rf.nextIndex[i] - 1}
				args.PrevLogTerm = rf.log[currentIndex].Term
				args.LogEntries = rf.log[currentIndex+1:]
				reply := &AppendEntriesReply{}
				go rf.SendAppendEntries(i, args, reply)
			} else {
				// Required index is inside snapshot. send the snapshot to the follower

			}
		}
	}
	//fmt.Printf("heartbeats sent, next Indices: %v\n", rf.nextIndex)
	go rf.CreateTimer(2)
}

//
// Periodically checks if majority of peers have added entries to their logs
//
func (rf *Raft) CheckMajorityCommit() {
	for {
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		if rf.status != 2 {
			break
		}
		var minval, count int = 1e9, 0
		for i := range rf.peers {
			if i != rf.me {
				if rf.matchIndex[i] > rf.commitIndex && rf.log[rf.matchIndex[i]].Term == rf.currentTerm {
					count++
					if minval > rf.matchIndex[i] {
						minval = rf.matchIndex[i]
					}
				}
			}
		}
		//fmt.Printf("%v\n", minval)
		if minval != 1e9 && rf.log[minval].Term == rf.currentTerm && count >= len(rf.peers)/2 {
			rf.commitIndex = minval
		}
		rf.mu.Unlock()
	}
	rf.mu.Unlock()
}

//
// AppendEntriesRPC handler for sender
//
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		//TODO
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//fmt.Printf("Appendentries reply %v\n", reply.Success)
		//fmt.Printf("server %v log: %v\n", rf.me, rf.log)
		if reply.Success == true {
			//update the next index of the replier
			rf.nextIndex[server] = args.PrevLogIndex + len(args.LogEntries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.LogEntries)
		} else {
			// First check if failure is due to leader's outdated term
			if reply.Term > rf.currentTerm {
				// Drop down to follower!!
				rf.currentTerm = reply.Term
				rf.status = 0
				//fmt.Printf("%v stepped down due to oudated term\n", rf.me)
				go rf.CreateTimer(0)
			} else {
				//Set nextIndex to the conflictIndex
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
	}
	return ok
}

//
// AppendEntriesRPC receiver handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("%v received APRPC from %v\n", rf.me, args.LeaderID)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("Server %v received AppendEntries\n", rf.me)

	if args.LeadersTerm >= rf.currentTerm {
		rf.status = 0
		rf.currentTerm = args.LeadersTerm
	}

	reply.Term = rf.currentTerm

	// check term first
	if rf.currentTerm > args.LeadersTerm {
		reply.Success = false
		//fmt.Printf("False reply to heartbeat by %v because of outdated term\n", rf.me)
		go rf.CreateTimer(0)
		return
	}

	// check if log entry present at prevLogIndex with prevLogEntry
	if args.PrevLogIndex > rf.lastLogIndex || args.PrevLogIndex < rf.lastIncludedIndex || rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.Success = false
		//fmt.Printf("False reply from %v due to log inconsistency\n", rf.me)
		// Is prevLogIndex even in the log?
		if args.PrevLogIndex >= rf.lastLogIndex {
			reply.ConflictIndex = rf.lastLogIndex
		} else if args.PrevLogIndex < rf.lastIncludedIndex {
			reply.ConflictIndex = -1
		} else { //term mismatch
			j := args.PrevLogIndex - rf.lastIncludedIndex
			i := j
			for ; j >= 0 && rf.log[j].Term == rf.log[i].Term; j-- {
			}
			reply.ConflictIndex = j + 1
		}
		go rf.CreateTimer(0)
		return
	}

	// append entries to the log
	for i, v := range args.LogEntries {
		var j = args.PrevLogIndex - rf.lastIncludedIndex + i + 1
		if j < len(rf.log) {
			if rf.log[j].Term != v.Term {
				//truncate the slice
				//fmt.Printf("truncating %v's log from index: %v\n", args.PrevLogIndex, j)
				rf.log = rf.log[:j]
				rf.log = append(rf.log, v)
			}
		} else {
			rf.log = append(rf.log, v)
		}
	}
	rf.lastLogIndex = rf.lastIncludedIndex + len(rf.log) - 1

	//fmt.Printf("server %v log: %v\n", rf.me, rf.log)
	//check commit index
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.lastLogIndex {
			rf.commitIndex = rf.lastLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	//rf.DebugPrintRaftStatus()
	reply.Success = true
	rf.status = 0
	go rf.CreateTimer(0)
}

//
// Applies log entries to the state machine
//
func (rf *Raft) ApplyToStateMachine() {

	for {
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{Command: rf.log[rf.lastApplied-rf.lastIncludedIndex].Command, CommandIndex: rf.lastApplied, CommandValid: true}
			//fmt.Printf("%v applied %v to state machine\n", rf.me, rf.log[rf.lastApplied])
		}
		rf.persist()
		rf.mu.Unlock()
	}

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

	// Your initialization code here (2A, 2B, 2C).

	rf.status = 0
	rf.applyCh = applyCh

	// votedFor should be nil (-1)
	rf.votedFor = -1

	rf.lastLogIndex = 0
	rf.log = append(rf.log, &Log{"creation", 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//fmt.Printf("%v\n", peers[me])
	rf.CreateTimer(0)

	go rf.ApplyToStateMachine()

	return rf
}

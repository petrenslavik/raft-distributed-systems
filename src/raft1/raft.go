package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"log"
	"math/rand"
	"slices"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type RaftState string

const (
	FollowerState  RaftState = "FollowerState"
	CandidateState RaftState = "CandidateState"
	LeaderState    RaftState = "LeaderState"

	ElectionTimeout time.Duration = 550 * time.Millisecond
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	majorityThreshold int
	votesReceived     []bool
	lastHeartBeat     time.Time
	state             RaftState
	currentTerm       int
	votedFor          int

	applyCh     chan raftapi.ApplyMsg
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LeaderState
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	if err := encoder.Encode(rf.currentTerm); err != nil {
		log.Fatalf("readPersist: encode currentTerm: %v", err)
	}
	if err := encoder.Encode(rf.votedFor); err != nil {
		log.Fatalf("readPersist: encode votedFor: %v", err)
	}
	if err := encoder.Encode(rf.log); err != nil {
		log.Fatalf("readPersist: encode log: %v", err)
	}

	raftstate := buffer.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	if err := decoder.Decode(&rf.currentTerm); err != nil {
		log.Fatalf("readPersist: decode currentTerm: %v", err)
	}
	if err := decoder.Decode(&rf.votedFor); err != nil {
		log.Fatalf("readPersist: decode votedFor: %v", err)
	}
	if err := decoder.Decode(&rf.log); err != nil {
		log.Fatalf("readPersist: decode log: %v", err)
	}

}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	log.Println("Server", rf.me, "was requested to vote for", args.CandidateId, ", candidate term is", rf.currentTerm, "my term is", args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	myLastLogIndex := len(rf.log) - 1
	myLastLogTerm := rf.log[myLastLogIndex].Term

	myLogOlder := false
	if myLastLogTerm > args.LastLogTerm {
		myLogOlder = true
	} else if myLastLogTerm == args.LastLogTerm {
		myLogOlder = myLastLogIndex > args.LastLogIndex
	} else {
		myLogOlder = false
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && !myLogOlder {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	if reply.VoteGranted {
		log.Println("Server", rf.me, "is voting for", args.CandidateId, "for term", args.Term)
	}
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
func (rf *Raft) sendRequestVote(requestedServerId, term, lastLogIndex, lastLogTerm int) {
	log.Println("Server", rf.me, "sends request for vote to", requestedServerId)
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}
	isSucceeded := rf.peers[requestedServerId].Call("Raft.RequestVote", &args, &reply)

	if !isSucceeded {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if rf.state == LeaderState || term != rf.currentTerm {
		return
	}

	if reply.VoteGranted {
		rf.votesReceived[requestedServerId] = true
		log.Println("Server", rf.me, "received vote from", requestedServerId)

		if rf.countVotes() >= rf.majorityThreshold {
			log.Println("Server", rf.me, "won election")
			rf.state = LeaderState
			for i := range len(rf.peers) {
				rf.nextIndex[i] = len(rf.log)
			}
			for i := range len(rf.peers) {
				rf.matchIndex[i] = rf.commitIndex
			}
			go rf.runHeartbeatCycle()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.currentTerm++
	rf.state = CandidateState
	rf.votedFor = rf.me
	rf.votesReceived = make([]bool, len(rf.peers))
	rf.votesReceived[rf.me] = true

	log.Println("Server", rf.me, "sending request for votes")
	lastLogIndex := len(rf.log) - 1

	for index := range rf.peers {
		if index != rf.me {
			go rf.sendRequestVote(index, rf.currentTerm, lastLogIndex, rf.log[lastLogIndex].Term)
		}
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state != LeaderState {
		return rf.commitIndex, rf.currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.matchIndex[rf.me] = len(rf.log) - 1
	log.Println("Leader server", rf.me, "added new entry", command, "to log. \n Now log has", len(rf.log), "entries and its content is", rf.log)
	return len(rf.log) - 1, rf.currentTerm, true
}

func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		if time.Since(rf.lastHeartBeat) > ElectionTimeout {
			rf.lastHeartBeat = time.Now()
			go rf.startElection()
		}
		rf.mu.Unlock()

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyTicker() {
	for {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
			commandIndex := rf.lastApplied
			log.Println("Server", rf.me, "applying command:", entry.Command, "with index ", rf.lastApplied, ". CommitIndex - ", rf.commitIndex)
			rf.mu.Unlock()
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: commandIndex,
			}
			rf.mu.Lock()
			log.Println("Server", rf.me, "applied command:", entry.Command, "with index ", rf.lastApplied, ". CommitIndex - ", rf.commitIndex)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 10 + (rand.Int63() % 11)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	log.Println("Server", rf.me, "started")
	// Your initialization code here (3A, 3B, 3C).
	rf.votedFor = -1
	rf.majorityThreshold = (len(peers) + 1) / 2
	rf.log = []LogEntry{{
		Term: 0,
	}}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyTicker()
	return rf
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	LeaderCommit int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	FailedTerm       int
	FailedFirstIndex int
}

// Commited - majority of servers has the entry
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.state = FollowerState
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	rf.lastHeartBeat = time.Now()

	log.Println("Server", rf.me, "has", len(rf.log), "entries. Leader", args.LeaderId, "think common log index is", args.PrevLogIndex, "\nMy log:", rf.log, "\n New Entries:", args.Entries)
	if len(rf.log) <= args.PrevLogIndex {
		rf.buildFailedReply(rf.log[len(rf.log)-1].Term, args.PrevLogIndex, reply)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.buildFailedReply(rf.log[args.PrevLogIndex].Term, args.PrevLogIndex, reply)
		return
	}

	reply.Success = true
	entriesPointer := 0
	for logPointer := args.PrevLogIndex + 1; logPointer < len(rf.log) && entriesPointer < len(args.Entries); logPointer++ {
		if rf.log[logPointer].Term != args.Entries[entriesPointer].Term {
			rf.log = rf.log[:logPointer]
			break
		}
		entriesPointer++
	}
	rf.log = append(rf.log, args.Entries[entriesPointer:]...)
	log.Println("Server", rf.me, "replicated entries", len(rf.log), rf.log)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
}

func (rf *Raft) sendEntries(requestedServerId, term, logIndexToAppend, logTermToAppend, commitIndex int, entries []LogEntry) {
	args := AppendEntriesArgs{
		LeaderId:     rf.me,
		Term:         term,
		PrevLogIndex: logIndexToAppend,
		PrevLogTerm:  logTermToAppend,
		LeaderCommit: commitIndex,
		Entries:      entries,
	}

	reply := AppendEntriesReply{}
	receivedResponse := rf.peers[requestedServerId].Call("Raft.AppendEntries", &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if !receivedResponse {
		if term == rf.currentTerm {
			log.Println("Leader server", rf.me, "No response for appendEntries from", requestedServerId)
		}
		return
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if rf.state != LeaderState || rf.currentTerm != args.Term || logIndexToAppend != rf.nextIndex[requestedServerId]-1 {
		return
	}

	if !reply.Success {
		log.Println("Leader server", rf.me, "unsucessful response for appendEntries from", requestedServerId, "failed term -", reply.FailedTerm, "failed term first index - ", reply.FailedFirstIndex)
		log.Println("Leader server", rf.me, rf.log)
		rf.nextIndex[requestedServerId] = reply.FailedFirstIndex
		return
	}

	log.Println("Leader server", rf.me, "updating nextIndex, matchIndex and commitIndex for", requestedServerId, "\nRequest was", args)
	rf.nextIndex[requestedServerId] = args.PrevLogIndex + len(entries) + 1
	rf.matchIndex[requestedServerId] = args.PrevLogIndex + len(entries)
	commitIndexBefore := rf.commitIndex
	log.Println("Leader server", rf.me, "Updating replicas counter from ", rf.matchIndex[requestedServerId]+1, "to next index ", rf.nextIndex[requestedServerId], "log length", len(rf.log))
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp, rf.matchIndex)
	slices.Sort(tmp)
	median := tmp[(len(tmp)-1)/2]
	log.Println("Leader server", rf.me, "computed match majority is", median, "matchIndex arr", rf.matchIndex)
	if rf.log[median].Term == rf.currentTerm {
		rf.commitIndex = median
	}
	if commitIndexBefore != rf.commitIndex {
		log.Println("Leader server", rf.me, "new commit index - ", rf.commitIndex, "\nLeader log", rf.log)
	}
}

func (rf *Raft) runHeartbeatCycle() {
	for {
		rf.mu.Lock()

		if rf.state != LeaderState {
			rf.mu.Unlock()
			return
		}

		rf.lastHeartBeat = time.Now()
		for index := range rf.peers {
			if index != rf.me {
				logIndexToAppend := rf.nextIndex[index]
				log.Println("Leader server", rf.me, "last index for", index, "server is ", logIndexToAppend)
				go rf.sendEntries(index, rf.currentTerm, logIndexToAppend-1, rf.log[logIndexToAppend-1].Term, rf.commitIndex, rf.log[logIndexToAppend:])
			}
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) buildFailedReply(failedTerm, prevLogIndex int, reply *AppendEntriesReply) {
	reply.Success = false
	reply.FailedTerm = failedTerm
	reply.FailedFirstIndex = prevLogIndex
	for i := len(rf.log) - 1; i >= 0; i-- {
		if reply.FailedTerm == rf.log[i].Term {
			reply.FailedFirstIndex = i
		}
	}
	reply.FailedFirstIndex = max(reply.FailedFirstIndex, 1)
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = FollowerState
}

func (rf *Raft) countVotes() int {
	n := 0
	for _, v := range rf.votesReceived {
		if v {
			n++
		}
	}
	log.Println("Server", rf.me, "total votes is", n)
	return n
}

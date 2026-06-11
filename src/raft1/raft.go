package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
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
	totalVotes        int
	lastHeartBeat     time.Time
	state             RaftState
	currentTerm       int
	votedFor          int

	applyCh                chan raftapi.ApplyMsg
	log                    []LogEntry
	commitIndex            int
	lastApplied            int
	nextIndex              []int
	matchIndex             []int
	replicatedEntryCounter []int
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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	log.Println("Server", rf.me, "was requested to vote for", args.CandidateId)
	log.Println("Server", rf.me, "term is", rf.currentTerm, "Candidate term is", args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FollowerState
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
		log.Println(rf.me, "is voting for", args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(me, requestedServerId, term, lastLogIndex, lastLogTerm int) {
	log.Println("Server", rf.me, "sends request for vote to", requestedServerId)
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}
	rf.peers[requestedServerId].Call("Raft.RequestVote", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FollowerState
		return
	}

	if rf.state == LeaderState {
		return
	}

	if reply.VoteGranted {
		rf.totalVotes++
		if rf.totalVotes >= rf.majorityThreshold {
			log.Println("Election won by", rf.me)
			rf.state = LeaderState
			for i := range len(rf.peers) {
				rf.nextIndex[i] = len(rf.log)
			}
			rf.replicatedEntryCounter = make([]int, len(rf.log))
			for i := range len(rf.replicatedEntryCounter) {
				if rf.commitIndex >= i {
					rf.replicatedEntryCounter[i] = rf.majorityThreshold
				} else {
					rf.replicatedEntryCounter[i] = 1
				}
			}
			go rf.runHearbeatCycle()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeartBeat = time.Now()
	rf.currentTerm++
	rf.state = CandidateState
	rf.votedFor = rf.me
	rf.totalVotes = 1

	log.Println("Server", rf.me, "sending request for votes")
	lastLogIndex := len(rf.log) - 1

	for index := range rf.peers {
		if index != rf.me {
			go rf.sendRequestVote(rf.me, index, rf.currentTerm, lastLogIndex, rf.log[lastLogIndex].Term)
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LeaderState {
		return rf.commitIndex, rf.currentTerm, false
	}

	log.Println("Leader server", rf.me, "added new entry to log, new length is", len(rf.log)+1)
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.replicatedEntryCounter = append(rf.replicatedEntryCounter, 1)

	return len(rf.log) - 1, rf.currentTerm, true
}

func (rf *Raft) ticker() {
	for {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if time.Since(rf.lastHeartBeat) > ElectionTimeout {
			log.Println("Server", rf.me, "starts election")
			go rf.startElection()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyTicker() {
	for {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			log.Println("Server", rf.me, "applying command - ", rf.lastApplied, ". CommitIndex - ", rf.commitIndex)
			entry := rf.log[rf.lastApplied]
			commandIndex := rf.lastApplied
			rf.mu.Unlock()
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: commandIndex,
			}
			rf.mu.Lock()
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
	rf.replicatedEntryCounter = []int{len(rf.peers)}
	gob.Register(LogEntry{})
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
	Term    int
	Success bool
}

// Commited - majority of servers has the entry
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Println("Server", rf.me, "received heartbeat from", args.LeaderId)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.lastHeartBeat = time.Now()
	rf.currentTerm = args.Term
	rf.state = FollowerState

	log.Println("Server", rf.me, "has", len(rf.log), "entries. Leader log index is", args.PrevLogIndex)
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	reply.Success = true
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	log.Println("Server", rf.me, "replicated entries", len(rf.log), rf.log)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
}

func (rf *Raft) sendEntries(me, requestedServerId, term, logIndexToAppend, logTermToAppend, commitIndex int, entries []LogEntry) {
	args := AppendEntriesArgs{
		LeaderId:     me,
		Term:         term,
		PrevLogIndex: logIndexToAppend,
		PrevLogTerm:  logTermToAppend,
		LeaderCommit: commitIndex,
		Entries:      entries,
	}

	reply := AppendEntriesReply{}
	receivedResponse := rf.peers[requestedServerId].Call("Raft.AppendEntries", &args, &reply)
	if !receivedResponse {
		log.Println("Leader server", rf.me, "No response for appendEntries from", requestedServerId)
		return
	}
	log.Println("Leader server", rf.me, "Received response for appendEntries from", requestedServerId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FollowerState
		return
	}

	if !reply.Success {
		rf.nextIndex[requestedServerId]--
		return
	}

	log.Println("Leader server", rf.me, "updating nextIndex, matchIndex and commitIndex for", requestedServerId)
	rf.nextIndex[requestedServerId] += len(entries)
	for i := rf.matchIndex[requestedServerId] + 1; i < rf.nextIndex[requestedServerId]; i++ {
		rf.replicatedEntryCounter[i]++
		if rf.replicatedEntryCounter[i] >= rf.majorityThreshold {
			rf.commitIndex = i
		}
	}
	rf.matchIndex[requestedServerId] = rf.nextIndex[requestedServerId] - 1
	log.Println("Leader server", rf.me, "new commit index - ", rf.commitIndex, rf.replicatedEntryCounter)
}

func (rf *Raft) runHearbeatCycle() {
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
				// if logIndexToAppend >= len(rf.log) {
				// 	log.Println("Server", rf.me, "sending empty heartbeat to", index, "server")
				// 	go rf.sendHeartBeat(rf.me, index, rf.currentTerm, logIndexToAppend, rf.commitIndex)
				// } else {
				log.Println("Leader server", rf.me, "last index for", index, "server is ", logIndexToAppend)
				// log.Println(rf.log)
				go rf.sendEntries(rf.me, index, rf.currentTerm, logIndexToAppend-1, rf.log[logIndexToAppend-1].Term, rf.commitIndex, rf.log[logIndexToAppend:])
				//}
			}
		}

		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

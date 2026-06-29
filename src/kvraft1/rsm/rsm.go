package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Req any
	Id  int
	Me  int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type Key struct {
	Id    int
	Me    int
	Index int
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	results map[Key]chan any
	id      int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		results:      make(map[Key]chan any, 0),
	}

	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}

	go rsm.Read()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	// your code here
	rsm.mu.Lock()
	op := Op{Req: req, Id: rsm.id, Me: rsm.me}
	rsm.id++

	index, initialTerm, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	responseChan := make(chan any, 1)
	channelKey := rsm.constructKey(op, index)
	rsm.results[channelKey] = responseChan
	rsm.mu.Unlock()
	DPrintln("RSM", rsm.me, "submitted op", op.Id, "at index", index)
	defer func() {
		rsm.mu.Lock()
		delete(rsm.results, channelKey)
		rsm.mu.Unlock()
	}()
	for {
		select {
		case result := <-responseChan:
			return rpc.OK, result
		case <-time.After(20 * time.Millisecond):
			currentTerm, isLeader := rsm.rf.GetState()
			if !isLeader || initialTerm != currentTerm {
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

func (rsm *RSM) Read() {
	for result := range rsm.applyCh {
		if result.CommandValid {
			rsm.applyCommand(result)
		} else {
			DPrintln("RSM", rsm.me, "restoring snapshot at index", result.SnapshotIndex)
			rsm.sm.Restore(result.Snapshot)
		}
	}
}

func (rsm *RSM) applyCommand(message raftapi.ApplyMsg) {
	op := message.Command.(Op)
	opResult := rsm.sm.DoOp(op.Req)
	DPrintln("RSM", rsm.me, "applied op", op.Id, "from", op.Me, "at index", message.CommandIndex)

	rsm.mu.Lock()
	channel, exists := rsm.results[rsm.constructKey(op, message.CommandIndex)]
	rsm.mu.Unlock()

	if exists {
		DPrintln("RSM", rsm.me, "delivering result for op", op.Id, "at index", message.CommandIndex)
		channel <- opResult
	}

	rsm.maybeSnapshot(message.CommandIndex)
}

func (rsm *RSM) maybeSnapshot(commandIndex int) {
	if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
		smSnapshot := rsm.sm.Snapshot()
		rsm.rf.Snapshot(commandIndex, smSnapshot)
	}
}

func (rsm *RSM) constructKey(op Op, index int) Key {
	return Key{
		Id:    op.Id,
		Me:    op.Me,
		Index: index,
	}
}

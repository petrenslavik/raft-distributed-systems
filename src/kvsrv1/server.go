package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Data struct {
	version rpc.Tversion
	value   string
}

type KVServer struct {
	mu       sync.Mutex
	keyValue map[string]Data
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		keyValue: map[string]Data{},
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data, exists := kv.keyValue[args.Key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = data.value
	reply.Version = data.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	data, exists := kv.keyValue[args.Key]
	if !exists {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}

		kv.keyValue[args.Key] = Data{
			version: args.Version + 1,
			value:   args.Value,
		}
		reply.Err = rpc.OK
		return
	}

	if args.Version != data.version {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.keyValue[args.Key] = Data{
		version: args.Version + 1,
		value:   args.Value,
	}
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}

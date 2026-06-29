package kvraft

import (
	"bytes"
	"log"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type Data struct {
	Version rpc.Tversion
	Value   string
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	keyValue map[string]Data
}

// ===== State Machine =====

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15

type Put struct {
	Key     string
	Value   string
	Version rpc.Tversion
}

type PutResponse struct {
	Err rpc.Err
}

type Get struct {
	Key string
}

type GetResponse struct {
	Value   string
	Version rpc.Tversion
	Err     rpc.Err
}

func (kv *KVServer) DoOp(req any) any {
	switch casted := req.(type) {
	case Put:
		data, exists := kv.keyValue[casted.Key]
		if !exists {
			if casted.Version != 0 {
				return PutResponse{Err: rpc.ErrNoKey}
			}

			kv.keyValue[casted.Key] = Data{
				Version: casted.Version + 1,
				Value:   casted.Value,
			}
			return PutResponse{Err: rpc.OK}
		}

		if casted.Version != data.Version {
			return PutResponse{Err: rpc.ErrVersion}
		}

		kv.keyValue[casted.Key] = Data{
			Version: casted.Version + 1,
			Value:   casted.Value,
		}
		return PutResponse{Err: rpc.OK}
	case Get:
		data, exists := kv.keyValue[casted.Key]

		if !exists {
			return GetResponse{Err: rpc.ErrNoKey}
		}

		return GetResponse{Value: data.Value, Version: data.Version, Err: rpc.OK}
	default:
		log.Fatalf("DoOp should execute only Put and not %T", req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	//log.Printf("%d: snapshot", rs.me)
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.keyValue)
	return buffer.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	if decoder.Decode(&kv.keyValue) != nil {
		log.Fatalf("Server %v couldn't decode keyValue map", kv.me)
	}
}

// ===== Server logic =====

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, getResponse := kv.rsm.Submit(Get{
		Key: args.Key,
	})

	reply.Err = err
	if err == rpc.OK {
		casted := getResponse.(GetResponse)
		reply.Err = casted.Err
		reply.Value = casted.Value
		reply.Version = casted.Version
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, putResponse := kv.rsm.Submit(Put{
		Key:     args.Key,
		Version: args.Version,
		Value:   args.Value,
	})

	reply.Err = err
	if err == rpc.OK {
		casted := putResponse.(PutResponse)
		reply.Err = casted.Err
	}
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(Put{})
	labgob.Register(Get{})

	kv := &KVServer{
		me:       me,
		keyValue: map[string]Data{},
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}

package shardgrp

import (
	"bytes"
	"log"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type Data struct {
	Version rpc.Tversion
	Value   string
}

type ShardState int

const (
	DontOwnState ShardState = iota
	FrozenState
	WorkingState
)

type Config struct {
	State         ShardState
	ConfigVersion shardcfg.Tnum
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	shardConfiguration map[shardcfg.Tshid]Config
	keyValue           map[shardcfg.Tshid]map[string]Data
}

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

type FreezeShard struct {
	ShardId       shardcfg.Tshid
	ConfigVersion shardcfg.Tnum
}

type FreezeShardResponse struct {
	State []byte
	Num   shardcfg.Tnum
	Err   rpc.Err
}

type InstallShard struct {
	ShardId       shardcfg.Tshid
	State         []byte
	ConfigVersion shardcfg.Tnum
}

type InstallShardResponse struct {
	Err rpc.Err
}

type DeleteShard struct {
	ShardId       shardcfg.Tshid
	ConfigVersion shardcfg.Tnum
}

type DeleteShardResponse struct {
	Err rpc.Err
}

func (kv *KVServer) DoOp(req any) any {
	switch casted := req.(type) {
	case Put:
		shardId := shardcfg.Key2Shard(casted.Key)
		config := kv.shardConfiguration[shardId]
		if config.State == DontOwnState || config.State == FrozenState {
			return PutResponse{Err: rpc.ErrWrongGroup}
		}
		shardMap, exists := kv.keyValue[shardId]
		if !exists {
			shardMap = make(map[string]Data)
			kv.keyValue[shardId] = shardMap
		}

		data, exists := shardMap[casted.Key]
		if !exists {
			if casted.Version != 0 {
				return PutResponse{Err: rpc.ErrNoKey}
			}

			shardMap[casted.Key] = Data{
				Version: casted.Version + 1,
				Value:   casted.Value,
			}
			return PutResponse{Err: rpc.OK}
		}

		if casted.Version != data.Version {
			return PutResponse{Err: rpc.ErrVersion}
		}

		shardMap[casted.Key] = Data{
			Version: casted.Version + 1,
			Value:   casted.Value,
		}
		return PutResponse{Err: rpc.OK}
	case Get:
		shardId := shardcfg.Key2Shard(casted.Key)
		config := kv.shardConfiguration[shardId]
		if config.State == DontOwnState || config.State == FrozenState {
			return GetResponse{Err: rpc.ErrWrongGroup}
		}

		shardMap, exists := kv.keyValue[shardId]
		if !exists {
			return GetResponse{Err: rpc.ErrNoKey}
		}

		data, exists := shardMap[casted.Key]
		if !exists {
			return GetResponse{Err: rpc.ErrNoKey}
		}

		return GetResponse{Value: data.Value, Version: data.Version, Err: rpc.OK}
	case FreezeShard:
		config := kv.shardConfiguration[casted.ShardId]
		if config.ConfigVersion > casted.ConfigVersion {
			return FreezeShardResponse{
				Num: config.ConfigVersion,
				Err: rpc.ErrVersion,
			}
		}

		if config.State == DontOwnState {
			return FreezeShardResponse{
				Num: config.ConfigVersion,
				Err: rpc.ErrVersion,
			}
		}

		kv.shardConfiguration[casted.ShardId] = Config{
			ConfigVersion: casted.ConfigVersion,
			State:         FrozenState,
		}

		shardMap := kv.keyValue[casted.ShardId]
		buffer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(buffer)
		encoder.Encode(shardMap)
		state := buffer.Bytes()

		return FreezeShardResponse{
			State: state,
			Num:   casted.ConfigVersion,
			Err:   rpc.OK,
		}
	case InstallShard:
		config := kv.shardConfiguration[casted.ShardId]
		if config.ConfigVersion >= casted.ConfigVersion {
			return InstallShardResponse{
				Err: rpc.ErrVersion,
			}
		}

		kv.shardConfiguration[casted.ShardId] = Config{
			ConfigVersion: casted.ConfigVersion,
			State:         WorkingState,
		}

		var shardMap map[string]Data
		buffer := bytes.NewBuffer(casted.State)
		decoder := labgob.NewDecoder(buffer)
		decoder.Decode(&shardMap)
		//DPrintf("Server %v-%v shard %v map is %+v\n", kv.gid, kv.me, casted.ShardId, shardMap)
		kv.keyValue[casted.ShardId] = shardMap

		return InstallShardResponse{
			Err: rpc.OK,
		}
	case DeleteShard:
		config := kv.shardConfiguration[casted.ShardId]
		if config.ConfigVersion > casted.ConfigVersion {
			return DeleteShardResponse{
				Err: rpc.ErrVersion,
			}
		}

		kv.shardConfiguration[casted.ShardId] = Config{
			ConfigVersion: casted.ConfigVersion,
			State:         DontOwnState,
		}

		delete(kv.keyValue, casted.ShardId)
		//DPrintf("Server %v-%v DeleteShard %v. KeyValue map is %+v\n", kv.gid, kv.me, casted.ShardId, kv.keyValue)
		return DeleteShardResponse{
			Err: rpc.OK,
		}
	default:
		log.Fatalf("DoOp should execute only Put and not %T", req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.keyValue)
	encoder.Encode(kv.shardConfiguration)
	return buffer.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	if decoder.Decode(&kv.keyValue) != nil {
		log.Fatalf("Server %v couldn't decode keyValue map", kv.me)
	}
	if decoder.Decode(&kv.shardConfiguration) != nil {
		log.Fatalf("Server %v couldn't decode shardConfiguration map", kv.me)
	}
}

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
	err, putResponse := kv.rsm.Submit(Put{
		Key:     args.Key,
		Version: args.Version,
		Value:   args.Value,
	})

	reply.Err = err
	if err == rpc.OK {
		casted := putResponse.(PutResponse)
		reply.Err = casted.Err
		DPrintf("Server %v-%v put response was %#v\n", kv.gid, kv.me, reply)
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, freezeShardResponse := kv.rsm.Submit(FreezeShard{
		ConfigVersion: args.Num,
		ShardId:       args.Shard,
	})

	reply.Err = err
	if err == rpc.OK {
		casted := freezeShardResponse.(FreezeShardResponse)
		reply.Err = casted.Err
		reply.Num = casted.Num
		reply.State = casted.State
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, freezeShardResponse := kv.rsm.Submit(InstallShard{
		ConfigVersion: args.Num,
		ShardId:       args.Shard,
		State:         args.State,
	})

	reply.Err = err
	if err == rpc.OK {
		casted := freezeShardResponse.(InstallShardResponse)
		reply.Err = casted.Err
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, freezeShardResponse := kv.rsm.Submit(DeleteShard{
		ConfigVersion: args.Num,
		ShardId:       args.Shard,
	})

	reply.Err = err
	if err == rpc.OK {
		casted := freezeShardResponse.(DeleteShardResponse)
		reply.Err = casted.Err
	}
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(Put{})
	labgob.Register(Get{})
	labgob.Register(FreezeShard{})
	labgob.Register(InstallShard{})
	labgob.Register(DeleteShard{})

	kv := &KVServer{
		gid:                gid,
		me:                 me,
		keyValue:           map[shardcfg.Tshid]map[string]Data{},
		shardConfiguration: map[shardcfg.Tshid]Config{},
	}

	if gid == shardcfg.Gid1 {
		for shard := range shardcfg.NShards {
			kv.shardConfiguration[shardcfg.Tshid(shard)] = Config{
				ConfigVersion: shardcfg.NumFirst,
				State:         WorkingState,
			}
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}

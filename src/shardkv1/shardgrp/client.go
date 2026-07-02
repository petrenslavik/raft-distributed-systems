package shardgrp

import (
	"context"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ErrorNoResponse rpc.Err = "ErrorNoResponse"
	RetryDelay              = 20 * time.Millisecond
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{
		Key: key,
	}
	reply := rpc.GetReply{}

	err, _ := ck.loop(func() (rpc.Err, bool) {
		ok := ck.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		return reply.Err, ok
	})

	if err == ErrorNoResponse {
		return "", 0, ErrorNoResponse
	}

	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := rpc.PutReply{}

	err, isFirstTry := ck.loop(func() (rpc.Err, bool) {
		ok := ck.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		return reply.Err, ok
	})

	if err == ErrorNoResponse {
		return ErrorNoResponse
	}

	if !isFirstTry && reply.Err == rpc.ErrVersion {
		return rpc.ErrMaybe
	}

	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	reply := shardrpc.FreezeShardReply{}

	err, _ := ck.loop(func() (rpc.Err, bool) {
		ok := ck.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)
		return reply.Err, ok
	})

	if err == ErrorNoResponse {
		return nil, ErrorNoResponse
	}

	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	reply := shardrpc.InstallShardReply{}

	err, _ := ck.loop(func() (rpc.Err, bool) {
		ok := ck.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)
		return reply.Err, ok
	})

	if err == ErrorNoResponse {
		return ErrorNoResponse
	}

	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	reply := shardrpc.DeleteShardReply{}

	err, _ := ck.loop(func() (rpc.Err, bool) {
		ok := ck.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)
		return reply.Err, ok
	})

	if err == ErrorNoResponse {
		return ErrorNoResponse
	}

	return reply.Err
}

func (ck *Clerk) loop(call func() (rpc.Err, bool)) (rpc.Err, bool) {
	isFirstTry := true
	initialServer := ck.leader
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	for {
		err, ok := call()
		if !ok || err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			isFirstTry = false

			if err == rpc.ErrWrongLeader {
				initialServer = ck.leader
			} else if initialServer == ck.leader {
				return ErrorNoResponse, false
			}

			time.Sleep(RetryDelay)
		} else {
			break
		}

		select {
		case <-ctx.Done():
			return ErrorNoResponse, isFirstTry
		default:
			continue
		}
	}
	// DPrintf("Client.Put result is %v is first try %v args were %+v", reply.Err, isFirstTry, args)
	return rpc.OK, isFirstTry
}

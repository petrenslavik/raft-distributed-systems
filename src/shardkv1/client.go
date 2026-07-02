package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

const (
	RetryDelay = 20 * time.Millisecond
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	for {
		cfg := ck.sck.Query()
		shardId := shardcfg.Key2Shard(key)
		groupId, servers, _ := cfg.GidServers(shardId)
		shardGroupClient := shardgrp.MakeClerk(ck.clnt, servers)
		ck.rcks[groupId] = shardGroupClient
		// DPrintln("Client.Get getting key", key, "shard", shardId)
		value, version, err := shardGroupClient.Get(key)
		DPrintln("Client.Get received key", key, "shard", shardId, "value", value, "version", version, "response", err)
		//DPrintln("Client.Put config received")
		if err == rpc.ErrWrongGroup || err == shardgrp.ErrorNoResponse {
			time.Sleep(RetryDelay)
			continue
		}
		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	var isFirstTry = true
	for {
		cfg := ck.sck.Query()
		groupId, servers, _ := cfg.GidServers(shardcfg.Key2Shard(key))
		shardGroupClient := shardgrp.MakeClerk(ck.clnt, servers)
		ck.rcks[groupId] = shardGroupClient
		DPrintln("Client.Put sending key", key, "value", value, "version", version)
		err := shardGroupClient.Put(key, value, version)
		DPrintln("Client.Put received key", key, "value", value, "version", version, "response", err)

		if err == rpc.ErrWrongGroup || err == shardgrp.ErrorNoResponse {
			isFirstTry = false
			time.Sleep(RetryDelay)
			continue
		}

		if err == rpc.ErrVersion && !isFirstTry {
			return rpc.ErrMaybe
		}
		return err
	}
}

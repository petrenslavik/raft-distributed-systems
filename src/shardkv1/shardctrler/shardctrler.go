package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	ConfigKey      = "Config"
	ApplyConfigKey = "ApplyConfig"
	LockKey        = "Lock"
	LockAvailable  = "Available"
	RetryDelay     = 20 * time.Millisecond
	LockDuration   = 1 * time.Second
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	id             string
	currentVersion rpc.Tversion
	lockVersion    rpc.Tversion
	wasLeader      bool
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt, id: kvtest.RandValue(8)}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

func (sck *ShardCtrler) Id() string {
	return sck.id
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	defer func() {
		DPrintln(sck.id, "finished init.")
	}()

	DPrintln(sck.id, "Change configuration from init.")
	sck.changeConfiguration()
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	sck.Put(ConfigKey, cfg.String(), sck.currentVersion)
	sck.Put(ApplyConfigKey, cfg.String(), sck.currentVersion)
	sck.currentVersion++
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	sck.saveApplyConfig(new)
	sck.changeConfiguration()
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	val, version, _ := sck.IKVClerk.Get(ConfigKey)
	sck.currentVersion = version
	cfg := shardcfg.FromString(val)
	return cfg
}

func (sck *ShardCtrler) GetConfigToApply() *shardcfg.ShardConfig {
	val, version, _ := sck.IKVClerk.Get(ApplyConfigKey)
	sck.currentVersion = version
	cfg := shardcfg.FromString(val)
	return cfg
}

func (sck *ShardCtrler) saveApplyConfig(new *shardcfg.ShardConfig) *shardcfg.ShardConfig {
	DPrintln(sck.id, "Got new configuration to apply", new.Num)
	for {
		savedConfigStr, saveConfigVersion, err := sck.IKVClerk.Get(ApplyConfigKey)
		savedConfig := shardcfg.FromString(savedConfigStr)
		if savedConfig.Num > new.Num {
			DPrintln(sck.id, "New configuration", new.Num, "is older than saved", savedConfig.Num)
			return savedConfig
		}

		if savedConfig.Num == new.Num {
			DPrintln(sck.id, "Saved configuration", new.Num, "has the same version, pulling this config")
			return savedConfig
		}

		DPrintln(sck.id, "Saving new configuration", new.Num)
		err = sck.IKVClerk.Put(ApplyConfigKey, new.String(), saveConfigVersion)
		if err == rpc.OK {
			DPrintln(sck.id, "New configuration", new.Num, "saved.")
			return new
		}
		DPrintln(sck.id, "New configuration", new.Num, "not saved, response was ", err)
		time.Sleep(RetryDelay)
	}
}

func (sck *ShardCtrler) changeConfiguration() {
	for {
		DPrintln(sck.id, "Trying to get lock", sck.lockVersion)
		isAcquiredLock, canGetLeadership := sck.acquireLock()
		if !canGetLeadership {
			return
		}
		if isAcquiredLock {
			DPrintln(sck.id, "Lock acquired", sck.currentVersion, "was leader is now", sck.wasLeader)
			break
		}
		DPrintln(sck.id, "Waiting to reacquire")
		time.Sleep(LockDuration)
	}

	defer sck.releaseLock()
	oldCfg := sck.Query()
	newCfg := sck.GetConfigToApply()
	DPrintln(sck.id, "Shard configuration", oldCfg.Num, "->", newCfg.Num)

	if oldCfg.Num >= newCfg.Num {
		return
	}

	DPrintln(sck.id, "Changing shard configuration", oldCfg.Num, "->", newCfg.Num)
	for index, groupId := range oldCfg.Shards {
		shardId := shardcfg.Tshid(index)
		newGroupId := newCfg.Shards[shardId]
		if groupId != newGroupId {
			DPrintln(sck.id, "Moving shard", shardId, "from group", groupId, "to", newGroupId)
			sck.moveShardToGroup(shardId, newCfg.Num, oldCfg.Groups[groupId], newCfg.Groups[newGroupId])
		}
		isAcquiredLock, canGetLeadership := sck.acquireLock()
		if !isAcquiredLock || !canGetLeadership {
			return
		}
	}

	DPrintln(sck.id, "Shard configuration updated, saving config", oldCfg.Num, "->", newCfg.Num)
	for {
		currentConfig := sck.Query()
		if currentConfig.Num >= newCfg.Num {
			DPrintln(sck.id, "Shard configuration was already updated", currentConfig.Num, "->", newCfg.Num)
			return
		}
		err := sck.IKVClerk.Put(ConfigKey, newCfg.String(), sck.currentVersion)
		if err == rpc.OK {
			sck.currentVersion++
			DPrintln(sck.id, "Shard configuration", oldCfg.Num, "->", newCfg.Num, "saved")
			return
		}
		DPrintln(sck.id, "Shard configuration", oldCfg.Num, "->", newCfg.Num, "wasnt saved", err)
	}
}

func (sck *ShardCtrler) moveShardToGroup(shardId shardcfg.Tshid, configVersion shardcfg.Tnum, oldClusterServers, newClusterServers []string) {
	oldClerk := shardgrp.MakeClerk(sck.clnt, oldClusterServers)
	newClerk := shardgrp.MakeClerk(sck.clnt, newClusterServers)

	var shardState []byte
	for {
		res, err := oldClerk.FreezeShard(shardId, configVersion)
		DPrintln(sck.id, "Freeeze", shardId, "response", err)
		if err == shardgrp.ErrorNoResponse {
			isAcquiredLock, canGetLeadership := sck.acquireLock()
			if !isAcquiredLock || !canGetLeadership {
				return
			}
			time.Sleep(RetryDelay)
			continue
		}
		shardState = res
		break
	}

	for {
		err := newClerk.InstallShard(shardId, shardState, configVersion)
		DPrintln(sck.id, "Install", shardId, "response", err)
		if err == shardgrp.ErrorNoResponse {
			isAcquiredLock, canGetLeadership := sck.acquireLock()
			if !isAcquiredLock || !canGetLeadership {
				return
			}
			time.Sleep(RetryDelay)
			continue
		}
		break
	}

	for {
		err := oldClerk.DeleteShard(shardId, configVersion)
		DPrintln(sck.id, "Delete", shardId, "response", err)
		if err == shardgrp.ErrorNoResponse {
			isAcquiredLock, canGetLeadership := sck.acquireLock()
			if !isAcquiredLock || !canGetLeadership {
				return
			}
			time.Sleep(RetryDelay)
			continue
		}
		break
	}
}

func (sck *ShardCtrler) acquireLock() (bool, bool) {
	for {
		switch sck.Put(LockKey, sck.id, sck.lockVersion) {
		case rpc.OK:
			sck.lockVersion++
			sck.wasLeader = true
			return true, true
		case rpc.ErrVersion, rpc.ErrMaybe:
			state, version, _ := sck.Get(LockKey)
			switch state {
			case sck.id:
				sck.lockVersion = version + 1
				sck.wasLeader = true
				return true, true
			case LockAvailable:
				if sck.wasLeader {
					return false, false
				}
				sck.lockVersion = version
				time.Sleep(RetryDelay)
				continue
			default:
				if sck.wasLeader {
					return false, false
				}
				sck.lockVersion = version
				return false, true
			}
		}
	}
}

func (sck *ShardCtrler) releaseLock() {
	sck.Put(LockKey, LockAvailable, sck.lockVersion)
	sck.lockVersion++
}

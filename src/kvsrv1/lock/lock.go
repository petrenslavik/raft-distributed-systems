package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const (
	availableState = "Available"
	retryInterval  = 20 * time.Millisecond
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockname string
	id       string
	version  rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockname: lockname,
		id:       kvtest.RandValue(8),
	}

	return lk
}

func (lk *Lock) resync() bool {
	state, version, _ := lk.ck.Get(lk.lockname)
	switch state {
	case lk.id:
		lk.version = version + 1
		return true
	case availableState:
		lk.version = version
	default:
		lk.version = version + 1
	}
	return false
}

func (lk *Lock) Acquire() {
	for {
		switch lk.ck.Put(lk.lockname, lk.id, lk.version) {
		case rpc.OK:
			lk.version++
			return
		case rpc.ErrVersion, rpc.ErrMaybe:
			if lk.resync() {
				return
			}
			time.Sleep(retryInterval)
		}
	}
}

func (lk *Lock) Release() {
	lk.ck.Put(lk.lockname, availableState, lk.version)
	lk.version++
}

package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockKey  string
	holderID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockKey:  l,
		holderID: kvtest.RandValue(8),
	}
	return lk
}

// Acquire 实现阻塞式获取锁
// 必须持续尝试直到成功获得锁
func (lk *Lock) Acquire() {
	for {
		value, version, rpcErr := lk.ck.Get(lk.lockKey)
		// 锁暂未被持有
		if rpcErr == rpc.ErrNoKey {
			err := lk.ck.Put(lk.lockKey, lk.holderID, 0)
			if err == rpc.OK {
				return
			}
			continue
		} else if value == "" {
			err := lk.ck.Put(lk.lockKey, lk.holderID, version)
			if err == rpc.OK {
				return
			}
			continue
		}
		if value == lk.holderID {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

// Release 释放当前持有的锁
// 必须确保只有锁的持有者能调用该方法
func (lk *Lock) Release() {
	value, version, rpcErr := lk.ck.Get(lk.lockKey)
	if rpcErr == rpc.ErrNoKey {
		return
	}
	// 只由持有者释放
	if value != lk.holderID {
		return
	}
	lk.ck.Put(lk.lockKey, "", version)
}

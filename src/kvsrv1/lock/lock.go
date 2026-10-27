package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	ClientID string
	Name     string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	lk.ClientID = kvtest.RandValue(8)
	lk.Name = lockname
	return lk
}

func (lk *Lock) Acquire() {

	for {
		clientID, version, err := lk.ck.Get(lk.Name)
		if err != rpc.OK {
			if err == rpc.ErrNoKey {
				err = lk.ck.Put(lk.Name, lk.ClientID, 0)
				if err != rpc.OK {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				return
			}
		}
		switch {
		case clientID == "":
			{
				err = lk.ck.Put(lk.Name, lk.ClientID, version)
				if err != rpc.OK {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				return
			}
		case clientID == lk.ClientID:
			{
				return
			}
		case clientID != lk.ClientID:
			{
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
	}
}

func (lk *Lock) Release() {
	for {
		clientID, version, err := lk.ck.Get(lk.Name)
		if err != rpc.OK {
			return
		}
		switch clientID {
		case lk.ClientID:
			{
				err = lk.ck.Put(lk.Name, "", version)
				if err != rpc.OK {
					continue
				}
			}
		default:
			{
				return
			}
		}
	}
}

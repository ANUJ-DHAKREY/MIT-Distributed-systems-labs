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

type entry struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]entry
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu:   sync.Mutex{},
		data: make(map[string]entry),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// fmt.Println("3")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	existingVal, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	// fmt.Println("4")
	*reply = rpc.GetReply{
		Err:     rpc.OK,
		Value:   existingVal.value,
		Version: existingVal.version,
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	existingVal, ok := kv.data[args.Key]
	if !ok {
		if args.Version == 0 {
			entry := entry{
				value:   args.Value,
				version: 1,
			}
			kv.data[args.Key] = entry
			*reply = rpc.PutReply{
				Err: rpc.OK,
			}
			return
		}
		*reply = rpc.PutReply{
			Err: rpc.ErrNoKey,
		}
		return
	}

	if existingVal.version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}
	existingVal.value = args.Value
	existingVal.version = args.Version + 1
	kv.data[args.Key] = existingVal
	*reply = rpc.PutReply{
		Err: rpc.OK,
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}

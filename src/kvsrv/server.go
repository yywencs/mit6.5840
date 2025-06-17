package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	data     map[string]string
	Requests map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key

	kv.mu.Lock()
	defer kv.mu.Unlock()

	value := kv.data[key]
	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	value := args.Value
	ID := args.ArgId
	argType := args.ArgType

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if argType == Request {
		_, exist := kv.Requests[ID]
		if exist {
			return
		}
		kv.data[key] = value
		kv.Requests[ID] = value
	} else {
		delete(kv.Requests, ID)
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key
	value := args.Value
	ID := args.ArgId
	argType := args.ArgType

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if argType == Request {
		oldValue, exist := kv.Requests[ID]
		if exist {
			reply.Value = oldValue
			return
		}
		oldValue = kv.data[key]
		kv.data[key] = oldValue + value
		reply.Value = oldValue
		kv.Requests[ID] = oldValue
	} else if args.ArgType == Report {
		delete(kv.Requests, ID)
		return
	}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.Requests = make(map[int64]string)
	// You may need initialization code here.

	return kv
}

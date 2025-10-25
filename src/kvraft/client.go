package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LeaderId  int
	ClientId  int64
	CommandId int
}

const retryInterval = 100 * time.Millisecond

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.LeaderId = 0
	ck.ClientId = nrand()
	ck.CommandId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key, ck.ClientId, ck.CommandId}
	value := ""

	for {
		reply := GetReply{}
		fmt.Println("send Get")
		if !ck.servers[ck.LeaderId].Call("KVServer.Get", &args, &reply) || reply.Err != OK {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			fmt.Println("Get reply: ", reply)
			time.Sleep(retryInterval)
			continue
		}
		value = reply.Value
		fmt.Println("Get reply: ", reply)
		break
	}
	ck.CommandId += 1

	return value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, ClientId: ck.ClientId, CommandId: ck.CommandId}

	for {
		reply := PutAppendReply{}
		fmt.Println("send PutAppend!")
		if !ck.servers[ck.LeaderId].Call("KVServer."+op, &args, &reply) || reply.Err != OK {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			fmt.Println("PutAppend reply: ", reply)
			time.Sleep(retryInterval)
			continue
		}
		fmt.Println("PutAppend reply: ", reply)
		break
	}
	ck.CommandId += 1
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

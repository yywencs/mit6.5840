package kvraft

import "time"

const sendOpInterval = 300 * time.Millisecond

type OperationType string

const (
	GetOp    = "GetOp"
	PutOp    = "PutOp"
	AppendOp = "AppendOp"
	NoOp     = "NoOp"
)

type Op struct {
	OpType    OperationType
	Key       string
	Value     string
	ClientId  int64
	CommandId int
}

func (kv *KVServer) sendNoOp() {
	for !kv.killed() {
		if kv.isLeader() {
			command := Op{OpType: NoOp}
			kv.submitToRaft(&command)
		}
		time.Sleep(sendOpInterval)
	}
}

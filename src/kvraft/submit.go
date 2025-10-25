package kvraft

import (
	"fmt"
)

func (kv *KVServer) waitForSubmit(command *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		return ErrWrongLeader, ""
	}

	if !kv.isApplied(command) {
		kv.submitToRaft(command)
		fmt.Println("submitToraft")
		if kv.successOrTimeout(command.ClientId) == ErrNotApplied {
			fmt.Println("time out")
			return ErrNotApplied, ""
		}
	}

	if kv.isApplied(command) {
		fmt.Println("isApplied")
		if command.OpType == GetOp {
			return kv.processOp(command)
		}
		return OK, ""
	}
	return ErrNotApplied, ""
}

func (kv *KVServer) successOrTimeout(index int64) {
	ch := make(chan interface{}, 1)
	kv.waitCh[index] = ch
	defer delete(kv.waitCh, index)

}

func (kv *KVServer) processOp(command *Op) (Err, string) {
	if value, ok := kv.data[command.Key]; ok {
		return OK, value
	} else {
		return ErrNoKey, ""
	}
}

package kvraft

import "fmt"

func (kv *KVServer) recivedApplyChan() {
	for ch := range kv.applyCh {
		if kv.killed() {
			break
		}
		kv.mu.Lock()

		if ch.CommandValid {
			command := ch.Command.(Op)

			if command.OpType == NoOp {
				continue
			}
			fmt.Println("from ch", ch)

			if !kv.isApplied(&command) {
				kv.lastCommand[command.ClientId] = command.CommandId
				kv.addTodata(&command)
				kv.notify(&command)
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) notify(command *Op) {
	index := command.ClientId
	kv.waitCh[index] <- true
}

func (kv *KVServer) addTodata(command *Op) {
	if command.OpType == GetOp {
		return
	}
	if command.OpType == PutOp {
		kv.data[command.Key] = command.Value
	} else if command.OpType == AppendOp {
		kv.data[command.Key] += command.Value
	}
}

func (kv *KVServer) submitToRaft(command *Op) (int, bool) {
	fmt.Println("submit: ", command)
	index, _, isLeader := kv.rf.Start(*command)
	return index, isLeader
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *KVServer) isApplied(command *Op) bool {
	clientId, commandId := command.ClientId, command.CommandId
	last, ok := kv.lastCommand[clientId]
	return ok && commandId >= last
}

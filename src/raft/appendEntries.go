package raft

import (
	"sort"
)

func (rf *Raft) requestAppendEntries() {
	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}

		go func(peerId int) {
			for {
				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}

				next := rf.nextIndex[peerId]

				// if next <= rf.matchIndex[peerId] {
				// 	next = rf.matchIndex[peerId] + 1
				// }

				DPrintf("S%d send append to S%d\n", rf.me, peerId)
				if next <= rf.lastIncludedIndex {
					rf.mu.Unlock()
					rf.InstallSnapshot(peerId)
					return
				}

				idx := next - rf.lastIncludedIndex
				lastLogIndex, _ := rf.getLastLog()
				containEntries := lastLogIndex >= next

				entries := make([]LogEntry, len(rf.logs[idx:]))
				if containEntries {
					copy(entries, rf.logs[idx:])
					DPrintf("%v Send AppendEntries to S%d, next is %d, entries is %v\n", rf, peerId, next, entries)
				}
				prevLogIndex := next - 1
				prevLogTerm := rf.logs[idx-1].Term
				args := AppendEnrtiesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex}
				reply := AppendEnrtiesReply{}

				rf.mu.Unlock()
				if !rf.sendAppendEntries(peerId, &args, &reply) {
					return
				}

				rf.mu.Lock()

				if args.Term != rf.currentTerm || rf.state != LEADER {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					rf.changeState(FOLLOWER)
					rf.currentTerm = reply.Term
					rf.persist(nil)
					rf.mu.Unlock()
					return
				}

				if reply.Success {

					if containEntries {
						rf.matchIndex[peerId] = prevLogIndex + len(entries)
						rf.nextIndex[peerId] = prevLogIndex + len(entries) + 1
						indexArr := make([]int, len(rf.peers))
						copy(indexArr, rf.matchIndex)
						DPrintf("S%d: %v\n", rf.me, indexArr)
						sort.Ints(indexArr)
						newCommitIndex := indexArr[(len(indexArr)-1)/2]
						if newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex-rf.lastIncludedIndex].Term == rf.currentTerm {
							rf.commitIndex = newCommitIndex
							rf.applyCond.Signal()
						}
					}
					rf.mu.Unlock()
					return
				} else {
					if reply.XTerm == -1 {
						rf.nextIndex[peerId] = reply.XIndex
					} else {
						lastIndexOfXTerm := rf.findLastIndexOfTerm(reply.XTerm)
						if lastIndexOfXTerm == -1 {
							rf.nextIndex[peerId] = reply.XIndex
						} else {
							rf.nextIndex[peerId] = lastIndexOfXTerm + 1
						}
					}
					rf.mu.Unlock()
				}
			}

		}(peerId)
	}
}

func (rf *Raft) HandleAppendEntries(args *AppendEnrtiesArgs, reply *AppendEnrtiesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.XTerm = -1
	reply.XIndex = -1
	reply.Xlen = -1

	if args.Term < rf.currentTerm {
		return
	}

	rf.resetElectionTimer()
	DPrintf("S%d Get HeartBeat from S%d\n", rf.me, args.LeaderID)

	if args.Term == rf.currentTerm && rf.state == CANDIDATE {
		rf.changeState(FOLLOWER)
		rf.persist(nil)
	}

	if args.Term > rf.currentTerm {
		rf.changeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.persist(nil)
		reply.Term = rf.currentTerm
	}

	lastLogIndex, _ := rf.getLastLog()

	if lastLogIndex < args.PrevLogIndex {
		reply.XIndex = lastLogIndex + 1
		DPrintf("%v lastLogIndex < args.PrevLogIndex from S%d, return", rf, args.LeaderID)
		return
	}

	idx := args.PrevLogIndex - rf.lastIncludedIndex
	if idx > 0 && rf.logs[idx].Term != args.PrevLogTerm {
		reply.XTerm = rf.logs[idx].Term
		for i := idx; i > 0; i-- {
			if rf.logs[i].Term != reply.XTerm {
				reply.XIndex = rf.logs[i].Index
				break
			}
		}
		DPrintf("%v rf.logs[idx].Term != args.PrevLogTerm from S%d, return", rf, args.LeaderID)
		return
	}

	if len(args.Entries) > 0 {
		// DPrintf("%v, idx: %d,rf.logs: %v, entries: %v\n", rf, idx, rf.logs, args.Entries)

		i, j := idx+1, 0
		if i < 0 {
			i, j = 1, rf.lastIncludedIndex-args.PrevLogIndex
		}
		for i < len(rf.logs) && j < len(args.Entries) && rf.logs[i].Term == args.Entries[j].Term {
			i += 1
			j += 1
		}
		if j < len(args.Entries) {
			newLogs := make([]LogEntry, i+len(args.Entries[j:]))
			copy(newLogs[:i], rf.logs[:i])
			copy(newLogs[i:], args.Entries[j:])
			rf.logs = newLogs
			rf.persist(nil)
			DPrintf("%v appends log entries: %v", rf, args.Entries[j:])
		}

	}

	reply.Success = true

	newCommitIndex := min(args.LeaderCommit, rf.logs[len(rf.logs)-1].Index)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}

}

func (rf *Raft) applySubmit() {

	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		applyMsgs := []ApplyMsg{}

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entry := rf.logs[i-rf.lastIncludedIndex]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			applyMsgs = append(applyMsgs, applyMsg)
			rf.lastApplied += 1
		}

		DPrintf("%v applies log up to index=%d", rf, rf.lastApplied)

		rf.mu.Unlock()

		for _, msg := range applyMsgs {
			rf.mu.Lock()
			if msg.CommandIndex <= rf.lastIncludedIndex {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
		}
	}

}

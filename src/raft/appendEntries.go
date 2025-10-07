package raft

import (
	"sort"
	"time"
)

func (rf *Raft) sendAppendEntries(server int, args *AppendEnrtiesRequest, reply *AppendEnrtiesResponse) bool {
	done := make(chan bool, 1)

	go func() {
		done <- rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	}()

	select {
	case ok := <-done:
		return ok
	case <-time.After(RPC_TIMEOUT): // 例如100ms
		return false
	}
}

func (rf *Raft) requestAppendEntries() {
	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}

		go func(peerId int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				next := rf.nextIndex[peerId]

				if next <= rf.matchIndex[peerId] {
					rf.mu.Unlock()
					return
				}

				lastLogIndex, _ := rf.getLastLog()
				containEntries := lastLogIndex >= next

				entries := make([]LogEntry, len(rf.logs[next:]))
				if containEntries {
					copy(entries, rf.logs[next:])
					// entries = rf.logs[next:]
				}
				prevLogIndex := next - 1
				prevLogTerm := rf.logs[next-1].Term
				args := AppendEnrtiesRequest{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex}
				reply := AppendEnrtiesResponse{}

				rf.mu.Unlock()
				if !rf.sendAppendEntries(peerId, &args, &reply) {
					return
				}

				rf.mu.Lock()
				DPrintf("%v Send AppendEntries to S%d\n", rf, peerId)

				if args.Term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				// fmt.Printf("Leader: %d; term: %d; args.term: %d; log is ", rf.me, rf.currentTerm, args.Term)
				// fmt.Println(rf.logs)

				if reply.Term > rf.currentTerm {
					rf.changeState(Follower)
					rf.currentTerm = reply.Term
					rf.persist()
					rf.mu.Unlock()
					return
				}

				if reply.Success {

					if containEntries {
						rf.matchIndex[peerId] = prevLogIndex + len(entries)
						rf.nextIndex[peerId] = prevLogIndex + len(entries) + 1
						indexArr := make([]int, len(rf.peers))
						copy(indexArr, rf.matchIndex)
						sort.Ints(indexArr)

						newCommitIndex := indexArr[(len(indexArr)-1)/2]
						// fmt.Println(indexArr)
						// fmt.Println(rf.logs)
						if newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex].Term == rf.currentTerm {
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
					rf.nextIndex[peerId]--
					rf.mu.Unlock()
				}
			}

		}(peerId)
	}
}

func (rf *Raft) HandleAppendEntries(args *AppendEnrtiesRequest, reply *AppendEnrtiesResponse) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.XTerm = -1
	reply.XIndex = -1
	reply.Xlen = -1

	if len(args.Entries) > 0 {
		DPrintf("%v received AppendEntries from S%d (term=%d, prevLogIndex=%d, prevLogTerm=%d, entries=%d)",
			rf, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	}

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.changeState(Follower)
	}

	if args.Term > rf.currentTerm {
		rf.changeState(Follower)
		rf.currentTerm = args.Term
		rf.persist()
		reply.Term = rf.currentTerm
	}
	rf.resetElectionTimer()

	lastLogIndex, _ := rf.getLastLog()

	if lastLogIndex < args.PrevLogIndex {
		reply.XIndex = lastLogIndex + 1
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.logs[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.logs[i].Term != reply.XTerm {
				reply.XIndex = rf.logs[i].Index
				break
			}
		}
		return
	}

	if len(args.Entries) > 0 {
		// DPrintf("Follower: %d, preIndex: %d, entries: %v, logs: %v\n", rf.me, args.PrevLogIndex, args.Entries, rf.logs)

		i, j := args.PrevLogIndex+1, 0
		for i < len(rf.logs) && j < len(args.Entries) && rf.logs[i].Term == args.Entries[j].Term {
			i += 1
			j += 1
		}
		rf.logs = append(rf.logs[:i], args.Entries[j:]...)
		rf.persist()

		DPrintf("%v appends log entries: %+v", rf, args.Entries[j:])
	}

	reply.Success = true

	newCommitIndex := min(args.LeaderCommit, len(rf.logs)-1)
	if newCommitIndex > rf.commitIndex {
		// DPrintf("Follower %d: newCommitIndex: %d; oldCommitIndex: %d; lastApplied: %d\n", rf.me, newCommitIndex, rf.commitIndex, rf.lastApplied)
		rf.commitIndex = newCommitIndex
		// state := [3]string{"Leader", "Follower", "Candidate"}
		// fmt.Printf("[S%d | term=%d | state=%s | logLen=%d | commit=%d | lastApplied=%d] | lastLogIndex=%d\n",
		// 	rf.me, rf.currentTerm, state[rf.state], len(rf.logs), rf.commitIndex, rf.lastApplied, rf.logs[len(rf.logs)-1].Index)
		rf.applyCond.Signal()
	}

}

func (rf *Raft) applySubmit() {

	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied && !rf.killed() {
			rf.applyCond.Wait()
		}
		applyMsgs := []ApplyMsg{}

		lastIdx := len(rf.logs) - 1
		commitIdx := rf.commitIndex

		if commitIdx > lastIdx {
			commitIdx = lastIdx
		}

		DPrintf("%v updates commitIndex=%d (before=%d)", rf, commitIdx, rf.lastApplied)

		for i := rf.lastApplied + 1; i <= commitIdx; i++ {
			entry := rf.logs[i]
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
			rf.applyCh <- msg
		}
	}

}

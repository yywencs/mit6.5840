package raft

import (
	"sort"
)

type AppendEnrtiesRequest struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEnrtiesResponse struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEnrtiesRequest, reply *AppendEnrtiesResponse) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
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

				entries := []LogEntry{}
				if containEntries {
					entries = rf.logs[next:]
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

				if args.Term != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				// fmt.Printf("Leader: %d; term: %d; args.term: %d; log is ", rf.me, rf.currentTerm, args.Term)
				// fmt.Println(rf.logs)

				if reply.Term > rf.currentTerm {
					rf.changeState(Follower)
					rf.currentTerm = reply.Term
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

	if args.Term < rf.currentTerm {
		return
	}

	rf.resetElectionTimer()

	if args.Term > rf.currentTerm {
		rf.changeState(Follower)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}

	lastLogIndex, _ := rf.getLastLog()

	if lastLogIndex < args.PrevLogIndex {
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
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

		// DPrintf("after append: Follower: %d, logs is %v", rf.me, rf.logs)
	}

	reply.Success = true

	newCommitIndex := min(args.LeaderCommit, rf.logs[len(rf.logs)-1].Index)
	if newCommitIndex > rf.commitIndex {
		// DPrintf("Follower %d: newCommitIndex: %d; oldCommitIndex: %d; lastApplied: %d\n", rf.me, newCommitIndex, rf.commitIndex, rf.lastApplied)
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
		// DPrintf("entry submit")
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entry := rf.logs[i]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			applyMsgs = append(applyMsgs, applyMsg)
			rf.lastApplied += 1
		}
		// DPrintf("no: %d; isLeader: %t; submit success, commmitIndex: %d, lastApplied: %d\n", rf.me, rf.state == Leader, rf.commitIndex, rf.lastApplied)
		// fmt.Println(rf.logs)

		rf.mu.Unlock()

		for _, applyMsg := range applyMsgs {
			rf.applyCh <- applyMsg
		}
	}

}

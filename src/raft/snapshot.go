package raft

func (rf *Raft) InstallSnapshot(peerId int) {
	rf.mu.Lock()
	args := SnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Snapshot:          rf.persister.ReadSnapshot(),
	}
	reply := SnapshotReply{}

	DPrintf(dSnap, "%v send snapshot to S%d\n", rf, peerId)

	rf.mu.Unlock()
	if !rf.sendSnapshot(peerId, &args, &reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(dSnap, "S%d get reply from S%d\n", rf.me, peerId)

	if args.Term != rf.currentTerm || rf.state != LEADER {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.changeState(FOLLOWER)
		rf.currentTerm = reply.Term
		rf.persist(nil)
		return
	}
	rf.nextIndex[peerId] = rf.lastIncludedIndex + 1
	rf.matchIndex[peerId] = rf.lastIncludedIndex
}

func (rf *Raft) HandleSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.persist(nil)
		reply.Term = rf.currentTerm
	}

	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.lastIncludedIndex || args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	DPrintf(dSnap, "%v HandleSnapshot, leaderID: %d, LastIncludedIndex: %d, LastIncludedTerm: %d\n", rf, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	index := args.LastIncludedIndex - rf.lastIncludedIndex

	if index < len(rf.logs) && rf.logs[index].Index == args.LastIncludedIndex && rf.logs[index].Term == args.LastIncludedTerm {
		logs := make([]LogEntry, len(rf.logs[index+1:]))
		copy(logs, rf.logs[index+1:])
		rf.logs = append([]LogEntry{{args.LastIncludedIndex, args.LastIncludedTerm, nil}}, logs...)
	} else {
		rf.logs = []LogEntry{{args.LastIncludedIndex, args.LastIncludedTerm, nil}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.persist(args.Snapshot)
	rf.mu.Unlock()

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.applyCh <- applyMsg
}

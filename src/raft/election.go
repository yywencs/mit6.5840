package raft

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.changeState(CANDIDATE)
	rf.persist(nil)
	lastLogIndex, lastLogTerm := rf.getLastLog()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	nPeers := len(rf.peers)
	cnt := 1
	DPrintf("%v starts election (lastLogIndex=%d, lastLogTerm=%d)",
		rf, lastLogIndex, lastLogTerm)
	rf.mu.Unlock()

	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go func(peerId int) {
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(peerId, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if args.Term != rf.currentTerm {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.changeState(FOLLOWER)
				rf.persist(nil)
				return
			}

			if reply.VoteGranted && rf.state == CANDIDATE {
				cnt += 1
				if cnt > nPeers/2 {
					rf.changeState(LEADER)
					rf.reinitIndex()
					DPrintf("%v becomes LEADER", rf)
					rf.requestAppendEntries()
					return
				}
			}
		}(peerId)
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	DPrintf("S%d get RequestVote from S%d\n", rf.me, args.CandidateId)

	if args.Term < rf.currentTerm {
		return
	}

	if rf.currentTerm < args.Term {
		rf.changeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.persist(nil)
		reply.Term = rf.currentTerm
	}

	lastLogIndex, lastLogTerm := rf.getLastLog()
	if (args.LastLogTerm < lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.persist(nil)
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.

// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().

// look at the comments in ../labrpc/labrpc.go for more details.

// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

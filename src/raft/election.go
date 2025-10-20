package raft

import (
	"sync"
)

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
	DPrintf(dVote, "%v starts election (lastLogIndex=%d, lastLogTerm=%d)",
		rf, lastLogIndex, lastLogTerm)
	rf.mu.Unlock()

	var mu sync.Mutex
	mu.Lock()
	cnt := 1
	mu.Unlock()

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

			if reply.VoteGranted {
				mu.Lock()
				cnt += 1
				DPrintf(dVote, "%v get voted of %d\n", rf, cnt)
				if cnt*2 > nPeers {
					rf.changeState(LEADER)
					rf.reinitIndex()
					DPrintf(dVote, "%v becomes LEADER", rf)
					rf.requestAppendEntries()
				}
				mu.Unlock()
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

	if args.Term < rf.currentTerm {
		DPrintf(dVote, "S%d get RequestVote from S%d, but args.Term < rf.currentTerm\n", rf.me, args.CandidateId)
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
		DPrintf(dVote, "S%d get RequestVote from S%d, but lastLogIndex not eligible\n", rf.me, args.CandidateId)
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		DPrintf(dVote, "S%d get RequestVote from S%d, finish voted\n", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		rf.persist(nil)
		reply.VoteGranted = true
		rf.resetElectionTimer()
	} else {
		DPrintf(dVote, "S%d get RequestVote from S%d, but have voted to %d\n", rf.me, args.CandidateId, rf.votedFor)
	}
}

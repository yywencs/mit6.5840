package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func resetElectionTimeout() time.Duration {
	duration := 150 + (rand.Int63() % 150)
	return time.Duration(duration) * time.Millisecond
}

func (rf *Raft) getLastLog() (int, int) {
	index := rf.logs[len(rf.logs)-1].Index
	term := rf.logs[len(rf.logs)-1].Term
	return index, term
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) changeState(state State) {
	if state == Candidate {
		rf.state = Candidate
		rf.currentTerm += 1
		rf.voteFor = rf.me
	} else if state == Follower {
		rf.state = Follower
		rf.voteFor = -1
		rf.resetElectionTimer()
		rf.heartbeatTimer.Stop()

	} else if state == Leader {
		rf.state = Leader
		rf.resetHeartBeatTimer()
		rf.electionTimeout.Stop()
	}
}

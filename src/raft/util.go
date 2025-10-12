package raft

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) String() string {
	state := [3]string{"Leader", "Follower", "Candidate"}
	return fmt.Sprintf(
		"[S%d | term=%d | state=%s | lastLogIndex=%d | commit=%d | lastApplied=%d | lastIncludedIndex=%d | lastIncludedTerm=%d]",
		rf.me, rf.currentTerm, state[rf.state], rf.logs[len(rf.logs)-1].Index, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex, rf.lastIncludedTerm,
	)
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

func (rf *Raft) findLastIndexOfTerm(term int) int {
	for i := len(rf.logs) - 1; i >= 0; i-- {
		if rf.logs[i].Term == term {
			return rf.logs[i].Index
		}
	}
	return -1
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) changeState(state State) {
	if state == CANDIDATE {
		rf.state = CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = rf.me
	} else if state == FOLLOWER {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetElectionTimer()
		rf.heartbeatTimer.Stop()
	} else if state == LEADER {
		rf.state = LEADER
		rf.resetHeartBeatTimer()
		rf.electionTimeout.Stop()
	}
}

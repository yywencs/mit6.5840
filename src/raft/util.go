package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

// Debugging
const Debug = true

func LogToFile() {
	logFile := "debug.log"
	// 检查文件是否存在
	if _, err := os.Stat(logFile); err == nil {
		// 文件存在则删除
		os.Remove(logFile)
	}

	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}

	// 将标准 logger 的输出重定向到文件
	log.SetOutput(f)
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) String() string {
	state := [3]string{"Leader", "Follower", "Candidate"}
	return fmt.Sprintf(
		"[S%d | term=%d | state=%s | logLen=%d | commit=%d | lastApplied=%d | lastLogIndex=%d]",
		rf.me, rf.currentTerm, state[rf.state], len(rf.logs), rf.commitIndex, rf.lastApplied, rf.logs[len(rf.logs)-1].Index,
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

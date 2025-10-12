package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

const HEARTBEATTIME = time.Duration(50) * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state             State       // rf当前的状态
	heartbeatTimer    *time.Timer // 心跳timer
	electionTimeout   *time.Timer // 超时选举 timer
	currentTerm       int         //当前的任期
	votedFor          int         // 投票给了哪个节点
	logs              []LogEntry  // 日志
	nextIndex         []int       // 要传给节点的下一个index
	matchIndex        []int       // 节点已经匹配的index
	commitIndex       int         // 已经提交的index
	lastApplied       int         //  已经返回给客户端的index
	applyCond         *sync.Cond
	applyCh           chan ApplyMsg
	lastIncludedIndex int
	lastIncludedTerm  int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

func (rf *Raft) reinitIndex() {
	lastLogIndex, _ := rf.getLastLog()
	for peerId := range rf.peers {
		rf.nextIndex[peerId] = lastLogIndex + 1
		if peerId == rf.me {
			rf.matchIndex[peerId] = lastLogIndex
		} else {
			rf.matchIndex[peerId] = 0
		}
	}
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	return raftstate
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	raftstate := rf.encodeState()
	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	DPrintf("readPersist\n")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("readPersist\n")
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		return
	}
	DPrintf("%v load len of log is %d\n", rf, len(logs))
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.logs = logs
	rf.lastIncludedIndex = rf.logs[0].Index
	rf.lastIncludedTerm = rf.logs[0].Term
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || index <= rf.lastIncludedIndex || index > rf.lastApplied {
		return
	}

	newIndex := index - rf.lastIncludedIndex
	rf.lastIncludedTerm = rf.logs[newIndex].Term
	rf.lastIncludedIndex = index

	logs := make([]LogEntry, len(rf.logs[newIndex+1:]))
	copy(logs, rf.logs[newIndex+1:])

	rf.logs = append([]LogEntry{{rf.lastIncludedIndex, rf.lastIncludedTerm, nil}}, logs...)
	rf.persist(snapshot)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return index, term, false
	}

	index, _ = rf.getLastLog()
	index++
	term = rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{Index: index, Term: term, Command: command})
	rf.persist(nil)

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	go rf.requestAppendEntries()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimeout.C:
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				rf.startElection()
				rf.resetElectionTimer()
			} else {
				rf.mu.Unlock()
			}
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == LEADER {
				DPrintf("S%d heartbeat Time Out\n", rf.me)
				rf.requestAppendEntries()
				rf.resetHeartBeatTimer()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) resetHeartBeatTimer() {

	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C:
		default:
		}
	}
	rf.heartbeatTimer.Reset(HEARTBEATTIME)
}

func (rf *Raft) resetElectionTimer() {

	if !rf.electionTimeout.Stop() {
		select {
		case <-rf.electionTimeout.C:
		default:
		}
	}
	rf.electionTimeout.Reset(resetElectionTimeout())
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		state:             FOLLOWER,
		heartbeatTimer:    time.NewTimer(HEARTBEATTIME),
		currentTerm:       0,
		votedFor:          -1,
		electionTimeout:   time.NewTimer(resetElectionTimeout()),
		logs:              []LogEntry{{Index: 0, Term: 0, Command: nil}},
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		commitIndex:       0,
		lastApplied:       0,
		applyCh:           applyCh,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadRaftState())
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// Your initialization code here (3A, 3B, 3C).
	go rf.applySubmit()

	// initialize from state persisted before a crash

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

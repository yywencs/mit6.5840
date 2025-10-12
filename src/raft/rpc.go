package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // Candidate的任期
	CandidateId  int // Candidate的ID
	LastLogIndex int // Candidate最后日志的下标
	LastLogTerm  int // Candidate最后日志的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // FOLLOWER的任期
	VoteGranted bool // FOLLOWER是否投票给Candidate
}

type AppendEnrtiesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEnrtiesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	Xlen    int
}

type SnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type SnapshotReply struct {
	Term int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEnrtiesArgs, reply *AppendEnrtiesReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.HandleSnapshot", args, reply)
	return ok
}

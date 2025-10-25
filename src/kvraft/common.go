package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotApplied  = "ErrNotApplied"
)

type Err string

type RequestType byte

const (
	Request RequestType = iota
	Report
)

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	ClientId  int64
	CommandId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	CommandId int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

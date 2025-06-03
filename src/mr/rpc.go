package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type WorkType int

const (
	MapWork WorkType = iota
	ReduceWork
)

type AskType int

const (
	AskTask AskType = iota
	Finished
	Failed
)

type ReplyType int

const (
	Work ReplyType = iota
	Exit
	Wait
)

type TaskParams struct {
	TaskName  WorkType
	MapNo     int
	ReduceNo  int
	AttemptId int
}

type Args struct {
	TaskInfo   TaskParams
	AskMessage AskType
}

type Reply struct {
	TaskInfo     TaskParams
	MapFile      string
	ReduceNum    int
	ReplyMessage ReplyType
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

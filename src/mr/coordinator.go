package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int
type WorkerState int

var n_reduce, n_map int

var MapCompleted, AllCompleted bool = false, false

const (
	Pending TaskState = iota
	InProgress
	Completed
)

type TotalTasks struct {
	taskStates    []TaskState
	LastHeartbeat []int64
	finished      int
	attemptId     []int
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	files       []string
	mapTasks    TotalTasks
	reduceTasks TotalTasks
	exitTime    int64
}

var replyTypeNames = []string{
	"Work",
	"Exit",
	"Wait",
}

var askTypeName = []string{
	"AskTask",
	"Finished",
	"Failed",
}

var workTypeName = []string{
	"MapWork",
	"ReduceWork",
}

func (c *Coordinator) allocateTask(reply *Reply, TaskName WorkType) {
	log.Printf("Allocate Task: WorkType: %v\n", workTypeName[TaskName])
	params := &c.mapTasks
	if TaskName == ReduceWork {
		params = &c.reduceTasks
	}

	reply.ReplyMessage = Work
	for i, f := range params.taskStates {
		if f == Pending {
			params.taskStates[i] = InProgress
			params.attemptId[i] += 1
			if TaskName == MapWork {
				reply.TaskInfo.MapNo = i
				reply.MapFile = c.files[i]
				log.Printf("Allocate Map Task: MapNO: %d\n", i)
			} else {
				reply.TaskInfo.ReduceNo = i
				log.Printf("Allocate Reduce Task: ReduceNO: %d\n", i)
			}
			reply.TaskInfo.AttemptId = params.attemptId[i]
			params.LastHeartbeat[i] = time.Now().Unix()
			reply.TaskInfo.TaskName = TaskName
			reply.ReduceNum = n_reduce
			return
		}
	}
	reply.ReplyMessage = Wait
}

func (c *Coordinator) taskFinished(args *Args, reply *Reply) error {
	log.Printf("Finished Task, WorkType: %v\n", workTypeName[args.TaskInfo.TaskName])
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.ReplyMessage = Wait

	if args.TaskInfo.TaskName == MapWork {
		log.Printf("Finished Task, MapNO %d\n", args.TaskInfo.MapNo)
		MapNo := args.TaskInfo.MapNo
		log.Printf("args.TaskInfo.attemptId: %d, attemptId[MapNo]: %d\n", args.TaskInfo.MapNo, c.mapTasks.attemptId[MapNo])
		if args.TaskInfo.AttemptId != c.mapTasks.attemptId[MapNo] {
			return nil
		}
		c.mapTasks.taskStates[MapNo] = Completed
		// fmt.Printf("map_task have finished %d", c.mapTasks.finished)
	} else if args.TaskInfo.TaskName == ReduceWork {
		log.Printf("Reduce Task, MapNO %d\n", args.TaskInfo.MapNo)
		ReduceNo := args.TaskInfo.ReduceNo
		if args.TaskInfo.AttemptId != c.reduceTasks.attemptId[ReduceNo] {
			return nil
		}
		c.reduceTasks.taskStates[ReduceNo] = Completed
		// fmt.Printf("reduceTasks have finished %d", c.reduceTasks.finished)
	}
	return nil
}

func (c *Coordinator) taskFailed(args *Args, reply *Reply) error {
	log.Printf("Failed Task, TaskName %v\n", workTypeName[args.TaskInfo.TaskName])
	reply.ReplyMessage = Exit
	taskInfo := args.TaskInfo

	c.mu.Lock()
	if taskInfo.TaskName == MapWork {
		log.Printf("Failed Task, MapNO %d\n", args.TaskInfo.MapNo)
		taskID := taskInfo.MapNo
		if args.TaskInfo.AttemptId != c.mapTasks.attemptId[taskID] {
			return nil
		}
		c.mapTasks.taskStates[taskID] = Pending
	} else {
		log.Printf("Reduce Task, ReduceNO %d\n", args.TaskInfo.MapNo)
		taskID := taskInfo.ReduceNo
		if args.TaskInfo.AttemptId != c.reduceTasks.attemptId[taskID] {
			return nil
		}
		c.reduceTasks.taskStates[taskID] = Pending
	}

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) Handler(args *Args, reply *Reply) error {
	var err error = nil
	c.mu.Lock()
	IsExit := AllCompleted
	c.mu.Unlock()

	if IsExit {
		reply.ReplyMessage = Exit
		return nil
	}

	switch args.AskMessage {
	case AskTask:
		err = c.askTask(args, reply)
	case Finished:
		err = c.taskFinished(args, reply)
	case Failed:
		err = c.taskFailed(args, reply)
	}
	return err
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) askTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	if !MapCompleted {
		c.allocateTask(reply, MapWork)
	} else if !AllCompleted {
		c.allocateTask(reply, ReduceWork)
	} else {
		reply.ReplyMessage = Exit
	}
	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	logFile, _ := os.OpenFile("../app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	log.SetOutput(logFile)
	log.Println("****************************start coordiantor****************************")
	go http.Serve(l, nil)
}

func (c *Coordinator) testHeartBeat(tasks *TotalTasks) bool {
	state := true
	for i, taskState := range tasks.taskStates {
		if taskState != Completed {
			state = false
		}
		if taskState == InProgress {
			if time.Now().Unix()-tasks.LastHeartbeat[i] > 10 {
				log.Printf("Task Timeout, TaskNO %d\n", i)
				tasks.taskStates[i] = Pending
			}
		}
	}
	return state
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := false

	c.mu.Lock()
	defer c.mu.Unlock()

	//log.Println("Map Task Test")
	mapState := c.testHeartBeat(&c.mapTasks)
	//log.Println("Reduce Task Test")
	reduceState := c.testHeartBeat(&c.reduceTasks)

	log.Printf("mapState: %t, reduceState: %t", mapState, reduceState)

	if mapState {
		MapCompleted = true
	}
	if mapState && reduceState {
		AllCompleted = true
	}

	// Your code here.
	if AllCompleted {
		// log.Println("ALLCompleted")
		if c.exitTime == 0 {
			// log.Println("Exit countdown started")
			c.exitTime = time.Now().Unix()
		} else if time.Now().Unix()-c.exitTime >= 3 {
			// log.Println("Coordinator exiting after 3s")
			return true
		}
	}

	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	file_num := len(files)

	c := Coordinator{}
	c.files = files
	n_map = file_num
	n_reduce = nReduce
	c.mapTasks = TotalTasks{make([]TaskState, file_num), make([]int64, file_num), 0, make([]int, file_num)}
	c.reduceTasks = TotalTasks{make([]TaskState, nReduce), make([]int64, nReduce), 0, make([]int, nReduce)}
	c.exitTime = 0
	c.server()
	return &c
}

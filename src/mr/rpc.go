package mr

import (
	"os"
	"strconv"
	"time"
)

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type MrTaskType int

const (
	//TODO: add task type for idle (still waiting on in-progress tasks) and Done???
	MapTask MrTaskType = iota
	ReduceTask
	WaitTask
	DoneTask
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	MrTask MrTaskType
	// Filenames []string
	Filename  string
	NMap      int
	MapNum    int
	NReduce   int
	ReduceNum int
	Start     time.Time
	State     TaskState
	WorkerId  int
}

var TheWaitTask = Task{MrTask: WaitTask}
var TheDoneTask = Task{MrTask: DoneTask}

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	Task Task
}

type ReportTaskDoneArgs struct {
	Task Task
}

type ReportTaskDoneReply struct{}

type ReportArgs struct{}

type ReplyArgs struct{}

type DoneArgs struct{}
type DoneReply struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//
// import "strconv"

// import "fmt"

type Master struct {
	// Your definitions here.
	mu             sync.Mutex
	files          []string
	nReduce        int
	tasks          []Task
	mapTasks       []Task
	mapComplete    int
	reduceTasks    []Task
	reduceComplete int
	allDone        bool
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTask(args RequestTaskArgs, reply *RequestTaskReply) error {
	//TODO: TEST THIS!!!!!!!
	m.mu.Lock()
	defer m.mu.Unlock()

	var taskList []Task

	// Figure out if we are doing map or reduce and assign the respective task list.
	if m.mapComplete < len(m.mapTasks) {
		taskList = m.mapTasks
	} else if m.reduceComplete < len(m.reduceTasks) {
		taskList = m.reduceTasks
	} else {
		return errors.New("All Tasks completed.")
	}

	// pick first available idle task
	for i := range taskList {
		task := &taskList[i]
		if task.State == Idle {
			task.State = InProgress
			task.Start = time.Now()
			*reply = RequestTaskReply{*task} //TODO: is this everything the reply needs?
			return nil
		}
	}

	// If any Task is still InProgress this will be set to true. If this reamins false all tasks are done.
	stillWaiting := false
	for i := range taskList {
		task := &taskList[i]
		if task.State == InProgress {
			stillWaiting = true
			if time.Since(task.Start) > 10*time.Second {
				//TODO: mark new worker in task??? -> we aren't communicating with worker, so no.
				task.Start = time.Now()
				*reply = RequestTaskReply{*task}
				return nil
			}
		}
	}
	if !stillWaiting {
		*reply = RequestTaskReply{TheDoneTask}
		return errors.New("All Tasks completed.")
	}

	*reply = RequestTaskReply{TheWaitTask}
	return nil
}

func (m *Master) findExecutableTask(taskList []Task) (Task, bool) {

	for i := range m.reduceTasks {
		task := &m.reduceTasks[i]
		if task.State == Idle {
			task.State = InProgress
			task.Start = time.Now()
			// 	*reply = RequestTaskReply{*task} //TODO: is this everything the reply needs?
			// 	return nil
			return *task, true
		}
	}
	for {
		// If any Task is still InProgress this will be set to true. If this reamins false all tasks are done.
		stillWaiting := false
		for i := range m.reduceTasks {
			task := &m.reduceTasks[i]
			if task.State == InProgress {
				stillWaiting = true
				if time.Since(task.Start) > 10*time.Second {
					//TODO: mark new worker in task???
					task.Start = time.Now()
					// *reply = RequestTaskReply{*task}
					// return nil
					return *task, true
				}
			}
		}
		if !stillWaiting {
			break
		}
		m.mu.Unlock()
		time.Sleep(time.Second)
		m.mu.Lock()
	}
	return Task{}, false
}

func (m *Master) ReportTaskDone(args ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	workerTask := args.Task
	if workerTask.MrTask == MapTask {
		for i := range m.mapTasks {
			task := &m.mapTasks[i]
			if task.Filename == workerTask.Filename {
				task.State = Completed
				m.mapComplete += 1
				// if m.mapComplete == len(m.mapTasks) {
				// 	m.allDone = true
				// }
			}
		}
	} else if workerTask.MrTask == ReduceTask {
		for i := range m.reduceTasks {
			task := &m.reduceTasks[i]
			if task.Filename == workerTask.Filename {
				task.State = Completed
				m.reduceComplete += 1
				// if m.mapComplete == len(m.mapTasks) {
				// 	m.allDone = true
				// }
			}
		}
	}

	return errors.New("Task completion confirmed.")
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// ret := false
	// ret := true

	// Your code here.
	// fmt.Println("Still running...")
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.allDone

	// return errors.New("All Tasks finished.")
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	fmt.Printf("Mster files: %v\n", files)
	fmt.Printf("Master nReduce: %v\n", nReduce)

	m := Master{}

	// Your code here.
	if len(files) <= 0 {
		m.allDone = true
	} else {
		m.files = files
		m.nReduce = nReduce

		for i, f := range m.files {
			task := Task{
				MrTask: MapTask,
				// Filenames: []string{f},
				Filename: f,
				NReduce:  m.nReduce,
				MapNum:   i,
				State:    Idle,
			}
			m.mapTasks = append(m.mapTasks, task)
		}
		for i := 0; i < nReduce; i++ {
			task := Task{
				MrTask:    ReduceTask,
				NMap:      len(m.mapTasks),
				ReduceNum: i,
				State:     Idle,
			}
			m.reduceTasks = append(m.reduceTasks, task)
		}
	}

	fmt.Printf("Master mapTasks: %v\n", m.mapTasks)
	fmt.Printf("Master reduceTasks: %v\n", m.reduceTasks)

	m.server()
	return &m
}

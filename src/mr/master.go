package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "errors"

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

func (m *Master) RequestTaskOld(args *int, reply *Task) error {
	// reply.MrTask = MapTask
	// reply.Filenames = m.files
	// reply.nReduce = m.nReduce
	// *reply = Task{
	// 	mrTask:    mapTask,
	// 	filenames: m.files,
	// 	nReduce:   m.nReduce,
	// 	start:     time.Now(), //TODO: assign this when the Task is handed over?
	// }
	// return nil
	//TODO: This locking will be sketchy once workers call completed on tasks while this is waiting (see time.Sleep() below)

	//TODO: check idle
	//TODO: check in-progress (not reassignable)/(reassignable)
	//TODO: check done
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.allDone {
		return errors.New("No uncompleted Tasks left.")
	}
	for _, task := range m.tasks {
		if task.State == Idle {
			//TODO: Should it be set to in progress here? or when it is handed off to the worker?
			task.State = InProgress
			task.Start = time.Now()
			*reply = task
			return nil
		}
	}
	//TODO: 10 sec wait time and completed? if it still has to wait, not everything is completetd
	for {
		stillWaiting := false
		for _, task := range m.tasks {
			if task.State == InProgress {
				stillWaiting = true
				if time.Since(task.Start) < 10*time.Second {
					*reply = task
					return nil
				}
			}
		}
		if !stillWaiting {
			break
		}
		//TODO: Who waits? the master or the worker? Worker kinda seems smarter
		// the worker can frequently re-poll, but if the master lets the worker hang it might crash in between
		time.Sleep(time.Second)
	}
	m.allDone = true
	return errors.New("No uncompleted Tasks left.")

}

func (m *Master) RequestTaskOldV2(args RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.allDone {
		return errors.New("No uncompleted Tasks left.")
	}

	//TODO: REDUCE TASKS!!!! This only does map tasks for now.
	//TODO: implement mapDone/reduceDone?
	//TODO: implement list for map/reduce tasks seperately? and remove done tasks on report?

	for i := range m.tasks {
		task := &m.tasks[i]
		if task.State == Idle {
			task.State = InProgress
			task.Start = time.Now()
			*reply = RequestTaskReply{*task} //TODO: is this everything the reply needs?
			return nil
		}
	}
	for {
		// If any Task is still InProgress this will be set to true. If this reamins false all tasks are done.
		stillWaiting := false
		for i := range m.tasks {
			task := &m.tasks[i]
			if task.State == InProgress {
				stillWaiting = true
				if time.Since(task.Start) > 10*time.Second {
					//TODO: mark new worker in task???
					task.Start = time.Now()
					*reply = RequestTaskReply{*task}
					return nil
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

	m.allDone = true
	return errors.New("All Tasks completed.")
}

func (m *Master) RequestTaskOldV3(args RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	//TODO: Code duplication because of locking. Locking is a result of needing to check wether there are still tasks incomplete. A check on the completed count actuallz resolves this. then the waiting can occur in a calling function. the polling logic simply needs to check for idle or in progress tasks and return one or none. Checking for idle and inProgress can be done in a single loop then. It returns the idle process as soon as it finds it and keeps waiting otherwise.

	// or simply set the fucking right tasklist at the toplevel in if/else if/else and let the last else return if all else is done

	if m.mapComplete < len(m.mapTasks) {
		// find map task and return

		for i := range m.mapTasks {
			task := &m.mapTasks[i]
			if task.State == Idle {
				task.State = InProgress
				task.Start = time.Now()
				*reply = RequestTaskReply{*task} //TODO: is this everything the reply needs?
				return nil
			}
		}
		for {
			// If any Task is still InProgress this will be set to true. If this reamins false all tasks are done.
			stillWaiting := false
			for i := range m.mapTasks {
				task := &m.mapTasks[i]
				if task.State == InProgress {
					stillWaiting = true
					if time.Since(task.Start) > 10*time.Second {
						//TODO: mark new worker in task???
						task.Start = time.Now()
						*reply = RequestTaskReply{*task}
						return nil
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
	}

	if m.reduceComplete < len(m.reduceTasks) {
		// find reduce task and return

		for i := range m.reduceTasks {
			task := &m.reduceTasks[i]
			if task.State == Idle {
				task.State = InProgress
				task.Start = time.Now()
				*reply = RequestTaskReply{*task} //TODO: is this everything the reply needs?
				return nil
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
						*reply = RequestTaskReply{*task}
						return nil
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
	}

	return errors.New("All Tasks completed.")

}

func (m *Master) RequestTaskOldV4(args RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var taskList []Task

	if m.mapComplete < len(m.mapTasks) {
		taskList = m.mapTasks
	} else if m.reduceComplete < len(m.reduceTasks) {
		taskList = m.reduceTasks
	} else {
		return errors.New("All Tasks completed.")
	}

	for i := range taskList {
		task := &taskList[i]
		if task.State == Idle {
			task.State = InProgress
			task.Start = time.Now()
			*reply = RequestTaskReply{*task} //TODO: is this everything the reply needs?
			return nil
		}
	}
	//TODO: make master sleep or make worker sleep?
	for {
		// If any Task is still InProgress this will be set to true. If this reamins false all tasks are done.
		stillWaiting := false
		for i := range taskList {
			task := &taskList[i]
			if task.State == InProgress {
				stillWaiting = true
				if time.Since(task.Start) > 10*time.Second {
					//TODO: mark new worker in task???
					task.Start = time.Now()
					*reply = RequestTaskReply{*task}
					return nil
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

	return errors.New("All Tasks completed.")

}

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

	} else {
		//TODO: Reduce Tasks complete?
	}

	return errors.New("Task completion confirmed.")
}

func (m *Master) getExecutableTask() (Task, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, task := range m.tasks {
		if task.State == Idle {
			//TODO: Should it be set to in progress here? or when it is handed off to the worker?
			task.State = InProgress
			return task, true
		}
	}
	for _, task := range m.tasks {
		if task.State == InProgress {
			return task, true
		}
	}
	return Task{}, false
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
func (m *Master) Done(args *DoneArgs, reply *DoneReply) error {
	// ret := false
	// ret := true

	// Your code here.
	// fmt.Println("Still running...")
	m.mu.Lock()
	defer m.mu.Unlock()
	// ret = m.allDone

	return errors.New("All Tasks finished.")
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	if len(files) <= 0 {
		m.allDone = true
	} else {
		m.files = files
		m.nReduce = nReduce

		for _, f := range m.files {
			task := Task{
				MrTask: MapTask,
				// Filenames: []string{f},
				Filename: f,
				NReduce:  m.nReduce,
				State:    Idle,
			}
			m.mapTasks = append(m.mapTasks, task)
		}
		for i := 0; i < nReduce; i++ {
			task := Task{
				MrTask:  ReduceTask,
				NReduce: i,
				State:   Idle,
			}
			m.reduceTasks = append(m.reduceTasks, task)
		}
	}

	m.server()
	return &m
}

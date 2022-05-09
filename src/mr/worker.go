package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(dir)

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	// args := 99
	// reply := Task{}
	doneMessage := "Master reports all tasks are done. Shutting down..."

	// continuously poll master for tasks until it reports "done".
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		done := !call("Master.RequestTask", &args, &reply)
		if done {
			fmt.Println(doneMessage)
			return
		}

		mrTask := reply.Task
		fmt.Printf("Received Task: %v\n", mrTask)
		switch mrTask.MrTask {
		case MapTask:
			fmt.Println("Received mapTask: " + mrTask.Filename)
			//TODO: Does the task need an id or something so it can be reported done?
			doMap(mapf, mrTask)
			doneArgs := ReportTaskDoneArgs{mrTask}
			doneReply := ReportTaskDoneReply{}
			call("Master.ReportTaskDone", &doneArgs, &doneReply)
		case ReduceTask:
			fmt.Println("Received reduceTask: " + mrTask.Filename)
			doReduce(reducef, mrTask)
			doneArgs := ReportTaskDoneArgs{mrTask}
			doneReply := ReportTaskDoneReply{}
			call("Master.ReportTaskDone", &doneArgs, &doneReply)
		case WaitTask:
			fmt.Println("Received WaitTask. Sleeping...")
			time.Sleep(time.Second)
		case DoneTask:
			fmt.Println(doneMessage)
			return
		}
	}
}

// func doMap(mapf func(string, string) []KeyValue,
// 	filenames []string, nReduce int) {

// 	intermediate := []KeyValue{}
// 	for _, filename := range filenames {
// 		file, err := os.Open(filename)
// 		if err != nil {
// 			log.Fatalf("cannot read %v", filename)
// 		}
// 		content, err := ioutil.ReadAll(file)
// 		if err != nil {
// 			log.Fatalf("cannot read %v", filename)
// 		}
// 		file.Close()
// 		kva := mapf(filename, string(content))
// 		intermediate = append(intermediate, kva...)
// 	}
func doMap(mapf func(string, string) []KeyValue, mrTask Task) {

	filename := mrTask.Filename
	nReduce := mrTask.NReduce
	mapNum := mrTask.MapNum

	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	encoders := make([]*json.Encoder, nReduce)
	for i := range encoders {
		oFileName := mapFileName(mapNum, i)
		oFile, err := os.Create(oFileName)
		if err != nil {
			log.Fatalf("cannot create or open %v", oFileName)
		}
		defer oFile.Close()
		w := bufio.NewWriter(oFile)
		defer w.Flush()
		enc := json.NewEncoder(w)
		encoders[i] = enc
	}

	for _, kv := range intermediate {
		reduceNum := ihash(kv.Key) % nReduce
		err := encoders[reduceNum].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv: %v; in reduceNum: %v", kv, reduceNum)
		}
	}

}

func doReduce(reducef func(string, []string) string, mrTask Task) {
	kva := []KeyValue{}
	for i := 0; i < mrTask.NMap; i++ {
		inFile, err := os.Open(mapFileName(i, mrTask.ReduceNum))
		if err != nil {
			log.Fatalf("cannot read %v", inFile)
		}
		dec := json.NewDecoder(inFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(mrTask.ReduceNum)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	fmt.Println("Reduce Task finished.")
}

func mapFileName(mapTaskNum, reduceTaskNum int) string {
	return "mr-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(reduceTaskNum)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// // declare an argument structure.
	// args := ExampleArgs{}

	// // fill in the argument(s).
	// args.X = 99

	// // declare a reply structure.
	// reply := ExampleReply{}

	// // send the RPC request, wait for the reply.
	// call("Master.Example", &args, &reply)

	// // reply.Y should be 100.
	// fmt.Printf("reply.Y %v\n", reply.Y)
	args := 99
	reply := Task{}
	call("Master.RequestTask", &args, &reply)
	fmt.Printf("The Reply!!!::: %v\n", reply.Filename)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

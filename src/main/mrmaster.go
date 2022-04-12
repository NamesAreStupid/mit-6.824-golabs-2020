package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

// import "../mr"
import "mit-6.824-golabs-2020/src/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeMaster(os.Args[1:], 10)
	args := mr.DoneArgs{}
	reply := mr.DoneReply{}
	for m.Done(&args, &reply) != nil {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}

package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
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

	// Your worker implementation here.
	CallMaster()

}

func Map (args *Args, reply *Reply) {

    args.FileName = reply.FileName
    args.JobNumber = reply.JobNumber
    args.JobType = 0
    args.Inprogress = false
    args.Finished = true

    call("Master.JobDone", &args, &reply) 
}

func Reduce (args *Args, reply *Reply) {



    args.FileName = reply.FileName
    args.JN = reply.JN
    args.JobType = 1
    args.Inprogress = false
    args.Finished = true

    call("Master.JobDone", &args, &reply) 
}


func CallMaster() {
    args  := Args{}
    reply := Reply{}

    for {
        call("Master.RPCRequest", &args, &reply)
        
        if reply.JobType == 0 {
            fmt.Println("Reply From Master 1: ", reply)
            Map(&args, &reply)

        } else if reply.JobType == 1 {
            fmt.Println("Reply From Master 2: ", reply.FileName, reply.JN)
            Reduce(&args, &reply)

        } else if reply.JobType == 2 { break }
        
        time.Sleep(100 * time.Millisecond)
    }
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
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

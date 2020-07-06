package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
import "time"
import "strconv"


const mapPhase = "Map"
const reducePhase = "Reduce"

type Master struct {
	// Your definitions here.
    files []string
    state string
    NReucers int
    phase int
    tasks map [int]Job
    reduceTasks map [int]Job
    mux sync.Mutex    
}

func (m *Master) GetTaskToStart ()(Job) {
    
    jb := Job {
        FileName : "phase",
        JobNumber : -1,
        JobType : 0,
        JN : 0,
        Inprogress : false,
        Finished : false,
    } 

    if m.phase == 0 {
        for i := 0; i < len(m.tasks); i ++ {
            if m.tasks[i].Inprogress == false && m.tasks[i].Finished == false {
                
                if thisProduct, ok := m.tasks[i]; ok {
                    thisProduct.Inprogress = true
                    jb.Inprogress = true
                    m.tasks[i] = thisProduct
                }

                jb.FileName = m.tasks[i].FileName
                jb.JobNumber = m.tasks[i].JobNumber
                jb.JobType = 0
                return jb
            }
        }
    } else if m.phase == 1 {   
        for i := 0; i < len(m.tasks); i ++ { 
            if m.reduceTasks[i].Inprogress == false && m.reduceTasks[i].Finished == false { 
                
                if thisProduct, ok := m.tasks[i]; ok { 
                    thisProduct.Inprogress = true
                    jb.Inprogress = true
                    m.reduceTasks[i] = thisProduct
                }

                jb.JN = m.reduceTasks[i].JobNumber
                jb.FileName = "mr-out-" + strconv.Itoa(i)
                jb.JobNumber = m.reduceTasks[i].JobNumber
                jb.JobType = 1
                return jb
            }
        } 
    } else {
        jb.FileName = "Phase 3"
        return jb
    }
    return jb
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) RPCRequest(args *Args, reply *Reply) error { 

    m.mux.Lock()
    job := m.GetTaskToStart()
    m.mux.Unlock()

    reply.FileName = job.FileName
    reply.JobNumber = job.JobNumber 
    reply.JobType = job.JobType
    reply.JN = job.JN

    fmt.Println("reply : ",reply)
    
    if reply.JobNumber != -1 {

        go func(jobno int) {
            time.Sleep(5 * time.Second)
            m.mux.Lock()
            
            if m.phase == 0 && reply.JobType == 0 {
                if thisProduct, ok := m.tasks[jobno]; ok {
                    thisProduct.Inprogress = false
                    m.tasks[jobno] = thisProduct
                }
            } else if m.phase == 1 && reply.JobType == 1 {
                if thisProduct, ok := m.tasks[jobno]; ok {
                    thisProduct.Inprogress = false
                    m.reduceTasks[jobno] = thisProduct
                }
            }
            
            if m.IsPhaseCompleted() { 
                m.phase += 1
                time.Sleep(2 * time.Second)
            } 
            m.mux.Unlock()
            
        }(reply.JobNumber)  
    }
    
    return nil
}

// CheckSum
func (m *Master) IsPhaseCompleted() bool {
    
    if m.phase == 0 {
        for i := 0; i < len(m.tasks); i ++ {
            if  !m.tasks[i].Finished { return false }
        }
    } else if m.phase == 1 {
        for i := 0; i < len(m.reduceTasks); i ++ {
            if  !m.reduceTasks[i].Finished { return false }
        }
    }
    return true
}


func (m *Master) JobDone(args *Args, reply *Reply) error {
        
    if args.JobType == 0 {
        if P, ok := m.tasks[args.JobNumber]; ok {
           P.Inprogress = false
           P.Finished = true
           m.tasks[args.JobNumber] = P
        }

    }else if args.JobType == 1 {
        if P, ok := m.tasks[args.JobNumber]; ok {
           P.Inprogress = false
           P.Finished = true
           m.reduceTasks[args.JobNumber] = P
        }
    }
    
    return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil { log.Fatal("listen error:", e) }
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
    
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
    
    tasks := make(map[int]Job)
    reduceTasks := make(map[int]Job)
    
    for i := 0; i < len(files); i++ {
        tasks[i] = Job {
            FileName: files[i],
            JobNumber: i,
            JobType: 0,
            Inprogress: false,
            Finished: false,
        }
    }
    
    for i := 0; i < len(files); i++ {
        reduceTasks[i] = Job {
            FileName: "",
            JobNumber: i,
            JobType: 1,
            Inprogress: false,
            Finished: false,
        }  
    }
    
    m.tasks = tasks
    m.reduceTasks = reduceTasks
    m.NReucers = nReduce
    m.phase = 0

	m.server()
	return &m
}

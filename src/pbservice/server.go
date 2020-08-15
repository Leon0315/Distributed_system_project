package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"../viewservice"
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here
	viewshost  string // host viewserver's address
	keyValue   map[string]string
	cView      viewservice.View // current viewservice's view
	operations map[uint]PutAppendArgs
	history    map[int64]struct{}

	primaryhost bool
	backuphost  bool
	backupready bool

	primaryClerk *Clerk
	backupClerk  *Clerk

	primaylastno uint
	backuplastno uint
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.primaryhost && !pb.backuphost {
		reply.Err = ErrIdle
		reply.Value = ""
		return nil
	} else if pb.backuphost && !pb.backupready {
		reply.Err = ErrBackupNotReady
		reply.Value = ""
		return nil
	}

	if pb.primaryhost {
		if args.Viewnum != pb.cView.Viewnum && args.Isfp {
			reply.Err = ErrWrongView
			reply.Value = ""
			return nil
		} else if pb.cView.Backup != "" {
			tmpargs := GetArgs{Key: args.Key, Isfp: true, Viewnum: pb.cView.Viewnum}
			tmpreply := GetReply{}
			ok := pb.backupClerk.BackupGet(tmpargs, &tmpreply, pb.cView.Backup)
			if !ok {
				reply.Err = ErrLostBackup
				reply.Value = ""
				return nil
			} else {
				if tmpreply.Err == ErrWrongView {
					reply.Err = ErrWrongView
					reply.Value = ""
					return nil
				}
			}
		}
	}
	value, ok := pb.keyValue[args.Key]
	if !ok {
		reply.Err = ErrNoKey
	}
	reply.Value = value
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.backuphost && !pb.backupready {
		pb.operations[args.No] = *args
		return nil
	}

	_, err := pb.history[args.Id]
	if err {
		return nil
	}

	cv, ok := pb.keyValue[args.Key]

	if args.Op == "Put" {
		if pb.primaryhost {
			if pb.backupClerk != nil {
				pb.backupClerk.BackupPut(args.Key, args.Value, pb.primaylastno+1, args.Id)
			}
			pb.keyValue[args.Key] = "" + args.Value
			pb.primaylastno += 1
		}

		if pb.backuphost {
			if pb.backuplastno+1 != args.No {
				pb.operations[args.No] = *args
			} else {
				pb.keyValue[args.Key] = "" + args.Value
				pb.backuplastno += 1
			}
		}
	}

	if args.Op == "Append" {
		if pb.primaryhost {
			if pb.backupClerk != nil {
				pb.backupClerk.BackupAppend(args.Key, args.Value, pb.primaylastno+1, args.Id)
			}
			if ok {
				pb.keyValue[args.Key] = cv + args.Value
			} else {
				pb.keyValue[args.Key] = args.Value
			}
			pb.primaylastno += 1
		}

		if pb.backuphost {
			if pb.backuplastno+1 != args.No {
				pb.operations[args.No] = *args
			} else {
				if ok {
					pb.keyValue[args.Key] = cv + args.Value
				} else {
					pb.keyValue[args.Key] = args.Value
				}
				pb.backuplastno += 1
			}
		}
	}
	pb.history[args.Id] = struct{}{}
	return nil
}

func (pb *PBServer) GetDB(args *GetDBArgs, reply *GetDBReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	reply.Time = time.Now()

	if pb.primaryhost {
		keyValuecp := make(map[string]string)
		for key, value := range pb.keyValue {
			keyValuecp[key] = value
		}

		historycp := make(map[int64]struct{})
		for key, value := range pb.history {
			historycp[key] = value
		}

		reply.DBState = DBState{
			Data:    keyValuecp,
			History: historycp,
			Plastno: pb.primaylastno,
		}
		reply.Err = ""
	} else {
		reply.DBState = DBState{
			Data:    nil,
			History: nil,
			Plastno: 0,
		}
		reply.Err = ErrWrongServer
	}
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	view, error := pb.vs.Ping(pb.cView.Viewnum)

	if error != nil {
		return
	}

	if pb.cView == view {
		return
	}

	pb.mu.Lock()
	defer pb.mu.Unlock()

	curview := pb.cView
	pb.cView = view

	pb.primaryhost = pb.me == view.Primary
	pb.backuphost = pb.me == view.Backup

	changed := false
	if pb.primaryhost && curview.Primary != pb.me {
		changed = true
	}
	if pb.backuphost && curview.Backup != pb.me {
		changed = true
	}

	if pb.primaryhost {
		pb.primaryClerk = nil
		if view.Backup != "" {
			pb.backupClerk = MakeClerk(pb.viewshost, view.Backup)
		}
		pb.backupready = false
		pb.operations = make(map[uint]PutAppendArgs)
		if changed {
			pb.primaylastno = pb.backuplastno
		}
	}

	if pb.backuphost {
		pb.backupClerk = nil
		if view.Primary != "" {
			pb.primaryClerk = MakeClerk(pb.viewshost, view.Primary)
		}
		pb.backupready = false
		pb.operations = make(map[uint]PutAppendArgs)

		go func() {
			var ok bool
			reply := GetDBReply{}
			for {
				tmpargs := GetDBArgs{}
				tmpreply := GetDBReply{}

				if pb.primaryClerk != nil {
					ok = pb.primaryClerk.GetDB(tmpargs, &tmpreply)
					if ok && tmpreply.Err == "" {
						reply = tmpreply
						break
					} else {
						pb.primaryClerk.updateEndpoints()
					}
				}
				time.Sleep(time.Millisecond * 20) // sleep a while
			}

			pb.mu.Lock()
			defer pb.mu.Unlock()

			pb.history = reply.DBState.History
			pb.keyValue = reply.DBState.Data
			pb.backuplastno = reply.DBState.Plastno
			pb.backupready = true
		}()

		if pb.backupready {
			for {
				args, ok := pb.operations[pb.backuplastno+1]
				if !ok {
					break
				} else {
					_, hok := pb.history[args.Id]
					if hok {
						continue
					}

					value, ok := pb.keyValue[args.Key]

					switch args.Op {
					case Put:
						pb.keyValue[args.Key] = "" + args.Value
					case Append:
						if ok {
							pb.keyValue[args.Key] = value + args.Value
						} else {
							pb.keyValue[args.Key] = args.Value
						}
					}
					pb.backuplastno += 1
					pb.history[args.Id] = struct{}{}
				}
			}
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)

	// Your pb.* initializations here.
	pb.viewshost = vshost
	pb.cView = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
	pb.keyValue = make(map[string]string)
	pb.history = make(map[int64]struct{})
	pb.operations = make(map[uint]PutAppendArgs)

	pb.primaryhost = false
	pb.backuphost = false
	pb.backupready = false

	pb.primaryClerk = nil
	pb.backupClerk = nil

	pb.primaylastno = 0
	pb.backuplastno = 0

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}

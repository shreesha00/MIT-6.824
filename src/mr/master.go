package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const UNASSIGNED = 0
const RUNNING = 1
const DONE = 2

type Master struct {
	// Your definitions here.
	NMaps        int         // number of Map tasks. Equivalent to number of files
	NReduce      int         // number of Reduce tasks. Equivalent to nReduce
	MapFiles     []string    // list of Map files
	MapStatus    map[int]int // status of each Map task. 0 -> unassigned, 1 -> running, 2 -> done
	ReduceStatus map[int]int // status of each Reduce task. 0 -> unassigned, 1 -> running, 2 -> done
	lock         sync.Mutex  // lock for the Master structure
	cond         *sync.Cond  // condition variable on lock
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) Task(args *AskArgs, reply *AskReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	for {
		finished_map := 0
		running_map := 0
		var map_task, map_status int
		for map_task, map_status = range m.MapStatus {
			if map_status == DONE {
				finished_map++
			} else if map_status == RUNNING {
				running_map++
			}
		}

		if finished_map == m.NMaps { // All Map tasks have finished
			var reduce_task, reduce_status int
			finished_reduce := 0
			running_reduce := 0
			for reduce_task, reduce_status = range m.ReduceStatus {
				if reduce_status == DONE {
					finished_reduce++
				} else if reduce_status == RUNNING {
					running_reduce++
				}
			}

			if finished_reduce == m.NReduce { // All tasks done. Request worker to exit
				//
				// fill the reply data
				//
				reply.TaskType = EXITTASK
				reply.MapTaskNumber = -1
				reply.ReduceTaskNumber = -1
				reply.NReduce = m.NReduce
				reply.MapFilename = ""
				reply.NMaps = m.NMaps
				reply.IntermediateFiles = []string{}
				return nil
			}

			if finished_reduce+running_reduce == m.NReduce { // if there does not exist an unassigned reduce task
				m.cond.Wait()
				continue
			}

			//
			// obtain an unassigned reduce task
			//
			for reduce_task, reduce_status = range m.ReduceStatus {
				if reduce_status == UNASSIGNED {
					break
				}
			}

			//
			// fill the reply data
			//
			reply.TaskType = REDUCETASK
			reply.MapTaskNumber = -1
			reply.ReduceTaskNumber = reduce_task
			reply.NReduce = m.NReduce
			reply.MapFilename = ""
			reply.NMaps = m.NMaps

			filenames := []string{}
			for i := 0; i < m.NMaps; i++ {
				filename := fmt.Sprintf("mr-%v-%v.json", i, reduce_task)
				filenames = append(filenames, filename)
			}
			reply.IntermediateFiles = filenames

			m.ReduceStatus[reduce_task] = RUNNING
			go m.taskTimeout(REDUCETASK, reduce_task)

			return nil

		}
		if finished_map+running_map == m.NMaps { // if there does not exist an unassigned Map task
			m.cond.Wait()
			continue
		}

		//
		// obtain an unassigned map task
		//
		for map_task, map_status = range m.MapStatus {
			if map_status == UNASSIGNED {
				break
			}
		}

		//
		// fill the reply data
		//
		reply.TaskType = MAPTASK
		reply.MapTaskNumber = map_task
		reply.ReduceTaskNumber = -1
		reply.NReduce = m.NReduce
		reply.MapFilename = m.MapFiles[map_task]
		reply.NMaps = m.NMaps
		reply.IntermediateFiles = []string{}

		m.MapStatus[map_task] = RUNNING
		go m.taskTimeout(MAPTASK, map_task)

		return nil
	}
}

func (m *Master) taskTimeout(tasktype int, task int) {
	timer := time.NewTimer(10 * time.Second)

	<-timer.C

	m.lock.Lock()
	defer m.lock.Unlock()

	if tasktype == MAPTASK {
		if m.MapStatus[task] == RUNNING {
			m.MapStatus[task] = UNASSIGNED
			m.cond.Broadcast()
		}
	} else {
		if m.ReduceStatus[task] == RUNNING {
			m.ReduceStatus[task] = UNASSIGNED
			m.cond.Broadcast()
		}
	}
}

func (m *Master) Completed(args *CompletedArgs, reply *CompletedReply) error {

	m.lock.Lock()
	defer m.lock.Unlock()

	switch args.TaskType {
	case MAPTASK:
		{
			m.MapStatus[args.MapTaskNumber] = DONE
			m.cond.Broadcast()
		}
	case REDUCETASK:
		{
			m.ReduceStatus[args.ReduceTaskNumber] = DONE
			m.cond.Broadcast()
		}
	default:
		{
			fmt.Println("Invalid Case") // does not occur
		}
	}

	return nil
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
	ret := false

	// Your code here.
	m.lock.Lock()
	map_finished := 0
	reduce_finished := 0
	for _, status := range m.MapStatus {
		if status == DONE {
			map_finished++
		}
	}
	for _, status := range m.ReduceStatus {
		if status == DONE {
			reduce_finished++
		}
	}
	if map_finished == m.NMaps && reduce_finished == m.NReduce {
		ret = true
	}
	m.lock.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	// initialization
	m.lock = sync.Mutex{}
	m.cond = sync.NewCond(&m.lock)
	m.MapFiles = files
	m.NReduce = nReduce
	m.NMaps = len(files)
	m.MapStatus = make(map[int]int)
	m.ReduceStatus = make(map[int]int)
	for i := 0; i < m.NMaps; i++ {
		m.MapStatus[i] = UNASSIGNED
	}
	for i := 0; i < m.NReduce; i++ {
		m.ReduceStatus[i] = UNASSIGNED
	}

	m.server()
	return &m
}

package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		reply, status := CallTask()

		if !status {
			break
		}

		switch reply.TaskType {
		case MAPTASK:
			{
				//
				// open the file and read its contents
				//
				file, err := os.Open(reply.MapFilename)
				if err != nil {
					log.Fatalf("cannot open %v", reply.MapFilename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.MapFilename)
				}
				file.Close()
				kva := mapf(reply.MapFilename, string(content))

				//
				// partition intermediate output into NReduce files by using (hash(key) % NReduce) as a partitioning function
				//
				files := []*os.File{}
				filenames := []string{}
				encoders := []*json.Encoder{}

				for i := 0; i < reply.NReduce; i++ {
					filename := fmt.Sprintf("mr-%v-%v-%v.json", reply.MapTaskNumber, i, os.Getpid())
					file, _ := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					files = append(files, file)
					encoders = append(encoders, json.NewEncoder(file))
				}

				for _, kv := range kva {
					ind := ihash(kv.Key) % reply.NReduce
					encoders[ind].Encode(&kv)
				}

				for i, file := range files {
					file.Close()
					newFilename := fmt.Sprintf("mr-%v-%v.json", reply.MapTaskNumber, i)
					err := os.Rename(file.Name(), newFilename)
					filenames = append(filenames, newFilename)
					if err != nil {
						log.Fatalf("cannot atomically rename temporary file to %v", newFilename)
					}
				}

				args := CompletedArgs{}
				args.MapTaskNumber = reply.MapTaskNumber
				args.ReduceTaskNumber = -1
				args.TaskType = MAPTASK
				args.IntermediateFiles = filenames

				status := CallCompleted(args)
				if !status {
					break
				}
			}
		case REDUCETASK:
			{
				// obtain all intermediate output for given reduce task and store it in an intermediate structure
				intermediate := []KeyValue{}

				for i := 0; i < reply.NMaps; i++ {
					IntermediateFilename := reply.IntermediateFiles[i]
					file, err := os.Open(IntermediateFilename)
					if err != nil {
						log.Fatalf("cannot open intermediate file %v", IntermediateFilename)
					}

					decoder := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := decoder.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
				}

				// sort intermediate structure by key
				sort.Sort(ByKey(intermediate))

				// create temporary file
				oname := fmt.Sprintf("mr-out-%v-%v", reply.ReduceTaskNumber, os.Getpid())
				ofile, _ := os.Create(oname)

				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to output file.
				//
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}

				// close the temporary file and atomically rename to the final name
				ofile.Close()
				final_oname := fmt.Sprintf("mr-out-%v", reply.ReduceTaskNumber)
				err := os.Rename(ofile.Name(), final_oname)
				if err != nil {
					log.Fatalf("cannot atomically rename temporary file to %v", final_oname)
				}

				args := CompletedArgs{}
				args.MapTaskNumber = -1
				args.ReduceTaskNumber = reply.ReduceTaskNumber
				args.TaskType = REDUCETASK
				args.IntermediateFiles = []string{}

				status := CallCompleted(args)
				if !status {
					break
				}
			}
		case EXITTASK:
			{
				break
			}
		default:
			fmt.Println("Invalid") // does not occur
		}
	}

}

// function to make an RPC call to the master requesting for a task
func CallTask() (reply AskReply, status bool) {
	reply = AskReply{}
	args := AskArgs{}

	status = call("Master.Task", &args, &reply)

	return
}

func CallCompleted(args CompletedArgs) (status bool) {
	reply := CompletedReply{}

	status = call("Master.Completed", &args, &reply)

	return
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

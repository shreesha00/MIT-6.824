package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0
const TIMEOUT_PERIOD = 3 * raft.HTC

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string // key in the operation
	Value     string // value in the operation
	Type      string // operation type, one of PutOp, GetOp or AppendOp. If GetOp, Value field is populated as empty
	ClerkId   int64  // id of the client that requested this operation
	RequestId int64  // id of the request within the client
}

// Structure used by the ApplyChannelReader thread to signal waiting Get/Put/Append operations
// This structure contains the required results of the executed operation
type Result struct {
	Err       Err    // whether the operation succeeded
	Value     string // value returned by operation
	ClerkId   int64  // id of the client that requested this operation
	RequestId int64  // id of the request within the client
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dict            map[string]string   // the kv store
	perIndexChannel map[int]chan Result // per index channel to signal a waiting RPC
	lastApplied     map[int64]int64     // last put or append request index applied by each client
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// populate operation structure
	newOperation := Op{
		Key:       args.Key,
		Value:     "",
		Type:      GetOp,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}

	// start replication on the given operation
	index, _, isLeader := kv.rf.Start(newOperation)

	// if not leader, return immediately
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}

	// Wait for the raft layer to replicate the given operation
	ok, result := kv.WaitForIndex(index, newOperation)

	// if ok is false, this means that the operation timed out. The client can re-issue the operation through another server
	if !ok {
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}

	// populate the reply structure using the results of the applied operation
	reply.Err = result.Err
	reply.Value = result.Value
}

// Function that waits on the per index channel to obtain results from the ApplyChannelReader thread after replicating the particular operation
func (kv *KVServer) WaitForIndex(index int, operation Op) (bool, Result) {

	// get the channel for the corresponding index
	channel := kv.GetChannelEntry(index)

	// select statement is used to ensure that we don't wait forever on the channel if this node gets partitioned and hence is not able to replicate
	// the timer gets signalled in such a case and we can tell the client to retry with a different server
	select {
	case result := <-channel:
		{
			// erase memory for the per index channel map as it will no longer be used for this index
			kv.RemoveChannelEntry(index)

			// return success only if the returned operation is identical to issued operation
			return (result.ClerkId == operation.ClerkId) && (result.RequestId == operation.RequestId), result
		}
	case <-time.After(time.Duration(TIMEOUT_PERIOD) * time.Millisecond):
		// let the client know that it can retry with a different server
		return false, Result{
			Err: ErrWrongLeader,
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// populate operation structure
	newOperation := Op{
		Key:       args.Key,
		Value:     args.Value,
		Type:      args.Op,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
	}

	// start replication on the given operation
	index, _, isLeader := kv.rf.Start(newOperation)

	// if not leader, return immediately
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Wait for the raft layer to replicate the given operation
	ok, result := kv.WaitForIndex(index, newOperation)

	// if ok is false, this means that the operation timed out. The client can re-issue the operation through another server
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	// successful operation
	reply.Err = result.Err
}

// Function to get channel entry for an index. Creates a channel if it doesn't already exist and returns the same
func (kv *KVServer) GetChannelEntry(index int) chan Result {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	channel, ok := kv.perIndexChannel[index]

	// create channel entry if it doesn't exist
	if !ok {
		channel = make(chan Result, 1)
		kv.perIndexChannel[index] = channel
	}

	return channel
}

// Function to remove a channel entry for an index
func (kv *KVServer) RemoveChannelEntry(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.perIndexChannel, index)
}

// Thread to apply successfully replicated operations as they appear on the Apply channel
func (kv *KVServer) ApplyChannelReader() {
	for msg := range kv.applyCh {
		// non-applyable operation
		if !msg.CommandValid {
			continue
		}

		// get channel corresponding to the operation index
		channel := kv.GetChannelEntry(msg.CommandIndex)

		// get the operation to be applied
		operation := msg.Command.(Op)

		result := Result{
			ClerkId:   operation.ClerkId,
			RequestId: operation.RequestId,
			Err:       OK,
			Value:     "",
		}
		if operation.Type == GetOp {
			// apply operation and get the result
			kv.ApplyOperation(operation, &result)
		} else {
			// duplicate detection logic
			lastApplied, ok := kv.lastApplied[operation.ClerkId]

			// apply only newer operations
			if !ok || operation.RequestId > lastApplied {
				// apply operation and get the result
				kv.ApplyOperation(operation, &result)

				// update last applied put or append request Id for this client
				kv.lastApplied[operation.ClerkId] = operation.RequestId
			}
		}

		// signal result on the corresponding channel
		channel <- result
	}
}

// Function to apply a given operation, modify the state if needed and populate the result structure
func (kv *KVServer) ApplyOperation(operation Op, result *Result) {
	if operation.Type == GetOp {
		value, ok := kv.dict[operation.Key]
		if !ok {
			result.Err = ErrNoKey
		} else {
			result.Value = value
		}
	} else if operation.Type == PutOp {
		kv.dict[operation.Key] = operation.Value
	} else {
		kv.dict[operation.Key] += operation.Value
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dict = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.perIndexChannel = make(map[int]chan Result)

	go kv.ApplyChannelReader()
	return kv
}

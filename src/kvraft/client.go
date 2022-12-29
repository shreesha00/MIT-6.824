package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"

	"../labrpc"
)

type Clerk struct {
	me           int64               // id of the clerk
	servers      []*labrpc.ClientEnd // list of servers
	cachedLeader int64               // last seen leader
	idMutex      sync.Mutex          // mutex to assign ids of operations
	maxRequestId int64               // maximum request id assigned at any point
	leaderMutex  sync.Mutex          // mutex to synchronize access to the cached leader

	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.me = nrand()
	ck.maxRequestId = 0
	ck.cachedLeader = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// populate argument structure
	args := GetArgs{
		Key:       key,
		ClerkId:   ck.me,
		RequestId: ck.GetRequestId(),
	}

	// reply structure
	var reply GetReply

	// first contact previous known leader
	i := atomic.LoadInt64(&ck.cachedLeader)
	value := ""
	for ; ; i = (i + 1) % int64(len(ck.servers)) {
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader { // in case of a successful reply
			if reply.Err == "OK" {
				value = reply.Value
			}
			break
		}
	}

	// update last known leader
	atomic.StoreInt64(&ck.cachedLeader, i)
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// populate argument structure
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.me,
		RequestId: ck.GetRequestId(),
	}

	// reply structure
	var reply PutAppendReply

	// first contact previous known leader
	i := atomic.LoadInt64(&ck.cachedLeader)
	for ; ; i = (i + 1) % int64(len(ck.servers)) {
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			break
		}
	}

	// update last known leader
	atomic.StoreInt64(&ck.cachedLeader, i)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) GetRequestId() int64 {
	atomic.AddInt64(&ck.maxRequestId, 1)
	newId := atomic.LoadInt64(&ck.maxRequestId)
	return newId
}

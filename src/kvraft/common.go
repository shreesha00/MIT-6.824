package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	PutOp    = "Put"
	GetOp    = "Get"
	AppendOp = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClerkId   int64
	RequestId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClerkId   int64
	RequestId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

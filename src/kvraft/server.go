package kvraft

import (
	"labgob"
	"labrpc"
	"mylabs/src/raft"
	"sync"
	"sync/atomic"
	"time"
)

type OpType string

const (
	GetOp    OpType = "Get"
	PutOp    OpType = "Put"
	AppendOp OpType = "Append"
)

const RaftTimeout = time.Millisecond * 500

// Op is the command which will be committed in the Raft Log.
type Op struct {
	Key       string
	Value     string
	OpType    OpType
	RequestID int64
	ClientID  int64
}

type Result struct {
	Op
	raft.ApplyMsg
	ResultValue string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	data        map[string]string
	idToChannel map[int]chan Result // after submitting a command, which channel to watch on
	ack         map[int64]int64

	maxraftstate int // snapshot if log grows this big

}

func (kv *KVServer) GetLeaderAndTerm(args *GetLeaderArgs, reply *GetLeaderReply) {
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	reply.CurrentTerm = term
	return
}

func (kv *KVServer) processOp(op Op) (Result, Err) {
	var targetChannel chan Result
	var ok bool

	expectedIndex, expectedTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		return Result{}, ErrWrongLeader
	}

	kv.mu.Lock()
	targetChannel, ok = kv.idToChannel[expectedIndex]
	if ! ok {
		targetChannel = make(chan Result)
		kv.idToChannel[expectedIndex] = targetChannel
	}
	kv.mu.Unlock()

	// Now we need to wait for this Op to appear on the applyCh channel
	// Once we receive some Op, we need to check the following:
	// : If the index matches but not the command, this Server is no longer leader, return failure.
	// : If command and index match, process the command.
	// : A timeout (of this Server waiting for Raft) may also occur and needs to be taken care of.
	var msg Result
	timeout := make(chan int)
	go func() {
		time.Sleep(RaftTimeout)
		close(timeout)
	}()
	select {
	case msg = <-targetChannel:
		currentTerm, isLeader := kv.rf.GetState()
		if currentTerm != expectedTerm || !isLeader {
			return Result{}, ErrTermMismatch
		}
		// If this is the expected index but we got a different command from Raft Leader,
		// we need to submit again.
		returnedOp := msg.Command.(Op)
		if returnedOp != op {
			return Result{}, ErrDiffCmdSameIndex
		}
	case <-timeout:
		// we failed to get any response at all from the Raft cluster, retry with a new leader
		return Result{}, ErrRaftTimeout
	}
	return msg, OK
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key:       args.Key,
		OpType:    GetOp,
		RequestID: args.RequestID,
		ClientID:  args.ClientID,
	}
	if result, err := kv.processOp(op); err == OK {
		reply.Value = result.ResultValue
		reply.Err = OK
	} else {
		reply.Err = err
	}
	return
}

// PutAppend RPC handler. It can be called concurrently and so must handle
// duplicate requests.
// Must also handle when this KVServer is Raft leader but crashes before it can
// replicate to peers or gets disconnected from the network.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		OpType:    PutOp,
		RequestID: args.RequestID,
		ClientID:  args.ClientID,
	}
	if args.Op == "Append" {
		op.OpType = AppendOp
	}
	_, reply.Err = kv.processOp(op)
	return
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
	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.idToChannel = make(map[int]chan Result)

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.watchForApplyCh()

	return kv
}

func (kv *KVServer) watchForApplyCh() {
	ticker := time.NewTicker(RaftTimeout)
	defer ticker.Stop()
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			op := msg.Command.(Op)
			result := kv.applyOp(op)
			result.ApplyMsg = msg
			channel, present := kv.idToChannel[msg.CommandIndex]
			if present {
				// If this applyCh message comes in after RPC handler of the same request times out,
				// then no one is listening on this channel and this statement will block.
				// Need to provide a default case for this scenario.
				select {
				case channel <- result:
				default:
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) dedup(op Op) bool {
	lastRequestID, ok := kv.ack[op.ClientID]
	if ok && lastRequestID >= op.RequestID {
		return true
	}
	kv.ack[op.ClientID] = op.RequestID
	return false
}

func (kv *KVServer) applyOp(op Op) Result {
	var result Result
	result.Op = op
	oldval, oldok := kv.data[op.Key]
	if oldok {
		result.ResultValue = oldval
	} else {
		oldval = ""
	}
	switch op.OpType {
	case GetOp:
		result.ResultValue = oldval
	default:
		if kv.dedup(op) {
			return result
		}
		if op.OpType == PutOp {
			kv.data[op.Key] = op.Value
		} else {
			kv.data[op.Key] = oldval + op.Value
		}
	}
	return result
}

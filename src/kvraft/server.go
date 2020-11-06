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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	data        map[string]string
	idToChannel map[int64]chan raft.ApplyMsg // after submitting a command, which channel to watch on
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

func (kv *KVServer) processOp(op Op) (err Err) {
	var targetChannel chan raft.ApplyMsg
	kv.dlog("Processing op %+v", op)
	kv.mu.Lock()
	kv.dlog("Submitting op %+v to Start()", op)
	expectedIndex, expectedTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.dlog("is not the leader")
		err = ErrWrongLeader
		return
	}
	targetChannel, ok := kv.idToChannel[op.RequestID]
	if ! ok {
		targetChannel = make(chan raft.ApplyMsg)
		kv.idToChannel[op.RequestID] = targetChannel
	}
	kv.dlog("releasing lock for op %+v", op)
	kv.mu.Unlock()

	// Now we need to wait for this Op to appear on the applyCh channel
	// Once we receive some Op, we need to check the following:
	// 1. If this command's index is what we expect, if not, we feed it back into the applyCh
	// 2. If the index matches but not the command, this Server is no longer leader, return failure
	// 3. If command and index match, process the command.
	// A timeout (of this Server waiting for Raft) may also occur and needs to be taken care of.
	var msg raft.ApplyMsg
	timeout := make(chan int)
	go func() {
		time.Sleep(RaftTimeout)
		close(timeout)
	}()
	kv.dlog("Now waiting on the targetchannel for op %+v", op)
	select {
	case msg = <-targetChannel:
		kv.dlog("Received %+v on the channel", msg)
		currentTerm, isLeader := kv.rf.GetState()
		if currentTerm != expectedTerm || !isLeader {
			err = ErrTermMismatch
			return
		}
		if msg.CommandIndex != expectedIndex {
			err = ErrIndexSkipped
			return
		}
		// If this is the expected index but we got a different command from Raft Leader,
		// we need to submit again.
		returnedOp := msg.Command.(Op)
		if returnedOp != op {
			err = ErrDiffCmdSameIndex
			return
		}
		err = OK
	case <-timeout:
		kv.dlog("Op %+v timed out..", op)
		// we failed to get any response at all from the Raft cluster, retry with a new leader
		err = ErrRaftTimeout
	}
	return err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key:       args.Key,
		OpType:    GetOp,
		RequestID: args.RequestID,
		ClientID:  args.ClientID,
	}
	if err := kv.processOp(op); err == OK {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		val, ok := kv.data[op.Key]
		if !ok {
			val = ""
		}
		reply.Value = val
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
	reply.Err = kv.processOp(op)
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
	labgob.Register(GetLeaderArgs{})
	labgob.Register(GetLeaderReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.idToChannel = make(map[int64]chan raft.ApplyMsg)

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
			kv.dlog("acquired the lock for msg %+v..", msg)
			op := msg.Command.(Op)
			kv.applyOp(op)
			channel, present := kv.idToChannel[op.RequestID]
			if present {
				kv.dlog("sending to the channel..")
				// If this applyCh message comes in after RPC handler of the same request times out,
				// then no one is listening on this channel and this statement will block.
				// Need to provide a default case for this scenario.
				select {
				case channel <- msg:
				default:
					kv.dlog("unable to send to channel..")
				}
				kv.dlog("out of select...")
			}
			kv.dlog("releasing the lock for msg %+v...", msg)
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

func (kv *KVServer) applyOp(op Op) {
	if kv.dedup(op) {
		return
	}
	switch op.OpType {
	case GetOp:
	default:
		oldval, oldok := kv.data[op.Key]
		if !oldok {
			oldval = ""
		}
		if op.OpType == PutOp {
			kv.data[op.Key] = op.Value
		} else {
			kv.data[op.Key] = oldval + op.Value
		}
	}
}

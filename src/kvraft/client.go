package kvraft

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync/atomic"
	"time"
)

const (
	GetRPCName       = "KVServer.Get"
	PutAppendRPCName = "KVServer.PutAppend"
)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	currentLeader int
	clientID	int64
}

var counter int64

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func nextId() int64 {
	return atomic.AddInt64(&counter, 1)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.currentLeader = -1
	ck.clientID = nrand()
	return ck
}

func (ck *Clerk) findTermAndLeader() {
	var getLeaderReply GetLeaderReply
	for {
		for i, _ := range ck.servers {
			ok := ck.servers[i].Call("KVServer.GetLeaderAndTerm", &GetLeaderArgs{}, &getLeaderReply)
			if ok && getLeaderReply.Err == OK {
				ck.currentLeader = i
				return
			}
		}
		time.Sleep(time.Millisecond * 500)
	}
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
	args := GetArgs{
		Key: key,
		RequestID:  nextId(),
		ClientID: ck.clientID,
	}
	var reply GetReply
	ck.makeRPC(GetRPCName, &args, &reply)
	return reply.Value
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
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		RequestID:  nextId(),
		ClientID: ck.clientID,
	}
	var reply PutAppendReply
	ck.makeRPC(PutAppendRPCName, &args, &reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) makeRPC(rpcName string, args interface{}, reply interface{}) {
	if ck.currentLeader == -1 {
		ck.findTermAndLeader()
	}
	for {
		var err Err = OK
		ok := ck.servers[ck.currentLeader].Call(rpcName, args, reply)
		if ok {
			switch rpcName {
			case GetRPCName:
				val := reply.(*GetReply)
				err = val.Err
			case PutAppendRPCName:
				val := reply.(*PutAppendReply)
				err = val.Err
			}
			if err == OK {
				return
			}
		}
		if !ok || err != OK {
			ck.dlog("RPC %s failed with err=%+v ok = %v, will find leader and try again", rpcName, err, ok)
			ck.findTermAndLeader()
		}
	}
}

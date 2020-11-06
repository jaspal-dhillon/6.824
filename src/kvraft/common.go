package kvraft

import (
	"fmt"
	"log"
)

const KVDebug = false

func (ck *Clerk) dlog(format string, args ...interface{}) {
	if !KVDebug {
		return
	}
	format = fmt.Sprintf("[CLERK %p] ", ck) + format
	log.Printf(format, args...)
}

func (kv *KVServer) dlog(format string, args ...interface{}) {
	if !KVDebug {
		return
	}
	format = fmt.Sprintf("[KVServer %d] ", kv.me) + format
	log.Printf(format, args...)
}

const (
	OK                  = "OK"
	ErrRaftTimeout      = "ErrRaftTimeout"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrIndexSkipped     = "ErrIndexSkipped"
	ErrDiffCmdSameIndex = "ErrDiffCmdSameIndex"
	ErrTermMismatch     = "ErrTermMismatch"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	RequestID int64
	ClientID	int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	RequestID int64
	ClientID	int64
}

type GetReply struct {
	Err   Err
	Value string
}

type GetLeaderArgs struct {
}

type GetLeaderReply struct {
	Err         Err
	CurrentTerm int
}

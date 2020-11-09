package raft

import (
	"bytes"
	"fmt"
	"labgob"
	"log"
	"sync/atomic"
	"time"
)

const RaftDebug = false

func (rf *Raft) dlog(format string, args ...interface{}) {
	if !RaftDebug {
		return
	}
	format = fmt.Sprintf("[%d] ", rf.id) + format
	log.Printf(format, args...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) SaveSnapshot(serverData []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// We will truncate all logs from starting till the lastIncludedIndex
	// Then update the rf.lastInitial{Index,Term}
	// Then save the Raft and Server data to persistent storage aka persister.
	oldlen := len(rf.log)
	olds := lastIncludedIndex-(rf.lastLogInitialIndex+1)
	rf.lastLogInitialTerm = rf.log[(lastIncludedIndex - (rf.lastLogInitialIndex + 1))].Term
	rf.log = rf.log[(lastIncludedIndex-(rf.lastLogInitialIndex+1))+1:]
	rf.lastLogInitialIndex = lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.getRaftData(), serverData)
	rf.dlog("old len of log = %d, new len = %d, new initial index %d term %d" +
		" logs truncated from %d", oldlen, len(rf.log),
		lastIncludedIndex, rf.lastLogInitialTerm, olds)
	// Snapshot saved successfully!
}

func (rf *Raft) getRaftData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.lastLogInitialIndex)
	e.Encode(rf.lastLogInitialTerm)
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getRaftData())
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.votedFor = -1
		rf.currentTerm = 0
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastInitialTerm, lastInitialIndex int
	var log []Log
	if d.Decode(&votedFor) != nil || d.Decode(&currentTerm) != nil || d.Decode(&log) != nil || d.Decode(&lastInitialIndex) != nil || d.Decode(&lastInitialTerm) != nil {
		panic("Decoding error..")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastLogInitialIndex = lastInitialIndex
		rf.lastLogInitialTerm = lastInitialTerm
		rf.commitIndex = lastInitialIndex
		rf.lastApplied = lastInitialIndex
		rf.dlog("Loading currentTerm=%d votedFor=%d commitIndex=%d", currentTerm, votedFor, lastInitialIndex)
	}
	// Also adjust lastTerm, lastIndex for AppendEntry RPC consistency check.
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) becomeFollower(Term int) {
	rf.dlog("becomes Follower with term=%d; log=%v", Term, rf.log)
	rf.votedFor = -1
	rf.currentTerm = Term
	rf.persist()
	rf.electionResetEvent = time.Now()
	rf.state = Follower

	go rf.runElectionTimer()
}

// expect rf.mu to be Locked
func (rf *Raft) getLastIndexAndTerm() (int, int) {
	if len(rf.log) > 0 {
		lastIndex := len(rf.log) - 1
		lastTerm := rf.log[lastIndex].Term
		return lastIndex + rf.lastLogInitialIndex + 1, lastTerm
	}
	return rf.getLastInitialIndexAndTerm()
}

func (rf *Raft) getLastInitialIndexAndTerm() (int, int) {
	return rf.lastLogInitialIndex, rf.lastLogInitialTerm
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type SnapshotArgs struct {
	Term            int
	InitialLogIndex int
	InitialLogTerm  int
	ServerData      []byte
}

type SnapshotReply struct {
	Term int
}

func (rf *Raft) sendSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	rf.lastLogInitialIndex = args.InitialLogIndex
	rf.lastLogInitialTerm = args.InitialLogTerm
	rf.lastApplied = args.InitialLogIndex
	rf.commitIndex = args.InitialLogIndex
	rf.log = rf.log[0:0]
	rf.persister.SaveStateAndSnapshot(rf.getRaftData(), args.ServerData)
	rf.applyCh <- ApplyMsg{ReadSnapshotFromPersister: true, CommandValid: false}
}

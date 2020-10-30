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

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.votedFor = -1
		rf.currentTerm = 0
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log []Log
	if d.Decode(&votedFor) != nil || d.Decode(&currentTerm) != nil || d.Decode(&log) != nil {
		panic("Decoding error..")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.dlog("Loading currentTerm=%d votedFor=%d log=%v", currentTerm, votedFor, log)
	}
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
		return lastIndex, lastTerm
	}
	return -1, -1
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

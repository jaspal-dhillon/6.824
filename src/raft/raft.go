package raft

import (
	"bytes"
	"fmt"
	"labgob"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

const (
	HeartbeatsInterval            = 50 * time.Millisecond
	MinimumElectionTimeout        = time.Duration(150) * time.Millisecond
	RandomElectionTimeoutInterval = 400
)

type Log struct {
	Command interface{}
	Term    int
	Noop    bool
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
	Dead
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("InvalidState")
	}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const RaftDebug = false

func (rf *Raft) dlog(format string, args ...interface{}) {
	if !RaftDebug {
		return
	}
	format = fmt.Sprintf("[%d] ", rf.id) + format
	log.Printf(format, args...)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	id        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan<- ApplyMsg     // to notify tester that this Raft peer has committed a log entry

	// Persistent Raft state on all servers
	currentTerm int
	votedFor    int
	log         []Log

	// Volatile state on all servers
	commitIndex        int
	lastApplied        int
	state              RaftState
	electionResetEvent time.Time

	// Volatile state on all leaders
	nextIndex  []int
	matchIndex []int
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Optimization on page 8.
	ConflictIndex int
	ConflictTerm  int
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dlog("AppendEntries: %+v", args)
	if args.Term > rf.currentTerm {
		rf.dlog("... term out of date in AppendEntries")
		rf.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rf.currentTerm {
		if rf.state != Follower {
			rf.becomeFollower(args.Term)
		}
		rf.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0
			for {
				if newEntriesIndex >= len(args.Entries) || logInsertIndex >= len(rf.log) {
					break
				}
				if rf.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				newEntriesIndex++
				logInsertIndex++
			}
			if newEntriesIndex < len(args.Entries) {
				rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
			}
			rf.persist()
			rf.dlog("AE RPC handler: new log=%+v LeaderCommit=%d commitIndex=%d", rf.log, args.LeaderCommit, rf.commitIndex)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
				rf.dlog("AE RPC handler: setting commitIndex to %d", rf.commitIndex)
			}
		} else {
			if args.PrevLogIndex >= len(rf.log) {
				reply.ConflictIndex = len(rf.log)
				reply.ConflictTerm = -1
			} else {
				// PrevLogTerm at PrevLogIndex does not match.
				// Return the first index of this term.
				reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if rf.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}
	reply.Term = rf.currentTerm
	rf.dlog("AppendEntries reply: %+v", *reply)
	return
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, rf.currentTerm, rf.votedFor)
	if args.Term > rf.currentTerm {
		rf.dlog("... term out of date in RequestVote")
		rf.becomeFollower(args.Term)
	}

	reply.VoteGranted = false
	lastIndex, lastTerm := rf.getLastIndexAndTerm()
	if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.electionResetEvent = time.Now()
	}
	reply.Term = rf.currentTerm
	rf.dlog("... RequestVote reply: %+v", reply)
	return
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.dlog("Submit received by %v: %v", rf.state, command)
	if rf.state != Leader {
		return -1, -1, false
	}
	rf.log = append(rf.log, Log{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.persist()
	lastIndex, lastTerm := rf.getLastIndexAndTerm()
	rf.dlog("leader log=%v, returning %d %d", rf.log, lastIndex, lastTerm)
	return lastIndex, lastTerm, true
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

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[id]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.id = me
	rf.applyCh = applyCh

	rf.state = Follower
	rf.votedFor = -1
	rf.lastApplied = -1
	rf.commitIndex = -1
	rf.electionResetEvent = time.Now()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.readPersist(persister.ReadRaftState())

	go rf.runElectionTimer()
	go rf.updateApplyChan()

	return rf
}

func (rf *Raft) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return MinimumElectionTimeout
	} else {
		return MinimumElectionTimeout + time.Duration(rand.Intn(RandomElectionTimeoutInterval))*time.Millisecond
	}
}

// single-shot election
func (rf *Raft) runElectionTimer() {
	timeout := rf.electionTimeout()
	rf.mu.Lock()
	termStarted := rf.currentTerm
	rf.mu.Unlock()
	rf.dlog("election timer started (%v), term=%d", timeout, termStarted)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		rf.mu.Lock()

		if rf.state != Follower && rf.state != Candidate {
			rf.dlog("in election timer state=%s, bailing out", rf.state)
			rf.mu.Unlock()
			return
		}

		if rf.currentTerm != termStarted {
			rf.dlog("in election timer term changed from %d to %d, bailing out", termStarted, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

		if elapsed := time.Since(rf.electionResetEvent); elapsed >= timeout {
			rf.startElection()
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
	}
}

// This should be called holding the rf.mu Lock
func (rf *Raft) startElection() {

	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.id
	rf.persist()
	rf.electionResetEvent = time.Now()
	savedCurrentTerm := rf.currentTerm
	lastIndex, lastTerm := rf.getLastIndexAndTerm()
	rf.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, rf.log)

	var votes int32 = 1

	for peerIndex := range rf.peers {
		if peerIndex == rf.id {
			continue
		}
		go func(id int) {
			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateID:  rf.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			reply := RequestVoteReply{}
			rf.dlog("sending RequestVote to %d: %+v", id, args)
			if ok := rf.sendRequestVote(id, &args, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				rf.dlog("received RequestVoteReply %+v", reply)

				if rf.state != Candidate {
					return
				}

				if reply.Term > savedCurrentTerm {
					rf.becomeFollower(reply.Term)
					return
				}

				if reply.VoteGranted {
					rf.dlog("Got vote from %d", id)
					votesTotal := int(atomic.AddInt32(&votes, 1))
					if votesTotal*2 > len(rf.peers) {
						rf.dlog("Won the election")
						rf.startLeader()
						return
					}
				}
			}
		}(peerIndex)
	}

	go rf.runElectionTimer()
}

// Expect rf.mu to be Locked
func (rf *Raft) startLeader() {
	rf.state = Leader
	for peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = -1
	}
	// Add one no-op command, to commit everything from previous terms
	rf.log = append(rf.log, Log{
		Term: rf.currentTerm,
		Noop: true,
	})
	rf.dlog("becomes Leader; term=%d, log=%v", rf.currentTerm, rf.log)

	go func() {
		ticker := time.NewTicker(HeartbeatsInterval)
		defer ticker.Stop()
		for {
			rf.sendHeartbeats()
			<-ticker.C

			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}()
}

// one round of heartbeats
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	savedTerm := rf.currentTerm
	rf.mu.Unlock()

	//rf.dlog("Sending out heartbeats..")
	for peer := range rf.peers {
		if peer == rf.id {
			continue
		}
		go func(id int) {
			var entries []Log
			var prevLogIndex, prevLogTerm int
			prevLogIndex = -1
			prevLogTerm = -1
			// in each heartbeat, figure out the new log entries and add them
			// need to lock the rf.mu Lock to read these entries
			rf.mu.Lock()
			ni := rf.nextIndex[id]
			leaderCommit := rf.commitIndex
			if len(rf.log) > ni {
				entries = rf.log[ni:]
			} else {
				entries = rf.log[0:0]
			}
			if ni > 0 {
				prevLogIndex = ni - 1
				prevLogTerm = rf.log[prevLogIndex].Term
			}
			rf.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         savedTerm,
				LeaderId:     rf.id,
				LeaderCommit: leaderCommit,
				Entries:      entries,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
			}
			var reply AppendEntriesReply
			rf.dlog("sending AppendEntries to %v: ni=%d, args=%+v", id, ni, args)
			if ok := rf.sendAppendEntries(id, &args, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > savedTerm {
					rf.becomeFollower(reply.Term)
				}
				if rf.state == Leader && reply.Term == savedTerm {
					if reply.Success {
						rf.nextIndex[id] = ni + len(entries)
						rf.matchIndex[id] = rf.nextIndex[id] - 1
						// do we need to increment commitIndex ? check now
						savedCommit := rf.commitIndex
						for i := savedCommit + 1; i < len(rf.log); i++ {
							if rf.log[i].Term == savedTerm {
								matches := 1
								for peer := range rf.peers {
									if peer == rf.id {
										continue
									}
									if rf.matchIndex[peer] >= i {
										matches++
									}
								}
								if matches*2 > len(rf.peers) {
									rf.dlog("setting leader commitIndex to %d", i)
									rf.commitIndex = i
								}
							}
						}
					} else {
						if reply.ConflictTerm == -1 {
							rf.nextIndex[id] = reply.ConflictIndex
						} else {
							var lastIndexTerm int = -1
							for i := len(rf.log) - 1; i >= 0; i-- {
								if rf.log[i].Term == reply.ConflictTerm {
									lastIndexTerm = i
									break
								}
							}
							if lastIndexTerm >= 0 {
								rf.nextIndex[id] = lastIndexTerm + 1
							} else {
								rf.nextIndex[id] = reply.ConflictIndex
							}
						}
					}
				}
			}
		}(peer)
	}
}

// function to act on committed entries and send them on the applyCh channel
func (rf *Raft) updateApplyChan() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		rf.mu.Lock()
		if rf.commitIndex < rf.lastApplied {
			panic("should have never happened")
		}
		// no entries to process
		if rf.commitIndex == rf.lastApplied {
			rf.mu.Unlock()
			continue
		}
		// some committed entries need to be sent

		logCopy := rf.log[rf.lastApplied+1 : rf.commitIndex+1]
		rf.dlog("sending out entries %+v on applyCh", logCopy)
		lastAppliedIndex := rf.lastApplied + 1
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		for index, entry := range logCopy {
			if entry.Noop {
				continue
			}
			rf.applyCh <- ApplyMsg{
				Command:      entry.Command,
				CommandIndex: lastAppliedIndex + index,
				CommandValid: true,
			}
		}
	}
}

package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

// import "bytes"
// import "labgob"

const (
	HeartbeatsInterval = 50 * time.Millisecond
	MinimumElectionTimeout = time.Duration(150) * time.Millisecond
	RandomElectionTimeoutInterval = 400
)

type Log struct {
	Command interface{}
	Term int
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


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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

	// Persistent Raft state on all servers
	currentTerm int
	votedFor int
	log []Log

	// Volatile state on all servers
	commitIndex int
	lastApplied int
	state RaftState
	electionResetEvent time.Time

	// Volatile state on all leaders
	nextIndex []int
	matchIndex []int

}

func (rf *Raft) dlog(format string, args ...interface{}) {
	format = fmt.Sprintf("[%d] ", rf.id) + format
	log.Printf(format, args...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
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
		reply.Success = true
		rf.electionResetEvent = time.Now()
	}
	reply.Term = rf.currentTerm
	rf.dlog("AppendEntries reply: %+v", *reply)
	return
}


type RequestVoteArgs struct {
	Term int
	CandidateID	int
	LastLogIndex int
	LastLogTerm int
}

type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d]", args, rf.currentTerm, rf.votedFor)
	if args.Term > rf.currentTerm {
		rf.dlog("... term out of date in RequestVote")
		rf.becomeFollower(args.Term)
	}

	reply.VoteGranted = false
	if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateID) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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

	// Your initialization code here (2A, 2B, 2C).

	rf.state = Follower
	rf.votedFor = -1
	rf.electionResetEvent = time.Now()
	go rf.runElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return MinimumElectionTimeout
	} else {
		return MinimumElectionTimeout + time.Duration(rand.Intn(RandomElectionTimeoutInterval)) * time.Millisecond
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
		<- ticker.C

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
func (rf *Raft) startElection()  {

	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.id
	rf.electionResetEvent = time.Now()
	savedCurrentTerm := rf.currentTerm
	rf.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, rf.log)

	var votes int32 = 1


	for peerIndex := range rf.peers {
		if peerIndex == rf.id {
			continue
		}
		go func(id int) {
			args := RequestVoteArgs{
				Term: savedCurrentTerm,
				CandidateID: rf.id,
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
			args := AppendEntriesArgs{
				Term: savedTerm,
				LeaderId: rf.id,
			}
			var reply AppendEntriesReply
			rf.dlog("sending AppendEntries to %v: ni=%d, args=%+v", id, 0, args)
			if ok := rf.sendAppendEntries(id, &args, &reply); ok {
				if reply.Term > savedTerm {
					rf.mu.Lock()
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
				}
			}
		}(peer)
	}
}
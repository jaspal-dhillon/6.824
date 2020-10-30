package raft

import "time"

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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


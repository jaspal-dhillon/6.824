package raft

import (
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

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

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

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


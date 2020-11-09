package raft

import "time"

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
		im := rf.lastLogInitialIndex + 1
		if args.PrevLogIndex == rf.lastLogInitialIndex ||
			(args.PrevLogIndex - im < len(rf.log) && args.PrevLogIndex -im >= 0 && rf.log[args.PrevLogIndex - im].Term == args.PrevLogTerm) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1 - im
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
			rf.dlog("AE RPC handler: new log=%% LeaderCommit=%d commitIndex=%d", args.LeaderCommit, rf.commitIndex)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log) + im -1)
				rf.dlog("AE RPC handler: setting commitIndex to %d", rf.commitIndex)
			}
		} else {
			if args.PrevLogIndex >= len(rf.log) + im {
				reply.ConflictIndex = len(rf.log) + im
				reply.ConflictTerm = -1
			} else {
				// PrevLogTerm at PrevLogIndex does not match.
				// Return the first index of this term.
				if args.PrevLogIndex < im {
					reply.ConflictIndex = im
					return
				}
				reply.ConflictTerm = rf.log[args.PrevLogIndex - im].Term
				var i int
				for i = args.PrevLogIndex - 1 - im; i >= 0; i-- {
					if rf.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1 + im
			}
		}
	}
	reply.Term = rf.currentTerm
	rf.dlog("AppendEntries reply: %+v", *reply)
	return
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

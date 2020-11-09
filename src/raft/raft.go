package raft

import (
	"sync"
	"time"
)
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

	ReadSnapshotFromPersister bool
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
	currentTerm         int
	votedFor            int
	log                 []Log
	lastLogInitialIndex int
	lastLogInitialTerm  int

	// Volatile state on all servers
	commitIndex        int
	lastApplied        int
	state              RaftState
	electionResetEvent time.Time

	// Volatile state on all leaders
	nextIndex  []int
	matchIndex []int
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
		Noop:    false,
	})
	rf.persist()
	lastIndex, lastTerm := rf.getLastIndexAndTerm()
	rf.dlog("leader log=%v, returning %d %d", rf.log, lastIndex, lastTerm)
	return lastIndex, lastTerm, true
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
	rf.lastLogInitialIndex = -1
	rf.lastLogInitialTerm = -1
	rf.electionResetEvent = time.Now()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.readPersist(persister.ReadRaftState())

	go rf.runElectionTimer()
	go rf.updateApplyChan()

	return rf
}

// Expect rf.mu to be Locked
func (rf *Raft) startLeader() {
	rf.state = Leader

	for peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log) + rf.lastLogInitialIndex + 1
		rf.matchIndex[peer] = -1
	}
	rf.dlog("becomes Leader; term=%d, log=%v", rf.currentTerm, rf.log)

	// Add one no-op command, to commit everything from previous terms, as
	// mentioned in Section 8 of the paper.
	// The tester (config.go:L194) can't deal with this condition, it expects
	// all logs to be continous, starting from index 1. So, we'll just add a log
	// in case leader starts with empty log.
	if len(rf.log)+rf.lastLogInitialIndex+1 == 0 {
		rf.log = append(rf.log, Log{
			Command: nil,
			Term:    rf.currentTerm,
			Noop:    true,
		})
		rf.persist()
	}

	go func() {
		ticker := time.NewTicker(HeartbeatsInterval)
		defer ticker.Stop()
		for {
			rf.sendHeartbeats()
			<-ticker.C

			rf.mu.Lock()
			if rf.state != Leader || rf.killed() {
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
			var needToInstallSnapshot bool = false
			// in each heartbeat, figure out the new log entries and add them
			// need to lock the rf.mu Lock to read these entries
			rf.mu.Lock()
			prevLogIndex, prevLogTerm = rf.getLastInitialIndexAndTerm()
			ni := rf.nextIndex[id]
			leaderCommit := rf.commitIndex
			im := rf.lastLogInitialIndex + 1
			if ni-im >= 0 {
				// heartbeat possible
				entries = rf.log[ni-im:]
			} else {
				// need to install snapshot
				needToInstallSnapshot = true
			}
			if ni > im {
				prevLogIndex = ni - 1
				prevLogTerm = rf.log[prevLogIndex-im].Term
			}
			rf.mu.Unlock()

			if needToInstallSnapshot {
				// If this is the case of a new member joining or a lagging peer catching up,
				// leader needs to install the snapshot on it to reset it.
				rf.mu.Lock()
				args := SnapshotArgs{
					InitialLogIndex: rf.lastLogInitialIndex,
					InitialLogTerm:  rf.lastLogInitialTerm,
					ServerData:      rf.persister.ReadSnapshot(),
					Term:            rf.currentTerm,
				}
				rf.mu.Unlock()
				var reply SnapshotReply
				rf.dlog("Sending out snapshot to peer %d", id)
				if ok := rf.sendSnapshot(id, &args, &reply); !ok {
					rf.dlog("Failed to send Snapshot RPC")
				} else {
					rf.mu.Lock()
					rf.nextIndex[id] = args.InitialLogIndex + 1
					rf.mu.Unlock()
				}
				return
			}

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
				im := rf.lastLogInitialIndex + 1
				// After getting the reply, we may have done a snapshot and hence
				// rf.log , initial index might have changed.
				if rf.state == Leader && reply.Term == savedTerm {
					if reply.Success {
						rf.nextIndex[id] = ni + len(entries)
						rf.matchIndex[id] = rf.nextIndex[id] - 1
						savedCommit := rf.commitIndex
						for i := savedCommit + 1 - im; i>=0 && i < len(rf.log); i++ {
							if rf.log[i].Term == savedTerm {
								matches := 1
								for peer := range rf.peers {
									if peer == rf.id {
										continue
									}
									if rf.matchIndex[peer] >= i + im {
										matches++
									}
								}
								if matches*2 > len(rf.peers) {
									rf.dlog("setting leader commitIndex to %d", i + im)
									rf.commitIndex = i + im
								}
							}
						}
					} else {
						// Optimization mentioned on Page 8 of the paper.
						if reply.ConflictTerm == -1 {
							rf.nextIndex[id] = reply.ConflictIndex
						} else {
							var lastIndexTerm int = -1
							for i := len(rf.log) - 1 + im; i >= 0; i-- {
								if rf.log[i-im].Term == reply.ConflictTerm {
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
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.commitIndex < rf.lastApplied {
			panic("should have never happened")
		}
		// no entries to process
		if rf.commitIndex == rf.lastApplied {
			rf.mu.Unlock()
			continue
		}
		// Some committed entries need to be sent.
		im := rf.lastLogInitialIndex + 1
		rf.dlog("need to send out some entries on applyCh, from %d to %d, im = %d lastApplied %d commitIndex %d " +
			"len %d",rf.lastApplied+1-im , rf.commitIndex+1-im, im, rf.lastApplied, rf.commitIndex, len(rf.log))
		logCopy := rf.log[rf.lastApplied+1-im : rf.commitIndex+1-im]
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

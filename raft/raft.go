package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

const Debugn = 4

type CommitEntry struct {
	Command interface{}

	Index int

	Term int
}

type nodeState int

const (
	Follower nodeState = iota
	Candidate
	Leader
	Dead
)

const (
	ElectionTimeout  = 10 * time.Second
	HeartBeatTimeout = ElectionTimeout / 6
)

func (s nodeState) String() string {
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
		panic("unreachable")
	}
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type RaftNode struct {
	mu sync.Mutex

	id int

	peerIds []int

	server *Server

	commitChan chan<- CommitEntry

	newCommitReadyChan chan struct{}

	triggerAEChan chan struct{}

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex        int
	lastApplied        int
	state              nodeState
	electionResetEvent time.Time

	nextIndex  map[int]int
	matchIndex map[int]int
}

func InitRaftNode(id int, peerIds []int, server *Server, ready <-chan interface{}, commitChan chan<- CommitEntry) *RaftNode {
	n := new(RaftNode)
	n.id = id
	n.peerIds = peerIds
	n.server = server
	n.commitChan = commitChan
	n.newCommitReadyChan = make(chan struct{}, 16)
	n.triggerAEChan = make(chan struct{}, 1)
	n.state = Follower
	n.votedFor = -1
	n.commitIndex = -1
	n.lastApplied = -1
	n.nextIndex = make(map[int]int)
	n.matchIndex = make(map[int]int)

	go func() {
		// wait for ready signal
		<-ready
		n.mu.Lock()
		n.electionResetEvent = time.Now()
		n.mu.Unlock()
		n.runElectionTimer()
	}()

	go n.commitChanSender()
	return n
}

func (n *RaftNode) ReportState() (id int, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.id, n.currentTerm, n.state == Leader
}

func (n *RaftNode) Submit(command interface{}) bool {
	n.mu.Lock()
	n.dlog(0, "Submit received by %v: %v", n.state, command)
	if n.state == Leader {
		n.log = append(n.log, LogEntry{Command: command, Term: n.currentTerm})
		n.dlog(1, "... log=%v", n.log)
		n.mu.Unlock()
		n.triggerAEChan <- struct{}{}
		return true
	}

	n.mu.Unlock()
	return false
}

// Stop stops this node, cleaning up its state. This method returns quickly, but
// it may take a bit of time (up to ~election timeout) for all goroutines to
// exit.
func (n *RaftNode) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = Dead
	n.dlog(1, "becomes Dead")
}

func (n *RaftNode) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = Follower
	n.dlog(1, "back from the Dead")
}

// dlog logs a debugging message if Debugn > 0.
func (n *RaftNode) dlog(level int, format string, args ...interface{}) {
	if Debugn >= level {
		format = fmt.Sprintf("[%d] ", n.id) + format
		log.Printf(format, args...)
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (n *RaftNode) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == Dead {
		return nil
	}
	lastLogIndex, lastLogTerm := n.lastLogIndexAndTerm()
	n.dlog(0, "RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, n.currentTerm, n.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > n.currentTerm {
		n.dlog(0, "... term out of date in RequestVote")
		n.becomeFollower(args.Term)
	}

	if n.currentTerm == args.Term &&
		(n.votedFor == -1 || n.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		n.votedFor = args.CandidateId
		n.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = n.currentTerm
	n.dlog(0, "... RequestVote reply: %+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// section 5.3
	ConflictIndex int
	ConflictTerm  int
}

func (n *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == Dead {
		return nil
	}
	n.dlog(5, "AppendEntries: %+v", args)

	if args.Term > n.currentTerm {
		n.dlog(4, "... term out of date in AppendEntries")
		n.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == n.currentTerm {
		if n.state != Follower {
			n.becomeFollower(args.Term)
		}
		n.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(n.log) && args.PrevLogTerm == n.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent
			// in the RPC.
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(n.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if n.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				n.dlog(5, "... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				n.log = append(n.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				n.dlog(5, "... log is now: %v", n.log)
			}

			// Set commit index.
			if args.LeaderCommit > n.commitIndex {
				n.commitIndex = min(args.LeaderCommit, len(n.log)-1)
				n.dlog(5, "... setting commitIndex=%d", n.commitIndex)
				n.newCommitReadyChan <- struct{}{}
			}
		} else {

			if args.PrevLogIndex >= len(n.log) {
				reply.ConflictIndex = len(n.log)
				reply.ConflictTerm = -1
			} else {
				reply.ConflictTerm = n.log[args.PrevLogIndex].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if n.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = n.currentTerm
	n.dlog(5, "AppendEntries reply: %+v", *reply)
	return nil
}

func (n *RaftNode) electionTimeout() time.Duration {
	return time.Duration(ElectionTimeout.Milliseconds()+rand.Int63()%150) * time.Millisecond
}

// runElectionTimer is func for routine that checks if electionTimeout is elapsed
// and if true starts the new election
func (n *RaftNode) runElectionTimer() {
	timeoutDuration := n.electionTimeout()
	n.mu.Lock()
	termStarted := n.currentTerm
	n.mu.Unlock()
	n.dlog(5, "election timer started (%v), term=%d", timeoutDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		n.mu.Lock()
		if n.state != Candidate && n.state != Follower {
			n.dlog(5, "in election timer state=%s, bailing out", n.state)
			n.mu.Unlock()
			return
		}

		if termStarted != n.currentTerm {
			n.dlog(5, "in election timer term changed from %d to %d, bailing out", termStarted, n.currentTerm)
			n.mu.Unlock()
			return
		}

		if elapsed := time.Since(n.electionResetEvent); elapsed >= timeoutDuration {
			n.startElection()
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
	}
}

func (n *RaftNode) startElection() {
	n.state = Candidate
	n.currentTerm += 1
	savedCurrentTerm := n.currentTerm
	n.electionResetEvent = time.Now()
	n.votedFor = n.id
	n.dlog(2, "becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, n.log)

	votesReceived := 1

	// Send RequestVote to all other servers
	for _, peerId := range n.peerIds {
		go func(peerId int) {
			n.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := n.lastLogIndexAndTerm()
			n.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  n.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			n.dlog(3, "sending RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply
			if err := n.server.Call(peerId, "RaftNode.RequestVote", args, &reply); err == nil {
				n.mu.Lock()
				defer n.mu.Unlock()
				n.dlog(3, "received RequestVoteReply %+v", reply)

				if n.state != Candidate {
					n.dlog(3, "while waiting for reply, state = %v", n.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					n.dlog(3, "term out of date in RequestVoteReply")
					n.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						if votesReceived*2 > len(n.peerIds)+1 {
							// Won the election!
							n.dlog(2, "wins election with %d votes", votesReceived)
							n.becomeLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	go n.runElectionTimer()
}

func (n *RaftNode) becomeFollower(term int) {
	n.dlog(4, "becomes Follower with term=%d; log=%v", term, n.log)
	n.state = Follower
	n.currentTerm = term
	n.votedFor = -1
	n.electionResetEvent = time.Now()

	go n.runElectionTimer()
}

func (n *RaftNode) becomeLeader() {
	n.state = Leader

	for _, peerId := range n.peerIds {
		n.nextIndex[peerId] = len(n.log)
		n.matchIndex[peerId] = -1
	}
	n.dlog(2, "becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", n.currentTerm, n.nextIndex, n.matchIndex, n.log)

	go func(heartbeatTimeout time.Duration) {
		n.leaderSendEntries()

		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()
		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true

				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-n.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}

				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				//if this is not a leader anymore, stop the heartbeat loop.
				n.mu.Lock()
				if n.state != Leader {
					n.mu.Unlock()
					return
				}
				n.mu.Unlock()
				n.leaderSendEntries()
			}
		}
	}(HeartBeatTimeout)
}

// leaderSendEntries sends AEs to all peers, collects their
// replies and adjusts n's state.
func (n *RaftNode) leaderSendEntries() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	savedCurrentTerm := n.currentTerm
	n.mu.Unlock()

	for _, peerId := range n.peerIds {
		go func(peerId int) {
			n.mu.Lock()
			ni := n.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = n.log[prevLogIndex].Term
			}
			entries := n.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.commitIndex,
			}
			n.mu.Unlock()
			n.dlog(5, "sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
			var reply AppendEntriesReply
			if err := n.server.Call(peerId, "RaftNode.AppendEntries", args, &reply); err == nil {
				n.mu.Lock()
				defer n.mu.Unlock()
				if reply.Term > n.currentTerm {
					n.dlog(5, "term out of date in heartbeat reply")
					n.becomeFollower(reply.Term)
					return
				}

				if n.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						n.nextIndex[peerId] = ni + len(entries)
						n.matchIndex[peerId] = n.nextIndex[peerId] - 1

						savedCommitIndex := n.commitIndex
						for i := n.commitIndex + 1; i < len(n.log); i++ {
							if n.log[i].Term == n.currentTerm {
								matchCount := 1
								for _, peerId := range n.peerIds {
									if n.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(n.peerIds)+1 {
									n.commitIndex = i
								}
							}
						}
						n.dlog(5, "AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, n.nextIndex, n.matchIndex, n.commitIndex)
						if n.commitIndex != savedCommitIndex {
							n.dlog(4, "leader sets commitIndex := %d", n.commitIndex)
							// Commit index changed: the leader considers new entries to be
							// committed. Send new entries on the commit channel to this
							// leader's clients, and notify followers by sending them AEs.
							n.newCommitReadyChan <- struct{}{}
							n.triggerAEChan <- struct{}{}
						}
					} else {
						if reply.ConflictTerm >= 0 {
							lastIndexOfTerm := -1
							for i := len(n.log) - 1; i >= 0; i-- {
								if n.log[i].Term == reply.ConflictTerm {
									lastIndexOfTerm = i
									break
								}
							}
							if lastIndexOfTerm >= 0 {
								n.nextIndex[peerId] = lastIndexOfTerm + 1
							} else {
								n.nextIndex[peerId] = reply.ConflictIndex
							}
						} else {
							n.nextIndex[peerId] = reply.ConflictIndex
						}
						n.dlog(3, "AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
					}
				}
			}
		}(peerId)
	}
}

func (n *RaftNode) lastLogIndexAndTerm() (int, int) {
	if len(n.log) > 0 {
		lastIndex := len(n.log) - 1
		return lastIndex, n.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

// commitChanSender is routine func responsible for sending committed entries on
// n.commitChan. It watches newCommitReadyChan for notifications and calculates
// which new entries are ready to be sent.
func (n *RaftNode) commitChanSender() {
	for range n.newCommitReadyChan {
		// Find which entries we have to apply.
		n.mu.Lock()
		savedTerm := n.currentTerm
		savedLastApplied := n.lastApplied
		var entries []LogEntry
		if n.commitIndex > n.lastApplied {
			entries = n.log[n.lastApplied+1 : n.commitIndex+1]
			n.lastApplied = n.commitIndex
		}
		n.mu.Unlock()
		n.dlog(5, "commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			n.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	n.dlog(5, "commitChanSender done")
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

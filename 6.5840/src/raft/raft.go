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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	state    State
	term     int
	votedFor int
	votes    map[int]RequestVoteReply

	leaderAlive bool
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.term
	isleader = (rf.state == Leader)
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	From int
	To   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Granted bool
	From    int
	To      int
}

type AppendEntriesArgs struct {
	Term   int
	Leader int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	granted := args.Term > rf.term || (args.Term == rf.term && (rf.votedFor == -1 || rf.votedFor == args.From))
	//fmt.Println(time.Now(), "peer", rf.me, "current term", rf.term, "state", rf.state, "receive RequestVote from", args.From, ",term", args.Term)

	if args.Term > rf.term {
		rf.term = args.Term
		rf.state = Follower
		rf.votedFor = args.From
		rf.votes = make(map[int]RequestVoteReply)
	}

	if reply != nil {
		reply.Term = rf.term
		reply.Granted = granted
		reply.From = rf.me
		reply.To = args.From
	}
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term >= rf.term {
		rf.term = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.votes = make(map[int]RequestVoteReply)
		rf.leaderAlive = true
		// fmt.Println(time.Now(), "Leader", args.Leader, "send heartbeat to", rf.me)
		if reply != nil {
			reply.Term = rf.term
			reply.Success = true
		}
	} else {
		if reply != nil {
			reply.Term = rf.term
			reply.Success = false
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	fmt.Println("Kill peer", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
OUTER:
	for !rf.killed() {
		// Your code here (2A)
		rf.mu.Lock()
		if rf.state == Candidate || rf.state == Follower {
			if rf.leaderAlive {
				rf.leaderAlive = false
				rf.mu.Unlock()
				ms := 50 + (rand.Int63() % 300)
				time.Sleep(time.Duration(ms) * time.Millisecond)
			} else {
				fmt.Printf("Peer %d switch from %d to %d with new term %d\n", rf.me, rf.state, Candidate, rf.term+1)
				rf.state = Candidate
				rf.term++
				rf.votedFor = rf.me
				rf.votes = make(map[int]RequestVoteReply)
				rf.votes[rf.me] = RequestVoteReply{
					Term:    rf.term,
					Granted: true,
					From:    rf.me,
					To:      rf.me,
				}

				for i := range rf.peers {
					if _, ok := rf.votes[i]; !ok {
						request := RequestVoteArgs{
							Term: rf.term,
							From: rf.me,
							To:   i,
						}

						go func(s int, args *RequestVoteArgs) {
							var reply RequestVoteReply
							if rf.sendRequestVote(s, args, &reply) {
								rf.mu.Lock()
								fmt.Println("peer", rf.me, "send RequestVote to", args.To, ", term:", args.Term)
								if reply.Term > rf.term {
									rf.term = reply.Term
									rf.state = Follower
									rf.votedFor = -1
									rf.votes = make(map[int]RequestVoteReply)
								} else if reply.Term == rf.term {
									rf.votes[s] = reply
									grantedVotes := 0
									for _, v := range rf.votes {
										if v.Granted && v.Term == rf.term {
											fmt.Println("grant votes from:", v.From, ",term:", v.Term, ", peers_number:", len(rf.peers))
											grantedVotes++
										}
									}

									if (2 * grantedVotes) > len(rf.peers) {
										fmt.Println("peer", rf.me, "becomes leader of term", rf.term)
										rf.state = Leader
									}
								}
								rf.mu.Unlock()
							}
						}(i, &request)
					}
				}
				rf.mu.Unlock()
				i := 0
				for ; i < 10; i++ {
					rf.mu.Lock()
					if rf.state == Leader {
						rf.leaderAlive = true
						rf.mu.Unlock()
						goto OUTER
					}

					if rf.leaderAlive {
						rf.mu.Unlock()
						break
					}

					rf.mu.Unlock()
					ms := 5
					time.Sleep(time.Duration(ms) * time.Millisecond)
				}
				rf.mu.Lock()
				rf.leaderAlive = false
				rf.mu.Unlock()
				ms := (10-int64(i))*5 + (rand.Int63() % 300)
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
		} else {
			// Send Heartbeat to other peers
			heartBeat := AppendEntriesArgs{
				Term:   rf.term,
				Leader: rf.me,
			}
			for i := range rf.peers {
				if i != rf.me {
					go func(peer int, hb *AppendEntriesArgs) {
						var heartBeatAck AppendEntriesReply
						if rf.sendAppendEntries(peer, hb, &heartBeatAck) {
							rf.mu.Lock()
							if heartBeatAck.Term > rf.term {
								rf.term = heartBeatAck.Term
								rf.state = Follower
							}
							rf.mu.Unlock()
						}
					}(i, &heartBeat)
				}
			}
			rf.mu.Unlock()

			// Sleep for a while before send another heart beat to establish authority
			follower := false
			for i := 0; i < 5; i++ {
				ms := 5
				time.Sleep(time.Duration(ms) * time.Millisecond)
				rf.mu.Lock()
				follower = (rf.state == Follower)
				rf.mu.Unlock()
				if follower {
					fmt.Println("Switch from leader to follower", rf.me)
					rf.mu.Lock()
					rf.leaderAlive = false
					rf.mu.Unlock()
					ms := (10-int64(i))*5 + (rand.Int63() % 300)
					time.Sleep(time.Duration(ms) * time.Millisecond)
					break
				}
			}
		}
	}
	// fmt.Println("ticker completes it job", rf.me, rf.killed())
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// Every node starts as follower, waits randomized election timeout before kicks off leader election
	rf.state = Follower
	rf.term = 0
	rf.votedFor = -1
	rf.votes = make(map[int]RequestVoteReply)

	// 初始化时假设有一个 leader
	rf.leaderAlive = true

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

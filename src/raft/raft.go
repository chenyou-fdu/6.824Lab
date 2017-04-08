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

import "sync"
import "labrpc"

import (
    "fmt"
    "time"
    "math/rand"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
    Index       int
    Command     interface{}
    UseSnapshot bool   // ignore for lab2; only used in lab3
    Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu             sync.Mutex          // Lock to protect shared access to this peer's state
    peers          []*labrpc.ClientEnd // RPC end points of all peers
    persister      *Persister          // Object to hold this peer's persisted state
    me             int                 // this peer's index into peers[]
    // Your data here (2A, 2B, 2C).
    raft_state     int                 // 0: follower 1: candidate 2: leaders
    cur_term       int                 // current term of this Raft Node
    voted_for      int                 // the id of other candidate this raft node has voted for
    
    raft_timer     *time.Timer
    vote_received  int 

    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    var term int
    var isleader bool
    // Your code here (2A).
    if rf.raft_state == 0 || rf.raft_state == 1 {
        isleader = false
    } else {
        isleader = true
    }
    term = rf.cur_term
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
    // e := gob.NewEncoder(w)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // data := w.Bytes()
    // rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    // Your code here (2C).
    // Example:
    // r := bytes.NewBuffer(data)
    // d := gob.NewDecoder(r)
    // d.Decode(&rf.xxx)
    // d.Decode(&rf.yyy)
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }
}

type AppendEntriesArgs struct {
    term int
    leader_id int
}

type AppendEntriesReply struct {
    term int
    success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    reply.success = true
    reply.term = rf.cur_term
    if rf.cur_term < args.term {
        reply.success = false
    }
}

func (rf *Raft) TimerHandler() {
    rf.mu.Lock()
    fmt.Println("Time Out My Ass")    
    // time out as follower state
    if rf.raft_state == 0 {
        // start an election as candidate
        rf.raft_state = 1
        rf.cur_term++
        rf.vote_received = 1
        // invoke an election
        for idx := range rf.peers {
            if idx == rf.me {
                continue
            }
            rf.mu.Unlock()
            go func() {
                rf.mu.Lock()
                if rf.raft_state != 1 {
                    return
                }
                args := RequestVoteArgs{rf.cur_term, rf.me}
                reply := RequestVoteReply{-1, false}
                rf.sendRequestVote(idx, &args, &reply) 
                if reply.VoteGranted {
                    rf.vote_received++
                    if rf.vote_received >= len(rf.peers) / 2 {
                        // win the election 
                        rf.raft_state = 2
                        rf.ResetTimer()
                    }
                } else {
                    if reply.CurrentTerm > rf.cur_term {
                        rf.cur_term = reply.CurrentTerm
                        rf.raft_state = 0
                    }
                }
                rf.mu.Unlock()
            }()
            rf.mu.Lock()
        }
        rf.ResetTimer() 
    } else if rf.raft_state == 1 {
        rf.cur_term++
    }
    rf.mu.Unlock()
}

func (rf *Raft) ResetTimer() {
    // need to stop the non-timeout timer firstly    
    if(rf.raft_timer != nil) {
        rf.raft_timer.Stop()
    } 
    // restart the timer for next round timeout
    rf.raft_timer = time.AfterFunc(time.Millisecond * time.Duration(GetRandomTime()), rf.TimerHandler);
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term int  // candidate's term
    CandidateId int    //
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).
    CurrentTerm int   // current term 
    VoteGranted bool  // candidate received vote or not
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.voted_for == -1 {
        rf.voted_for = args.CandidateId
        rf.cur_term = args.Term
        reply.CurrentTerm = args.Term
        reply.VoteGranted = true
    } else if rf.voted_for != -1 || rf.cur_term >= args.Term {
        reply.CurrentTerm = rf.cur_term
        reply.VoteGranted = true
    }
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    // Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    // rf is a pointer to a Raft node
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me
    // starts with term 0 and follower state
    rf.cur_term = 0
    rf.raft_state = 0

    rf.raft_timer = nil
    rf.voted_for = -1
    // Your initialization code here (2A, 2B, 2C).
    /*for idx := range peers {
        fmt.Println("TestShit",idx)
        //args := AppendEntriesArgs{rf.cur_term, 0}
        //reply := &AppendEntriesReply{}
        //peer.Call("Raft.AppendEntries", args, reply)
    }*/

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    rf.ResetTimer()

    return rf
}

// get random time between 150~300 msec
func GetRandomTime() uint {
    rand.Seed(time.Now().UnixNano())  
    rand_time_num := rand.Intn(150) + 150
    return uint(rand_time_num)
}
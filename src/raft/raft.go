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
    //"fmt"
    "time"
    "math/rand"
    "log"
    "os"
    "strconv"
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

type LogEntry struct {
    Term int
    Command interface{}
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
    voted_for      int                // the id of other candidate this raft node has voted for
    log            []LogEntry
    vote_received  int   
    committed_idx  int
    lastapplied    int

    next_idx       []int
    match_idx      []int

    raft_timer     *time.Timer
    raft_logger    *log.Logger
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
    Term int
    LeaderId int
    PrevLogIndex int
    PrevLogTerm int
    Entries []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    CurrentTerm int
    Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    reply.Success = true
    reply.CurrentTerm = args.Term
    // Figure 2 Receiver implementation 1
    if rf.cur_term > args.Term {
        reply.Success = false
    // Recieved a HeartBeat, Figure 2 All Servers Rulls 2
    } else if len(args.Entries) == 0 {
        // recognize the PRC caller's leadership
        // set reply and current term
        rf.cur_term = args.Term
        // conver reciever to follower
        rf.raft_state = 0
    // Figure 2 Receiver implementation 2
    } else if args.PrevLogIndex - 1 > len(rf.log) - 1 {
        reply.Success = false
    // Figure 2 Receiver implementation 3
    } else if rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm {
        reply.Success = false
        rf.log = rf.log[0 : (args.PrevLogIndex - 1) + 1]
    } else {
        // Figure 2 Receiver implementation 4
        for _, entry := range args.Entries {
            rf.log = append(rf.log, entry)
        }
        // Figure 2 Receiver implementation 5
        if args.LeaderCommit > rf.committed_idx {
            if args.LeaderCommit < len(rf.log) {
                rf.committed_idx = args.LeaderCommit
            } else {
                rf.committed_idx = len(rf.log)
            }
        }
        // Figure 2 All Servers Rules 1
        if rf.committed_idx > rf.lastapplied {
            // applied? WTF?
            rf.lastapplied = rf.committed_idx
        }
    }
    // reset election timeout
    rf.mu.Unlock()
    rf.ResetTimer(false)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) HeartbeatHandler(server int) {
    rf.mu.Lock()
    // check raft state
    if rf.raft_state != 2 {
        return
    }
    // log index starts with 1
    PrevLogIndex := len(rf.log)
    var PrevLogTerm int
    // PrevLogIndex equals 0 means no log here
    if PrevLogIndex == 0 {
        PrevLogTerm = 0
    } else {
        PrevLogTerm = rf.log[len(rf.log)-2].Term
    }
    var send_logs []LogEntry
    // Figure 2 Leaders Rulls 3
    // leader have log entries to sent to this follower
    rf.raft_logger.Println("HeartbeatHandler: Leader Log", rf.log)
    if len(rf.log) != 0 && len(rf.log) >= rf.next_idx[server] {
        for idx := rf.next_idx[server]; idx < len(rf.log); idx++ {
            send_logs = append(send_logs, rf.log[idx])
        }
    }
    args := AppendEntriesArgs {rf.cur_term, rf.me, PrevLogIndex, PrevLogTerm, send_logs, rf.committed_idx}
    reply := AppendEntriesReply {-1, false}
    rf.mu.Unlock()
    rf.sendAppendEntries(server, &args, &reply)
    if reply.Success {
        // TODO..
    } else {
        rf.mu.Lock()
        if reply.CurrentTerm > rf.cur_term {
            rf.cur_term = reply.CurrentTerm
            rf.raft_state = 0
        }
        rf.mu.Unlock()
    }

}

func (rf *Raft) ElectionHandler(server int) {
    rf.mu.Lock()
    // check the raft state
    if rf.raft_state == 2 {
        return
    }
    args := RequestVoteArgs{rf.cur_term, rf.me}
    reply := RequestVoteReply{-1, false}
    rf.mu.Unlock()
    rf.sendRequestVote(server, &args, &reply)
    // received peers' vote
    if reply.VoteGranted {
        rf.mu.Lock()
        rf.vote_received++
        // win the election 
        if rf.vote_received > len(rf.peers) / 2 && rf.raft_state == 1{
            rf.raft_state = 2
            rf.raft_logger.Println("ElectionHandler: Win the Election", rf.vote_received, "out of", len(rf.peers))
            // reset two log slice
            rf.match_idx = make([]int, len(rf.peers))
            rf.next_idx = make([]int, len(rf.peers))
            // reset a heartbeat timer for leader
            rf.mu.Unlock()
            // kick off the initial round of heartbeat PRC
            for idx := 0; idx < len(rf.peers); idx++ {
                if idx == rf.me {
                    continue
                }
                go rf.HeartbeatHandler(idx)
            }
            rf.ResetTimer(true)
        } else {
            // do not win, simply unlock 
            rf.mu.Unlock()
        }
    // request vote failed
    } else {
        rf.mu.Lock()
        if reply.CurrentTerm > rf.cur_term {
            rf.cur_term = reply.CurrentTerm
            rf.raft_state = 0
        }
        rf.mu.Unlock()
    }
}

func (rf *Raft) TimerHandler() {
    rf.mu.Lock()

    // time out as follower state or candidate state
    if rf.raft_state == 0 || rf.raft_state == 1 {
        rf.raft_logger.Println("TimerHandler: Election Time Out")
        // convert follwer to candidate
        rf.raft_state = 1
        // increase term
        rf.cur_term++
        // vote for itself
        rf.vote_received = 1
        rf.voted_for = rf.me
        rf.mu.Unlock()

        // send RequestVote to peers in parallel
        for idx := 0; idx < len(rf.peers); idx++ {
            if idx == rf.me {
                continue
            }
            // send out RPC in goroutine
            go rf.ElectionHandler(idx)
        }
        // Set Timer for Election Timeout
        rf.ResetTimer(false) 
    } else if rf.raft_state == 2 {
        // timeout as Leader, send out heart beat PRC
        rf.mu.Unlock()
        for idx := 0; idx < len(rf.peers); idx++ {
            if idx == rf.me {
                continue
            }
            go rf.HeartbeatHandler(idx)
        }
        rf.ResetTimer(true) 
    }
}

func (rf *Raft) ResetTimer(is_heartbeat bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    // need to stop the non-timeout timer firstly    
    if(rf.raft_timer != nil) {
        rf.raft_timer.Stop()
    } 
    var rand_time_num time.Duration
    if is_heartbeat {
        // lab restriction, no more than 10 heartbeats per second
        rand_time_num = time.Duration(150)
    } else {
        // election timeout between 250 ~ 400 ms
        // larger than paper's 150 ~ 300 ms due to lab restriction
        rand_time_num = time.Duration(rand.Intn(150) + 250)
    }
    // restart the timer for next round timeout
    rf.raft_timer = time.AfterFunc(time.Millisecond * rand_time_num, rf.TimerHandler);
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
    // has not voted for other peer or itself
    if rf.voted_for == -1 && rf.cur_term < args.Term {
        rf.voted_for = args.CandidateId
        rf.cur_term = args.Term
        reply.CurrentTerm = args.Term
        reply.VoteGranted = true
        // reset timer for election timeout
        rf.mu.Unlock()
        rf.ResetTimer(false)
        rf.mu.Lock()
    // has already voted for peer of itself or in preceding term
    } else if rf.voted_for != -1 || rf.cur_term >= args.Term {
        reply.CurrentTerm = rf.cur_term
        reply.VoteGranted = true
    }
    rf.mu.Unlock()
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
    rf.mu.Lock()
    // to check is raft a leader
    if rf.raft_state != 2 {
        isLeader = false
        rf.mu.Unlock()
        return index, term, isLeader
    }
    // push back new log entry
    new_log_entry := LogEntry {rf.cur_term, command}
    rf.log = append(rf.log, new_log_entry)
    index = len(rf.log) - 1
    term = rf.cur_term
    // Your code here (2B).
    rf.raft_logger.Println("Start: Called", command)

    rf.mu.Unlock()
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

    rf.committed_idx = 0
    rf.lastapplied = 0
    rf.match_idx = make([]int, len(peers))
    rf.next_idx = make([]int, len(peers))

    rf.raft_logger = log.New(os.Stdout, strconv.Itoa(rf.me) + " Logger: ", log.Lshortfile|log.Lmicroseconds)

    // Your initialization code here (2A, 2B, 2C).

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    // set the seed once for each raft node
    rand.Seed(time.Now().UnixNano() + int64(me))  
    // reset timer for inital timeout
    rf.ResetTimer(false)

    return rf
}


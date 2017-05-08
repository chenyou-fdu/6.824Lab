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
    "log"
    "sort"
    "encoding/gob"
    "bytes"
    //"os"
    //"strconv"
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
    mu                  sync.Mutex          // Lock to protect shared access to this peer's state
    peers               []*labrpc.ClientEnd // RPC end points of all peers
    persister           *Persister          // Object to hold this peer's persisted state
    me                  int                 // this peer's index into peers[]
    // Your data here (2A, 2B, 2C).
    cur_state           int                 // 0: follower 1: candidate 2: leaders
    cur_term            int                 // current term of this Raft Node
    voted_for           int                // the id of other candidate this raft node has voted for
    log                 []LogEntry
    vote_received       int   
    committed_idx       int
    last_applied_idx    int

    next_idx            []int
    match_idx           []int
     
    raft_timer          *time.Timer
    raft_logger         *log.Logger
    raft_apply_ch       chan ApplyMsg
    raft_applied_sigal  chan bool
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

}

// functions for timer
func GetRandomTime(raft_state int) time.Duration {
    var rand_time_num time.Duration
    if raft_state == 2 {
        // lab restriction, no more than 10 heartbeats per second
        rand_time_num = time.Duration(120)
    } else {
        // election timeout between 250 ~ 400 ms
        // larger than paper's 150 ~ 300 ms due to lab restriction
        rand_time_num = time.Duration(rand.Intn(150) + 250)
    }
    return rand_time_num * time.Millisecond
}

func (rf *Raft) NodeTimer() {
    for {
        <-rf.raft_timer.C
        rf.mu.Lock()
        if rf.cur_state == 2 {
            // send out HeartBeats
            go rf.BoardcastHeartbeat()
        } else {
            // start election
            go rf.BroadcastElection()
        }
        rf.raft_timer.Reset(GetRandomTime(rf.cur_state))
        rf.mu.Unlock()
    }
}
// need to hold lock for this function
func (rf *Raft) ResetTimer(raft_state int) {
    if !rf.raft_timer.Stop() {
        <-rf.raft_timer.C
    }
    rf.raft_timer.Reset(GetRandomTime(raft_state))
}

// function for applied msg 
// we need feedbacm the applied message sequential
func (rf *Raft) ApplyMessage() {
    for {
        <-rf.raft_applied_sigal
        rf.mu.Lock()
        logs_to_applied := rf.log[(rf.last_applied_idx - 1) + 1 : rf.committed_idx]
        old_applied_idx := rf.last_applied_idx
        //DPrintf(fmt.Sprintf("%v: logs_to_applied %v", rf.me, logs_to_applied))
        if rf.committed_idx > rf.last_applied_idx {
            rf.last_applied_idx = rf.committed_idx
        }
        rf.mu.Unlock()
        for i := 0; i < len(logs_to_applied); i++ {
            message := ApplyMsg {old_applied_idx + 1 + i, logs_to_applied[i].Command, false, nil}
            rf.raft_apply_ch <- message
        }
    }
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    var term int
    var isleader bool
    // Your code here (2A).
    if rf.cur_state == 2 {
        isleader = true
    } else {
        isleader = false
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
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.cur_term)
    e.Encode(rf.voted_for)
    e.Encode(rf.log)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
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
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.cur_term)
    d.Decode(&rf.voted_for)
    d.Decode(&rf.log)
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
    ConflictTerm int
    ConflictIndex int
    Term int
    Success bool
}

func (rf *Raft) BoardcastHeartbeat() {
    rf.mu.Lock()
    if rf.cur_state != 2 {
        rf.mu.Unlock()
        return
    }
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        prev_log_index := rf.next_idx[i] - 1
        prev_log_term := -1
        if prev_log_index > 0 {
            prev_log_term = rf.log[prev_log_index-1].Term
        }
        args := AppendEntriesArgs {
            Term: rf.cur_term,
            LeaderId: rf.me,
            PrevLogIndex: prev_log_index,
            PrevLogTerm: prev_log_term,
            Entries: rf.log[rf.next_idx[i]-1:],
            LeaderCommit: rf.committed_idx }
        go rf.HeartbeatSender(i, args)
    }
    rf.mu.Unlock()
}

func (rf *Raft) HeartbeatSender(server_idx int, args AppendEntriesArgs) {
    DPrintf(fmt.Sprintf("%v: Send Out heartbeats to %v", rf.me, server_idx))
    reply := AppendEntriesReply {-1, -1, -1, false}
    is_ok := rf.sendAppendEntries(server_idx, &args, &reply)
    rf.mu.Lock()
    if rf.cur_term == args.Term && is_ok {
        if reply.Success {
            if args.PrevLogIndex + len(args.Entries) > rf.match_idx[server_idx] {
                rf.next_idx[server_idx] = args.PrevLogIndex + len(args.Entries) + 1
                rf.match_idx[server_idx] = args.PrevLogIndex + len(args.Entries)
                if args.PrevLogIndex + len(args.Entries) > rf.committed_idx {
                    go rf.CheckPeerCommitIdx()
                }
            }
        } else {
            // Figure 2 All Servers Rules 2
            if rf.cur_term < reply.Term {
                rf.cur_state = 0
                rf.cur_term = reply.Term
                rf.voted_for = -1
                rf.persist()
            } else {
                DPrintf(fmt.Sprintf("%v: Conflict %v", rf.me, reply.ConflictIndex))
                rf.next_idx[server_idx] = reply.ConflictIndex
                prev_log_index := rf.next_idx[server_idx] - 1 
                
                prev_log_term := -1
                if prev_log_index > 0 {
                    prev_log_term = rf.log[prev_log_index-1].Term
                }
                var retry_entries []LogEntry
                if rf.next_idx[server_idx] > 0 {
                    retry_entries = rf.log[rf.next_idx[server_idx]-1:]
                }
                retry_args := AppendEntriesArgs {
                    Term: rf.cur_term,
                    LeaderId: rf.me,
                    PrevLogIndex: prev_log_index,
                    PrevLogTerm: prev_log_term,
                    //Entries: rf.log[args.PrevLogIndex+1-1:],
                    Entries: retry_entries,//rf.log[rf.next_idx[server_idx]-1:],
                    LeaderCommit: rf.committed_idx }
                go rf.HeartbeatSender(server_idx, retry_args)
            }
        }
    }
    rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    DPrintf(fmt.Sprintf("%v: Receive heartbeat with %v", rf.me, args))
    reply.Term = rf.cur_term

    // Figure 2 AppendEntries RPC 1
    if args.Term < rf.cur_term {
        reply.Success = false
    } else {
        rf.cur_state = 0
        rf.ResetTimer(rf.cur_state)
        // Figure 2 All Servers Rules 2
        if rf.cur_term < args.Term {
            rf.voted_for = -1
            rf.cur_term = args.Term
            rf.persist()
        }
        // Figure 2 AppendEntries RPC 2 & 3
        if args.PrevLogIndex > 0 && (args.PrevLogIndex > len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex-1].Term) {
            reply.ConflictIndex = len(rf.log)
            if args.PrevLogIndex < len(rf.log) {
                reply.ConflictIndex = args.PrevLogIndex
                for reply.ConflictIndex > 0 {
                    if rf.log[reply.ConflictIndex-1].Term != args.PrevLogTerm {
                        break
                    }
                    reply.ConflictIndex--
                }
            }
            reply.Success = false
        } else {
            reply.Success = true
            entry_start_idx := 0
            // Figure 2 AppendEntries RPC 3
            if len(args.Entries) > 0 {
                for i := args.PrevLogIndex + 1 - 1; i >= 0 && i < len(rf.log) && entry_start_idx < len(args.Entries); i++{
                    if rf.log[i].Term != args.Entries[entry_start_idx].Term {
                        rf.log = rf.log[:i]
                        break
                    }
                    entry_start_idx++
                }
                // Figure 2 AppendEntries RPC 4
                for i := entry_start_idx; i < len(args.Entries); i++ {
                    rf.log = append(rf.log, args.Entries[i])
                }
                if entry_start_idx < len(args.Entries) {
                    rf.persist()
                }
            }
            // Figure 2 AppendEntries RPC 5
            committed_idx_before := rf.committed_idx
            if args.LeaderCommit > rf.committed_idx {
                if args.LeaderCommit < len(rf.log) {
                    rf.committed_idx = args.LeaderCommit
                } else {
                    rf.committed_idx = len(rf.log)
                }
            }
            if committed_idx_before < rf.committed_idx {
                go func() {
                    rf.raft_applied_sigal <- true
                }()
            }
        }
    }
    rf.mu.Unlock()
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) CheckPeerCommitIdx() {
    rf.mu.Lock()
    tmp_list := make([]int, len(rf.match_idx))
    copy(tmp_list, rf.match_idx)
    tmp_list[rf.me] = len(rf.log)
    sort.Ints(tmp_list)
    N := tmp_list[len(rf.peers)/2]
    //DPrintf(fmt.Sprintf("%v: tmp %v N %v c_idx %v", rf.me, tmp_list, N, rf.committed_idx))
    if N > rf.committed_idx && rf.log[N-1].Term == rf.cur_term {
        rf.committed_idx = N
        go func() {
            rf.raft_applied_sigal <- true
        }()
    }
    rf.mu.Unlock() 
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term int  // candidate's term
    CandidateId int    
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).
    Term int   // current term 
    VoteGranted bool  // candidate received vote or not
}

// election part

func (rf *Raft) BroadcastElection() {
    rf.mu.Lock()
    if rf.cur_state == 2 {
        rf.mu.Unlock()
        return
    }
    DPrintf(fmt.Sprintf("%v: Start Election", rf.me))
    // increase term, vote for itself, convert to candidate
    rf.cur_term++
    rf.voted_for = rf.me
    rf.vote_received = 1
    rf.cur_state = 1
    rf.persist()
    // unraced vars
    size := len(rf.peers)
    me := rf.me
    last_log_term := -1
    last_log_index := len(rf.log)
    if last_log_index > 0 {
        last_log_term = rf.log[last_log_index-1].Term
    }
    cur_term := rf.cur_term
    args := RequestVoteArgs{
        Term: cur_term, 
        CandidateId: me,
        LastLogIndex: last_log_index,
        LastLogTerm: last_log_term}
    rf.mu.Unlock()
    // channel waitting for vote result
    // -2 timeout, -1: stop, 0: reject, 1: vote
    vote_res_channel := make(chan int, size-1)
    for i := 0; i < size; i++ {
        if i == me {
            continue
        } 
        go rf.ElectionSender(i, args, vote_res_channel)
    }
    rf.ElectionResHandler(cur_term, vote_res_channel)
}

func (rf *Raft) ElectionSender(server_idx int, args RequestVoteArgs, vote_res_channel chan int) {
    reply := RequestVoteReply{
        Term: -1, 
        VoteGranted: false}
    is_ok := rf.sendRequestVote(server_idx, &args, &reply)
    rf.mu.Lock()
    // handle response
    if rf.cur_term != args.Term {
        // outdated, stop election
        vote_res_channel <- -1
        rf.mu.Unlock()
        return
    } 
    if is_ok {
        if reply.VoteGranted {
            // get vote
            vote_res_channel <- 1
        } else {
            // All Servers Rules 2
            if rf.cur_term < reply.Term {
                rf.cur_term = reply.Term 
                rf.cur_state = 0
                rf.persist()
                rf.ResetTimer(rf.cur_state)
                vote_res_channel <- -1
            } else {
                vote_res_channel <- 0
            }
        }
    } else {
        // get reply timeout
        vote_res_channel <- -2
    }
    rf.mu.Unlock()
}

func (rf *Raft) ElectionResHandler(sender_cur_term int, vote_res_channel chan int) {
    // this node itself
    vote_cnt := 1
    reject_cnt := 1
    is_finish := false
    for !is_finish {
        vote_res := <-vote_res_channel
        rf.mu.Lock()
        // term changed between RequestVote period
        if sender_cur_term != rf.cur_term {
            is_finish = true
            rf.mu.Unlock()
            continue
        } 
        // handle the vote res
        if vote_res == 1 {
            vote_cnt++
            if vote_cnt >= len(rf.peers)/2 + 1 {
                DPrintf(fmt.Sprintf("%v: Win Election", rf.me))
                rf.cur_state = 2
                for i := 0; i < len(rf.peers); i++ {
                    rf.next_idx[i] = len(rf.log) + 1
                    rf.match_idx[i] = 0
                }
                // boardcast heartbeats
                go rf.BoardcastHeartbeat()
                rf.ResetTimer(rf.cur_state)
                is_finish = true
            }
        } else if vote_res == 0 || vote_res == -2 {
            reject_cnt++
            if reject_cnt >= len(rf.peers)/2 + 1 {
                rf.cur_state = 0
                rf.ResetTimer(rf.cur_state)
                is_finish = true
            }
        } else {
            // receive stop from channel, stop the loop
            is_finish = true
        }
        rf.mu.Unlock()
    }
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    CheckLogUpToDate := func (arg_term int, arg_idx int, cur_term int, cur_idx int) bool {
        if arg_term > cur_term {
            return true
        }
        if arg_term == cur_term && arg_idx >= cur_idx {
            return true
        }
        return false
    }
    rf.mu.Lock()
    reply.Term = rf.cur_term
    // Figure 2 RequestVote RPC Rules 1
    if args.Term < rf.cur_term {
        reply.VoteGranted = false
    } else {
        // Figure 2 All Servers Rules 2
        if args.Term > rf.cur_term {
            rf.cur_term = args.Term
            rf.voted_for = -1
            rf.cur_state = 0
        }
        last_log_term := -1
        last_log_index := len(rf.log)
        if last_log_index > 0 {
            last_log_term = rf.log[last_log_index-1].Term
        }
        // Figure 2 RequestVote RPC Rules 2
        if (rf.voted_for == -1 || rf.voted_for == args.CandidateId) && 
            CheckLogUpToDate(args.LastLogTerm, args.LastLogIndex, last_log_term, last_log_index) {
            reply.VoteGranted = true
            //DPrintf(fmt.Sprintf("%v: Vote %v : %v, %v : %v", rf.me, last_log_term, last_log_index, args.LastLogTerm, args.LastLogIndex))
            rf.cur_state = 0
            rf.voted_for = args.CandidateId
            rf.ResetTimer(rf.cur_state)
        } else {
            reply.VoteGranted = false
        }
        if reply.VoteGranted || args.Term > rf.cur_term {
            rf.cur_state = 0
            rf.persist()
        }
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
    if rf.cur_state != 2 {
        isLeader = false
        rf.mu.Unlock()
        return index, term, isLeader
    }
    // Figure 2 Leaders Rules 2 Part 1
    // push back new log entry
    new_log_entry := LogEntry {rf.cur_term, command}
    rf.log = append(rf.log, new_log_entry)
    index = len(rf.log)
    term = rf.cur_term
    rf.ResetTimer(rf.cur_state)

    rf.persist()
    
    go rf.BoardcastHeartbeat()
    DPrintf(fmt.Sprintf("%v: Called %v", rf.me, new_log_entry))
    // Your code here (2B).
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
    rf.mu.Lock()
    rf.persist()
    rf.mu.Unlock()
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
    rf.cur_state = 0
    rf.cur_term = 0
    rf.voted_for = -1

    rf.vote_received = 0

    rf.committed_idx = 0
    rf.last_applied_idx = 0

    rf.match_idx = make([]int, len(peers))
    rf.next_idx = make([]int, len(peers))
    for idx := 0; idx < len(peers); idx++ {
        rf.next_idx[idx] = len(rf.log) + 1
        rf.match_idx[idx] = 0
    }

    // set the seed once for each raft node
    rand.Seed(time.Now().UnixNano() + int64(me))
    rf.raft_timer = time.NewTimer(GetRandomTime(rf.cur_state))
    // start background timer
    go rf.NodeTimer()

    rf.raft_apply_ch = applyCh
    rf.raft_applied_sigal = make(chan bool)
    go rf.ApplyMessage()
    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    return rf
}


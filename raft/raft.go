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
import "sync/atomic"
import "github.com/zilmano/raftproj/labrpc"

import "math/rand"
import "fmt"
import "time"
import "log"

// import "bytes"
// import "../labgob"



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


const RANDOM_TIMER_MAX = 600 // max value in ms
const RANDOM_TIMER_MIN = 300 // max value in ms
const HEARTBEAT_RATE = 5 // in hz, n beats a second

type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int
}

type LogEntry struct {
    Command interface{}
    Term int
}


type PeerState int
const (
    Follower = iota
    Candidate 
    Leader
)



//
// A Go object implementing a single Raft peer.
//

type Raft struct {
    mu        sync.Mutex          // Lock to protect shared access to this peer's state
    peers     []*labrpc.ClientEnd // RPC end points of all peers
    persister *Persister          // Object to hold this peer's persisted state
    me        int                 // this peer's index into peers[]
    dead      int32               // set by Kill()
    
    
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain. 
    // Persistent Data

    currentTerm int
    votedFor int
    log []LogEntry

    // Volatile Data
    commitIndex int
    lastApplied int

    nextIndex []int
    matchInex []int

    state PeerState
    gotHeartbeat bool
    heartbeatTerm int

      
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    var term int
    var isleader bool

    term = rf.currentTerm
    isleader = false
    if rf.state == Leader {
        isleader = true
    } 

    // Your code here (2A).
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


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

//TODO: Why do they tell us to start field names with capital letter if it is not done in Raft struct in the skeleton code?
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    CandidateTerm int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
    // Your data here (2A).
    LeaderTerm int
    LeaderId int
    LastLogIndex int
    LastLogTerm int
    LogEntries []LogEntry
    
}

type RequestVoteReply struct {
    // Your data here (2A).
    FollowerTerm int
    VoteGranted bool
}

type AppendEntriesReply struct {
    // Your data here (2A).
    CurrentTerm int
    Success bool
}


//
// example RequestVote RPC handler.
//

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    
    // TODO: Ask professor/TA if we need a lock here, as all the appendEntries set the heartbeat to 'true'
    //       so maybe technically we don't need it?
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.gotHeartbeat = true
    rf.heartbeatTerm = args.LeaderTerm

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).
    fmt.Printf("\nI the Peer %d got Vote Request from cadidate %d!\n\n",rf.me, args.CandidateId)
    rf.mu.Lock()
    defer rf.mu.Unlock() // TODO: ask professor/TA about this atomisitc and if mutex is needed.
    reply.FollowerTerm = rf.currentTerm
    
    logUpToDate := false
    if len(rf.log) == 0 {
        if args.LastLogIndex == -1 {
            logUpToDate = true
        }
    } else if rf.log[len(rf.log)-1].Term > args.LastLogTerm {
        logUpToDate = true
    } else if rf.log[len(rf.log)-1].Term  == args.LastLogTerm && 
              len(rf.log) >= (args.LastLogIndex+1) {
        logUpToDate = true
    }
    
    reply.VoteGranted = rf.currentTerm <= args.CandidateTerm && 
                        (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
                        logUpToDate
    
    if reply.VoteGranted {
        fmt.Printf("\nPeer %d: Vote for cadidate %d Granted!",rf.me, args.CandidateId)
    } else {
        fmt.Printf("\nPeer %d: Vote for cadidate %d Denied :/",rf.me, args.CandidateId)
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


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) sendHeartbeats() {
    
    // TODO: make the nexty 6 lines into a function later?
    rf.mu.Lock()
    numPeers := len(rf.peers)
    lastLogIndex := -1
    lastLogTerm := -1
    if len(rf.log) > 0 {
        lastLogIndex = numPeers-1
        lastLogTerm = rf.log[numPeers-1].Term 
    }
    rf.mu.Unlock()

    var args = AppendEntriesArgs {
        LeaderTerm : rf.currentTerm,
        LeaderId: rf.me,
        LastLogIndex: lastLogIndex,
        LastLogTerm: lastLogTerm,
        //LogEntries: ...  Leave log entries empty for now for heartbeats.
    }

    var replies = make([]AppendEntriesReply, numPeers) 
    for id := 0; id < numPeers; id++ {
        go rf.sendAppendEntries(id, &args, &replies[id])
    }

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
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    // Your initialization code here (2A, 2B, 2C).
    rf.dead = 0

    rf.currentTerm = 0
    rf.votedFor = -1
    rf.commitIndex = -1
    rf.lastApplied = -1
    rf.state = Follower

    rf.heartbeatTerm = -1
    rf.gotHeartbeat = false


    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    go func() {
        for {
            if rf.killed() {
                return 
            }

            time.Sleep(1/HEARTBEAT_RATE * time.Second) 
            if rf.state == Leader {
                go rf.sendHeartbeats() 
            }
        }

    } ()

    // Start Peer State Machine
    go func() {
        // Run forver
        for {
            if rf.killed() {
                return 
            }
            switch rf.state {
            case Follower:
                fmt.Printf("\n\npeer %d: I am candidate!!\n\n",rf.me)
                snoozeTime := rand.Float64()*(RANDOM_TIMER_MAX-RANDOM_TIMER_MIN) + RANDOM_TIMER_MIN
                time.Sleep(time.Duration(snoozeTime) * time.Millisecond) 
                rf.mu.Lock()
                if rf.gotHeartbeat && rf.heartbeatTerm < rf.currentTerm {
                    // Figure out what raft needs to do in this case of hearbeat from leader with a term that is not up-to-date.
                    //log.Fatal("Error: Got hearbeat from a leader with term less then mine. Not implemented yet. Peer %d", rf.me)
                    log.Fatal("Error: Got hearbeat from a leader with term less then mine. Not implemented yet")
                } else {
                    rf.state = Candidate
                }
                rf.mu.Unlock()
            
            case Candidate:
                fmt.Printf("\n\npeer %d: I am candidate!!",rf.me)
                
                rf.mu.Lock()
                rf.currentTerm++
                lastLogIndex := -1
                lastLogTerm := -1
                voteCount := 0


                // TODO: figure out what to with mutex when reading. Atomic? Lock?
                numPeers := len(rf.peers)
                
                if len(rf.log) > 0 {
                    lastLogIndex = numPeers-1
                    lastLogTerm = rf.log[numPeers-1].Term 
                }  
                
                var args = RequestVoteArgs {
                    CandidateTerm: rf.currentTerm,
                    CandidateId: rf.me,
                    LastLogIndex: lastLogIndex,
                    LastLogTerm: lastLogTerm,
                }

                var replies = make([]RequestVoteReply,numPeers)
                voteCount++
                for id:=0; id < (numPeers-1); id++ {
                    if id != rf.me  {
                        go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
                           ok := rf.sendRequestVote(id, args, reply)
                           if !ok {
                             reply.VoteGranted = false  
                           }
                        } (id, &args, &replies[id])
                    }
                }
                rf.mu.Unlock()

                // Sleep for enough for the messages and votes to happen, may be can be less then that, it depends on network communication
                snoozeTime := rand.Float64()*(RANDOM_TIMER_MAX-RANDOM_TIMER_MIN) + RANDOM_TIMER_MIN
                time.Sleep(time.Duration(snoozeTime) * time.Millisecond) 

                rf.mu.Lock()
                if (rf.gotHeartbeat && rf.currentTerm <= rf.heartbeatTerm) {
                    rf.state = Follower
                    rf.currentTerm = rf.heartbeatTerm
                } else {
                    for id:=0; id < numPeers; id++ {
                        if replies[id].VoteGranted {
                            voteCount++
                        }    
                    }

                    if voteCount > numPeers/2 {
                        rf.state = Leader
                    } 

                    rf.sendHeartbeats()
                }
                rf.mu.Unlock()

            case Leader:
                fmt.Printf("peer %d: I am leader!!\n\n",rf.me)
                time.Sleep(1/HEARTBEAT_RATE * time.Second)
                rf.mu.Lock()
                if rf.gotHeartbeat && rf.heartbeatTerm > rf.currentTerm {
                    // Figure out what raft needs to do in this case of hearbeat from leader with a term that is not up-to-date.
                    //log.Fatal("Error: Got hearbeat from a leader with term less then mine. Not implemented yet. Peer %d", rf.me)
                    rf.state = Follower
                } else if rf.gotHeartbeat && rf.heartbeatTerm == rf.currentTerm {
                    //log.Fatal("Fatal Error: Have two leaders in the same term!!!Peer %d", rf.me)
                    log.Fatal("Fatal Error: Have two leaders in the same term!!!")
                
                }
                rf.mu.Unlock() 

            }
            // sleep for the randomized time
            // if heartbeat recieved - restart the timeer
            // if timer elapsed - start election round
            rf.mu.Lock()
            rf.gotHeartbeat = false
            rf.heartbeatTerm = -1 // TODO: do we need to reset this at all?
            // need to reset votedFor as well.
            rf.mu.Unlock()    
        }
    } ()

    return rf
}

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
import "github.com/zilmano/raftproj/labgob"
import "math/rand"
//import "fmt"
import "time"
import "log"
import "math"
import "bytes"
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
const RANDOM_TIMER_MIN = 200 // max value in ms
const NETWORK_DELAY_BOUND = 10 // max value in ms
const HEARTBEAT_RATE = 5.0 // in hz, n beats a second

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
    applyCh chan ApplyMsg
    

    nextIndex []int
    matchIndex []int

    state PeerState
    gotHeartbeat bool
    
}


func find(input []int, elem int)(bool){

    for i := 0; i < len(input); i++ {
        if(input[i]==elem){
            return true
        }
        
    }
    return false
    
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    var term int
    var isleader bool

    rf.mu.Lock()
    defer rf.mu.Unlock()
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

//    fmt.Printf("\n\nPersist test: currentTerm is %d and peer %d voted for %d\nLog state is %v\n",rf.currentTerm,rf.me,rf.votedFor,rf.log)
    byte_array := new(bytes.Buffer)
    encoder := labgob.NewEncoder(byte_array)

    encoder.Encode(rf.currentTerm)
    encoder.Encode(rf.votedFor)

    rf.mu.Lock()            //Should we use lock over entire function?
  //  defer rf.mu.Unlock()
    
    log := append([]LogEntry(nil), rf.log...)
    log_length := len(log)
    
    rf.mu.Unlock()
    //tmp := append([]int(nil), arr...)
    for index := 0; index < log_length; index++ {
        encoder.Encode(log[index])
        encoder.Encode(log[index])

    }
    
    encoded_array := byte_array.Bytes()
    rf.persister.SaveRaftState(encoded_array)





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

    encoded_array := bytes.NewBuffer(data)
    decoder := labgob.NewDecoder(encoded_array)

    var currentTerm int         //Do we need to provide type?
    var votedFor int


    if (decoder.Decode(&currentTerm) != nil || decoder.Decode(&votedFor) != nil ){


    }else{
        rf.currentTerm = currentTerm
        rf.votedFor = votedFor
    }

    for {
        var logEntry_2 = LogEntry {}
        var logEntry = LogEntry {}                // Do we need to store decoded data as command and Term?

        if (decoder.Decode(&logEntry) != nil || decoder.Decode(&logEntry_2) != nil ){

            break // is it correct to break after first nil?

        }else{

            rf.log = append(rf.log,logEntry)
            
        }
    }

    // is this necessary?

    for id := 0; id < len(rf.peers); id++ {
    //    rf.nextIndex[id] = len(rf.log)
                               
    }




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
    PrevLogIndex int
    PrevLogTerm int
    LogEntries []LogEntry
    LeaderCommitIndex int
    
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

func (rf *Raft) CheckTerm(peerTerm int) bool {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.currentTerm < peerTerm {
        rf.currentTerm = peerTerm

        go rf.persist() // Saving state
        rf.state = Follower
        rf.votedFor = -1
        rf.gotHeartbeat = false
        return false
    } 
    return true
}

func (rf *Raft) ApplyChannel(commandIndex int, prevCommitIndex int) {
//    fmt.Printf("DBG::AppyChannel::peer %d: prevCommitIndex %d currCommitIndex %d\n", rf.me, prevCommitIndex, commandIndex)
    for commitIndex := prevCommitIndex+1; commitIndex <= commandIndex; commitIndex++ { 
        var oneApplyMsg ApplyMsg
        oneApplyMsg.CommandValid = true
        // Yaikes! Tester expects the log to start from index 1, while we start from index 0., need to increase +1
        oneApplyMsg.CommandIndex = commitIndex+1
   //     fmt.Printf("DBG::AppyChannel::peer %d: ApplyMsg %d\n", rf.me, oneApplyMsg.CommandIndex)
        oneApplyMsg.Command = rf.log[commitIndex].Command
        rf.applyCh <- oneApplyMsg
    }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    
    // TODO: Ask professor/TA if we need a lock here, as all the appendEntries set the heartbeat to 'true'
    //       so maybe technically we don't need it?
    if len(args.LogEntries) == 0 { 
//        fmt.Printf("Peer %d term %d: Got heartbeat from leader %d\n",rf.me, rf.currentTerm, args.LeaderId)
    }
    
    rf.CheckTerm(args.LeaderTerm)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    
    reply.CurrentTerm = rf.currentTerm
    reply.Success=true
    if rf.currentTerm == args.LeaderTerm {
        rf.gotHeartbeat = true
    }
    
    //if(len(args.LogEntries)==1){

    if args.LeaderTerm < rf.currentTerm{
        reply.Success=false
        return 
    }
    
    rf.gotHeartbeat = true
        
//    fmt.Printf("\nchecking some false conditions. PrevLogIndex of leader is %d and length of our log is %d. logEntries is %v\n",args.PrevLogIndex,((len(rf.log))), args.LogEntries)
    if args.PrevLogIndex >= len(rf.log) {
        reply.Success = false
        return
    }

    if((args.PrevLogIndex > -1) && (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm )){
 //       fmt.Printf("\nsuccess set to false\n")
        reply.Success = false
        return
    }

    if len(args.LogEntries) > 0 { 
        
        joinIndex := len(args.LogEntries)
 
        var logIndex int
        for index := 0; index < len(args.LogEntries); index++ {
            logIndex = index + args.PrevLogIndex + 1
            if  logIndex >= len(rf.log) {
                joinIndex = index
                break
            } else if rf.log[logIndex].Term != args.LogEntries[index].Term {
                joinIndex = index
                rf.log = rf.log[0:logIndex]
                break
            }
        }

  //      fmt.Printf("Join Index %d\n", joinIndex)
        rf.log = append(rf.log, args.LogEntries[joinIndex:]...)

        go rf.persist() // Saving state

        // Discuss: What would happen if the packets get lost. would RPC return false. clues.

        // Discuss: 4. Handle success case
//        fmt.Printf("Entry Appended succesfull to peer %d\n",rf.me)
//        fmt.Printf("Peer %d Update log %v\n",rf.me,rf.log)
           
    }

    prevCommitIndex := rf.commitIndex
//    fmt.Printf("PrevCommitIndex: %d\n", rf.commitIndex)
    if (args.LeaderCommitIndex > rf.commitIndex){
        if(args.LeaderCommitIndex<len(rf.log)){
            rf.commitIndex = args.LeaderCommitIndex
        } else {
            rf.commitIndex = len(rf.log)-1
        }
 //       fmt.Printf("Follower comming to log\n")
        rf.ApplyChannel(rf.commitIndex, prevCommitIndex)
    }
 }



//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
//    fmt.Printf("\n -> I the Peer %d in got Vote Request from cadidate %d!\n",rf.me, args.CandidateId)
    
    rf.CheckTerm(args.CandidateTerm)     
    rf.mu.Lock()
    defer rf.mu.Unlock() // TODO: ask professor/TA about this atomisitc and if mutex is needed.
    
    reply.FollowerTerm = rf.currentTerm
    // 2B code - fix if needed
    logUpToDate := false
    if len(rf.log) == 0 {
        logUpToDate = true
    } else if rf.log[len(rf.log)-1].Term < args.LastLogTerm {
        logUpToDate = true
    } else if rf.log[len(rf.log)-1].Term  == args.LastLogTerm && 
        len(rf.log) <= (args.LastLogIndex+1) {
        logUpToDate = true
    }
    // 2B code end
    
    reply.VoteGranted = (rf.currentTerm <= args.CandidateTerm && 
                        (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
                        logUpToDate) 

    if reply.VoteGranted {
        rf.votedFor = args.CandidateId
        go rf.persist() // Saving state
 //       fmt.Printf("-> I the Peer %d say: Vote for cadidate %d Granted!\n",rf.me, args.CandidateId)
    } else {
   //     fmt.Printf("-> I the Peer %d say: Vote for cadidate %d Denied :/\n",rf.me, args.CandidateId)
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
//    fmt.Printf("   sendRequestProc: sendRequest to %d from %d\n", server, args.CandidateId)
    // Why is there no lock here? We are accessing a common variable.
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
//    fmt.Printf("   sendAppendEntriesProc: append entries RPC to %d from %d\n", server, args.LeaderId)
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf *Raft) sendHeartbeats() {
    // TODO: make the nexty 6 lines into a function later?
    rf.mu.Lock()
    numPeers := len(rf.peers)
    
    // 2B code start - change if needed
    lastLogIndex := -1
    lastLogTerm := -1
    myId := rf.me
    if len(rf.log) > 0 {
        /*
        lastLogIndex = numPeers-1
        fmt.Printf("\nNumber of peers is %d within sendHeartbeats\n",numPeers)  // Discuss 1
        lastLogTerm = rf.log[numPeers-1].Term 
        */
        lastLogIndex = len(rf.log)-1
        lastLogTerm = rf.log[lastLogIndex].Term

    }
    
    var args = AppendEntriesArgs {
        LeaderTerm : rf.currentTerm,
        LeaderId: rf.me,
        LeaderCommitIndex: rf.commitIndex,
        PrevLogIndex: lastLogIndex,
        PrevLogTerm: lastLogTerm,
        //LogEntries: ...  Leave log entries empty for now for heartbeats.
    }
    rf.mu.Unlock()
    //2B code end.

    for id := 0; id < numPeers; id++ {
        if id != myId {
            // TODO: Ask Instructors: Will we eventually run out of space if keep sending these go routines and there are no answers? we wait until sendAppendEntries respond
            go func(serverId int) {
                var reply AppendEntriesReply
                rf.sendAppendEntries(serverId, &args, &reply)
                rf.CheckTerm(reply.CurrentTerm)
                // 2B code goes here. 
                //(to process the replies, if we are using heartbeats to send entries as well)
            } (id)
        }
    }
}


func (rf *Raft) sendVoteRequests(replies []RequestVoteReply, numPeers int) {
 
//    fmt.Printf("   peer %d candidate: Sending requests to %d peers\n", rf.me, numPeers)
 
    // 2B code start - change if needed
    lastLogIndex := -1
    lastLogTerm := -1
    rf.mu.Lock()
    myId := rf.me // TODO: Ask instructors - I am latching the server id to avoid a lock later on line 479, is this a correct pattern?
                               // Ask instructors: what happens if we use a mutex.lock on one piece of code, and another piece of code access these variables without a mutex, will it wait for the mutex
                               // to unlock? Will it continue without retrieving a value? 
    if len(rf.log) > 0 {
        lastLogIndex = len(rf.log)-1
        lastLogTerm = rf.log[lastLogIndex].Term 
    }
    var args = RequestVoteArgs {
        CandidateTerm: rf.currentTerm,
        CandidateId: rf.me,
        LastLogIndex: lastLogIndex,
        LastLogTerm: lastLogTerm,
    }
    rf.mu.Unlock()
    //2B code end

    for id:=0; id < numPeers; id++ {
        if id != myId  {
            go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
               ok := rf.sendRequestVote(server, args, reply)
               rf.CheckTerm(reply.FollowerTerm)
               // TODO: Do I need the lock for reading onle?
               rf.mu.Lock()
               if !ok {
                 reply.VoteGranted = false  
      //           fmt.Printf("Peer %d candidate: Send request to peer %d failed, no connection.\n", rf.me, server)
               } else {
        //          fmt.Printf("Peer %d candidate: Send request to peer %d worked\n", rf.me, server)
               }
               rf.mu.Unlock()
    
            } (id, &args, &replies[id])
        }
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
    lastLogIndex := 0
    
    // TODO WED: check corner cases with -1
    rf.mu.Lock()
    term := rf.currentTerm
    myId := rf.me
    if len(rf.log) > 0 {
        lastLogIndex = len(rf.log)
        //term = rf.log[index].Term 
    }
    
    if rf.state != Leader || rf.killed() {
 //       fmt.Printf("Peer %d  is not valid Leader. Exiting/ \n", rf.me)
        rf.mu.Unlock()
        return lastLogIndex-1, term, false
    }
    
    var oneEntry LogEntry
    oneEntry.Command = command
    oneEntry.Term = term
    
    rf.log = append(rf.log, oneEntry)
//    fmt.Printf("START %d: Adding command %d. Leader new log size: %d\n", command, command , len(rf.log))
    go rf.persist() // Saving state
    rf.mu.Unlock()

    
    go func() {
        // Add a while loop. when successReply count greater than threhsold, commit. loop breaks when successReply is equal to peers
        // the for loop inside only iterates over the left peers.
        
        //committed := false
        successReplyCount := 0
        var receivedResponse []int
        receivedResponse = append(receivedResponse, myId)

        
        for true {
            if rf.killed() {
          //          fmt.Printf("*** Peer %d term %d: Terminated. Closing all outstanding Append Entries calls to followers.\n", myId, term)
                    return 
            }

            rf.mu.Lock()
            numPeers := len(rf.peers)
            rf.mu.Unlock()
            for id := 0; id < numPeers; id++ {
                //fmt.Printf("DBG::id %d recievedResponse %v",id , receivedResponse)
                if (!find(receivedResponse, id))  {
        //            fmt.Printf("START %d: Sending AppendEntries to peer %d\n", command, id) 
                    //fmt.Printf("DBG::lastLogIndex %d rf.nextIndex[%d] %d\n",lastLogIndex ,id, rf.nextIndex[id])
                    if lastLogIndex < rf.nextIndex[id] {
                        successReplyCount++
                        receivedResponse = append(receivedResponse,id)
                        continue
                    }

                    go func(serverId int) {
                        // TODO: you can prob remove the append to logEntries and assign directly, is logEntries is empty
                        
                        var logEntries []LogEntry
                        var args = AppendEntriesArgs {
                               LeaderId: myId,
                        }
                        
                        rf.mu.Lock()
                        if (rf.nextIndex[serverId] == -1) {
                            logEntries = append(logEntries, rf.log...)
                        } else {
                            logEntries = append(logEntries,rf.log[(rf.nextIndex[serverId]):]...)
                        }

                        if rf.nextIndex[serverId] == 0 {
                            args.PrevLogTerm = 0
                        } else {
                            args.PrevLogTerm = rf.log[rf.nextIndex[serverId]-1].Term
                        }
                        
                        latchLogLength := len(rf.log)
                    
                        args.LogEntries = logEntries
          //              fmt.Printf("START %d: Log entries to send to peer %d: %v\n", command, serverId, logEntries)
                        args.PrevLogIndex = rf.nextIndex[serverId]-1
                        args.LeaderTerm = term
                        args.LeaderCommitIndex = rf.commitIndex
                        rf.mu.Unlock()

                        var reply AppendEntriesReply
                        ok:=rf.sendAppendEntries(serverId, &args, &reply)
                        
                        if !ok {
                //           fmt.Printf("START %d: Append entries to peer %d wasn't delivered. Will retry.\n", command, serverId)
                        } else if !rf.CheckTerm(reply.CurrentTerm) || term < reply.CurrentTerm {
                //            fmt.Printf("START %d: Oooh. I am not the current Leader any more peer %d. I got a reply from someone with a higher term!!\n", command, rf.me)
                            return 
                        } else if reply.Success {
                            successReplyCount++
                            receivedResponse = append(receivedResponse,serverId)
                            rf.mu.Lock()
                            if (latchLogLength-1 > rf.matchIndex[serverId]) {
                                rf.matchIndex[serverId] = latchLogLength-1
                                rf.matchIndex[rf.me] = latchLogLength-1
                            }
                            // TODO: Ask the Prof about the correctness of this.
                            rf.nextIndex[serverId] = latchLogLength // len(rf.log())
                        
                   //         fmt.Printf("START %d: Recieve successReply AppendEntry for peer %d latchLogLength %d \n", command, serverId, latchLogLength)
                    //        fmt.Printf("START %d: DBG:: rf.matchIndex  %v \n", command, rf.matchIndex)
                            
                            rf.mu.Unlock()
                        // TODO: That was a cool bug here, with nextIndex being decremented when the message is not delivered
                        } else  {
                      //      fmt.Printf("START %d: Append entries to peer %d failed. Decrease nextIndex.\n", command, serverId)
                            rf.mu.Lock()
                            if rf.nextIndex[serverId] != 0 {
                                rf.nextIndex[serverId]-- 
                            }
                            rf.mu.Unlock()
                        } 
                    } (id)
                }
            }
            
//            fmt.Printf("\nSTART %d: sleeping before counting success replies\n", command)
            time.Sleep(time.Duration(NETWORK_DELAY_BOUND*time.Millisecond))
            
            rf.mu.Lock()
            if rf.state != Leader {
                rf.mu.Unlock()
                return 
            }
            rf.mu.Unlock()
            
  //          fmt.Printf("START %d: Counting votes...\n", command)
            votesForIndex := 0
            N :=  math.MaxInt32
            rf.mu.Lock()
            for i := 0; i < numPeers; i++ {
         //       fmt.Printf("DBG::votecount id:%d matchIndex %d\n", i, rf.matchIndex[i])
                if rf.matchIndex[i] > rf.commitIndex {
                    if rf.matchIndex[i] < N {
                        N = rf.matchIndex[i]
                    }
                    votesForIndex++
                    // TODO: Add the id of peers with match indexes bigger then lastLogIndex
                    //       To recievedResponse[]? 
                }
            }
//            fmt.Printf("START %d: Votes counted %d min commit index %d\n", command, votesForIndex, N)
            rf.mu.Unlock()

            if (votesForIndex > (numPeers/2)){ 
     //           fmt.Printf("START %d: Commiting entry as there is enough votes.\n", command)
                go func(){
                    //committed = true
                    rf.mu.Lock()
                    prevCommitIndex := rf.commitIndex
                    rf.commitIndex = N     // Discuss: 3. should we use lock?
                    //rf.log[N].Term = rf.currentTerm
                    if rf.commitIndex > prevCommitIndex {
                        rf.ApplyChannel(N, prevCommitIndex)
                    }
                    rf.mu.Unlock()
                }()
            }
        
            if successReplyCount == numPeers-1 {
//                fmt.Printf("START %d:Got confirmation from all peers that we are good. Killing this start.\n", command)
                return
            }

            rf.mu.Lock()
            if lastLogIndex < len(rf.log)-1 {
                rf.mu.Unlock()
                return
            }
            rf.mu.Unlock()

        }
    } ()
    
    // Your code here (2B code).
    return lastLogIndex+1, term, true
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

func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me
    rf.applyCh = applyCh

    // Your initialization code here (2A, 2B, 2C).
    rf.dead = 0

    rf.currentTerm = 0

    go rf.persist() // Saving state
    rf.votedFor = -1
    rf.commitIndex = -1
    rf.lastApplied = -1
    rf.state = Follower
    rf.gotHeartbeat = false

    rf.nextIndex = make([]int, len(rf.peers))
    rf.matchIndex = make([]int, len(rf.peers))
    for id := 0; id < len(rf.peers); id++ {

    
        rf.nextIndex[id] = 0
        rf.matchIndex[id] = 0
    }

 //   fmt.Printf("Finish 'Making' peer %d...\n", rf.me)

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    // Start Peer State Machine
    go func() {
        // Run forver
        for {
            
            if rf.killed() {
    //            fmt.Printf("*** Peer %d term %d: I have been terminated. Bye.",rf.me, rf.currentTerm)
                return 
            }
            
      //      fmt.Printf("-- Peer %d term %d, waiting to aquire lock on state.\n",rf.me, rf.currentTerm)
            rf.mu.Lock()
            state := rf.state
            rf.mu.Unlock()
            
            switch state {
            case Follower:
        //        fmt.Printf("-- Peer %d term %d, status update:  I am follolwer.\n",rf.me, rf.currentTerm)
                snoozeTime := rand.Float64()*(RANDOM_TIMER_MAX-RANDOM_TIMER_MIN) + RANDOM_TIMER_MIN
       //         fmt.Printf("   peer %d  term %d -- follower -- : Set election timer to time %f\n", rf.me, rf.currentTerm, snoozeTime)
                time.Sleep(time.Duration(snoozeTime) * time.Millisecond) 
                
                rf.mu.Lock()  
         //       fmt.Printf("   peer %d term %d -- follower -- : my election timer had elapsed.\n",rf.me, rf.currentTerm)
                if (!rf.gotHeartbeat) {
           //         fmt.Printf("-> Peer %d term %d -- follower --: did not get heartbeat during the election timer. Starting election!\n",rf.me, rf.currentTerm) 
                    rf.state = Candidate
                }
                rf.gotHeartbeat = false
                rf.mu.Unlock()
            

            case Candidate:
                rf.mu.Lock()
                rf.currentTerm++

                go rf.persist() // Saving state
             //   fmt.Printf("-- peer %d: I am candidate! Starting election term %d\n",rf.me, rf.currentTerm)

                numPeers := len(rf.peers) // TODO: figure out what to with mutex when reading. Atomic? Lock?
                rf.votedFor = rf.me
            //    oldTerm := rf.currentTerm // cache Old term before sleep for logging purposes  
                go rf.persist() // Saving state
                rf.mu.Unlock()
                
                voteCount := 1
                var replies = make([]RequestVoteReply, numPeers)
                
                
                rf.sendVoteRequests(replies, numPeers)

                snoozeTime := rand.Float64()*(RANDOM_TIMER_MAX-RANDOM_TIMER_MIN) + RANDOM_TIMER_MIN
//                fmt.Printf("   peer %d term %d -- candidate -- :Set snooze timer to time %f\n", rf.me, rf.currentTerm, snoozeTime)
                time.Sleep(time.Duration(snoozeTime) * time.Millisecond) 
                
                rf.mu.Lock()
  //              fmt.Printf("   peer %d term %d -- candidate -- :Waking up from snooze to count votes. %f\n", rf.me, oldTerm, snoozeTime)
                if (rf.state != Follower) {
    //                fmt.Printf("-> Peer %d term %d -- candidate --: Start Counting votes...\n\n",rf.me, rf.currentTerm)
                    
                    for id:=0; id < numPeers; id++ {
                        if id != rf.me && replies[id].VoteGranted {
                            voteCount++
                        }    
                    }

                    if voteCount > numPeers/2 {
                        // Initialize leader nextIndex and match index
                        for id:=0; id < numPeers; id++{
                            rf.nextIndex[id] = len(rf.log)
                            rf.matchIndex[id] = 0
                        }

      //                  fmt.Printf("   peer %d candidate: I am elected leader for term %d. voteCount:%d majority_treshold %d\n\n",rf.me,rf.currentTerm, voteCount, numPeers/2)
                        rf.state = Leader
        //                fmt.Printf("-> Peer %d leader of term %d: I send first heartbeat round to assert my authority.\n\n",rf.me, rf.currentTerm)
                        go rf.sendHeartbeats()
                        // sanity check: (if there is another leader in this term then it cannot be get the majority of votes)
                        if rf.gotHeartbeat {
                            log.Fatal("Two leaders won election in the same term!")
                        }
                    } else if rf.gotHeartbeat {
          //              fmt.Printf("   peer %d candidate of term %d: I got heartbeat from a leader. So I step down :) \n",rf.me, rf.currentTerm)
                        rf.state = Follower
                    } else {
            //            fmt.Printf("   peer %d candidate term %d: Did not have enough votes. Moving to a new election term.\n\n",rf.me,rf.currentTerm)
                    }  
                } else {
              //     fmt.Printf("   peer %d term %d -- candidate -- :I woke up, butsomeone with higher term answered. Reverting to follower :/ %f\n", rf.me, oldTerm, snoozeTime)
                 
                }
                rf.mu.Unlock()
                

            case Leader:
    //            fmt.Printf("-- Peer %d term %d: I am leader.\n\n",rf.me, rf.currentTerm)
                snoozeTime := (1/HEARTBEAT_RATE)*1000 
      //          fmt.Printf("   Leader %d term %d: snooze for %f\n", rf.me, rf.currentTerm, snoozeTime)
                
                time.Sleep(time.Duration(snoozeTime) * time.Millisecond)
                
                rf.mu.Lock()
                
                for id:=0; id< (len(rf.peers)); id++{
        //            fmt.Printf("   Leader %d term %d:  rf.nextIndex[%d] is %d\n", rf.me, rf.currentTerm, id, rf.nextIndex[id])
                }


                if rf.state != Follower {

                    if rf.gotHeartbeat  {
                        log.Fatal("Fatal Error: Have two leaders in the same term!!!")
                    }
          //          fmt.Printf("   peer %d term %d --leader-- : I send periodic heartbeat.\n",rf.me, rf.currentTerm)
                    go rf.sendHeartbeats()
                } 
                rf.mu.Unlock()

            }
        }
    } ()
    

    return rf
}

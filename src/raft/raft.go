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
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"

	//"bytes"
	//"labgob"
)
// import "bytes"
// import "labgob"



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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	votedFor int
	currentTerm int
	commitIndex int
	lastApplied int
	nextIndex[] int
	matchIndex[] int
	log[] LogEntry

	//extra
	election chan bool
	heartbeat chan bool
	state string
	applyCh chan ApplyMsg

	//log compaction
	lastIncludedIndex int
	lastIncludedTerm int
}

type LogEntry struct{
	LogIndex int
	Term int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	//fmt.Printf("Server %d, in Get State", rf.me)
	//fmt.Println()
	var term int = rf.currentTerm
	var isleader bool
	// Your code here (2A).
	if rf.state == "leader" {
		isleader = true;
	} else {
		isleader = false;
	}
	rf.mu.Unlock()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	//e.Encode((rf.votedFor))
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var term int
	//var votedFor int //don't know why not working with votedFor
	if d.Decode(&log) != nil || d.Decode(&term) != nil {
		panic("Something went wrong")
	} else {
		rf.log = log
		rf.currentTerm = term
		//rf.votedFor = votedFor
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
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
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//fmt.Println("Requesting Vote Before lock")
	rf.mu.Lock()
	//fmt.Printf("Server %d, in RequestVote", rf.me)
	//fmt.Println()

	//fmt.Println("Requesting Vote in RequestVote")
	//fmt.Printf("Args Term %d, in RequestVote for Server %d", args.Term,rf.me)
	//fmt.Println()
	//fmt.Printf("Server Term %d, in RequestVote for Server %d", rf.currentTerm,rf.me)
	//fmt.Println()
	//fmt.Printf("Server Voted for %d, in RequestVote for Server %d", rf.votedFor,rf.me)
	//fmt.Println()
	//fmt.Printf("Server candidate Id %d, in RequestVote for Server %d", args.CandidateId,rf.me)
	//fmt.Println()
	//fmt.Printf("Args lastTerm %d, in RequestVote for Server %d", args.LastLogTerm,rf.me)
	//fmt.Println()
	////fmt.Println(rf.log[len(rf.log)-1].term)
	//fmt.Printf("Args lastIndex %d, in RequestVote for Server %d", args.LastLogIndex, rf.me)
	//fmt.Println()

	//fmt.Println(rf.log[len(rf.log)-1].logIndex)
	if args.Term < rf.currentTerm {
		//fmt.Printf("Server %d, has higher term than arguments", rf.currentTerm)
		//fmt.Println()
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}else{
		rf.currentTerm = args.Term
		rf.state = "follower"
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			//fmt.Printf("VotedFor for Server %d, is either -1 or candidateId", rf.me)
			//fmt.Println()
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}

		if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
			//fmt.Printf("Args lastlog term is less than server %d, last term", rf.me)
			//fmt.Println()
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		} else if args.LastLogTerm == rf.log[len(rf.log)-1].Term {
			if args.LastLogIndex < rf.log[len(rf.log)-1].LogIndex{
				//fmt.Printf("Args lastlog term is same as the server %d, last term but index is not", rf.me)
				//fmt.Println()
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			}else{
				//fmt.Printf("Args lastlog term is same as the server %d, last term  as well its either greater or same index", rf.me)
				//fmt.Println()
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
			}
		}else{
			//fmt.Printf("Args lastlog term is greater than server %d, last term", rf.me)
			//fmt.Println()
			reply.Term = args.LastLogTerm
			reply.VoteGranted = true
			//rf.currentTerm = reply.Term
			rf.votedFor = args.CandidateId
		}

		rf.persist()
	}
	//rf.persist()
	//fmt.Println("Request Vote completed")
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Println("Requesting Vote in sendRequestVote")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries[] LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	NextTryIndex int
	UseNextTryIndex bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	//fmt.Printf("Server %d, in Append Entries", rf.me)
	//fmt.Println()
	//
	////fmt.Println("Requesting Vote in RequestVote")
	//fmt.Printf("Leaders Term is %d, for Server %d", args.Term,rf.me)
	//fmt.Println()
	//fmt.Printf("Leader is %d for Server %d", args.LeaderId,rf.me)
	//fmt.Println()
	//fmt.Printf("PrevLogIndex is %d for Server %d ",args.PrevLogIndex, rf.me)
	//fmt.Println()
	//fmt.Printf("PrevLogterm is %d for Server %d ",args.PrevLogTerm, rf.me)
	//fmt.Println()
	//fmt.Printf("Leaders commitIndex is %d for Server %d", args.LeaderCommit, rf.me)
	//fmt.Println()

	if args.Term < rf.currentTerm {
		//fmt.Printf("Server %d, args.Term < rf.currentTerm for args.Term %d for server who send it %d", rf.me,args.Term,args.LeaderId)
		//fmt.Println()
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextTryIndex = len(rf.log)
		reply.UseNextTryIndex = false
		//fmt.Printf("NextTryIndex is %d and args.Term < rf.currentTerm ", reply.NextTryIndex)
		//fmt.Println()
	}else{
		//Check whether the message is for heartbeats or appending logs
		if len(args.Entries) > 0 {
			//fmt.Printf("args.Entries > 0 for server %d",rf.me)
			//fmt.Println()
			//Test2C Fig8Unreliable will throw index out of range if condition not added
			if args.PrevLogIndex > len(rf.log)-1 || args.PrevLogIndex < 0 || rf.log[args.PrevLogIndex].LogIndex != args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				//fmt.Printf("Either term is not uptoDate or Index is not uptoDate with leader for server %d",rf.me)
				//fmt.Println()
				reply.Success = false
				reply.Term = rf.currentTerm
				rf.votedFor = args.LeaderId
				rf.state = "follower"
				rf.currentTerm = args.Term
				reply.UseNextTryIndex = false

				if args.PrevLogIndex > 1 && args.PrevLogIndex < len(rf.log){
					reply.NextTryIndex = args.PrevLogIndex - 1
				}

				if args.PrevLogIndex > 1 && args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
					//reply.NextTryIndex = args.PrevLogIndex
					term := rf.log[args.PrevLogIndex].Term
					for reply.NextTryIndex = args.PrevLogIndex - 1;
						reply.NextTryIndex > 1 && rf.log[reply.NextTryIndex].Term == term;
					reply.NextTryIndex-- {}

					//reply.NextTryIndex++
					//fmt.Printf("NextTryIndex is %d and log is not uptoDate ", reply.NextTryIndex)
					//fmt.Println()
				}

				if reply.NextTryIndex > 1 {
					reply.UseNextTryIndex = true
				}

				//rf.heartbeat <- true
			}else{
				//fmt.Printf("Term and index are uptoDate with leader for server %d",rf.me)
				//fmt.Println()
				reply.Success = true
				reply.Term = rf.currentTerm
				rf.votedFor = args.LeaderId
				rf.state = "follower"
				rf.currentTerm = args.Term
				rf.heartbeat <- true
				reply.UseNextTryIndex = true
				reply.NextTryIndex = args.PrevLogIndex

				rf.log = rf.log[:args.PrevLogIndex+1] //splice log to last committed index of leader

				//append new entries
				for i := 0; i < len(args.Entries); i++ {
					rf.log = append(rf.log, args.Entries[i])
					//fmt.Printf("Entries appended %d for Server %d",args.Entries[i],rf.me)
					//fmt.Println()
				}

				if len(rf.log)-1 < args.LeaderCommit { //Point no 5 of appendEntries RPC
					rf.commitIndex = len(rf.log) - 1
					//fmt.Printf("CommitIndex is %d which got changed as it is greater than args.leadercommit for Server  %d",rf.commitIndex,rf.me)
					//fmt.Println()
				}else{
					rf.commitIndex = args.LeaderCommit
				}

				//fmt.Printf("Last Applied is %d for Server %d",rf.lastApplied,rf.me)
				//fmt.Println()

				rf.applyMesaages()

			}
		}else{ //null entries for heartbeat
			if rf.log[len(rf.log)-1].LogIndex == args.PrevLogIndex && rf.log[len(rf.log)-1].Term == args.PrevLogTerm {
				//fmt.Printf("Inside null entries for heartbeat for Server %d",rf.me)
				//fmt.Println()
				reply.Success = true
				reply.Term = rf.currentTerm
				rf.votedFor = args.LeaderId
				rf.state = "follower"
				rf.currentTerm = args.Term
				reply.UseNextTryIndex = true
				reply.NextTryIndex = args.PrevLogIndex
				//rf.commitIndex = args.LeaderCommit
				rf.heartbeat <- true

				if len(rf.log)-1 < args.LeaderCommit {
					rf.commitIndex = len(rf.log) - 1
				}else{
					rf.commitIndex = args.LeaderCommit
				}
				rf.applyMesaages()
			}else{
				//fmt.Println("Inside else")
				reply.Term = rf.currentTerm
				rf.votedFor = args.LeaderId
				rf.state = "follower"
				rf.currentTerm = args.Term
				reply.Success = false
				reply.UseNextTryIndex = false

				if args.PrevLogIndex > 1 && args.PrevLogIndex < len(rf.log){
					reply.NextTryIndex = args.PrevLogIndex - 1
				}
				if args.PrevLogIndex > 1 && args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
					//reply.NextTryIndex = args.PrevLogIndex
					term := rf.log[args.PrevLogIndex].Term
					for reply.NextTryIndex = args.PrevLogIndex - 1;
						reply.NextTryIndex > 1 && rf.log[reply.NextTryIndex].Term == term;
					reply.NextTryIndex-- {}

					//reply.NextTryIndex++
					//fmt.Printf("NextTryIndex is %d and args.Entries is null ", reply.NextTryIndex)
					//fmt.Println()
				}

				if reply.NextTryIndex > 1 {
					reply.UseNextTryIndex = true
				}
				//rf.isLeaderElection = false
			}
		}
		rf.persist()
	}
	//rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
	Done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = len(rf.log)
	term = rf.currentTerm
	if rf.state != "leader" {
		isLeader = false
	}

	if isLeader {
		//fmt.Printf("Command received,%d",command)
		//fmt.Println()
		rf.log = append(rf.log, LogEntry{index, term, command})
		//fmt.Printf("Length of log at Start,%d",len(rf.log))
		//fmt.Println()
		rf.persist()
	}

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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.state = "follower"
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heartbeat = make(chan bool,1)
	rf.election = make(chan bool)
	rf.log = make([] LogEntry,1)
	rf.applyCh = applyCh

	//fmt.Println("Inside make");
	//num:= len(peers);
	//fmt.Print("Number of servers: 	");
	//fmt.Println(num)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			rf.mu.Lock()
			if rf.state == "follower" {
				//rf.mu.Lock()
				rf.checkfollower()
			} else if rf.state == "candidate" {
				//rf.mu.Lock()
				rf.checkCandidate()
			} else if rf.state == "leader" {
				//rf.mu.Lock()
				//fmt.Println("Inside leader")
				rf.checkLeader()
			}
			//go rf.checkLeader() //Repeatedly check for leader state.
		}
	}()


	return rf
}

func(rf * Raft) checkLeader(){
	//fmt.Printf("Server %d, Leader", rf.me)
	//fmt.Println()

	//rf.mu.Unlock()
	//rf.persist()
	for index:=  range rf.peers{
		if index != rf.me {
			//fmt.Printf("Inside loop of leader %d",rf.me)
			//fmt.Println()
			//rf.mu.Lock()
			//fmt.Printf("PrevLogIndex,%d",rf.nextIndex[index]-1)
			//fmt.Println()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      rf.log[rf.nextIndex[index]:],
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.log[rf.nextIndex[index]-1].LogIndex,
				PrevLogTerm:  rf.log[rf.nextIndex[index]-1].Term}
			reply:= AppendEntriesReply{}
			//rf.mu.Unlock()

			go func(index int){
				ok:= rf.sendAppendEntries(index,&args,&reply)
				if ok {
					rf.mu.Lock()
					if reply.Success == true{
						//fmt.Printf("Success for leader %d",rf.me)
						//fmt.Println()
						if len(args.Entries) > 0 && args.Term == rf.currentTerm {
							//fmt.Printf("Inside args.Entries > 0 and terms are same for index %d",index)
							//fmt.Println()
							rf.nextIndex[index]  = rf.nextIndex[index] + len(args.Entries) //update nextIndex
							if rf.nextIndex[index] > len(rf.log) { //removing redundant entries
								rf.nextIndex[index] = len(rf.log)
							}
							//fmt.Printf("Log length is %d", len(args.Entries))
							//fmt.Println()
							rf.updateCommittedIndex()
							rf.applyMesaages();
						}
						//rf.mu.Unlock()
					} else {
						//fmt.Printf("Inside else for server %d",rf.me)
						//fmt.Println()
						if reply.Term == rf.currentTerm { //Terms were same but index mismatched
							//rf.mu.Lock()
							//fmt.Printf("Inside else and Terms are same for server %d",rf.me)
							//fmt.Println()
							//if reply.NextTryIndex > 0{
							//	rf.nextIndex[index] = reply.NextTryIndex
							//}

							if reply.UseNextTryIndex == true {
								rf.nextIndex[index] = reply.NextTryIndex
							}else{
								rf.nextIndex[index] = 1
							}

							//fmt.Printf("Next Index: %d",rf.nextIndex[index] )
							//fmt.Println()
							//rf.mu.Unlock()
						} else { //higher term found
							//rf.mu.Lock()
							//fmt.Printf("Server %d, turned to follower", rf.me)
							//fmt.Println()
							rf.state = "follower"
							//rf.mu.Unlock()
							//rf.heartbeat <- true //created lots of problems
							//rf.mu.Unlock()
						}
					}
					rf.mu.Unlock()
				}
			}(index)
		}
	}
	rf.mu.Unlock()
	//select {
	//case <-rf.heartbeat: //if heartbeat received before winning election change to follower
	//	//fmt.Println("Candidate Heartbeat received")
	//	//rf.mu.Lock()
	//	fmt.Printf("Server %d, Leader received heartbeat changing to follower", rf.me)
	//	fmt.Println()
	//	rf.state = "follower"
	////default:
	//}
	time.Sleep(time.Millisecond * 100) // pause for loops as advised in lab description
}

func (rf *Raft) checkfollower(){
	//defer rf.mu.Unlock()
	//rf.mu.Lock()
	//fmt.Printf("Server %d, follower", rf.me)
	//fmt.Println()
	rf.mu.Unlock()
	select {
	case <-rf.heartbeat:
		//fmt.Printf("Server %d, follower heartbeat received", rf.me)
		//fmt.Println()
	case <-time.After(time.Duration(300+rand.Intn(300)) * time.Millisecond): //Referred https://telliott.io/2016/09/29/three-ish-ways-to-implement-timeouts-in-go.html
		rf.mu.Lock()
		//fmt.Printf("Server %d, follower timeout  and changed to candidate", rf.me)
		//fmt.Println()
		rf.state = "candidate"
		rf.mu.Unlock()
	}

	//rf.mu.Unlock()
}

func (rf *Raft) checkCandidate() {
	//rf.mu.Unlock()
	//rf.mu.Lock()
	//fmt.Println("Inside Candidate")
	//defer rf.mu.Unlock()
	//
	// fmt.Println("Inside candidate")
	//rf.mu.Lock()
	//fmt.Printf("Server %d, is candidate", rf.me)
	//fmt.Println()
	//fmt.Println("Inside Lock")
	rf.currentTerm = rf.currentTerm + 1 //Update currentTerm
	rf.votedFor = rf.me //Vote for itself
	//fmt.Println("Inside candidate2")
	//rf.mu.Unlock()

	args:= RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].LogIndex,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	voteCount:= 1

	rf.persist()
	for index:=  range rf.peers {
		if index!=rf.me{
			go func(index int){
				reply:= RequestVoteReply{}
				//fmt.Println("Before  sendRequestVote")
				ok:= rf.sendRequestVote(index,&args,&reply)
				//rf.mu.Lock()

				if ok {
					if reply.VoteGranted == true {
						rf.mu.Lock()
						//fmt.Printf("Server %d, Vote Granted", index)
						//fmt.Println()
						voteCount = voteCount + 1 //if vote granted increment by  1
						//rf.mu.Unlock()
						if voteCount > len(rf.peers) / 2 && rf.state == "candidate" { //if majority votes received and state still candidate change to leader
							//rf.mu.Lock()
							rf.state = "leader"
							//fmt.Printf("Server %d, changed to leader and won election", rf.me)
							//fmt.Println()
							//fmt.Println("Election set true")
							rf.election <- true //Election done
							//rf.mu.Unlock()
						}
						rf.mu.Unlock()
					} else {
						//fmt.Println("Vote Not Granted")
						rf.mu.Lock()
						if reply.Term > rf.currentTerm && rf.state == "candidate"{ //if reply term greater change to follower
							//fmt.Println("Changing to follower")
							rf.currentTerm = reply.Term //set current term of server with latest term
							//fmt.Printf("Server %d, candidate changed to follower", rf.me)
							//fmt.Println()
							rf.state = "follower"
							rf.votedFor = -1
						}
						rf.mu.Unlock()
					}
				}
				//rf.mu.Unlock()
			}(index)
		}
	}
	rf.mu.Unlock()

	select {
	case <-rf.heartbeat: //if heartbeat received before winning election change to follower
		//fmt.Println("Candidate Heartbeat received")
		rf.mu.Lock()
		//fmt.Printf("Server %d, candidate received heartbeat changing to follower", rf.me)
		//fmt.Println()
		rf.state = "follower"
		rf.persist()
		rf.mu.Unlock()
	case <-time.After(time.Duration(300+rand.Intn(300)) * time.Millisecond): //if timedout before winning election start again
		//fmt.Println("Candidate Timedout")
		//fmt.Printf("Server %d, candidate timedout", rf.me)
		//fmt.Println()
	case <- rf.election: //if election won , change state to leader and update Next indexes of servers
		rf.mu.Lock()
		//fmt.Println("election won")
		rf.state = "leader"
		//fmt.Printf("Server %d, changed to leader and won election", rf.me)
		//fmt.Println()
		rf.persist()
		rf.updateIndexes()

		//rf.checkLeader()
		rf.mu.Unlock()
	}

	//rf.mu.Unlock()
}

func(rf * Raft) updateIndexes(){
	//fmt.Println("Inside updateIndexes")
	rf.nextIndex = make([]int, len(rf.peers))
	var nextIndex int = len(rf.log)

	for i := range rf.peers {
		rf.nextIndex[i] = nextIndex
		//fmt.Printf("next Index is %d for server %d set by leader %d",nextIndex,i,rf.me)
		//fmt.Println()
	}
}

func (rf *Raft) updateCommittedIndex() {

	rf.matchIndex = make([]int, len(rf.nextIndex))
	copy(rf.matchIndex, rf.nextIndex)
	rf.matchIndex[rf.me] = len(rf.log)

	sort.Ints(rf.matchIndex)

	N := rf.matchIndex[len(rf.peers)/2] - 1

	if rf.log[N].Term == rf.currentTerm {
		//fmt.Printf("Updating commitIndex to %d",N)
		//fmt.Println()
		rf.commitIndex = N
	}

	if len(rf.log) < rf.matchIndex[rf.me] && 1!=1{
		for N:= rf.commitIndex + 1; N<len(rf.log);N++{
			count:=1
			for i:=0;i<len(rf.peers);i++{
				if rf.matchIndex[i] - 1 >= N {
					count++
				}
			}

			fmt.Println(count)

			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				break
			}
		}
	}

}

func (rf *Raft) applyMesaages() {

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.lastApplied++
		//fmt.Printf("Appying message to state machine where lastApplied is %d for Server %d",rf.lastApplied,rf.me)
		//fmt.Println()
		rf.applyCh <- ApplyMsg{Command: rf.log[i].Command, CommandIndex: rf.log[i].LogIndex, CommandValid: true}
	}
}

func (rf *Raft) timeout(){
	//fmt.Println("Start:")
	timeout:= make(chan bool,1)
	go func(){
		time.Sleep(time.Duration(300+rand.Intn(200)) * time.Millisecond) //timout of more than 300ms because the tester limits you to 10 heartbeats per second
		timeout <- true
		fmt.Println("Reset Timeout in timeout:")
	} ()

}
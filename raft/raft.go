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
	"DongoMQ/kitex_gen/api"
	"DongoMQ/kitex_gen/api/raft_operations"
	"bytes"
	"context"
	"encoding/json"
	"log"
	"sort"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const heartBeatTimeout time.Duration = 100 * time.Millisecond

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	TopicName    string
	PartName     string
	BeLeader     bool
	Leader       int
	CommandValid bool
	Command      Operation
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	topicName string
	partName  string

	mu        sync.RWMutex              // Lock to protect shared access to this peer's state
	peers     []*raft_operations.Client // RPC end points of all peers
	persister *Persister                // Object to hold this peer's persisted state
	me        int                       // this peer's index into peers[]
	dead      int32                     // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int   //当前任期
	votedFor    int   //当前任期投票对象
	log         []Log //日志,第一条为{Term:快照任期，Index:快照索引，Cmd:nil}

	electionTimeout time.Duration //选举超时时间
	time            time.Time     //开始计算选举超时的时间
	//heartBeatTimeout time.Duration //心跳超时时间
	//lastHeartBeat    time.Time     //上一次心跳时间
	state       string        //当前身份
	applyCh     chan ApplyMsg //应用状态机通道
	commitIndex int           //已提交的最高的日志条目的索引
	lastApplied int           //已经被应用到状态机的最高的日志条目的索引
	nextIndex   []int         //对于每一台服务器，发送到该服务器的下一个日志条目的索引
	matchIndex  []int         //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引
	applyCond   *sync.Cond
	snapshot    []byte //日志快照
}

type Log struct {
	Term     int
	Index    int
	BeLeader bool
	Leader   int
	Command  Operation
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == "Leader"
}

func (rf *Raft) RaftSize() (int, int) {
	rf.mu.Lock()
	XSize := rf.log[0].Index
	rf.mu.Unlock()

	return XSize, rf.persister.RaftStateSize()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		log.Printf("Persist failed for term %d:%v\n", rf.currentTerm, e)
	}
	raftState := w.Bytes()
	if rf.log[0].Index > 0 {
		rf.persister.Save(raftState, rf.snapshot)
	} else {
		rf.persister.Save(raftState, nil)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (3C).
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
	//  rf.yyy = yyy
	// }

	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := NewDecoder(r)
	var tmpCurrentTerm int
	var tmpVotedFor int
	var tmpLog []Log
	if d.Decode(&tmpCurrentTerm) != nil ||
		d.Decode(&tmpVotedFor) != nil ||
		d.Decode(&tmpLog) != nil {
		log.Printf("readPersist failed for term %d: %v", rf.currentTerm, d)
	} else {
		rf.currentTerm = tmpCurrentTerm
		rf.votedFor = tmpVotedFor
		rf.log = tmpLog
	}
	rf.lastApplied = rf.log[0].Index
	rf.commitIndex = rf.log[0].Index
}

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		DPrintf("%d 读取快照失败：无快照\n", rf.me)
		return
	}
	rf.snapshot = data
	DPrintf("%d 读取快照成功\n", rf.me)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index || index <= rf.log[0].Index {
		DPrintf("[%d] %d 拒绝 snapshot 请求 index %d\n", rf.currentTerm, rf.me, index)
		return
	}

	firstIndex := rf.log[0].Index
	pos := index - firstIndex
	rf.log[0].Index = index
	rf.log[0].Term = rf.log[pos].Term
	rf.log = append(rf.log[0:1], rf.log[pos+1:]...)
	rf.snapshot = snapshot
	rf.persist()
	DPrintf("[%d] %d 将索引 %d 及之前的日志压缩为快照\n", rf.currentTerm, rf.me, index)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int //候选人任期号
	CandidateId  int //请求选票的候选人的ID
	LastLogIndex int //候选人的最后日志条目的索引值
	LastLogTerm  int //候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	// Your code here (3A, 3B).
	reply := &RequestVoteReply{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[%d] %d的任期大于%d或者在当前任期已经投出过选票，拒绝投票\n", rf.currentTerm, rf.me, args.CandidateId)
		return reply
	}
	if args.Term > rf.currentTerm {
		rf.state = "Follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	term := rf.log[len(rf.log)-1].Term
	index := rf.log[len(rf.log)-1].Index
	if args.LastLogTerm < term || (args.LastLogTerm == term && args.LastLogIndex < index) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[%d} %d的日志比%d的日志新，拒绝投票\n", rf.currentTerm, rf.me, args.CandidateId)
		return reply
	}

	rf.votedFor = args.CandidateId
	rf.resetTimeout()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	DPrintf("[%d] %d agree %d to become leader\n", rf.currentTerm, rf.me, rf.votedFor)
	return reply
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Log          []Log
	LeaderCommit int
	LogIndex     int //leader最新日志的索引
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int //冲突Log任期号
	XIndex  int //对应任期号为XTerm的第一条Log的槽位号/下一条日志索引
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	reply := &AppendEntriesReply{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = -1
	reply.XIndex = -1
	if args.Term < rf.currentTerm {
		return reply
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = "Follower"
	rf.resetTimeout()
	if args.PrevLogIndex < rf.log[0].Index { //过期RPC
		reply.Success = true
		return reply
	} else if args.PrevLogIndex > rf.log[len(rf.log)-1].Index { //prevLogIndex处不存在日志
		reply.XTerm = -1
		reply.XIndex = rf.log[len(rf.log)-1].Index + 1
		DPrintf("[%d] %d's logs are less than the leader %d's\n", rf.currentTerm, rf.me, args.LeaderId)
		return reply
	} else if args.PrevLogIndex == rf.log[0].Index && args.PrevLogTerm != rf.log[0].Term {
		reply.XTerm = rf.log[0].Term
		reply.XIndex = args.PrevLogIndex - 1
		DPrintf("[%d] %d 's logs conflict with the leader's\n", rf.currentTerm, rf.me)
		return reply
	} else if args.PrevLogIndex != rf.log[0].Index && args.PrevLogTerm != rf.log[args.PrevLogIndex-rf.log[0].Index].Term {
		reply.XTerm = rf.log[args.PrevLogIndex-rf.log[0].Index].Term
		index := args.PrevLogIndex
		for index > rf.commitIndex && rf.log[index-rf.log[0].Index].Term == reply.XTerm {
			index--
		}
		reply.XIndex = index + 1
		DPrintf("[%d] %d 's logs conflict with the leader's\n", rf.currentTerm, rf.me)
		return reply
	}

	if len(args.Log) != 0 {
		rf.log = append(rf.log[:args.PrevLogIndex-rf.log[0].Index+1], args.Log...)
		DPrintf("[%d] %d successfully append logs to %d from the leader %d\n", rf.currentTerm, rf.me, rf.log[len(rf.log)-1].Index, args.LeaderId)
	}
	prevCommitIndex := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)+rf.log[0].Index-1)
	}
	if rf.commitIndex > prevCommitIndex {
		DPrintf("[%d] %d update commitIndex to %d according to the leader %d\n", rf.currentTerm, rf.me, rf.commitIndex, args.LeaderId)
		rf.applyCond.Signal()
	}
	reply.Success = true
	return reply
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	reply := &InstallSnapshotReply{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.log[0].Index {
		reply.Term = rf.currentTerm
		return reply
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = "Follower"
	rf.resetTimeout()
	if len(rf.log)+rf.log[0].Index-1 <= args.LastIncludedIndex {
		rf.log = make([]Log, 1)
	} else {
		rf.log = append(rf.log[0:1], rf.log[args.LastIncludedIndex-rf.log[0].Index:]...)
	}
	rf.snapshot = args.Snapshot
	rf.log[0].Index = args.LastIncludedIndex
	rf.log[0].Term = args.LastIncludedTerm

	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	DPrintf("[%d] %d 将日志快照 %d 应用到状态机\n", rf.currentTerm, rf.me, rf.log[0].Index)
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = rf.lastApplied
	reply.Term = rf.currentTerm
	DPrintf("[%d] %d 成功接收来自 %d 的日志快照到索引 %d\n", rf.currentTerm, rf.me, args.LeaderId, args.LastIncludedIndex)
	reply.Success = true
	return reply
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
func (rf *Raft) callRequestVote(server int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	reply, err := (*(rf.peers[server])).RequestVote(context.Background(), &api.RequestVoteArgs_{
		Term:         int8(args.Term),
		CandidateId:  int8(args.CandidateId),
		LastLogIndex: int8(args.LastLogIndex),
		LastLogTerm:  int8(args.LastLogTerm),
		TopicName:    rf.topicName,
		PartName:     rf.partName,
	})
	if err != nil {
		return nil, false
	} else {
		return &RequestVoteReply{
			Term:        int(reply.Term),
			VoteGranted: reply.VoteGranted,
		}, true
	}
}

func (rf *Raft) callAppendEntries(server int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	log_, _ := json.Marshal(args.Log)
	reply, err := (*(rf.peers[server])).AppendEntries(context.Background(), &api.AppendEntriesArgs_{
		Term:         int8(args.Term),
		LeaderId:     int8(args.LeaderId),
		PrevLogIndex: int8(args.PrevLogIndex),
		PrevLogTerm:  int8(args.PrevLogTerm),
		Log:          log_,
		LeaderCommit: int8(args.LeaderCommit),
		LogIndex:     int8(args.LogIndex),
		TopicName:    rf.topicName,
		PartName:     rf.partName,
	})
	if err != nil {
		return nil, false
	} else {
		return &AppendEntriesReply{
			Term:    int(reply.Term),
			Success: reply.Success,
			XTerm:   int(reply.XTerm),
			XIndex:  int(reply.XIndex),
		}, true
	}
}

func (rf *Raft) callInstallSnapshot(server int, args *InstallSnapshotArgs) (*InstallSnapshotReply, bool) {
	snapshot_, _ := json.Marshal(args.Snapshot)
	reply, err := (*(rf.peers[server])).InstallSnapshot(context.Background(), &api.InstallSnapshotArgs_{
		Term:              int8(args.Term),
		LeaderId:          int8(args.LeaderId),
		LastIncludedIndex: int8(args.LastIncludedIndex),
		LastIncludedTerm:  int8(args.LastIncludedTerm),
		Snapshot:          snapshot_,
		TopicName:         rf.topicName,
		PartName:          rf.partName,
	})
	if err != nil {
		return nil, false
	} else {
		return &InstallSnapshotReply{
			Term:    int(reply.Term),
			Success: reply.Success,
		}, true
	}
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
func (rf *Raft) Start(command Operation, beLeader bool, leader int) (int, int, bool) {
	// Your code here (3B).
	if _, is := rf.GetState(); is == false {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newLog := Log{
		Term:     rf.currentTerm,
		Index:    len(rf.log) + rf.log[0].Index,
		Command:  command,
		BeLeader: beLeader,
		Leader:   leader,
	}

	rf.log = append(rf.log, newLog)
	rf.persist()
	DPrintf("[%d] %d receive a new command %v,then append at %d\n", rf.currentTerm, rf.me, command, newLog.Index)
	return newLog.Index, newLog.Term, true
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetTimeout() {
	rf.time = time.Now()
	ms := 300 + (rand.Int63() % 300)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
	//DPrintf("[%d] %d reset timeout %v\n", rf.currentTerm, rf.me, rf.electionTimeout)
}

func (rf *Raft) startElection() {
	DPrintf("[%d] %d try to be Leader\n", rf.currentTerm, rf.me)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = "Candidate"
	rf.persist()

	voted := 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			if reply, ok := rf.callRequestVote(peer, &args); ok {
				DPrintf("[%d] %d 向 %d 发送requestVote\n", rf.currentTerm, rf.me, peer)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				defer rf.persist()
				if reply.VoteGranted {
					if rf.currentTerm != args.Term || rf.state != "Candidate" {
						return
					}
					voted++
					if voted >= len(rf.peers)/2+1 {
						DPrintf("[%d] %d get more than half votes,succeed to be leader\n", rf.currentTerm, rf.me)
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.log) + rf.log[0].Index
							rf.matchIndex[i] = rf.log[0].Index
						}
						rf.state = "Leader"
						go rf.BroadcastHeartBeat()

						//追加一条标志自己成为leader的日志
						go rf.Start(Operation{
							Cli_name: "Leader",
							Tpart:    rf.topicName + rf.partName,
							Topic:    rf.topicName,
							Part:     rf.partName,
						}, true, rf.me)
						return
					}
				} else if reply.Term > rf.currentTerm {
					rf.state = "Follower"
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					DPrintf("[%d] %d fail to be leader\n", rf.currentTerm, rf.me)
					return
				}
			}
		}(peer)
	}
}

func (rf *Raft) judgeElectionTimeout() bool {
	return time.Now().Sub(rf.time) > rf.electionTimeout
}

func (rf *Raft) updateCommitIndex() {
	prevCommitIndex := rf.commitIndex
	rf.matchIndex[rf.me] = rf.log[0].Index + len(rf.log) - 1
	nums := make([]int, len(rf.peers))
	copy(nums, rf.matchIndex)
	sort.Ints(nums)

	index := nums[len(nums)/2]
	for index > rf.commitIndex {
		if rf.log[index-rf.log[0].Index].Term == rf.currentTerm {
			break
		}
		index--
	}
	rf.commitIndex = index
	if rf.commitIndex > prevCommitIndex {
		DPrintf("[%d] %d update commitIndex to %d\n", rf.currentTerm, rf.me, rf.commitIndex)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		//if rf.judgeElectionTimeout() {
		//	rf.mu.Lock()
		//	if rf.state != "Leader" {
		//		rf.startElection()
		//	}
		//	rf.resetTimeout()
		//	rf.mu.Unlock()
		//}
		switch rf.state {
		case "Follower":
			fallthrough
		case "Candidate":
			if rf.judgeElectionTimeout() {
				rf.mu.Lock()
				rf.resetTimeout()
				rf.startElection()
				rf.mu.Unlock()
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// 发送 AppendEntries(日志复制/心跳) / InstallSnapshot
func (rf *Raft) BroadcastHeartBeat() {
	DPrintf("[%d] leader %d start to send heartbeats\n", rf.currentTerm, rf.me)
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != "Leader" {
			rf.mu.Unlock()
			return
		}
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			go rf.sendHeartbeat(peer)
		}
		rf.mu.Unlock()
		time.Sleep(heartBeatTimeout)
	}
}

func (rf *Raft) sendHeartbeat(peer int) {
	rf.mu.Lock()
	if rf.state != "Leader" {
		rf.mu.Unlock()
		return
	}
	sendInstallSnapshot := false
	if rf.nextIndex[peer] <= rf.log[0].Index {
		sendInstallSnapshot = true
	}
	if sendInstallSnapshot {
		args := rf.genInstallSnapshotArgs()
		rf.mu.Unlock()
		if reply, ok := rf.callInstallSnapshot(peer, &args); ok {
			rf.mu.Lock()
			rf.handleInstallSnapshotReply(peer, args, *reply)
			rf.persist()
			rf.mu.Unlock()
		}
	} else {
		args := rf.genAppendEntriesArgs(peer)
		rf.mu.Unlock()
		if reply, ok := rf.callAppendEntries(peer, &args); ok {
			rf.mu.Lock()
			rf.handleAppendEntriesReply(peer, args, *reply)
			rf.persist()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) genInstallSnapshotArgs() InstallSnapshotArgs {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Snapshot:          rf.snapshot,
	}
	return args
}

func (rf *Raft) handleInstallSnapshotReply(peer int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	if reply.Term > rf.currentTerm {
		rf.state = "Follower"
		rf.resetTimeout()
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		DPrintf("[%d] a new leader happen,%d become follower\n", rf.currentTerm, rf.me)
		return
	}
	if reply.Success {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
}

func (rf *Raft) genAppendEntriesArgs(peer int) AppendEntriesArgs {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Log:          nil,
		LeaderCommit: rf.commitIndex,
		LogIndex:     rf.log[len(rf.log)-1].Index,
	}
	for rf.nextIndex[peer] > rf.log[0].Index {
		args.PrevLogIndex = rf.nextIndex[peer] - 1
		if args.PrevLogIndex >= rf.log[0].Index+len(rf.log) {
			rf.nextIndex[peer]--
			continue
		}
		if args.PrevLogIndex == rf.log[0].Index {
			args.PrevLogTerm = rf.log[0].Term
		} else {
			args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.log[0].Index].Term
		}
		break
	}
	if rf.nextIndex[peer] <= rf.log[len(rf.log)-1].Index {
		args.Log = make([]Log, args.LogIndex+1-rf.nextIndex[peer])
		copy(args.Log, rf.log[rf.nextIndex[peer]-rf.log[0].Index:len(rf.log)])
		DPrintf("[%d] %d send %d %d logs from %d\n", rf.currentTerm, rf.me, peer, len(args.Log), rf.nextIndex[peer])
	} else {
		// DPrintf("just heartbeat")
	}
	return args
}

func (rf *Raft) handleAppendEntriesReply(peer int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if rf.state != "Leader" || args.Term != rf.currentTerm { //过期消息
		return
	}
	if reply.Term > rf.currentTerm {
		rf.state = "Follower"
		rf.resetTimeout()
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		DPrintf("[%d] a new leader happen,%d become follower\n", rf.currentTerm, rf.me)
		return
	}
	if reply.Success == true {
		if rf.nextIndex[peer] > args.LogIndex+1 {
			return
		}
		if args.LogIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = args.LogIndex
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.updateCommitIndex()
		return
	} else if reply.Success == false {
		rf.nextIndex[peer] = max(reply.XIndex, 1)
		DPrintf("[%d] %d change %d's nextIndex to %d\n", rf.currentTerm, rf.me, peer, rf.nextIndex[peer])
	}
}

func (rf *Raft) applyCommited() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		commitIndex := rf.commitIndex
		firstIndex := rf.lastApplied + 1 - rf.log[0].Index
		lastIndex := rf.commitIndex - rf.log[0].Index
		logs := make([]Log, lastIndex-firstIndex+1)
		copy(logs, rf.log[firstIndex:lastIndex+1])
		rf.mu.Unlock()

		for _, msg := range logs {
			DPrintf("[%d] %d apply the log %d %v\n", rf.currentTerm, rf.me, msg.Index, msg.Command)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      msg.Command,
				CommandIndex: msg.Index,
				BeLeader:     msg.BeLeader,
			}
			if applyMsg.BeLeader {
				applyMsg.TopicName = rf.topicName
				applyMsg.PartName = rf.partName
				applyMsg.Leader = msg.Leader
			}
			rf.applyCh <- applyMsg
		} //apply时比较耗时，这个时候不能持有锁

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex) //如果有日志快照被应用，需要考虑到被该日志快照覆盖的日志已经过期，不能再apply
		rf.mu.Unlock()
	}
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
func Make(peers []*raft_operations.Client, me int,
	persister *Persister, applyCh chan ApplyMsg, topicName, partName string) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.topicName = topicName //标识是代表哪个server的raft
	rf.partName = partName

	// Your initialization code here (3A, 3B, 3C).
	rf.state = "Follower"
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []Log{{0, 0, false, -1, Operation{}}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.resetTimeout()
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())
	rf.readSnapshot(rf.persister.ReadSnapshot())

	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommited()

	return rf
}

type Operation struct {
	Cli_name  string // client的唯一标识
	Cmd_index int64  // 操作id号
	Ser_index int64  // Server的id
	Operate   string // 这里的操作只有append
	Tpart     string // 这里的shard为topic_partition
	Topic     string
	Part      string
	Num       int

	Msg  []byte
	Size int8
}

package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

// raftapi/raft.go 文件定义了 Raft 必须向服务器（或测试程序）暴露的接口，
// 但每个函数的更多细节请参见下方注释。
//
// Make() 创建一个新的 Raft 节点，实现 Raft 接口。

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// 实现单个 Raft 节点的 Go 对象
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state  保护对该节点状态共享访问的锁
	peers     []*labrpc.ClientEnd // RPC end points of all peers 所有节点的 RPC 端点
	persister *tester.Persister   // Object to hold this peer's persisted state 保存该节点持久化状态的对象
	me        int                 // this peer's index into peers[]  该节点在 peers[] 中的索引
	dead      int32               // set by Kill() // 由 Kill() 设置

	currentTerm   int        // 当前已知的最新任期号（初始为 0，单调递增）
	votedFor      int        // 当前任期投票给的候选人ID（-1 表示未投票）
	log           []LogEntry // 日志条目数组（初始包含一个虚拟条目，索引为 0 的占位条目）
	state         StateType
	votesReceived int
	lastHeartBeat time.Time

	// 3B
	// Leader专用状态（选举成功后初始化）
	nextIndex   []int // 每个Follower的下一个待发送日志索引
	matchIndex  []int // 每个Follower已复制的最高日志索引
	commitIndex int   // 已知已提交的最高日志索引
	lastApplied int   // 已应用到状态机的最高日志索引

	// 你的数据字段（3A, 3B, 3C）
	// 查看论文图2了解 Raft 服务器必须维护的状态
}
type LogEntry struct {
	Term    int         // 该日志条目被创建时的任期号
	Command interface{} // 状态机命令（
}
type StateType int

const (
	Follower StateType = iota
	Candidate
	Leader
)

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	isleader = rf.state == Leader
	term = rf.currentTerm
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

// 将 Raft 的持久状态保存到稳定存储中，
// 以便在崩溃重启后可以恢复。
// 查看论文图2了解哪些状态应该持久化。
// 在实现快照之前，你应该传递 nil 作为 persister.Save() 的第二个参数。
// 实现快照后，传递当前快照（如果没有快照则为 nil）。

func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
// 恢复之前持久化的状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
// Raft 持久化日志的字节数？
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 服务层表示已创建包含直到并包括 index 的所有信息的快照。
// 这意味着服务不再需要该索引之前（包括该索引）的日志。
// Raft 现在应尽可能修剪其日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int // 接收方的任期号
	VoteGranted bool
}

// RequestVote RPC 处理程序示例。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 只给任期号大于等于自己当前任期的候选人投票
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1 // 重置投票状态
		rf.state = Follower
	}
	// 候选人的日志必须至少和自己一样新（通过比较最后一条日志）
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	IsCandidateNew := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if IsCandidateNew && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	return

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

// 使用 Raft 的服务（例如 k/v 服务器）希望开始
// 对下一个要追加到 Raft 日志的命令达成一致。如果该服务器
// 不是领导者，返回 false。否则开始达成一致并立即返回。
// 不能保证该命令一定会被提交到 Raft 日志，因为领导者
// 可能失败或选举失败。即使 Raft 实例已被终止，
// 该函数也应优雅返回。
//
// 第一个返回值是命令如果被提交将出现的索引。
// 第二个返回值是当前任期。
// 第三个返回值是 true 如果该服务器认为自己是领导者。

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
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
// 测试程序不会在每次测试后停止由 Raft 创建的 goroutine，
// 但会调用 Kill() 方法。你的代码可以使用 killed() 检查
// 是否已调用 Kill()。使用 atomic 可以避免加锁。
//
// 问题是长时间运行的 goroutine 会占用内存和 CPU 时间，
// 可能导致后续测试失败并产生混乱的调试输出。
// 任何有长时间运行循环的 goroutine 应调用 killed() 检查是否应停止。

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartBeat = time.Now()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	reply.Success = true // 心跳默认成功
}

func (rf *Raft) ticker() {
	// 发送心跳间隔是 50~250ms
	// 超时心跳间隔是
	for rf.killed() == false {
		ms := 50 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		_, IsLeader := rf.GetState()
		if IsLeader {
			// 发送心跳
			rf.sendHeartbeats()
		} else {
			// 心跳检测-超时
			rf.mu.Lock()
			lastheartbeat := rf.lastHeartBeat
			rf.mu.Unlock()
			if time.Since(lastheartbeat) > 400*time.Millisecond {
				rf.convertToCandidate()
			}
		}
	}
}
func (rf *Raft) sendHeartbeats() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				// 空 entries[] 表示心跳
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			rf.sendAppendEntries(peer, &args, &reply)
		}(peer)
	}
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartBeat = time.Now()
	rf.startElection()
}

func (rf *Raft) startElection() {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.votesReceived = 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.handleVoteReply(reply)
			}
		}(peer)
	}
}

func (rf *Raft) handleVoteReply(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// 退化为follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
		return
	}
	if rf.state == Candidate && reply.VoteGranted {
		rf.votesReceived++
		if rf.votesReceived > len(rf.peers)/2 {
			// 当选
			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers)) // 必须初始化
			rf.matchIndex = make([]int, len(rf.peers))
			go rf.sendHeartbeats() // 立即发送心跳巩固地位
		}
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
// 服务或测试程序希望创建一个 Raft 服务器。所有 Raft 服务器
// （包括本服务器）的端口在 peers[] 中。本服务器的端口是 peers[me]。
// 所有服务器的 peers[] 数组顺序相同。persister 是该服务器
// 保存其持久状态的地方，初始时也保存最近的状态（如果有）。
// applyCh 是一个通道，测试程序或服务期望 Raft 通过它发送 ApplyMsg 消息。
// Make() 必须快速返回，因此它应为任何长时间运行的工作启动 goroutine。

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0, Command: nil}}, // 索引 0 的虚拟条目

		state:         Follower,
		lastHeartBeat: time.Now(),

		// Leader 状态（暂为空）
		nextIndex:  nil,
		matchIndex: nil,
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

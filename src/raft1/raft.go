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
	"6.5840/labgob"
	"bytes"
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
	applyCh     chan raftapi.ApplyMsg
}
type LogEntry struct {
	Term    int         // 该日志条目被创建时的任期号
	Command interface{} // 状态机命令
	Index   int         // 正式的日志索引由1开始
}
type StateType int

const (
	Follower StateType = iota
	Candidate
	Leader
)

// 3A
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	isleader = rf.state == Leader
	term = rf.currentTerm
	return term, isleader
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
		rf.persist()
	}
	// 候选人的日志必须至少和自己一样新（通过比较最后一条日志）
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	IsCandidateNew := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if IsCandidateNew && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
	return

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) ticker() {
	// 发送心跳间隔是 50~250ms
	// 超时心跳间隔是
	for rf.killed() == false {
		ms := 50 + (rand.Int63() % 25)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		_, IsLeader := rf.GetState()
		if IsLeader {
			// 发送心跳
			rf.mu.Lock()
			rf.lastHeartBeat = time.Now()
			rf.mu.Unlock()
			rf.CloneToAll()
		} else {
			ms1 := 50 + (rand.Int63() % 200)
			time.Sleep(time.Duration(ms1) * time.Millisecond)
			// 心跳检测-超时
			rf.mu.Lock()
			lastheartbeat := rf.lastHeartBeat
			rf.mu.Unlock()
			if time.Since(lastheartbeat) > time.Duration(ms)+300*time.Millisecond {
				rf.convertToCandidate()
			}
		}
	}
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartBeat = time.Now()
	rf.persist()
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
		rf.persist()
		return
	}
	if rf.state == Candidate && reply.VoteGranted {
		rf.votesReceived++
		if rf.votesReceived > len(rf.peers)/2 {
			// 当选
			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers)) // 必须初始化
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log) //它会将所有 nextIndex 值初始化为其⽇志中最后⼀个条⽬之后的索引
			}
			rf.matchIndex = make([]int, len(rf.peers))
			//log.Printf("new: leader %v", rf.me)
			go rf.CloneToAll() // 立即发送心跳巩固地位
		}
	}
}

// 3B

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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if !isLeader {
		return -1, -1, false
	}

	index := len(rf.log) // 如果提交应该出现的索引
	newlog := &LogEntry{Term: term, Command: command, Index: index}
	//log.Printf("%v leader: %v", newlog, rf.me)
	rf.log = append(rf.log, *newlog)
	rf.persist()

	return index, term, isLeader
}

func (rf *Raft) CloneToAll() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			var args AppendEntriesArgs
			IsAppend := false
			if rf.nextIndex[peer] == len(rf.log) {
				args = AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
					Entries:      nil,
				}
			} else {
				args = AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[peer] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
					Entries:      rf.log[rf.nextIndex[peer]:],
					LeaderCommit: rf.commitIndex,
				}
				IsAppend = true
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				return
			}
			if reply.Success && IsAppend {
				rf.mu.Lock()
				rf.matchIndex[peer] = len(rf.log) - 1
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				rf.mu.Unlock()
				return
			}
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			if reply.Term > currentTerm { //退位
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				return
			}
			rf.mu.Lock()
			// 到这里就是 reply失败 next应该回退直到找到能够匹配的位置
			if rf.nextIndex[peer] > 1 && IsAppend { // 到这里 follower崩了不应该再回溯了
				if reply.ConflictTerm == -1 {
					rf.nextIndex[peer] = reply.ConflictIndex
				} else {
					lastIndex := -1
					for i := range rf.log {
						if rf.log[i].Term == reply.ConflictTerm {
							lastIndex = i
						}
					}
					if lastIndex != -1 {
						rf.nextIndex[peer] = lastIndex + 1
					} else {
						rf.nextIndex[peer] = reply.ConflictIndex
					}
				}
			}
			rf.mu.Unlock()
		}(peer)
	}

	if rf.commitIndex == len(rf.log)-1 {
		return
	}
	time.Sleep(50 * time.Millisecond)
	// 统计复制成功 超半数则提交了
	for N := len(rf.log) - 1; N > 0; N-- {
		num := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			isclone := rf.matchIndex[peer] >= N
			if isclone {
				num++
			}
		}
		issame := rf.log[N].Term == rf.currentTerm
		if num > len(rf.peers)/2 && issame {
			rf.commitIndex = N
			rf.persist()
			go rf.applyMsg()
			break
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// term<currentTerm 返回false
// 如果日志在preLogIndex处不包含 与prevLogTerm匹配的条目 返回false
// 如果现有条目与新条目冲突 (相同index，不同term)，删除现有条目及其后面的所有内容
// 如果 leaderCommit > commitIndex，则设置 commitIndex = min（leaderCommit，最后⼀个新条⽬的索引）
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartBeat = time.Now()
	//log.Printf("peer: %v from:%v %v peer term:%v args:%v", rf.me, args.LeaderId, args.Term, rf.currentTerm, args)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
		//if rf.currentTerm-args.Term <= 2 {
		//	reply.Success = false
		//	reply.Term = rf.currentTerm
		//	return
		//} else { //由于断连导致的
		//	rf.currentTerm = args.Term
		//}
	}
	if args.Entries == nil {
		reply.Term = rf.currentTerm
		reply.Success = true // 心跳默认成功
		if rf.commitIndex < args.LeaderCommit {
			last := len(rf.log) - 1
			lastIndex := rf.commitIndex
			rf.commitIndex = min(args.LeaderCommit, last)
			for rf.log[rf.commitIndex].Term != rf.currentTerm && rf.commitIndex > lastIndex { //只提交当前任期的
				rf.commitIndex--
			}
			rf.persist()
			go rf.applyMsg()
		}
		return
	} else {
		if len(rf.log)-1 < args.PrevLogIndex {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1
			return
		}
		if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			reply.ConflictIndex = args.PrevLogIndex
			for i := args.PrevLogIndex; i > 0; i-- {
				if rf.log[i].Term != reply.ConflictTerm {
					reply.ConflictIndex = i + 1
					break
				}
			}
			return
		}
		rf.log = rf.log[0 : args.PrevLogIndex+1] // 本地日志在此之后的都舍弃了
		rf.log = append(rf.log, args.Entries...) // 更新传过来的
		rf.persist()
		reply.Success = true
		reply.Term = rf.currentTerm

		return
	}
}

//func (rf *Raft) ApplyMsg() {
//	for {
//		time.Sleep(300 * time.Millisecond)
//		rf.applyMsg()
//	}
//}

func (rf *Raft) applyMsg() {
	rf.mu.Lock()
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		applyMsg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		//log.Printf("peer:%v %v :term:%v", rf.me, applyMsg, rf.log[rf.lastApplied].Term)
		rf.applyCh <- applyMsg
	}
	rf.mu.Unlock()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.commitIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
// 恢复之前持久化的状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		logs        []LogEntry
		currentTerm int
		votedFor    int
		commitIndex int
	)
	if d.Decode(&logs) != nil || d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&commitIndex) != nil {
		DPrintf("error")
	} else {
		rf.log = logs
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.commitIndex = commitIndex
	}
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
		nextIndex:   nil,
		matchIndex:  nil,
		applyCh:     applyCh,
		commitIndex: 0,
		lastApplied: 0,
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

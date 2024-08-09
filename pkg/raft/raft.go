package raft

import (
	"math/rand"
	"strconv"

	pb "kvdb/pkg/raftpb"

	"go.uber.org/zap"
)

const MAX_LOG_ENTRY_SEND = 1000

// raft 节点类型
type RaftState int

// raft 节点类型
const (
	CANDIDATE_STATE RaftState = iota
	FOLLOWER_STATE
	LEADER_STATE
)

// 就是一个 Raft 节点
type Raft struct {
	id          uint64    // 当前节点id
	state       RaftState // 节点类型： 候选人/跟随者/领导者
	leader      uint64    // leader id
	currentTerm uint64    // 当前任期
	voteFor     uint64    // 当前任期，投票过的对象
	raftlog     *RaftLog  // 日志
	cluster     *Cluster  // 集群节点

	electionTimeout       int // 选举-默认的超时时间
	randomElectionTimeout int // 选举-随机的超时时间 r.electionTimeout + rand.Intn(r.electionTimeout)
	electtionTick         int // 选举时钟

	heartbeatTimeout int // 心跳周期

	hearbeatTick  int                   // 心跳时钟
	Tick          func()                // 时钟函数,Leader为心跳时钟，其他为选举时钟
	hanbleMessage func(*pb.RaftMessage) // 消息处理函数,按节点状态对应不同处理
	Msg           []*pb.RaftMessage     // 待发送消息
	ReadIndex     []*ReadIndexResp      // 检查Leader完成的readindex
	logger        *zap.SugaredLogger
}

func NewRaft(id uint64, storage Storage, peers map[uint64]string, logger *zap.SugaredLogger) *Raft {

	raftlog := NewRaftLog(storage, logger)

	raft := &Raft{
		id:               id, // 节点id
		currentTerm:      raftlog.lastAppliedTerm,
		raftlog:          raftlog,
		cluster:          NewCluster(peers, raftlog.commitIndex, logger),
		electionTimeout:  10,
		heartbeatTimeout: 5,
		logger:           logger,
	}

	logger.Infof("实例: %s ,任期: %d ", strconv.FormatUint(raft.id, 16), raft.currentTerm)

	// 默认就是follower
	raft.SwitchFollower(0, raft.currentTerm)

	return raft
}

// 切换节点为Candidate
func (r *Raft) SwitchCandidate() {
	r.state = CANDIDATE_STATE // 候选人
	r.leader = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout) // 选举超时时间
	r.Tick = r.TickElection                                                    // 定时器处理函数
	r.hanbleMessage = r.HandleCandidateMessage

	// 以节点最新日志，重置同步进度状态，leader按进度发送日志
	lastIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	r.cluster.Foreach(func(_ uint64, p *ReplicaProgress) {
		p.NextIndex = lastIndex + 1
		p.MatchIndex = lastIndex
	})

	// 发起投票
	r.BroadcastRequestVote()
	// 重置时间为0
	r.electtionTick = 0
	r.logger.Debugf("成为候选者, 任期 %d , 选举周期 %d s", r.currentTerm, r.randomElectionTimeout)
}

// 切换节点为Follower
func (r *Raft) SwitchFollower(leaderId, term uint64) {

	r.state = FOLLOWER_STATE // follower

	r.leader = leaderId  // leader id
	r.currentTerm = term // 任期
	r.voteFor = 0        // 投票给谁了
	// r.cluster.ResetVoteResult()
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout) // 选举-最大超时时间
	r.Tick = r.TickElection                                                    // 定时器函数
	r.hanbleMessage = r.HandleFollowerMessage
	r.electtionTick = 0 // 选举定时器
	r.cluster.Reset()

	r.logger.Debugf("成为追随者, 领导者 %s, 任期 %d , 选举周期 %d s", strconv.FormatUint(leaderId, 16), term, r.randomElectionTimeout)
}

// 切换节点为Leader
func (r *Raft) SwitchLeader() {
	r.logger.Debugf("成为领导者, 任期: %d", r.currentTerm)

	r.state = LEADER_STATE // leader
	r.leader = r.id        // 节点id
	r.voteFor = 0          // 投票清空
	// r.cluster.ResetVoteResult()
	r.Tick = r.TickHeartbeat                // 心跳
	r.hanbleMessage = r.HandleLeaderMessage // 消息处理
	r.electtionTick = 0
	r.hearbeatTick = 0
	r.cluster.Reset()
	r.cluster.pendingChangeIndex = r.raftlog.lastAppliedIndex
}

// 心跳时钟跳动
func (r *Raft) TickHeartbeat() {
	r.hearbeatTick++

	// r.logger.Debugf("心跳时钟推进 %d", r.hearbeatTick)

	lastIndex, _ := r.raftlog.GetLastLogIndexAndTerm()

	if r.hearbeatTick >= r.heartbeatTimeout {
		r.hearbeatTick = 0
		r.BroadcastHeartbeat(nil) // 广播心跳

		r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
			if id == r.id {
				return
			}

			pendding := len(p.pending)
			// 重发消息
			// 1. 消息已经发送，但是还一直处于发送状态（而且经过了两次心跳依然还没有发送成功），需要进行重发

			// 2.正常触发发送是接收到 props消息，当没有接收到props消息，并且发送进度比本地的 lastIndex慢，也主动触发一次发送（目的尽快让远端和本地leader的进度一致）
			if !p.prevResp && pendding > 0 && p.MaybeLogLost(p.pending[0]) || (pendding == 0 && p.NextIndex <= lastIndex) {
				p.pending = nil
				r.SendAppendEntries(id)
			}

			// 重发快照,条件：上次快照在两次心跳内未发送完成
			if p.installingSnapshot && p.prevSnap != nil && p.MaybeSnapLost(p.prevSnap) {
				r.logger.Debugf("重发 %d_%s@%d_%d 偏移 %d", p.prevSnap.Level, strconv.FormatUint(p.prevSnap.LastIncludeIndex, 16), p.prevSnap.LastIncludeTerm, p.prevSnap.Segment, p.prevSnap.Offset)
				r.sendSnapshot(id, false)
			}

		})
	}

}

// 选举时钟跳动
func (r *Raft) TickElection() {
	r.electtionTick++ // +1

	// 说明已经超时了（没有leader发来心跳消息，来重置 electtionTick = 0，即：当前环境中应该是没有leader的）
	if r.electtionTick >= r.randomElectionTimeout {
		r.electtionTick = 0
		if r.state == CANDIDATE_STATE { // 如果是 候选人状态，发起投票请求
			r.BroadcastRequestVote()
		}
		if r.state == FOLLOWER_STATE { // 如果是跟随者，切换为 候选人状态
			r.SwitchCandidate()
		}
	}

}

// 处理消息
func (r *Raft) HandleMessage(msg *pb.RaftMessage) {

	// 消息不为空
	if msg == nil {
		return
	}

	if msg.Term < r.currentTerm { // 消息比较旧，直接返回
		r.logger.Debugf("收到来自 %s 过期 (%d) %s 消息 ", strconv.FormatUint(msg.From, 16), msg.Term, msg.MsgType)
		return
	} else if msg.Term > r.currentTerm { // 消息比较新
		// 消息非请求投票，集群发生选举，新任期产生
		if msg.MsgType != pb.MessageType_VOTE {
			// 日志追加、心跳、快照为leader发出，，节点成为该leader追随者
			if msg.MsgType == pb.MessageType_APPEND_ENTRY || msg.MsgType == pb.MessageType_HEARTBEAT || msg.MsgType == pb.MessageType_INSTALL_SNAPSHOT {
				r.SwitchFollower(msg.From, msg.Term)
			} else { // 变更节点为追随者，等待leader消息
				r.SwitchFollower(msg.From, 0)
			}
		}
	}

	r.hanbleMessage(msg)
}

// 候选人（消息处理）
func (r *Raft) HandleCandidateMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_VOTE: // 接收到投票请求
		grant := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if grant {
			r.electtionTick = 0 // 投票后，当前节点，短时间内不会触发下一次选举
		}
	case pb.MessageType_VOTE_RESP: // 接收到投票响应
		r.ReciveVoteResp(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.Success)

	case pb.MessageType_HEARTBEAT: // 心跳（宣誓主权），此时候选人需要变成follower
		r.SwitchFollower(msg.From, msg.Term)
		r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit, msg.Context) // 持久化

	case pb.MessageType_APPEND_ENTRY:
		r.SwitchFollower(msg.From, msg.Term)
		r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry) // 保存到 LogEntry + 持久化
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
	}
}

// 追随者处理消息
func (r *Raft) HandleFollowerMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_VOTE: // 投票
		grant := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if grant {
			r.electtionTick = 0
		}

		// &pb.RaftMessage{MsgType: pb.MessageType_READINDEX, Term: n.raft.currentTerm, Context: req}
	case pb.MessageType_READINDEX: // 询问Leader最新提交
		lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm() // lastLogIndex follower的日志索引
		r.cluster.AddReadIndex(msg.From, lastLogIndex, msg.Context)
		msg.To = r.leader // 转发给 leader
		msg.From = r.id
		r.send(msg)
	case pb.MessageType_READINDEX_RESP:
		r.ReadIndex = append(r.ReadIndex, &ReadIndexResp{ /// 保存在 ReadIndex 最后数据会到 n.readc中
			Req:   msg.Context,
			Index: msg.LastLogIndex,
			Send:  msg.To,
		})
	case pb.MessageType_HEARTBEAT: // 收到心跳
		r.electtionTick = 0 // 重置选举
		r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit, msg.Context)

	case pb.MessageType_APPEND_ENTRY: // 日志同步
		r.electtionTick = 0
		r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
	case pb.MessageType_PROPOSE: // 转发给leader
		msg.To = r.leader
		msg.Term = r.currentTerm
		msg.From = r.id
		r.send(msg)
	case pb.MessageType_INSTALL_SNAPSHOT:
		r.ReciveInstallSnapshot(msg.From, msg.Term, msg.Snapshot)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
	}
}

// 领导者处理消息
func (r *Raft) HandleLeaderMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_PROPOSE: // 客户端发送来的消息
		r.AppendEntry(msg.Entry) // 保存本地，并广播给 其他节点

	case pb.MessageType_READINDEX: // readindex向集群发送心跳检查是否为Leader
		r.BroadcastHeartbeat(msg.Context) // |timestamp|nodeid|seq| 用心跳的方式发送出去
		r.hearbeatTick = 0

		lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
		r.cluster.AddReadIndex(msg.From, lastLogIndex, msg.Context) // 同时本地针对 |timestamp|nodeid|seq| 记录一笔记录
	case pb.MessageType_VOTE:
		r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
	case pb.MessageType_VOTE_RESP:
		break
	case pb.MessageType_HEARTBEAT_RESP:
		r.ReciveHeartbeatResp(msg.From, msg.Term, msg.LastLogIndex, msg.Context)

	case pb.MessageType_APPEND_ENTRY_RESP: // leader 接收到的 日志同步响应
		r.ReciveAppendEntriesResult(msg.From, msg.Term, msg.LastLogIndex, msg.Success)

	case pb.MessageType_INSTALL_SNAPSHOT_RESP:
		r.ReciveInstallSnapshotResult(msg.From, msg.Term, msg.LastLogIndex, msg.Success)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
	}
}

// Leader 添加日志
func (r *Raft) AppendEntry(entries []*pb.LogEntry) {

	// 最新的日志索引
	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()

	for i, entry := range entries {
		if entry.Type == pb.EntryType_MEMBER_CHNAGE {
			r.cluster.pendingChangeIndex = entry.Index
			r.cluster.inJoint = true
		}

		// 每个日志的索引 进行递增
		entry.Index = lastLogIndex + 1 + uint64(i)
		entry.Term = r.currentTerm // 当前任期

	}
	r.raftlog.AppendEntry(entries) // 保存日志

	r.cluster.UpdateLogIndex(r.id, entries[len(entries)-1].Index) // 更新 leader 节点的进度

	r.BroadcastAppendEntries() // 广播给其他节点
}

// 变更集群成员
func (r *Raft) ApplyChange(change []*pb.MemberChange) error {
	err := r.cluster.ApplyChange(change)
	if err == nil && r.state == LEADER_STATE {
		r.BroadcastAppendEntries()
	}
	return err
}

// 广播日志
func (r *Raft) BroadcastAppendEntries() {
	r.cluster.Foreach(func(id uint64, _ *ReplicaProgress) {
		if id == r.id {
			return
		}
		r.SendAppendEntries(id)
	})
}

// 广播心跳
func (r *Raft) BroadcastHeartbeat(context []byte) {
	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id {
			return
		}

		// 对应节点的进度索引
		lastLogIndex := p.NextIndex - 1
		lastLogTerm := r.raftlog.GetTerm(lastLogIndex)

		r.send(&pb.RaftMessage{
			MsgType: pb.MessageType_HEARTBEAT,
			Term:    r.currentTerm,
			From:    r.id,
			To:      id,

			LastLogIndex: lastLogIndex, // 对应节点数据进度的索引
			LastLogTerm:  lastLogTerm,  // 对应节点数据进度的任期
			LastCommit:   r.raftlog.commitIndex,

			Context: context, // |timestamp|nodeid|seq|
		})
	})
}

// 发起投票
func (r *Raft) BroadcastRequestVote() {
	r.currentTerm++             // ********任期+1**********
	r.voteFor = r.id            // 记录（是否已经投票给谁了）
	r.cluster.ResetVoteResult() // 重置上次投票结果
	r.cluster.Vote(r.id, true)  // 记录投票结果（自己投票给自己）

	r.logger.Infof("%s 发起投票", strconv.FormatUint(r.id, 16))

	// 请求其他节点投票
	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id { // 如果是自己，跳过
			return
		}

		// 未提交日志（最新的那个日志，对应的二维坐标（任期+索引））
		lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()

		/*
			任期 + 最新数据任期+索引 （用来其他节点进行判断，你的任期是否够新 + 外加你的数据是否够新；保证拥有最新数据的节点才能当选）
		*/
		r.send(&pb.RaftMessage{
			MsgType:      pb.MessageType_VOTE, // 消息类型
			Term:         r.currentTerm,       // 任期
			From:         r.id,                // 发起者
			To:           id,                  // 接受者
			LastLogIndex: lastLogIndex,        // 最新索引
			LastLogTerm:  lastLogTerm,         // 最新任期
		})
	})

}

// 发送日志到指定节点（说明当前的是leader）
func (r *Raft) SendAppendEntries(to uint64) {

	p := r.cluster.progress[to]  // 判断本地是否有to的进度
	if p == nil || p.IsPause() { // 上次的结果还没有返回
		return
	}

	// 获取 to 的日志进度
	nextIndex := r.cluster.GetNextIndex(to)

	// 获取本地的上一个日志 索引+任期
	lastLogIndex := nextIndex - 1
	lastLogTerm := r.raftlog.GetTerm(lastLogIndex)
	maxSize := MAX_LOG_ENTRY_SEND

	if !p.prevResp {
		maxSize = 1
	}
	entries := r.raftlog.GetEntries(nextIndex, maxSize) // [nextIndex, nextIndex+maxSize] 获取本地日志
	size := len(entries)
	if size == 0 { // 无
		if nextIndex <= r.raftlog.lastAppliedIndex && p.prevResp {
			snapc, err := r.raftlog.GetSnapshot(nextIndex)
			if err != nil {
				r.logger.Errorf("获取快照失败: %v", err)
				return
			}

			r.cluster.InstallSnapshot(to, snapc)
			r.sendSnapshot(to, true)
			return
		}
	} else {
		// 更新本地进度
		r.cluster.AppendEntry(to, entries[size-1].Index) // false  +  len(pending) > 0
	}

	// 发送
	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_APPEND_ENTRY,
		Term:         r.currentTerm,
		From:         r.id,
		To:           to,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		LastCommit:   r.raftlog.commitIndex,
		Entry:        entries,
	})
}

// 发送快照到指定节点
func (r *Raft) sendSnapshot(to uint64, prevSuccess bool) {
	snap := r.cluster.GetSnapshot(to, prevSuccess)
	if snap == nil {
		r.SendAppendEntries(to)
		return
	}

	// r.logger.Debugf("发送%s  %d_%s@%d_%d 偏移 %d", strconv.FormatUint(to, 16), snap.Level, strconv.FormatUint(snap.LastIncludeIndex, 16), snap.LastIncludeTerm, snap.Segment, snap.Offset)

	msg := &pb.RaftMessage{
		MsgType:  pb.MessageType_INSTALL_SNAPSHOT,
		Term:     r.currentTerm,
		From:     r.id,
		To:       to,
		Snapshot: snap,
	}

	r.Msg = append(r.Msg, msg)
}

func (r *Raft) send(msg *pb.RaftMessage) {
	r.Msg = append(r.Msg, msg)
}

// 处理投票选举响应
func (r *Raft) ReciveVoteResp(from, term, lastLogTerm, lastLogIndex uint64, success bool) {

	leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	r.cluster.Vote(from, success) // 记录投票结果
	r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)

	voteRes := r.cluster.CheckVoteResult() // 检查投票结果
	if voteRes == VoteWon {                // 多数派（选举成功）
		r.logger.Debugf("节点 %s 发起投票, 赢得选举", strconv.FormatUint(r.id, 16))
		for k, v := range r.cluster.voteResp {
			if !v {
				r.cluster.ResetLogIndex(k, lastLogIndex, leaderLastLogIndex)
			}
		}
		r.SwitchLeader() // 切换成leader
		r.BroadcastAppendEntries()
	} else if voteRes == VoteLost { // 都拒绝本节点当选
		r.logger.Debugf("节点 %s 发起投票, 输掉选举", strconv.FormatUint(r.id, 16))
		r.voteFor = 0               // 本任期，投票的节点也重置（目的为了下一场选举超时，新选举使用这些参数）
		r.cluster.ResetVoteResult() // 重置投票结果
	}
}

// 处理日志添加响应  lastLogIndex term 对方的最后一条记录 索引+任期
func (r *Raft) ReciveAppendEntriesResult(from, term, lastLogIndex uint64, success bool) {
	leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()

	// 发送的 app_entry 成功
	if success {
		r.cluster.AppendEntryResp(from, lastLogIndex) // 更新本地 from 的同步进度

		if lastLogIndex > r.raftlog.commitIndex { // 如果本地对方的索引 大于 提交的进度 r.raftlog.commitIndex，说明 r.raftlog.commitIndex 的提交进度可能滞后了

			// 取已同步索引更新到lastcommit
			if r.cluster.CheckCommit(lastLogIndex) { // 说明 lastLogIndex 索引日志，已经得到大部分节点的 多数派共识（也就是可提交）

				prevApplied := r.raftlog.lastAppliedIndex
				r.raftlog.Apply(lastLogIndex, lastLogIndex) // 持久化
				r.BroadcastAppendEntries()

				// 检查联合共识是否完成
				if r.cluster.inJoint && prevApplied < r.cluster.pendingChangeIndex && lastLogIndex >= r.cluster.pendingChangeIndex {
					r.AppendEntry([]*pb.LogEntry{{Type: pb.EntryType_MEMBER_CHNAGE}})
					lastIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
					r.cluster.pendingChangeIndex = lastIndex
				}
			}
		} else if len(r.raftlog.waitQueue) > 0 {
			r.raftlog.NotifyReadIndex()
		}
		if r.cluster.GetNextIndex(from) <= leaderLastLogIndex { // 说明对方的日志进度很慢，加快同步进度
			r.SendAppendEntries(from)
		}
	} else {
		r.logger.Infof("节点 %s 追加日志失败, Leader记录节点最新日志: %d ,节点最新日志: %d ", strconv.FormatUint(from, 16), r.cluster.GetNextIndex(from)-1, lastLogIndex)
		// 对方拒绝了同步请求
		r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)
		r.SendAppendEntries(from)
	}
}

// 处理快照发送响应
func (r *Raft) ReciveInstallSnapshotResult(from, term, lastLogIndex uint64, installed bool) {
	if installed {
		leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
		r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)
		r.logger.Debugf("%s 快照更新 ,当前最后日志 %d ", strconv.FormatUint(from, 16), lastLogIndex)
	}
	r.sendSnapshot(from, true)

}

// 处理快照
func (r *Raft) ReciveInstallSnapshot(from, term uint64, snap *pb.Snapshot) {
	var installed bool
	if snap.LastIncludeIndex > r.raftlog.lastAppliedIndex {
		// r.logger.Debugf("收到%s  %d_%s@%d_%d 偏移 %d", strconv.FormatUint(from, 16), snap.Level, strconv.FormatUint(snap.LastIncludeIndex, 16), snap.LastIncludeTerm, snap.Segment, snap.Offset)
		installed, _ = r.raftlog.InstallSnapshot(snap)
	}

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()

	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_INSTALL_SNAPSHOT_RESP,
		Term:         r.currentTerm,
		From:         r.id,
		To:           from,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		Success:      installed,
	})
}

// 处理心跳响应
func (r *Raft) ReciveHeartbeatResp(mFrom, mTerm, mLastLogIndex uint64, context []byte) {

	if len(context) > 0 { // 心跳的特殊版（加了context透传），只是为了复用心跳消息这个定义
		resp := r.cluster.HeartbeatCheck(context, mFrom)
		if resp != nil { //  resp != nil  说明发送的心跳，得到了多数派的认可
			if resp.Send != 0 && resp.Send != r.id { // 说明是 follower 转发来的readindex
				r.send(&pb.RaftMessage{
					MsgType:      pb.MessageType_READINDEX_RESP,
					Term:         r.currentTerm,
					From:         r.id,       // leader
					To:           resp.Send,  // follower
					LastLogIndex: resp.Index, // leader的索引
					Context:      context,
				})
			} else {
				// 这里说明是 leader 自己的 readindex
				r.ReadIndex = append(r.ReadIndex, resp)
			}
		}
	}

	// p := r.cluster.progress[mFrom]
	// if p != nil && len(p.pending) == 0 && mLastLogIndex < p.NextIndex {
	// 	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	// 	p.ResetLogIndex(mLastLogIndex, lastLogIndex)
	// }
}

// 处理心跳
func (r *Raft) ReciveHeartbeat(mFrom, mTerm, mLastLogIndex, mLastCommit uint64, context []byte) {
	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()

	r.raftlog.Apply(mLastCommit, lastLogIndex) // 持久化

	r.send(&pb.RaftMessage{
		MsgType: pb.MessageType_HEARTBEAT_RESP,
		Term:    r.currentTerm,
		From:    r.id,
		To:      mFrom,
		Context: context,
	})
}

// 处理日志（这里的日志是 leader 发送来的） mLastLogTerm mLastLogIndex 表示前一个日志
func (r *Raft) ReciveAppendEntries(mLeader, mTerm, mLastLogTerm, mLastLogIndex, mLastCommit uint64, mEntries []*pb.LogEntry) {

	var accept bool

	// 检查前一个日志 是否和 mLastLogTerm, mLastLogIndex 相同
	if !r.raftlog.HasPrevLog(mLastLogIndex, mLastLogTerm) {
		r.logger.Infof("节点未含有上次追加日志: Index: %d, Term: %d ", mLastLogIndex, mLastLogTerm)
		accept = false
	} else {
		r.raftlog.AppendEntry(mEntries)
		accept = true
	}

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()

	// mLastCommit  leader 的提交进度
	// lastLogIndex  本地的最新索引
	r.raftlog.Apply(mLastCommit, lastLogIndex) // 持久化日志

	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_APPEND_ENTRY_RESP,
		Term:         r.currentTerm,
		From:         r.id,
		To:           mLeader,
		LastLogIndex: lastLogIndex, // 当前的最后一条日志 索引
		LastLogTerm:  lastLogTerm,  // 当前的最后一条日志 任期
		Success:      accept,
	})
}

// 处理选举投票请求（leader  follower）
func (r *Raft) ReciveRequestVote(mTerm, mCandidateId, mLastLogTerm, mLastLogIndex uint64) (success bool) {

	// if r.hearbeatTick < r.electionTimeout {
	// 	return false
	// }

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()

	// r.voteFor == 0 当前任期，还没有投过票
	// r.voteFor == mCandidateId 当前任期，已经给对方投过票了
	if r.voteFor == 0 || r.voteFor == mCandidateId {

		// mTerm > r.currentTerm 要求投票的任期比较大
		// 请求方最新日志编号大于等于自身日志最新编号
		if mTerm > r.currentTerm && mLastLogTerm >= lastLogTerm && mLastLogIndex >= lastLogIndex {
			r.voteFor = mCandidateId
			success = true
		}
	}

	r.logger.Debugf("候选人: %s, 投票: %t ", strconv.FormatUint(mCandidateId, 16), success)

	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_VOTE_RESP,
		Term:         mTerm,
		From:         r.id,
		To:           mCandidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		Success:      success,
	})
	return
}

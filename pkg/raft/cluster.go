package raft

import (
	pb "kvdb/pkg/raftpb"
	"strconv"

	"go.uber.org/zap"
)

type VoteResult int

// 选举状态
const (
	Voting VoteResult = iota
	VoteWon
	VoteLost
)

type ReadIndexResp struct {
	Req   []byte
	Index uint64
	Send  uint64
	ack   map[uint64]bool
}

// raft 集群对等节点状态
type Cluster struct {
	incoming map[uint64]struct{} // 当前/新集群节点

	outcoming map[uint64]struct{} // 旧集群节点

	pendingChangeIndex uint64          // 未完成变更日志
	inJoint            bool            // 是否正在进行联合共识
	voteResp           map[uint64]bool // 投票节点
	pendingReadIndex   map[string]*ReadIndexResp
	progress           map[uint64]*ReplicaProgress // 各节点进度
	logger             *zap.SugaredLogger
}

// from（如果是自己本来是主，from 为空；如果是follower转发来的，from就是follower的值） index 表示 leader 的日志进度  req 表示唯一标识（某个时间戳，某个节点，某次请求）
func (c *Cluster) AddReadIndex(from, index uint64, req []byte) {

	// |timestamp|nodeid|seq|
	c.pendingReadIndex[string(req)] = &ReadIndexResp{Req: req, Send: from, Index: index, ack: map[uint64]bool{}}
}

// node 对方节点 唯一标识
func (c *Cluster) HeartbeatCheck(req []byte, node uint64) *ReadIndexResp {

	// TODO：可以在这里加一个 随机检查，c.pendingReadIndex 中某些已经时间戳已经很久了，还存在，其实就是垃圾数据，不可能被处理了，随机挑选几个key检查下）
	k := string(req)
	p := c.pendingReadIndex[k]
	if p != nil {
		p.ack[node] = true                   //接收到一笔响应
		if len(p.ack) >= len(c.progress)/2 { // 说明 req 这个请求，已经得到了多数派的认可
			delete(c.pendingReadIndex, k) // 只有多数派，才删除（所以可能存在 pendingReadIndex 垃圾数据越来越多）
			return p
		}
	}
	return nil
}

// 检查选举结果
func (c *Cluster) CheckVoteResult() VoteResult {
	granted := 0
	reject := 0
	// 统计承认/拒绝数量
	for _, v := range c.voteResp {
		if v {
			granted++
		} else {
			reject++
		}
	}

	// most := len(c.progress)/2 + 1
	half := len(c.progress) / 2

	if granted >= half+1 { // 多数派（一半以上）
		return VoteWon
	} else if reject >= half { // 半数拒绝，选举失败
		return VoteLost
	}
	// 尚在选举
	return Voting
}

// 重置选举结果
func (c *Cluster) ResetVoteResult() {
	c.voteResp = make(map[uint64]bool)
}

// 记录节点投票结果
func (c *Cluster) Vote(id uint64, granted bool) {
	c.voteResp[id] = granted
}

// 获取节点待发送快照
func (c *Cluster) GetSnapshot(id uint64, prevSuccess bool) *pb.Snapshot {
	p := c.progress[id]

	if p != nil {
		return p.GetSnapshot(prevSuccess)
		// if !prevSuccess {
		// 	c.logger.Debugf("%s 前次快照未发送完成", strconv.FormatUint(id, 16))
		// 	return p.prevSnap
		// }

		// if p.snapc == nil {
		// 	c.logger.Debugf("%s 快照读取通道为空", strconv.FormatUint(id, 16))
		// 	return nil
		// }
		// snap := <-p.snapc
		// if snap == nil {
		// 	c.logger.Debugf("%s 读取快照为空", strconv.FormatUint(id, 16))
		// 	p.snapc = nil
		// 	p.installingSnapshot = false
		// }
		// p.prevSnap = snap

		// return snap
	} else {
		c.logger.Debugf("%s 未初始化完成，无法发送快照", strconv.FormatUint(id, 16))
	}

	return nil
}

// 记录节点正在发送快照
func (c *Cluster) InstallSnapshot(id uint64, snapc chan *pb.Snapshot) {
	p := c.progress[id]
	if p != nil {
		p.InstallSnapshot(snapc)
	}
}

// 检查是否能进行变更
func (c *Cluster) CanChange(changes []*pb.MemberChange) bool {
	return !(len(c.outcoming) > 0 && len(changes) > 0)
}

// 变更集群成员
func (c *Cluster) ApplyChange(changes []*pb.MemberChange) error {
	if len(changes) == 0 {
		// 成员变更数为0,当前变更为阶段2,清空旧集群数据
		c.outcoming = make(map[uint64]struct{})
		for k := range c.progress {
			_, exsit := c.incoming[k]
			if !exsit {
				delete(c.progress, k)
			}
		}
		c.inJoint = false
		c.logger.Debugf("清理旧集群信息完成, 当前集群成员数量: %d", len(c.incoming))
		return nil
	}
	// 转移集群数据到outcoming
	for k, v := range c.incoming {
		c.outcoming[k] = v
	}

	// 按变更更新成员
	for _, change := range changes {
		if change.Type == pb.MemberChangeType_ADD_NODE {
			c.progress[change.Id] = &ReplicaProgress{
				MatchIndex: 0,
				NextIndex:  1,
			}
			c.incoming[change.Id] = struct{}{}
			c.logger.Debugf("添加集群成员: %s ,新集群成员数量: %d", strconv.FormatUint(change.Id, 16), len(c.incoming))
		} else if change.Type == pb.MemberChangeType_REMOVE_NODE {
			delete(c.incoming, change.Id)
			c.logger.Debugf("移除集群成员: %s ,新集群成员数量: %d", strconv.FormatUint(change.Id, 16), len(c.incoming))
		}
	}
	return nil
}

// 节点是否暂停发送
func (c *Cluster) IsPause(id uint64) bool {
	p := c.progress[id]
	if p != nil {
		return p.IsPause()
	}
	return true
}

// 更新节点日志同步进度
func (c *Cluster) UpdateLogIndex(id uint64, lastIndex uint64) {
	p := c.progress[id]
	if p != nil {
		p.MatchIndex = lastIndex
		p.NextIndex = lastIndex + 1
	}
}

// 重置集群同步/投票状态
func (c *Cluster) Reset() {
	for _, rp := range c.progress {
		rp.Reset()
	}
	c.ResetVoteResult()
}

// 重新设置节点同步进度
func (c *Cluster) ResetLogIndex(id uint64, lastIndex uint64, leaderLastIndex uint64) {
	p := c.progress[id]
	if p != nil {
		p.ResetLogIndex(lastIndex, leaderLastIndex)
	}
}

// 节点添加日志
func (c *Cluster) AppendEntry(id uint64, lastIndex uint64) {
	p := c.progress[id]
	if p != nil {
		p.AppendEntry(lastIndex)
	}
}

// 节点响应添加日志
func (c *Cluster) AppendEntryResp(id uint64, lastIndex uint64) {
	p := c.progress[id]
	if p != nil {
		p.AppendEntryResp(lastIndex)
	}
}

// 获取节点下次发送日志
func (c *Cluster) GetNextIndex(id uint64) uint64 {
	p := c.progress[id]
	if p != nil {
		return p.NextIndex
	}
	return 0
}

// 获取节点已追加日志进度
func (c *Cluster) GetMacthIndex(id uint64) uint64 {
	p := c.progress[id]
	if p != nil {
		return p.MatchIndex
	}
	return 0
}

// 检查 index 是否已经得到多数派节点的确认（也就是多数派节点的同步进度，都有通过index 索引的数据）
func (c *Cluster) CheckCommit(index uint64) bool {

	// 新/旧集群都达到多数共识才允许提交
	incomingLogged := 0
	for id := range c.incoming {
		if index <= c.progress[id].MatchIndex {
			incomingLogged++
		}
	}
	incomingCommit := incomingLogged >= len(c.incoming)/2+1

	if len(c.outcoming) > 0 {
		outcomingLogged := 0

		// 遍历集中的所有的节点，看下节点的进度索引是否都大于 index ，说明 index 索引的数据，多数派的节点都已经同步到了（得到了多数派节点的认可），那么就可以更改 commit
		for id := range c.outcoming {
			if index <= c.progress[id].MatchIndex {
				outcomingLogged++
			}
		}
		return incomingCommit && (outcomingLogged >= len(c.outcoming)/2+1)
	}

	return incomingCommit
}

// 遍历节点进度
func (c *Cluster) Foreach(f func(id uint64, p *ReplicaProgress)) {
	for id, p := range c.progress {
		f(id, p)
	}
}

func NewCluster(peers map[uint64]string, lastIndex uint64, logger *zap.SugaredLogger) *Cluster {

	incoming := make(map[uint64]struct{})
	progress := make(map[uint64]*ReplicaProgress)
	for id := range peers {
		progress[id] = &ReplicaProgress{
			NextIndex:  lastIndex + 1,
			MatchIndex: lastIndex,
		}
		incoming[id] = struct{}{}
	}

	return &Cluster{
		incoming:         incoming,
		outcoming:        make(map[uint64]struct{}),
		voteResp:         make(map[uint64]bool),
		progress:         progress,
		pendingReadIndex: make(map[string]*ReadIndexResp),
		logger:           logger,
	}

}

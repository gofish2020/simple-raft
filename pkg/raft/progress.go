package raft

import pb "kvdb/pkg/raftpb"

// 节点同步状态
type ReplicaProgress struct {
	MatchIndex uint64 // 每个节点，已接收日志的索引

	NextIndex uint64 // 下次待发送日志的索引

	pending []uint64 // 未发送完成日志

	prevResp           bool              // 上次日志发送结果
	maybeLostIndex     uint64            // 可能丢失的日志,记上次发送未完以重发
	installingSnapshot bool              // 是否发送快照中
	snapc              chan *pb.Snapshot // 快照读取通道
	prevSnap           *pb.Snapshot      // 上次发送快照
	maybePrevSnapLost  *pb.Snapshot      // 可能丢失快照,标记上次发送未完成以重发
}

// 标记日志可能发送失败
func (rp *ReplicaProgress) MaybeLogLost(index uint64) bool {
	if index == rp.maybeLostIndex {
		rp.maybeLostIndex = 0
		return true
	}
	rp.maybeLostIndex = index
	return false
}

// 标记快照可能发送失败
func (rp *ReplicaProgress) MaybeSnapLost(snap *pb.Snapshot) bool {
	if rp.maybePrevSnapLost != nil && rp.maybePrevSnapLost == snap {
		rp.maybePrevSnapLost = nil
		return true
	}
	rp.maybePrevSnapLost = snap
	return false
}

// 记录日志发送中,如上次发送成功直接更新下次发送日志索引
func (rp *ReplicaProgress) AppendEntry(lastIndex uint64) {
	rp.pending = append(rp.pending, lastIndex)
	if rp.prevResp { // 我的理解，主要是为了第一次同步日志的时候（因为你也不知道是否通的，所以认为是不通的），当rp.prevResp= true，说明第一次同步的【响应】肯定是回来了， 说明网络是通畅的，本地直接更新进度即可，不用等到响应回来再更新
		rp.NextIndex = lastIndex + 1 // 本地认为网络通畅，直接更新了进度，但是对方实际又返回了，进度错误（没关系，会将 rp.NextIndex 重置设置会对方真实的进度 ，看代码 ResetLogIndex 中，就是对方拒绝了，将进度设定为真实的对方进度）
	}

	// 个人感觉，这个 rp.NextIndex 进度的数值是否正确没关系的，最终会被调整成正确的对方进度（因为进度错了，对方会回复错误，那就可以更新进度为正常了）
}

// 移除日志发送中,更新已接收日志
func (rp *ReplicaProgress) AppendEntryResp(lastIndex uint64) { // lastIndex 已经接收的日志的索引

	if rp.MatchIndex < lastIndex {
		rp.MatchIndex = lastIndex // 更新本地的日志进度
	}

	idx := -1
	for i, v := range rp.pending {
		if v == lastIndex {
			idx = i
		}
	}

	// 标记前次日志发送成功，更新下次发送
	if !rp.prevResp {
		rp.prevResp = true
		rp.NextIndex = lastIndex + 1 // 这里是响应回来，更新进度
	}

	if idx > -1 {
		// 清除之前发送
		rp.pending = rp.pending[idx+1:]
	}
}

// 标记正在发送快照
func (rp *ReplicaProgress) InstallSnapshot(snapc chan *pb.Snapshot) {
	rp.installingSnapshot = true
	rp.snapc = snapc
}

// 从快照对去通道获取快照
func (rp *ReplicaProgress) GetSnapshot(prevSuccess bool) *pb.Snapshot {

	if !prevSuccess {
		return rp.prevSnap
	}

	if rp.snapc == nil {
		return nil
	}
	snap := <-rp.snapc
	if snap == nil {
		rp.snapc = nil
		rp.installingSnapshot = false
	}
	rp.prevSnap = snap

	return snap
}

// 检查是否暂停发送,第一次发送未完成或安装正在发送快照时暂停日志发送
func (rp *ReplicaProgress) IsPause() bool {
	return rp.installingSnapshot || (!rp.prevResp && len(rp.pending) > 0)
}

// 更新日志发送进度    lastLogIndex 对方节点实际的日志进度  leaderLastLogIndex leader日志进度
// 执行这个函数，说明对方不认同发送的日志，需要将日志的进度索引修改为对方实际的索引
func (rp *ReplicaProgress) ResetLogIndex(lastLogIndex uint64, leaderLastLogIndex uint64) {

	// 节点最后日志小于leader最新日志
	if lastLogIndex < leaderLastLogIndex { // 说明 follower的进度慢了
		rp.NextIndex = lastLogIndex + 1 // 需要给它补齐
		rp.MatchIndex = lastLogIndex
	} else {
		rp.NextIndex = leaderLastLogIndex + 1 // follower的进度超前了， 按照 leader 的进度来
		rp.MatchIndex = leaderLastLogIndex
	}

	if rp.prevResp {
		rp.prevResp = false // 开启发送
		rp.pending = nil
	}
}

// 重置状态
func (rp *ReplicaProgress) Reset() {

	rp.pending = rp.pending[:0]
	rp.prevResp = false

	rp.maybeLostIndex = 0
	rp.installingSnapshot = false
	if rp.snapc != nil {
		close(rp.snapc)
		rp.snapc = nil
	}
	rp.prevSnap = nil
	rp.maybePrevSnapLost = nil
}

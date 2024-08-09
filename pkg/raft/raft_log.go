package raft

import (
	pb "kvdb/pkg/raftpb"

	"go.uber.org/zap"
)

type WaitApply struct {
	done  bool
	index uint64
	ch    chan struct{}
}

// 单次最大发送日志条数
const MAX_APPEND_ENTRY_SIZE = 1000

type RaftLog struct {
	logEnties []*pb.LogEntry // 未提交日志

	storage Storage // 存储日志 （状态机 - 持久化的存储sstable）

	commitIndex uint64 // 提交进度（表示已经得到多数派的认可的进度）

	lastAppliedIndex uint64 // 最后存储日志索引
	lastAppliedTerm  uint64 // 最后存储日志任期

	lastAppendIndex uint64       // 最后追加日志
	waitQueue       []*WaitApply // 等待提交通知
	logger          *zap.SugaredLogger
}

func NewRaftLog(storage Storage, logger *zap.SugaredLogger) *RaftLog {
	lastIndex, lastTerm := storage.GetLastLogIndexAndTerm()

	return &RaftLog{
		logEnties:        make([]*pb.LogEntry, 0),
		storage:          storage,
		commitIndex:      lastIndex,
		lastAppliedIndex: lastIndex,
		lastAppliedTerm:  lastTerm,
		lastAppendIndex:  lastIndex,
		logger:           logger,
	}
}

// 获取快照
func (l *RaftLog) GetSnapshot(index uint64) (chan *pb.Snapshot, error) {
	return l.storage.GetSnapshot(index)
}

// 获取指定索引及后续日志、或快照
func (l *RaftLog) GetEntries(index uint64, maxSize int) []*pb.LogEntry {
	// 请求日志已提交，从存储获取
	if index <= l.lastAppliedIndex {
		endIndex := index + MAX_APPEND_ENTRY_SIZE
		if endIndex >= l.lastAppliedIndex {
			endIndex = l.lastAppliedIndex + 1
		}
		return l.storage.GetEntries(index, endIndex)
	} else { // 请求日志未提交,从数组获取
		var entries []*pb.LogEntry
		for i, entry := range l.logEnties {
			if entry.Index == index {
				if len(l.logEnties)-i > maxSize {
					entries = l.logEnties[i : i+maxSize]
				} else {
					entries = l.logEnties[i:]
				}
				break
			}
		}
		return entries
	}
}

// 获取日志任期
func (l *RaftLog) GetTerm(index uint64) uint64 {

	// 检查未提交日志
	for _, entry := range l.logEnties {
		if entry.Index == index {
			return entry.Term
		}
	}

	// 检查最后提交
	if index == l.lastAppliedIndex {
		return l.lastAppliedTerm
	}

	// 查询存储
	return l.storage.GetTerm(index)
}

// 追加日志
func (l *RaftLog) AppendEntry(entry []*pb.LogEntry) {

	size := len(entry)
	if size == 0 {
		return
	}
	// 保存到本地预写日志中
	l.logEnties = append(l.logEnties, entry...)

	l.lastAppendIndex = entry[size-1].Index
}

// 添加快照
func (l *RaftLog) InstallSnapshot(snap *pb.Snapshot) (bool, error) {

	// 当前日志未提交,强制提交并更新快照
	if len(l.logEnties) > 0 {
		l.Apply(l.lastAppendIndex, l.lastAppendIndex)
	}

	// 添加快照到存储
	added, err := l.storage.InstallSnapshot(snap)
	if added { // 添加完成,更新最后提交
		l.ReloadSnapshot()
	}

	return added, err
}

// 检查是否含有指定日志
func (l *RaftLog) HasPrevLog(lastIndex, lastTerm uint64) bool {
	if lastIndex == 0 {
		return true
	}
	var term uint64
	size := len(l.logEnties)
	if size > 0 { // 检查 l.logEnties

		lastlog := l.logEnties[size-1] // 最后一条记录

		// 本地日志最后一条记录的索引 和 传入的索引相同
		if lastlog.Index == lastIndex {
			term = lastlog.Term
		} else if lastlog.Index > lastIndex { // 说明本地的日志比较新
			// 检查最后提交
			if lastIndex == l.lastAppliedIndex { // 已提交日志必然一致
				l.logEnties = l.logEnties[:0]
				return true
			} else if lastIndex > l.lastAppliedIndex { // lastIndex 在 l.lastAppliedIndex 和 lastlog.Index   之间
				// 检查未提交日志
				for i, entry := range l.logEnties[:size] {
					if entry.Index == lastIndex {
						term = entry.Term
						// 将leader上次追加后日志清理
						// 网络异常未收到响应导致leader重发日志/leader重选举使旧leader未同步数据失效
						l.logEnties = l.logEnties[:i+1] // 相当于只保留了 [0:i]之间的数据，[i+1:size-1] 之间的数据全部删掉了

						break
					}
				}
			}
		}
	} else if lastIndex == l.lastAppliedIndex {
		return true
	}

	b := term == lastTerm
	if !b {
		l.logger.Debugf("最新日志: %d, 任期: %d ,本地记录任期: %d", lastIndex, lastTerm, term)
		if term != 0 { // 当日志与leader不一致，删除内存中不一致数据,同任期日志记录
			for i, entry := range l.logEnties {
				if entry.Term == term {
					l.logEnties = l.logEnties[:i] // 只保留了 < term 的数据 [0,i-1]    >=term 的数据 [i:] 全被删去了
					break
				}
			}
		}
	}
	return b
}

// 清除本地有冲突日志，以Leader为准
func (l *RaftLog) RemoveConflictLog(entries []*pb.LogEntry) []*pb.LogEntry {

	appendSize := len(entries)
	logSize := len(l.logEnties)
	if appendSize == 0 || logSize == 0 {
		return entries
	}

	conflictIdx := appendSize
	exsitIdx := -1
	prevIdx := -1
	for n, entry := range entries {
		for i := prevIdx + 1; i < logSize; i++ {
			le := l.logEnties[i]
			if entry.Index == le.Index {
				if entry.Term != le.Term {
					conflictIdx = i
					break
				} else {
					exsitIdx = n
					break
				}
			}
			prevIdx = i
		}
		if conflictIdx != appendSize {
			l.logger.Debugf("删除冲突日志 %d ~ %d", l.logEnties[conflictIdx].Index, l.logEnties[appendSize-1].Index)
			l.logEnties = l.logEnties[:conflictIdx]
			break
		}
	}

	if exsitIdx == -1 {
		return entries
	}
	l.logger.Debugf("修剪entry中已存在日志 %d ~ %d ", entries[0].Index, entries[exsitIdx].Index)
	return entries[exsitIdx+1:]
}

func (l *RaftLog) WaitIndexApply(was []*WaitApply) {
	l.waitQueue = append(l.waitQueue, was...)
}

func (l *RaftLog) NotifyReadIndex() {

	cur := 0
	for _, wa := range l.waitQueue {
		if wa.index <= l.lastAppliedIndex {
			if wa.done {
				close(wa.ch)
			} else {
				select {
				case wa.ch <- struct{}{}:
					close(wa.ch)
				default:
					close(wa.ch)
				}
			}
			cur++
		} else {
			break
		}
	}

	if cur > 0 {
		l.waitQueue = l.waitQueue[cur:]
	}
}

// 提交日志     lastCommit  leader的提交进度   lastLogIndex 本地的最大索引
func (l *RaftLog) Apply(lastCommit, lastLogIndex uint64) {

	// // 更新 l.commitIndex 提交进度
	if lastCommit > l.commitIndex { // 发送来的提交进度 大于 本地

		if lastLogIndex > lastCommit {
			l.commitIndex = lastCommit // 相当于只提交到 lastCommit
		} else {
			l.commitIndex = lastLogIndex // 相当于本地的全部提交
		}
	}

	// l.commitIndex 可提交的索引    l.lastAppliedIndex 实际已经提交的索引
	if l.commitIndex > l.lastAppliedIndex {
		n := 0
		for i, entry := range l.logEnties {
			if l.commitIndex >= entry.Index { // 找到在 l.commitIndex 范围内的记录（小于）
				n = i
			} else {
				break
			}
		}

		entries := l.logEnties[:n+1] // 截取 [0:n]  表示待持久化的数据

		l.storage.Append(entries)                 // 持久化
		l.lastAppliedIndex = l.logEnties[n].Index // 更新 l.lastAppliedIndex 的值
		l.lastAppliedTerm = l.logEnties[n].Term

		// 剩下的 [n+1:]
		l.logEnties = l.logEnties[n+1:]

		// 是为了让 readindex ，知道持久化的进度已经达到了最新数据的索引
		l.NotifyReadIndex()
	}
}

// 本地日志的最新一条日志 索引+任期
func (l *RaftLog) GetLastLogIndexAndTerm() (lastLogIndex, lastLogTerm uint64) {
	if len(l.logEnties) > 0 {
		lastLog := l.logEnties[len(l.logEnties)-1]

		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	} else {
		lastLogIndex = l.lastAppliedIndex
		lastLogTerm = l.lastAppliedTerm
	}
	return
}

// 按快照更新最后提交
func (l *RaftLog) ReloadSnapshot() {
	lastIndex, lastTerm := l.storage.GetLastLogIndexAndTerm()

	l.logger.Debugf("快照已更新 ,当前最后日志 %d  ", lastIndex)

	if lastIndex > l.lastAppliedIndex {
		l.lastAppliedIndex = lastIndex
		l.lastAppliedTerm = lastTerm
	}
}

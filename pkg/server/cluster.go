package server

import (
	"crypto/sha1"
	"encoding/binary"
	"kvdb/pkg/raft"
	pb "kvdb/pkg/raftpb"
	"path"
	"strconv"

	"go.uber.org/zap"
)

type Config struct {
	Dir           string
	Name          string
	PeerAddress   string
	ServerAddress string
	Peers         map[string]string
	Logger        *zap.SugaredLogger
}

// 计算hash值
func GenerateNodeId(name string) uint64 {
	hash := sha1.Sum([]byte(name))
	return binary.BigEndian.Uint64(hash[:8]) // 截取hash 8位，并进行大端返回
}

func Bootstrap(conf *Config) *RaftServer {

	dir := path.Join(conf.Dir, conf.Name) // ./data/node1

	storage := raft.NewRaftStorage(dir, &raft.SimpleEncoding{}, conf.Logger)
	peers, err := storage.RestoreMember() // 读取日志中记录的成员组成

	if err != nil {
		conf.Logger.Errorf("恢复集群成员失败: %v", err)
	}

	var nodeId uint64 // 当前节点的id

	var node *raft.RaftNode // raft 节点
	metric := make(chan pb.MessageType, 1000)
	servers := make(map[uint64]*Peer, len(conf.Peers))

	if len(peers) != 0 {

		nodeId = GenerateNodeId(conf.Name) // 当前节点名

		node = raft.NewRaftNode(nodeId, storage, peers, conf.Logger)
	} else { // 第一次的时候，读取配置中的所有的节点组成

		// 全部的 id 和 ip地址
		peers = make(map[uint64]string, len(conf.Peers))
		// 遍历节点配置，生成各节点id， peers 保存id + 网络ip地址
		for name, address := range conf.Peers {
			id := GenerateNodeId(name)
			peers[id] = address
			if name == conf.Name { // 当前节点的id
				nodeId = id
			}
		}

		// 创建当前的 raftNode
		node = raft.NewRaftNode(nodeId, storage, peers, conf.Logger)
		node.InitMember(peers)
	}

	incomingChan := make(chan *pb.RaftMessage)

	// 将peers 转成 servers
	for id, address := range peers {
		conf.Logger.Infof("集群成员 %s 地址 %s", strconv.FormatUint(id, 16), address)
		if id == nodeId { // 除了自己
			continue
		}

		// 对端的id + 网络连接
		servers[id] = NewPeer(id, address, incomingChan, metric, conf.Logger)
	}

	// 满足raft 的 服务
	server := &RaftServer{
		logger:        conf.Logger,
		dir:           dir,
		id:            nodeId,
		name:          conf.Name,
		peerAddress:   conf.PeerAddress,
		serverAddress: conf.ServerAddress,
		peers:         servers,
		incomingChan:  incomingChan,
		encoding:      &raft.SimpleEncoding{},
		node:          node,
		storage:       storage,
		cache:         make(map[string]interface{}),
		metric:        metric,
		stopc:         make(chan struct{}),
	}

	return server
}

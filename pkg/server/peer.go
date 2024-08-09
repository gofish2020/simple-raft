package server

import (
	"context"
	"io"
	pb "kvdb/pkg/raftpb"
	"strconv"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 流接口，统一grpc 客户端流、服务器流使用
type Stream interface {
	Send(*pb.RaftMessage) error
	Recv() (*pb.RaftMessage, error)
}

// 远端连接信息
type Remote struct {
	address string

	conn   *grpc.ClientConn // socket 连接，用于客户端拨号使用
	client pb.RaftClient    // client 基于 conn 创建的 pb 客户端

	clientStream pb.Raft_ConsensusClient // clientStream 和  serverStream 的目的，仅仅只是对 stream的一个标识，知道是什么类型的 stream
	serverStream pb.Raft_ConsensusServer
}

type Peer struct {
	mu sync.Mutex
	wg sync.WaitGroup

	id     uint64  // 远端的id
	stream Stream  // 实际用来发送数据的双向流（ 这个 stream 即可以是 clientStream，也可以是 serverStream ），如果 stream == nil 说明没有建立过连接（也可能网络原因断开了），需要重新建立连接即可（这里的建立连接，即可以是作为客户端端主动建立连接，也可以是服务度胺被动建立的连接），不重要，重要的是建立的连接以后，就可以互相发送数据，就不存在client/server的区别了（双工效果）
	remote *Remote // 远端连接信息

	recvc chan *pb.RaftMessage // 一旦 stream 建立连接以后，就可以从 recvc 中获取到 stream上 的数据了（因为死循环会一直从stream中读取数据保存到 recvc中）

	metric chan pb.MessageType // 监控通道
	close  bool                // 是否准备关闭
	logger *zap.SugaredLogger
}

func NewPeer(id uint64, address string, recvc chan *pb.RaftMessage, metric chan pb.MessageType, logger *zap.SugaredLogger) *Peer {
	p := &Peer{
		id:     id,
		remote: &Remote{address: address},
		recvc:  recvc,
		metric: metric,
		logger: logger,
	}
	return p
}

// 批量发送消息到远端
func (p *Peer) SendBatch(msgs []*pb.RaftMessage) {
	p.wg.Add(1)
	var appEntryMsg *pb.RaftMessage
	var propMsg *pb.RaftMessage
	for _, msg := range msgs {
		if msg.MsgType == pb.MessageType_APPEND_ENTRY {
			if appEntryMsg == nil {
				appEntryMsg = msg
			} else {
				size := len(appEntryMsg.Entry)
				if size == 0 || len(msg.Entry) == 0 || appEntryMsg.Entry[size-1].Index+1 == msg.Entry[0].Index {
					appEntryMsg.LastCommit = msg.LastCommit
					appEntryMsg.Entry = append(appEntryMsg.Entry, msg.Entry...)
				} else if appEntryMsg.Entry[0].Index >= msg.Entry[0].Index {
					appEntryMsg = msg
				}
			}
		} else if msg.MsgType == pb.MessageType_PROPOSE {
			if propMsg == nil {
				propMsg = msg
			} else {
				propMsg.Entry = append(propMsg.Entry, msg.Entry...)
			}
		} else {
			p.send(msg)
		}
	}

	if appEntryMsg != nil {
		p.send(appEntryMsg)
	}

	if propMsg != nil {
		p.send(propMsg)
	}
	p.wg.Done()
}

// 通过流发送消息
func (p *Peer) send(msg *pb.RaftMessage) {

	// 消息不能位空
	if msg == nil {
		return
	}

	// p.stream 就是在 socket 基础上，创建好的 stream，用来接收和发送数据
	if p.stream == nil {

		// 如果 p.stream 不存在，最大的原因是，这里是客户端，需要主动去连接服务器（如果是服务端，这个stream应该是有值的）
		if err := p.Connect(); err != nil {
			return
		}
	}

	// 利用 p.stream 发送数据
	if err := p.stream.Send(msg); err != nil {
		p.logger.Errorf("发送消息 %s 到 %s 失败 ,日志数量: %d %v", msg.MsgType.String(), strconv.FormatUint(msg.To, 16), len(msg.Entry), err)
		return
	}

	p.metric <- msg.MsgType
}

// 阻塞流读取数据
func (p *Peer) Recv() {
	// 接收消息
	for {
		msg, err := p.stream.Recv()
		if err == io.EOF {
			p.stream = nil
			p.logger.Errorf("读取 %s 流结束", strconv.FormatUint(p.id, 16))
			return
		}

		if err != nil {
			p.stream = nil
			p.logger.Errorf("读取 %s 流失败： %v", strconv.FormatUint(p.id, 16), err)
			return
		}
		p.recvc <- msg
	}
}

// 停止
func (p *Peer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.wg.Wait()
	p.logger.Infof("关闭节点 %s 处理协程", strconv.FormatUint(p.id, 16))

	if p.remote.clientStream != nil {
		p.remote.clientStream.CloseSend()
	}

	if p.remote.conn != nil {
		p.remote.conn.Close()
	}
}

// 设置双向流，流在其他节点连入设置
func (p *Peer) SetStream(stream pb.Raft_ConsensusServer) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stream == nil {

		// 被动连接进来的stream（服务端）
		p.stream = stream
		p.remote.serverStream = stream

		// 关闭主动连接的stream（客户端）
		if p.remote.clientStream != nil {
			p.remote.clientStream.CloseSend()
			p.remote.conn.Close()
			p.remote.clientStream = nil
			p.remote.conn = nil
		}

		return true
	}
	return false
}

// 主动建立连接，可在断开后重连
func (p *Peer) Reconnect() error {

	if p.remote.clientStream != nil { // 先将之前的 clientStream 关闭，清理掉
		p.remote.clientStream.CloseSend()
		p.remote.clientStream = nil
		p.stream = nil
	}

	// 调用 Consensus 函数返回流（在基础的socket的基础上，实现流），最终的数据发送和接收要通过 stream 进行；（和普通的rpc 函数的调用后，直接返回结果不同）
	stream, err := p.remote.client.Consensus(context.Background())
	// var delay time.Duration
	for err != nil {
		p.logger.Errorf("连接raft服务 %s 失败: %v", p.remote.address, err)
		return err
		// delay++
		// if delay > 5 {
		// 	return fmt.Errorf("超过最大尝试次数10")
		// }
		// select {
		// case <-time.After(time.Second):
		// 	stream, err = p.remote.client.Consensus(context.Background())
		// case <-p.stopc:
		// 	return fmt.Errorf("任务终止")
		// }
	}

	p.logger.Debugf("创建 %s 读写流", strconv.FormatUint(p.id, 16))

	// 将流 保存起来
	p.stream = stream
	p.remote.clientStream = stream
	// 并开启协程，获取数据，并保存到 p.recvc 中
	go p.Recv()
	return nil
}

// 主动连接远端
func (p *Peer) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 如果 p.stream 不为空，直接返回
	if p.stream != nil {
		return nil
	}

	if p.remote.conn == nil {

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		// 利用 p.remote.address 地址，进行拨号
		conn, err := grpc.Dial(p.remote.address, opts...)
		if err != nil {
			p.logger.Errorf("创建连接 %s 失败: %v", strconv.FormatUint(p.id, 16), err)
			return err
		}

		// 记录下 socket
		p.remote.conn = conn
		// 将拨号的 conn 传递给 pb客户端函数，来作为实际的数据发送网络通道（grpc的函数调用）
		p.remote.client = pb.NewRaftClient(conn)
	}
	// 利用 p.remote.client 调用函数 Consensus

	return p.Reconnect()
}

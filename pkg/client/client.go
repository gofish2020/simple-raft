package client

import (
	"context"
	"fmt"
	pb "kvdb/pkg/clientpb"
	"math/rand"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	clientId uint64             // 客户端id
	reqSeq   uint64             // 请求序列号
	servers  []string           // 服务地址
	client   pb.KvdbClient      // grpc kv客户端
	logger   *zap.SugaredLogger // 日志对象
}

func (c *Client) sendRequest(req *pb.Request) {

}

func (c *Client) Put(key string, value string) error {

	kv := &pb.KvPair{Key: []byte(key), Value: []byte(value)} // kv 对

	c.reqSeq++ // 请求编号

	// 真正的发送 grpc请求到服务端
	resp, err := c.client.Put(context.Background(), &pb.Request{
		ClientId: c.clientId,
		Seq:      c.reqSeq,
		Cmd:      &pb.Command{OperateType: pb.Operate_PUT, Put: &pb.PutCommand{Data: []*pb.KvPair{kv}}},
	})

	if err != nil {
		return err
	}

	if resp.Success {
		return nil
	} else {
		return fmt.Errorf("添加数据失败")
	}
}

func (c *Client) changeConf(change *pb.ConfigCommand) error {

	req := &pb.Request{
		ClientId: c.clientId,
		Seq:      c.reqSeq,
		Cmd:      &pb.Command{OperateType: pb.Operate_CONFIG, Conf: change},
	}

	resp, err := c.client.Config(context.Background(), req)

	if err != nil {
		return err
	}

	if resp.Success {
		return nil
	} else {
		return fmt.Errorf("变更集群配置失败")
	}
}

func (c *Client) AddNode(servers map[string]string) error {
	return c.changeConf(&pb.ConfigCommand{Type: pb.ConfigType_ADD_NODE, Servers: servers})
}

func (c *Client) RemoveNode(servers map[string]string) error {
	return c.changeConf(&pb.ConfigCommand{Type: pb.ConfigType_REMOVE_NODE, Servers: servers})
}

func (c *Client) reconnect(leader string) error {
	// c.logger.Infof("集群leader变更为 %s 重新连接", leader)
	client, err := c.connect(leader)

	if err != nil {
		c.logger.Errorf("连接 %s 失败: %v", leader, err)
		return err
	}

	c.client = client
	return nil
}

func (c *Client) connect(address string) (pb.KvdbClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials())) // 不安全证书

	// 拨号（地址 + opts）
	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc连接失败: %v", err)
	}

	// 创建 grpc 客户端对象
	client := pb.NewKvdbClient(conn)

	// 发送 grpc 网络请求,注册客户端
	resp, err := client.Register(context.Background(), &pb.Auth{Token: "token"})

	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("连接集群失败: %v", err)
	}

	if resp.Success {
		c.clientId = resp.ClientId // 分配到客户端id
		return client, nil
	} else {
		conn.Close()
		return nil, nil
	}
}

func (c *Client) Connect() {

	var delay time.Duration
	for c.client == nil {

		// 随机选择一个 ip：port
		address := c.servers[rand.Intn(len(c.servers))]

		// 建立 grpc 连接，并注册客户端
		client, err := c.connect(address)
		if err != nil {
			// c.logger.Errorf("连接 %s 失败: %v", address, err)
		} else if client != nil {
			c.client = client
			c.logger.Infof("连接 %s 成功", address)
		}

		// 延迟一定时间
		if c.client == nil {
			delay++
			if delay > 100 {
				delay = 0
			}
			// 目的在于，间隔越来越长的时间去进行下一次 retry
			time.Sleep((delay/10 + 1) * time.Second)
		}
	}
}

func NewClient(servers []string, logger *zap.SugaredLogger) *Client {

	return &Client{
		servers: servers,
		logger:  logger,
	}
}

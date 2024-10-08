
本项目实现了一个简单的分布式kv数据库，共识算法使用Raft，下层存储使用lsm，节点间使用grpc进行通信。

已实现功能：
1. [领导选举](./doc/raft_learn_1.md)
2. [日志同步](./doc/raft_learn_2.md)
3. [日志压缩](./doc/raft_learn_3.md)
4. [成员变更](./doc/raft_learn_4.md)




## Raft 逻辑图

如何作为 键值服务 对外提供功能

![alt text](images/kvServer.drawio.png)

集群节点内部如何通信

这里利用了 grpc stream 来实现数据的传递。每个节点收到的数据保存在通道中，并且只有一个协程读取数据进行处理，所以不存在并发的问题。。 实际的业务处理都是单线程的
![alt text](images/raftServer.drawio.png)

每个角色的职责：

`leader`节点才会持有进度数据。这个非常重要，这样 `leader`才能知道接下来 `follower` 需要的数据进度是什么；


`leader` 在接收读请求的时候，需要确定自己确实是 多数派认可的 `leader`，才能返回数据（避免脑裂的`leader`不合时宜的返回数据）,这个是通过 `readindex`消息实现：
实现方案：
- 接收到读请求后，`leader`会主动发送一条`readindex`消息，利用`map`本地记录一笔唯一记录；然后通过心跳透传，唯一标记给`follower`；

- `follower`收到心跳，会返回心跳响应消息给`leader`。当`leader`收到心跳返回，看下有没有透传标记，同时修改本地唯一记录的**成功计数**，当达到多数派，说明 `leader`是有效的；否则就一直等待，直到超时错误（也就是本次的`leader`检测失败了）

![alt text](images/raft-role.drawio.png)


## 参考

- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [CONSENSUS: BRIDGING THEORY AND PRACTICE](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [Bigtable: A Distributed Storage System for Structured Data](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/68a74a85e1662fe02ff3967497f31fda7f32225c.pdf)
- [etcd/raft](https://github.com/etcd-io/etcd)
- [leveldb](https://github.com/google/leveldb)
- [goleveldb](https://github.com/syndtr/goleveldb)



    


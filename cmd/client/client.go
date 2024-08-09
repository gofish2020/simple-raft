package main

import (
	"flag"
	"kvdb/pkg/client"
	"kvdb/pkg/utils"
	"log"
	"math/rand"
	"os"
	"strings"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Servers []string `yaml:"servers"`
}

var configFile string
var addNode string
var removeNode string
var testPut int

func init() {
	flag.StringVar(&configFile, "f", "", "配置文件: -f config.yaml")
	flag.StringVar(&addNode, "add", "", "添加节点: -add raft_4=localhost:9204;")
	flag.StringVar(&removeNode, "remove", "", "移除节点: -remove raft_4=localhost:9204;")
	flag.IntVar(&testPut, "test", 0, "测试写入: -test 10000 ")
}

func main() {

	flag.Parse()

	// 读取 config.yaml 文件
	conf, err := os.ReadFile(configFile)
	if err != nil {
		log.Panicf("读取配置文件 %s 失败: %v", conf, err)
	}

	// 解析出服务器地址
	var config Config
	err = yaml.Unmarshal(conf, &config)

	if err != nil {
		log.Panicf("解析配置文件 %s 失败: %v", conf, err)
	}

	// 初始化日志
	logger := utils.GetLogger("client")
	sugar := logger.Sugar()

	sugar.Infof("servers: %v", config.Servers)

	// 客户端对象
	c := client.NewClient(config.Servers, sugar)

	// 连接服务
	c.Connect()

	// 这是具体的功能效果
	if addNode != "" { // raft_4=localhost:9204;
		nodes := make(map[string]string)
		for _, v := range strings.Split(addNode, ";") {
			node := strings.Split(v, "=")
			nodes[node[0]] = node[1] //  raft_4  localhost:9204
		}
		c.AddNode(nodes)
		return
	}

	if removeNode != "" {
		nodes := make(map[string]string)
		for _, v := range strings.Split(removeNode, ";") {
			node := strings.Split(v, "=")
			nodes[node[0]] = node[1]
		}
		c.RemoveNode(nodes)
		return
	}

	if testPut > 0 {
		for i := 0; i < testPut; i++ { // testPut 测试次数
			key := utils.RandStringBytesRmndr(rand.Intn(10) + 10)
			value := utils.RandStringBytesRmndr(20)
			// 随机 kv
			c.Put(string(key), string(value))
		}
	}

}

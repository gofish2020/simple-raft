package main

import (
	"encoding/json"
	"flag"
	"kvdb/pkg/server"
	"kvdb/pkg/utils"
	"log"
	"os"
	"path"

	"gopkg.in/yaml.v2"
)

type Config struct {
	NodeConf *NodeConfig `yaml:"node"`
}

type NodeConfig struct {
	WorkDir       string            `yaml:"workDir"`
	Name          string            `yaml:"name"`
	PeerAddress   string            `yaml:"peerAddress"`
	ServerAddress string            `yaml:"serverAddress"`
	Servers       map[string]string `yaml:"servers"`
}

var configFile string

func init() {
	flag.StringVar(&configFile, "f", "config.yaml", "配置文件")
}

func main() {

	flag.Parse()

	// 读取配置文件
	conf, err := os.ReadFile(configFile)
	if err != nil {
		log.Panicf("读取配置文件 %s 失败: %v", conf, err)
	}

	var config Config
	err = yaml.Unmarshal(conf, &config)

	if err != nil {
		log.Panicf("解析配置文件 %s 失败: %v", conf, err)
	}

	// ./data/node1
	logger := utils.GetLogger(path.Join(config.NodeConf.WorkDir, config.NodeConf.Name))
	sugar := logger.Sugar()

	json, _ := json.Marshal(config.NodeConf.Servers)

	sugar.Infof("配置文件: %s , %s", configFile, string(json))

	// 创建 RaftServer 对象
	server.Bootstrap(&server.Config{
		Dir:           config.NodeConf.WorkDir,       // ./data
		Name:          config.NodeConf.Name,          //  node1
		PeerAddress:   config.NodeConf.PeerAddress,   // localhost:3900
		ServerAddress: config.NodeConf.ServerAddress, // localhost:8900     kv服务的端口
		Peers:         config.NodeConf.Servers,       /*  node1: localhost:3900   node2: localhost:3901  node3: localhost:3902 */
		Logger:        sugar,
	}).Start()

}

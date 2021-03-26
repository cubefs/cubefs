package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"

	"github.com/tiglabs/raft/proto"
)

var confFile = flag.String("conf", "", "config file path")

type raftServerConfig struct {
	ID          uint64 `json:"id"`
	Addr        string `json:"addr"`
	GroupNum    int    `json:"groupNum"`
	Peers       []peer `json:"peers"`
	HeartPort   string `json:"heartPort"`
	ReplicaPort string `json:"replicaPort"`
	Listen      string `json:"listen"`
	WalDir      string `json:"walDir"`
	LogDir      string `json:"logDir"`
	LogLevel    string `json:"level"`
	StoreType   int    `json:"storeType"`
}

type peer struct {
	ID   uint64 `json:"id"`
	Addr string `json:"addr"`
}

func main() {
	flag.Parse()

	rConf := &raftServerConfig{}
	rConf.parseConfig(*confFile)
	logLevel = rConf.LogLevel
	storageType = rConf.StoreType
	walDir = rConf.WalDir
	initRaftLog(rConf.LogDir)

	resolver = initNodeManager()
	peers := make([]proto.Peer, 0)
	for _, p := range rConf.Peers {
		peer := proto.Peer{ID: p.ID}
		peers = append(peers, peer)
		resolver.addNodeAddr(p, rConf.ReplicaPort, rConf.HeartPort)
	}

	server := createRaftServer(rConf.ID, 0, 0, peers, true, false, rConf.GroupNum)
	server.conf = rConf
	server.startHttpService(rConf.Addr, rConf.Listen)
}

func (rConf *raftServerConfig) parseConfig(filePath string) {
	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(bytes, rConf)
	if err != nil {
		panic(err)
	}
}

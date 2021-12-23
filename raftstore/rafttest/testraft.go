package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/tiglabs/raft/proto"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
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
	DiskNum		int	   `json:"diskNum"`
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
	var err error
	profPort := 9999
	var profNetListener net.Listener = nil
	if profNetListener, err = net.Listen("tcp", fmt.Sprintf(":%v", profPort)); err != nil {
		fmt.Println(fmt.Sprintf("Fatal: listen prof port %v failed: %v", profPort, err))
		return
	}
	// 在prof端口监听上启动http API.
	go func() {
		_ = http.Serve(profNetListener, http.DefaultServeMux)
	}()
	rConf := &raftServerConfig{}
	rConf.parseConfig(*confFile)
	logLevel = rConf.LogLevel
	storageType = rConf.StoreType
	walDir = rConf.WalDir
	diskNum = rConf.DiskNum
	dataType = 1
	initRaftLog(rConf.LogDir)

	resolver = initNodeManager()
	peers := make([]proto.Peer, 0)
	for _, p := range rConf.Peers {
		peer := proto.Peer{ID: p.ID}
		peers = append(peers, peer)
		resolver.addNodeAddr(p, rConf.ReplicaPort, rConf.HeartPort)
	}

	fmt.Println(fmt.Sprintf("loglevel[%v], storageType[%v], walDir[%v], diskNum[%v], dataType[%v]", logLevel, storageType, walDir, diskNum, dataType))
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

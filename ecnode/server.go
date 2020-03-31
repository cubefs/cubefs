// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ecnode

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"runtime"
	"time"

	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"

	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
)

var (
	ErrIncorrectStoreType       = errors.New("Incorrect store type")
	ErrNoSpaceToCreatePartition = errors.New("No disk space to create a data partition")
	ErrNewSpaceManagerFailed    = errors.New("Creater new space manager failed")
)

const (
	DefaultCellName      = "cfs_cell1"
	DefaultDiskMaxErr    = 1
	DefaultDiskRetainMin = 5 * util.GB  // GB
	DefaultDiskRetainMax = 30 * util.GB // GB
)

const (
	ModuleName = "ecnode"
)

// Network protocol
const (
	NetworkProtocol = "tcp"
)

var (
	localIP, serverPort string

	gConnPool    = util.NewConnectPool()
	MasterClient = masterSDK.NewMasterClient(nil, false)
)

const (
	ConfigKeyLocalIP    = "localIP"    // string
	ConfigKeyPort       = "port"       // int
	ConfigKeyMasterAddr = "masterAddr" // array
	ConfigKeyDisks      = "disks"      // array
	ConfigKeyCell       = "cell"       // string
)

type EcNode struct {
	space           *SpaceManager
	clusterID       string
	port            string
	cellName        string
	localIP         string
	localServerAddr string
	nodeID          uint64
	raftDir         string
	raftHeartbeat   string
	raftReplica     string
	//raftStore raftstore.RaftStroe

	tcpListener net.Listener
	stopC       chan bool

	control common.Control
}

func NewServer() *EcNode {
	return &EcNode{}
}

func (e *EcNode) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	return e.control.Start(e, cfg, doStart)
}

func (e *EcNode) Shutdown() {
	e.control.Shutdown(e, doShutdown)
}

func (e *EcNode) Sync() {
	e.control.Sync()
}

func doStart(server common.Server, cfg *config.Config) (err error) {
	e, ok := server.(*EcNode)
	if !ok {
		return errors.New("Invalid Node Type!")
	}
	e.stopC = make(chan bool, 0)

	err = e.parseConfig(cfg)
	if err != nil {
		return
	}

	// register to master
	e.register(cfg)

	// TODO create space manager
	e.startSpaceManager(cfg)

	// TODO check local partition compare with master, if lack, then not start

	// start tcp listening
	err = e.startTCPService()
	if err != nil {
		return
	}
	go e.registerHandler()

	return
}

func doShutdown(server common.Server) {
	e, ok := server.(*EcNode)
	if !ok {
		return
	}
	close(e.stopC)
	e.stopTCPService()
	//e.stopRaftServer()
	return
}

func (e *EcNode) parseConfig(cfg *config.Config) (err error) {
	localIP = cfg.GetString(ConfigKeyLocalIP)
	serverPort = cfg.GetString(proto.ListenPort)

	regexpPort, err := regexp.Compile("^(\\d)+$")
	if err != nil {
		return fmt.Errorf("Err:no port")
	}
	if !regexpPort.MatchString(serverPort) {
		return fmt.Errorf("Err:port must string")
	}

	e.port = serverPort
	if len(cfg.GetArray(proto.MasterAddr)) == 0 {
		return fmt.Errorf("Err: masterAddr unavailable")
	}
	for _, ip := range cfg.GetArray(proto.MasterAddr) {
		MasterClient.AddNode(ip.(string))
	}
	log.LogDebugf("action[parseConfig] load masterAddrs(%v).", MasterClient.Nodes())
	log.LogDebugf("action[parseConfig] load port(%v).", e.port)
	return
}

func (e *EcNode) startTCPService() (err error) {
	log.LogInfo("Start: startTCPService")
	addr := fmt.Sprintf(":%s", e.port)

	l, err := net.Listen(NetworkProtocol, addr)
	log.LogDebugf("action[startTCPService] listen %v address(%v).", NetworkProtocol, addr)
	if err != nil {
		log.LogError("failed to listen, err:", err)
		return
	}
	e.tcpListener = l
	go func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.LogErrorf("action[startTCPService] failed to accept, err:%s", err.Error())
				break
			}
			log.LogDebugf("action[startTCPService] accept connection from %s.", conn.RemoteAddr().String())
			go e.serverConn(conn)
		}
	}(l)
	return
}

func (e *EcNode) stopTCPService() (err error) {
	if e.tcpListener != nil {

		e.tcpListener.Close()
		log.LogDebugf("action[stopTCPService] stop tcp service.")
	}
	return
}

func (e *EcNode) serverConn(conn net.Conn) {
	c, _ := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	packetProcessor := repl.NewReplProtocol(c, e.Prepare, e.OperatePacket, e.Post)
	packetProcessor.ServerConn()
}

func (e *EcNode) registerHandler() {
	server := http.Server{
		Addr: ":8080",
	}

	http.HandleFunc("/disks", e.getDiskAPI)
	http.HandleFunc("/partitions", e.getPartitionsAPI)
	http.HandleFunc("/partition", e.getPartitionAPI)
	http.HandleFunc("/extent", e.getExtentAPI)
	http.HandleFunc("/block", e.getBlockCrcAPI)
	http.HandleFunc("/stats", e.getStatAPI)
	http.HandleFunc("/raftStatus", e.getRaftStatusAPI)

	server.ListenAndServe()
}

func (e *EcNode) register(cfg *config.Config) {
	timer := time.NewTimer(0)

	for {
		select {
		case <-timer.C:
			ci, err := MasterClient.AdminAPI().GetClusterInfo()
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot get ip from master(%v) err(%v).",
					MasterClient.Leader(), err)
				timer.Reset(2 * time.Second)
				continue
			}

			masterAddr := MasterClient.Leader()
			e.clusterID = ci.Cluster
			if localIP == "" {
				localIP = string(ci.Ip)
			}
			e.localServerAddr = fmt.Sprintf("%s:%s", localIP, e.port)
			if !util.IsIPV4(localIP) {
				log.LogErrorf("action[registerToMaster] got an invalid local ip(%v) from master(%v).",
					localIP, masterAddr)
				timer.Reset(2 * time.Second)
				continue
			}

			nodeID, err := MasterClient.NodeAPI().AddEcNode(e.localServerAddr)
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot register this node to master[%v] err(%v).",
					masterAddr, err)
				timer.Reset(2 * time.Second)
				continue
			}

			e.nodeID = nodeID
			log.LogDebugf("register: register EcNode: nodeID(%v)", e.nodeID)
			return
		case <-e.stopC:
			timer.Stop()
			return
		}
	}
}

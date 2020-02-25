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
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"

	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
)

const (
	ModuleName = "ecnode"
)

// Network protocol
const (
	NetworkProtocol = "tcp"
)

// The status of the server
const (
	Standby uint32 = iota
	Start
	Running
	Shutdown
	Stopped
)

var (
	MasterClient = masterSDK.NewMasterClient(nil, false)
)

type EcNode struct {
	//space   *SpaceManager
	port            string
	localIP         string
	localServerAddr string
	nodeID          uint64
	raftDir         string
	raftHeartbeat   string
	raftReplica     string
	//raftStore raftstore.RaftStroe

	tcpListener net.Listener
	stopC       chan bool
	state       uint32
	wg          sync.WaitGroup
}

func NewServer() *EcNode {
	return &EcNode{}
}

func (e *EcNode) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	if atomic.CompareAndSwapUint32(&e.state, Standby, Start) {
		defer func() {
			if err != nil {
				atomic.StoreUint32(&e.state, Standby)
			} else {
				atomic.StoreUint32(&e.state, Running)
			}
		}()
		err = e.onStart(cfg)
		if err != nil {
			return
		}
		e.wg.Add(1)
	}
	return

}

func (e *EcNode) Shutdown() {
	if atomic.CompareAndSwapUint32(&e.state, Running, Shutdown) {
		e.onShutdown()
		e.wg.Done()
		atomic.StoreUint32(&e.state, Stopped)
	}
}

func (e *EcNode) Sync() {
	if atomic.LoadUint32(&e.state) == Running {
		e.wg.Wait()
	}
}

func (e *EcNode) onStart(cfg *config.Config) (err error) {
	e.stopC = make(chan bool, 0)

	err = e.parseConfig(cfg)
	if err != nil {
		return
	}

	//exporter.Init(ModuleName, cfg)

	// TODO register to master
	//e.register(cfg)

	// TODO start raft server

	// TODO create space manager

	// TODO check local partition compare with master, if lack, then not start

	// start tcp listening
	err = e.startTCPService()
	if err != nil {
		return
	}
	go e.registerHandler()

	return
}

func (e *EcNode) onShutdown() {
	close(e.stopC)
	e.stopTCPService()
	//e.stopRaftServer()
	return
}

func (e *EcNode) parseConfig(cfg *config.Config) (err error) {
	var port string

	port = cfg.GetString(proto.ListenPort)

	e.port = port
	if len(cfg.GetArray(proto.MasterAddr)) == 0 {
		return fmt.Errorf("Err: masterAddr unavailable")
	}

	// TODO master client
	//for _, ip := range cfg.GetArray(proto.MasterAddr) {
	//	MasterClient.AddNode(ip.(string))
	//}

	return
}

func (e *EcNode) startTCPService() (err error) {
	log.LogInfo("Start: startTCPService")
	addr := fmt.Sprintf(":#{e.port}")

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
	http.HandleFunc("/disks", e.getDiskAPI)
	http.HandleFunc("/partitions", e.getPartitionsAPI)
	http.HandleFunc("/partition", e.getPartitionAPI)
	http.HandleFunc("/extent", e.getExtentAPI)
	http.HandleFunc("/block", e.getBlockCrcAPI)
	http.HandleFunc("/stats", e.getStatAPI)
	http.HandleFunc("/raftStatus", e.getRaftStatusAPI)
}

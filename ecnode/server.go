// Copyright 2020 The CubeFS Authors.
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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"runtime"
	"sync"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"

	masterSDK "github.com/cubefs/cubefs/sdk/master"
)

var (
	ErrNewSpaceManagerFailed = errors.New("Creater new space manager failed")
)

const (
	DefaultDiskMaxErr    = 1
	DefaultDiskRetainMin = 5 * unit.GB // GB
)

// Network protocol
const (
	NetworkProtocol = "tcp"
)

var (
	localIP, serverPort, cellName string
	gConnPool                     = connpool.NewConnectPool()
	MasterClient                  = masterSDK.NewMasterClient(nil, false)
)

const (
	ConfigKeyLocalIP = "localIP" // string
	ConfigKeyDisks   = "disks"   // array
	ConfigKeyCell    = "cell"    // string

	defaultGetEcScrubInfo          = 30  //minute  //todo change hours
	defaultScrubCheckInterval      = 60  //minute  //todo change hours
	defaultComputeCrcInterval      = 5   //minute
	defaultAutoDeleteTinyInterval  = 3   //minute
	defaultRepairInterval          = 2   //minute
	defaultEcUpdateReplicaInterval = 300 // interval to update the replica
	defaultCheckDailMapInterval    = 10
	defaultDelDailMapInterval      = 1  //minute
	defaultScrubPeriodTime         = 60 //second
)

type EcNode struct {
	space           *SpaceManager
	clusterID       string
	port            string
	httpPort        string
	cellName        string
	localIP         string
	localServerAddr string
	nodeID          uint64
	scrubEnable     bool
	scrubPeriod     uint32
	maxScrubExtents uint8
	maxTinyDelCount uint8
	tcpListener     net.Listener
	stopC           chan bool

	control         common.Control
	nodeDialFailMap sync.Map
}

type HostZeroPartitions struct {
	connectStatus bool
	eps           []*EcPartition
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
		return errors.New("invalid Node Type")
	}

	e.stopC = make(chan bool, 0)

	err = e.parseConfig(cfg)
	if err != nil {
		return
	}
	repl.SetConnectPool(gConnPool)
	// register to master
	e.register()

	// TODO create space manager
	e.startSpaceManager(cfg)

	// start tcp listening
	err = e.startTCPService()
	if err != nil {
		return
	}
	go e.registerHandler()

	go e.scheduleToEcNode()
	return
}

func doShutdown(server common.Server) {
	e, ok := server.(*EcNode)
	if !ok {
		return
	}
	close(e.stopC)
	e.stopTCPService()
	e.space.Stop()
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
	cellName = cfg.GetString(ConfigKeyCell)
	if err != nil {
		return fmt.Errorf("Err:no port ")
	}
	e.cellName = cellName
	e.port = serverPort
	e.httpPort = cfg.GetString(proto.HttpPort)
	if len(cfg.GetSlice(proto.MasterAddr)) == 0 {
		return fmt.Errorf("Err: masterAddr unavailable")
	}
	for _, ip := range cfg.GetSlice(proto.MasterAddr) {
		MasterClient.AddNode(ip.(string))
	}
	e.maxTinyDelCount = MaxTinyDelHandle
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
		Addr: ":8090",
	}

	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("EcNode")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})

	http.HandleFunc("/partitions", e.getPartitionsAPI)
	http.HandleFunc("/extent", e.getExtentAPI)
	http.HandleFunc("/stats", e.getStatAPI)
	http.HandleFunc("/releasePartitions", e.releasePartitions)
	http.HandleFunc("/checkExtentChunkData", e.checkExtentChunkData)
	http.HandleFunc("/ecExtentHosts", e.getExtentHosts)
	http.HandleFunc("/getAllExtents", e.getAllExtents)
	http.HandleFunc("/getTinyDelInfo", e.getTinyDelInfo)
	http.HandleFunc("/extentCrc", e.getExtentCrc)
	http.HandleFunc("/setMaxTinyDelCount", e.setMaxTinyDelCount)
	http.HandleFunc("/setEcPartitionSize", e.setEcPartitionSize)

	server.ListenAndServe()
}

func (e *EcNode) register() {
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
			if !unit.IsIPV4(localIP) {
				log.LogErrorf("action[registerToMaster] got an invalid local ip(%v) from master(%v).",
					localIP, masterAddr)
				timer.Reset(2 * time.Second)
				continue
			}

			nodeID, err := MasterClient.NodeAPI().AddEcNode(e.localServerAddr, e.httpPort, e.cellName, EcNodeLatestVersion)
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

func (e *EcNode) getEcScrubInfo() {
	scrubInfo, err := MasterClient.NodeAPI().GetEcScrubInfo()
	if err != nil {
		log.LogErrorf("node[%v] getEcScrubInfo err[%v]", err, e.localServerAddr)
		return
	}
	e.scrubEnable = scrubInfo.ScrubEnable
	e.scrubPeriod = scrubInfo.ScrubPeriod
	e.maxScrubExtents = scrubInfo.MaxScrubExtents
	log.LogDebugf("getEcScrubInfo scrubEnable[%v] scrubPeriod[%v] min maxScrubExtents[%v]",
		e.scrubEnable, e.scrubPeriod, e.maxScrubExtents)
}

func (e *EcNode) scheduleToEcNode() {
	checkNetDialMapTimer := time.NewTimer(time.Second * defaultCheckDailMapInterval)
	getEcScrubInfoTimer := time.NewTimer(time.Minute * 0)

	for {
		select {
		case <-checkNetDialMapTimer.C:
			e.nodeDialFailMap.Range(func(key, value interface{}) bool {
				tm := value.(time.Time)
				if time.Since(tm).Minutes() > defaultDelDailMapInterval {
					e.nodeDialFailMap.Delete(key)
					log.LogDebugf("checkNodeNetDial delete key(%v)", key)
				}
				return true
			})
			checkNetDialMapTimer.Reset(time.Second * defaultCheckDailMapInterval)
		case <-getEcScrubInfoTimer.C:
			e.getEcScrubInfo()
			getEcScrubInfoTimer.Reset(time.Minute * defaultGetEcScrubInfo)
		case <-e.stopC:
			checkNetDialMapTimer.Stop()
			getEcScrubInfoTimer.Stop()
			return
		}
	}
}

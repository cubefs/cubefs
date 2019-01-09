// Copyright 2018 The Container File System Authors.
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

package datanode

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/repl"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/exporter"
	"github.com/tiglabs/containerfs/util/log"
	"os"
	"syscall"
)

var (
	ErrIncorrectStoreType       = errors.New("incorrect store type")
	ErrNoSpaceToCreatePartition = errors.New("no disk space to create a data partition")
	ErrBadConfFile              = errors.New("bad config file")

	LocalIP      string
	gConnPool    = util.NewConnectPool()
	MasterHelper = util.NewMasterHelper()
)

const (
	// TODO we need to find a better to expose this information.
	DefaultRackName = "cfs_rack1"
	DefaultRaftDir  = "raft"
)

const (
	ModuleName = "dataNode"
)

const (
	ConfigKeyPort          = "port"          // int
	ConfigKeyMasterAddr    = "masterAddr"    // array
	ConfigKeyRack          = "rack"          // string
	ConfigKeyDisks         = "disks"         // array
	ConfigKeyRaftDir       = "raftDir"       // string
	ConfigKeyRaftHeartbeat = "raftHeartbeat" // string
	ConfigKeyRaftReplicate = "raftReplica"   // string
)

// DataNode defines the structure of a data node.
type DataNode struct {
	space           *SpaceManager
	port            string
	rackName        string
	clusterID       string
	localIP         string
	localServerAddr string
	nodeID          uint64
	raftDir         string
	raftHeartbeat   string
	raftReplica     string
	raftStore       raftstore.RaftStore
	tcpListener     net.Listener
	stopC           chan bool
	state           uint32
	wg              sync.WaitGroup
}

func NewServer() *DataNode {
	return &DataNode{}
}

func (s *DataNode) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	if atomic.CompareAndSwapUint32(&s.state, Standby, Start) {
		defer func() {
			if err != nil {
				atomic.StoreUint32(&s.state, Standby)
			} else {
				atomic.StoreUint32(&s.state, Running)
			}
		}()
		if err = s.onStart(cfg); err != nil {
			return
		}
		s.wg.Add(1)
	}
	return
}

// Shutdown shuts down the current data node.
func (s *DataNode) Shutdown() {
	if atomic.CompareAndSwapUint32(&s.state, Running, Shutdown) {
		s.onShutdown()
		s.wg.Done()
		atomic.StoreUint32(&s.state, Stopped)
	}
}

// Sync keeps data node in sync.
func (s *DataNode) Sync() {
	if atomic.LoadUint32(&s.state) == Running {
		s.wg.Wait()
	}
}

// Workflow of starting up a data node.
func (s *DataNode) onStart(cfg *config.Config) (err error) {
	s.stopC = make(chan bool, 0)

	// parse the config file
	if err = s.parseConfig(cfg); err != nil {
		return
	}

	s.register()

	exporter.Init(s.clusterID, ModuleName, cfg)

	// start the raft server
	if err = s.startRaftServer(cfg); err != nil {
		return
	}

	// create space manager (disk, partition, etc.)
	if err = s.startSpaceManager(cfg); err != nil {
		return
	}

	// start tcp listening
	if err = s.startTCPService(); err != nil {
		return
	}

	go s.registerHandler()

	return
}

func (s *DataNode) onShutdown() {
	close(s.stopC)
	s.stopTCPService()
	s.stopRaftServer()
	return
}

func (s *DataNode) parseConfig(cfg *config.Config) (err error) {
	var (
		port       string
		regexpPort *regexp.Regexp
	)
	port = cfg.GetString(ConfigKeyPort)
	if regexpPort, err = regexp.Compile("^(\\d)+$"); err != nil {
		return
	}
	if !regexpPort.MatchString(port) {
		err = ErrBadConfFile
		return
	}
	s.port = port
	if len(cfg.GetArray(ConfigKeyMasterAddr)) == 0 {
		return ErrBadConfFile
	}
	for _, ip := range cfg.GetArray(ConfigKeyMasterAddr) {
		MasterHelper.AddNode(ip.(string))
	}
	s.rackName = cfg.GetString(ConfigKeyRack)
	if s.rackName == "" {
		s.rackName = DefaultRackName
	}
	log.LogDebugf("action[parseConfig] load masterAddrs[%v].", MasterHelper.Nodes())
	log.LogDebugf("action[parseConfig] load port[%v].", s.port)
	log.LogDebugf("action[parseConfig] load rackName[%v].", s.rackName)
	return
}

func (s *DataNode) startSpaceManager(cfg *config.Config) (err error) {
	s.space = NewSpaceManager(s.rackName)
	if err != nil || len(strings.TrimSpace(s.port)) == 0 {
		err = ErrBadConfFile
		return
	}

	s.space.SetRaftStore(s.raftStore)
	s.space.SetNodeID(s.nodeID)
	s.space.SetClusterID(s.clusterID)

	var wg sync.WaitGroup
	for _, d := range cfg.GetArray(ConfigKeyDisks) {
		log.LogDebugf("action[startSpaceManager] load disk raw config(%v).", d)

		// format "PATH:RESET_SIZE:MAX_ERR
		arr := strings.Split(d.(string), ":")
		if len(arr) != 3 {
			return ErrBadConfFile
		}
		path := arr[0]
		fileInfo, err := os.Stat(path)
		if err != nil {
			return ErrBadConfFile
		}
		if !fileInfo.IsDir() {
			return ErrBadConfFile
		}
		restSize, err := strconv.ParseUint(arr[1], 10, 64)
		if err != nil {
			return ErrBadConfFile
		}
		maxErr, err := strconv.Atoi(arr[2])
		if err != nil {
			return ErrBadConfFile
		}
		if restSize < util.GB*30 {
			restSize = 30 * util.GB
		}
		wg.Add(1)
		go func(wg *sync.WaitGroup, path string, restSize uint64, maxErrs int) {
			defer wg.Done()
			s.space.LoadDisk(path, restSize, maxErrs)
		}(&wg, path, restSize, maxErr)
	}
	wg.Wait()
	return nil
}

// registers the data node on the master to report the information such as IP address.
// The startup of a data node will be blocked until the registration succeeds.
func (s *DataNode) register() {
	var (
		err  error
		data []byte
	)

	timer := time.NewTimer(0)

	// get the IP address, cluster ID and node ID from the master
	for {
		select {
		case <-timer.C:
			data, err = MasterHelper.Request(http.MethodGet, proto.AdminGetIP, nil, nil)
			masterAddr := MasterHelper.Leader()
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot get ip from master(%v) err(%v).",
					masterAddr, err)
				timer.Reset(5 * time.Second)
				continue
			}
			cInfo := new(proto.ClusterInfo)
			// TODO Unhandled errors
			json.Unmarshal(data, cInfo)
			LocalIP = string(cInfo.Ip)
			s.clusterID = cInfo.Cluster
			s.localServerAddr = fmt.Sprintf("%s:%v", LocalIP, s.port)
			if !util.IP(LocalIP) {
				log.LogErrorf("action[registerToMaster] got an invalid local ip(%v) from master(%v).",
					LocalIP, masterAddr)
				timer.Reset(5 * time.Second)
				continue
			}
			// register this data node on the master
			params := make(map[string]string)
			params["addr"] = fmt.Sprintf("%s:%v", LocalIP, s.port)
			data, err = MasterHelper.Request(http.MethodPost, proto.AddDataNode, params, nil)
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot register this node to master[%] err(%v).",
					masterAddr, err)
				continue
			}

			nodeID := strings.TrimSpace(string(data))
			s.nodeID, err = strconv.ParseUint(nodeID, 10, 64)
			log.LogDebug("[tempDebug] nodeID=%v", s.nodeID)
			return
		case <-s.stopC:
			timer.Stop()
			return
		}
	}
}

func (s *DataNode) registerHandler() {
	http.HandleFunc("/disks", s.getDiskAPI)
	http.HandleFunc("/partitions", s.getPartitionsAPI)
	http.HandleFunc("/partition", s.getPartitionAPI)
	http.HandleFunc("/extent", s.getExtentAPI)
	http.HandleFunc("/stats", s.getStatAPI)
}

func (s *DataNode) startTCPService() (err error) {
	log.LogInfo("Start: startTCPService")
	addr := fmt.Sprintf(":%v", s.port)
	l, err := net.Listen(NetworkProtocol, addr)
	log.LogDebugf("action[startTCPService] listen %v address(%v).", NetworkProtocol, addr)
	if err != nil {
		log.LogError("failed to listen, err:", err)
		return
	}
	s.tcpListener = l
	go func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.LogErrorf("action[startTCPService] failed to accept, err:%s", err.Error())
				break
			}
			log.LogDebugf("action[startTCPService] accept connection from %s.", conn.RemoteAddr().String())
			go s.serveConn(conn)
		}
	}(l)
	return
}

func (s *DataNode) stopTCPService() (err error) {
	if s.tcpListener != nil {
		// TODO Unhandled errors
		s.tcpListener.Close()
		log.LogDebugf("action[stopTCPService] stop tcp service.")
	}
	return
}

func (s *DataNode) serveConn(conn net.Conn) {
	space := s.space
	space.Stats().AddConnection()
	c, _ := conn.(*net.TCPConn)
	// TODO Unhandled errors
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	c.SetLinger(10)
	packetProcessor := repl.NewReplProtocol(c, s.Prepare, s.OperatePacket, s.Post)
	packetProcessor.ServerConn()
}

// Increase the disk error count by one.
func (s *DataNode) incDiskErrCnt(partitionID uint64, err error, flag uint8) {
	if err == nil {
		return
	}
	dp := s.space.Partition(partitionID)
	if dp == nil {
		return
	}
	d := dp.Disk()
	if d == nil {
		return
	}
	if !IsDiskErr(err.Error()) {
		return
	}
	if flag == WriteFlag {
		d.incWriteErrCnt()
	} else if flag == ReadFlag {
		d.incReadErrCnt()
	}
}

func IsDiskErr(errMsg string) bool {
	if strings.Contains(errMsg, syscall.EIO.Error()) {
		return true
	}

	return false
}

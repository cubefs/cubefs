// Copyright 2018 The CubeFS Authors.
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

package master

import (
	"fmt"
	"go.uber.org/atomic"
	"net"
	"net/http/httputil"
	"regexp"
	"strconv"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// configuration keys
const (
	ClusterName       = "clusterName"
	ID                = "id"
	IP                = "ip"
	Port              = "port"
	LogLevel          = "logLevel"
	WalDir            = "walDir"
	StoreDir          = "storeDir"
	GroupID           = 1
	ModuleName        = "master"
	CfgRetainLogs     = "retainLogs"
	DefaultRetainLogs = 20000
	cfgTickInterval   = "tickInterval"
	cfgElectionTick   = "electionTick"
	SecretKey         = "masterServiceKey"
)

var (
	// regexps for data validation
	volNameRegexp = regexp.MustCompile("^[a-zA-Z0-9][a-zA-Z0-9_.-]{1,61}[a-zA-Z0-9]$")
	ownerRegexp   = regexp.MustCompile("^[A-Za-z][A-Za-z0-9_]{0,20}$")

	useConnPool = true //for test
	gConfig     *clusterConfig
)

// Server represents the server in a cluster
type Server struct {
	id               uint64
	clusterName      string
	ip               string
	port             string
	walDir           string
	storeDir         string
	retainLogs       uint64
	tickInterval     int
	electionTick     int
	leaderInfo       *LeaderInfo
	config           *clusterConfig
	cluster          *Cluster
	user             *User
	rocksDBStore     *raftstore.RocksDBStore
	raftStore        raftstore.RaftStore
	fsm              *MetadataFsm
	partition        raftstore.Partition
	wg               sync.WaitGroup
	reverseProxy     *httputil.ReverseProxy
	metaReady        atomic.Bool
	leaderVersion    atomic.Uint64
	leaderChangeChan chan uint64
	apiListener      net.Listener
}

// NewServer creates a new server
func NewServer() *Server {
	return &Server{}
}

// Start starts a server
func (m *Server) Start(cfg *config.Config) (err error) {
	m.config = newClusterConfig()
	gConfig = m.config
	m.leaderInfo = &LeaderInfo{}
	m.leaderChangeChan = make(chan uint64, 64)
	m.reverseProxy = m.newReverseProxy()
	if err = m.checkConfig(cfg); err != nil {
		log.LogError(errors.Stack(err))
		return
	}

	if m.rocksDBStore, err = raftstore.NewRocksDBStore(m.storeDir, LRUCacheSize, WriteBufferSize); err != nil {
		return
	}

	if err = m.createRaftServer(); err != nil {
		log.LogError(errors.Stack(err))
		return
	}
	m.initCluster()
	m.initUser()
	m.scheduleProcessLeaderChange()
	m.cluster.partition = m.partition
	m.cluster.idAlloc.partition = m.partition
	if err = m.partition.Start(); err != nil {
		return errors.Trace(err, "start raft partition failed")
	}
	MasterSecretKey := cfg.GetString(SecretKey)
	if m.cluster.MasterSecretKey, err = cryptoutil.Base64Decode(MasterSecretKey); err != nil {
		return fmt.Errorf("action[Start] failed %v, err: master service Key invalid = %s", proto.ErrInvalidCfg, MasterSecretKey)
	}

	m.cluster.scheduleTask()
	if err = m.startHTTPService(ModuleName, cfg); err != nil {
		return
	}
	exporter.Init(m.clusterName, ModuleName, cfg)
	exporter.RegistConsul(cfg)
	metricsService := newMonitorMetrics(m.cluster)
	metricsService.start()
	m.wg.Add(1)
	return nil
}

// Shutdown closes the server
func (m *Server) Shutdown() {
	var err error
	if m.apiListener != nil {
		if err = m.apiListener.Close(); err != nil {
			log.LogErrorf("close API net listener failed: %v", err)
		}
	}
	m.wg.Done()
}

// Sync waits for the execution termination of the server
func (m *Server) Sync() {
	m.wg.Wait()
}

func (m *Server) checkConfig(cfg *config.Config) (err error) {
	m.clusterName = cfg.GetString(ClusterName)
	m.ip = cfg.GetString(IP)
	m.port = cfg.GetString(proto.ListenPort)
	m.walDir = cfg.GetString(WalDir)
	m.storeDir = cfg.GetString(StoreDir)
	peerAddrs := cfg.GetString(cfgPeers)
	if m.ip == "" || m.port == "" || m.walDir == "" || m.storeDir == "" || m.clusterName == "" || peerAddrs == "" {
		return fmt.Errorf("%v,err:%v,%v,%v,%v,%v,%v,%v", proto.ErrInvalidCfg, "one of (ip,listen,walDir,storeDir,clusterName) is null",
			m.ip, m.port, m.walDir, m.storeDir, m.clusterName, peerAddrs)
	}
	if m.id, err = strconv.ParseUint(cfg.GetString(ID), 10, 64); err != nil {
		return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
	}
	m.config.heartbeatPort = cfg.GetInt64(heartbeatPortKey)
	m.config.replicaPort = cfg.GetInt64(replicaPortKey)
	if m.config.heartbeatPort <= 1024 {
		m.config.heartbeatPort = raftstore.DefaultHeartbeatPort
	}
	if m.config.replicaPort <= 1024 {
		m.config.replicaPort = raftstore.DefaultReplicaPort
	}
	fmt.Printf("heartbeatPort[%v],replicaPort[%v]\n", m.config.heartbeatPort, m.config.replicaPort)
	if err = m.config.parsePeers(peerAddrs); err != nil {
		return
	}
	nodeSetCapacity := cfg.GetString(nodeSetCapacity)
	if nodeSetCapacity != "" {
		if m.config.nodeSetCapacity, err = strconv.Atoi(nodeSetCapacity); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	if m.config.nodeSetCapacity < 64 {
		m.config.nodeSetCapacity = defaultNodeSetCapacity
	}

	metaNodeReservedMemory := cfg.GetString(cfgMetaNodeReservedMem)
	if metaNodeReservedMemory != "" {
		if m.config.metaNodeReservedMem, err = strconv.ParseUint(metaNodeReservedMemory, 10, 64); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	if m.config.metaNodeReservedMem < 32*1024*1024 {
		m.config.metaNodeReservedMem = defaultMetaNodeReservedMem
	}

	retainLogs := cfg.GetString(CfgRetainLogs)
	if retainLogs != "" {
		if m.retainLogs, err = strconv.ParseUint(retainLogs, 10, 64); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	if m.retainLogs <= 0 {
		m.retainLogs = DefaultRetainLogs
	}
	fmt.Println("retainLogs=", m.retainLogs)

	missingDataPartitionInterval := cfg.GetString(missingDataPartitionInterval)
	if missingDataPartitionInterval != "" {
		if m.config.MissingDataPartitionInterval, err = strconv.ParseInt(missingDataPartitionInterval, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}

	dataPartitionTimeOutSec := cfg.GetString(dataPartitionTimeOutSec)
	if dataPartitionTimeOutSec != "" {
		if m.config.DataPartitionTimeOutSec, err = strconv.ParseInt(dataPartitionTimeOutSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}

	numberOfDataPartitionsToLoad := cfg.GetString(NumberOfDataPartitionsToLoad)
	if numberOfDataPartitionsToLoad != "" {
		if m.config.numberOfDataPartitionsToLoad, err = strconv.Atoi(numberOfDataPartitionsToLoad); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	if m.config.numberOfDataPartitionsToLoad <= 40 {
		m.config.numberOfDataPartitionsToLoad = 40
	}
	if secondsToFreeDP := cfg.GetString(secondsToFreeDataPartitionAfterLoad); secondsToFreeDP != "" {
		if m.config.secondsToFreeDataPartitionAfterLoad, err = strconv.ParseInt(secondsToFreeDP, 10, 64); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	m.tickInterval = int(cfg.GetFloat(cfgTickInterval))
	m.electionTick = int(cfg.GetFloat(cfgElectionTick))
	if m.tickInterval <= 300 {
		m.tickInterval = 500
	}
	if m.electionTick <= 3 {
		m.electionTick = 5
	}
	return
}

func (m *Server) createRaftServer() (err error) {
	raftCfg := &raftstore.Config{
		NodeID:            m.id,
		RaftPath:          m.walDir,
		NumOfLogsToRetain: m.retainLogs,
		HeartbeatPort:     int(m.config.heartbeatPort),
		ReplicaPort:       int(m.config.replicaPort),
		TickInterval:      m.tickInterval,
		ElectionTick:      m.electionTick,
	}
	if m.raftStore, err = raftstore.NewRaftStore(raftCfg); err != nil {
		return errors.Trace(err, "NewRaftStore failed! id[%v] walPath[%v]", m.id, m.walDir)
	}
	fmt.Printf("peers[%v],tickInterval[%v],electionTick[%v]\n", m.config.peers, m.tickInterval, m.electionTick)
	m.initFsm()
	partitionCfg := &raftstore.PartitionConfig{
		ID:    GroupID,
		Peers: m.config.peers,
		SM:    m.fsm,

		GetStartIndex: func(firstIndex, lastIndex uint64) (startIndex uint64) { return m.fsm.applied },
	}
	m.partition = m.raftStore.CreatePartition(partitionCfg)
	return
}
func (m *Server) initFsm() {
	m.fsm = newMetadataFsm(m.rocksDBStore, m.retainLogs, m.raftStore.RaftServer())
	m.fsm.registerLeaderChangeHandler(m.handleLeaderChange)
	m.fsm.registerPeerChangeHandler(m.handlePeerChange)

	// register the handlers for the interfaces defined in the Raft library
	m.fsm.registerApplySnapshotHandler(m.handleApplySnapshot)
	m.fsm.restore()
}

func (m *Server) initCluster() {
	m.cluster = newCluster(m.clusterName, m.leaderInfo, m.fsm, m.partition, m.config)
	m.cluster.retainLogs = m.retainLogs
}

func (m *Server) initUser() {
	m.user = newUser(m.fsm, m.partition)
}

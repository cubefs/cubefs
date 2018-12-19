// Copyright 2018 The Containerfs Authors.
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
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/ump"
	"net/http/httputil"
	"strconv"
	"sync"
)

//config keys
const (
	ClusterName       = "clusterName"
	ID                = "id"
	IP                = "ip"
	Port              = "port"
	LogLevel          = "logLevel"
	WalDir            = "walDir"
	StoreDir          = "storeDir"
	GroupId           = 1
	UmpModuleName     = "master"
	CfgRetainLogs     = "retainLogs"
	DefaultRetainLogs = 20000
)

type Master struct {
	id           uint64
	clusterName  string
	ip           string
	port         string
	walDir       string
	storeDir     string
	retainLogs   uint64
	leaderInfo   *LeaderInfo
	config       *clusterConfig
	cluster      *Cluster
	rocksDBStore *raftstore.RocksDBStore
	raftStore    raftstore.RaftStore
	fsm          *MetadataFsm
	partition    raftstore.Partition
	wg           sync.WaitGroup
	reverseProxy *httputil.ReverseProxy
}

func NewServer() *Master {
	return &Master{}
}

func (m *Master) Start(cfg *config.Config) (err error) {
	m.config = newClusterConfig()
	m.leaderInfo = &LeaderInfo{}
	m.reverseProxy = m.newReverseProxy()
	if err = m.checkConfig(cfg); err != nil {
		log.LogError(errors.ErrorStack(err))
		return
	}
	ump.InitUmp(fmt.Sprintf("%v_%v", m.clusterName, UmpModuleName))
	m.rocksDBStore = raftstore.NewRocksDBStore(m.storeDir)
	m.initFsm()
	m.initCluster()
	if err = m.createRaftServer(); err != nil {
		log.LogError(errors.ErrorStack(err))
		return
	}
	m.cluster.partition = m.partition
	m.cluster.idAlloc.partition = m.partition
	m.cluster.scheduleTask()
	m.startHttpService()
	m.wg.Add(1)
	return nil
}

func (m *Master) Shutdown() {
	m.wg.Done()
}

func (m *Master) Sync() {
	m.wg.Wait()
}

func (m *Master) checkConfig(cfg *config.Config) (err error) {
	m.clusterName = cfg.GetString(ClusterName)
	m.ip = cfg.GetString(IP)
	m.port = cfg.GetString(Port)
	vfDelayCheckCrcSec := cfg.GetString(fileDelayCheckCrc)
	dataPartitionMissSec := cfg.GetString(dataPartitionMissSec)
	dataPartitionTimeOutSec := cfg.GetString(dataPartitionTimeOutSec)
	everyLoadDataPartitionCount := cfg.GetString(everyLoadDataPartitionCount)
	replicaNum := cfg.GetString(replicaNum)
	m.walDir = cfg.GetString(WalDir)
	m.storeDir = cfg.GetString(StoreDir)
	peerAddrs := cfg.GetString(cfgPeers)
	if m.retainLogs, err = strconv.ParseUint(cfg.GetString(CfgRetainLogs), 10, 64); err != nil {
		return fmt.Errorf("%v,err:%v", errBadConfFile, err.Error())
	}
	if m.retainLogs <= 0 {
		m.retainLogs = DefaultRetainLogs
	}
	fmt.Println("retainLogs=", m.retainLogs)
	if err = m.config.parsePeers(peerAddrs); err != nil {
		return
	}

	if m.id, err = strconv.ParseUint(cfg.GetString(ID), 10, 64); err != nil {
		return fmt.Errorf("%v,err:%v", errBadConfFile, err.Error())
	}

	if m.ip == "" || m.port == "" || m.walDir == "" || m.storeDir == "" || m.clusterName == "" {
		return fmt.Errorf("%v,err:%v", errBadConfFile, "one of (ip,port,walDir,storeDir,clusterName) is null")
	}

	if replicaNum != "" {
		if m.config.replicaNum, err = strconv.Atoi(replicaNum); err != nil {
			return fmt.Errorf("%v,err:%v", errBadConfFile, err.Error())
		}

		if m.config.replicaNum > 10 {
			return fmt.Errorf("%v,replicaNum(%v) can't too large", errBadConfFile, m.config.replicaNum)
		}
	}

	if vfDelayCheckCrcSec != "" {
		if m.config.FileDelayCheckCrcSec, err = strconv.ParseInt(vfDelayCheckCrcSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", errBadConfFile, err.Error())
		}
	}

	if dataPartitionMissSec != "" {
		if m.config.DataPartitionMissSec, err = strconv.ParseInt(dataPartitionMissSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", errBadConfFile, err.Error())
		}
	}
	if dataPartitionTimeOutSec != "" {
		if m.config.DataPartitionTimeOutSec, err = strconv.ParseInt(dataPartitionTimeOutSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", errBadConfFile, err.Error())
		}
	}
	if everyLoadDataPartitionCount != "" {
		if m.config.everyLoadDataPartitionCount, err = strconv.Atoi(everyLoadDataPartitionCount); err != nil {
			return fmt.Errorf("%v,err:%v", errBadConfFile, err.Error())
		}
	}
	if m.config.everyLoadDataPartitionCount <= 40 {
		m.config.everyLoadDataPartitionCount = 40
	}

	return
}

func (m *Master) createRaftServer() (err error) {
	raftCfg := &raftstore.Config{NodeID: m.id, WalPath: m.walDir, RetainLogs: m.retainLogs}
	if m.raftStore, err = raftstore.NewRaftStore(raftCfg); err != nil {
		return errors.Annotatef(err, "NewRaftStore failed! id[%v] walPath[%v]", m.id, m.walDir)
	}
	fmt.Println(m.config.peers)
	partitionCfg := &raftstore.PartitionConfig{
		ID:      GroupId,
		Peers:   m.config.peers,
		Applied: m.fsm.applied,
		SM:      m.fsm,
	}
	if m.partition, err = m.raftStore.CreatePartition(partitionCfg); err != nil {
		return errors.Annotate(err, "CreatePartition failed")
	}
	return
}
func (m *Master) initFsm() {
	m.fsm = newMetadataFsm(m.rocksDBStore)
	m.fsm.registerLeaderChangeHandler(m.handleLeaderChange)
	m.fsm.registerPeerChangeHandler(m.handlePeerChange)
	m.fsm.registerApplyHandler(m.handleApply)
	m.fsm.registerApplySnapshotHandler(m.handleApplySnapshot)
	m.fsm.restore()
}

func (m *Master) initCluster() {
	m.cluster = newCluster(m.clusterName, m.leaderInfo, m.fsm, m.partition, m.config)
	m.cluster.retainLogs = m.retainLogs
	m.loadMetadata()
}

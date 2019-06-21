// Copyright 2018 The Chubao Authors.
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

package metanode

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"strconv"
)

var (
	clusterInfo    *proto.ClusterInfo
	masterHelper   util.MasterHelper
	configTotalMem uint64
)

// The MetaNode manages the dentry and inode information of the meta partitions on a meta node.
// The data consistency is ensured by Raft.
type MetaNode struct {
	nodeId            uint64
	listen            string
	metadataDir       string // root dir of the metaNode
	raftDir           string // root dir of the raftStore log
	metadataManager   MetadataManager
	localAddr         string
	clusterId         string
	raftStore         raftstore.RaftStore
	raftHeartbeatPort string
	raftReplicatePort string
	httpStopC         chan uint8
	state             uint32
	wg                sync.WaitGroup
}

// Start starts up the meta node with the specified configuration.
//  1. Start and load each meta partition from the snapshot.
//  2. Restore raftStore fsm of each meta node range.
//  3. Start server and accept connection from the master and clients.
func (m *MetaNode) Start(cfg *config.Config) (err error) {
	if atomic.CompareAndSwapUint32(&m.state, StateStandby, StateStart) {
		defer func() {
			var newState uint32
			if err != nil {
				newState = StateStandby
			} else {
				newState = StateRunning
			}
			atomic.StoreUint32(&m.state, newState)
		}()
		if err = m.onStart(cfg); err != nil {
			return
		}
		m.wg.Add(1)
	}
	return
}

// Shutdown stops the meta node.
func (m *MetaNode) Shutdown() {
	if atomic.CompareAndSwapUint32(&m.state, StateRunning, StateShutdown) {
		defer atomic.StoreUint32(&m.state, StateStopped)
		m.onShutdown()
		m.wg.Done()
	}
}

func (m *MetaNode) onStart(cfg *config.Config) (err error) {
	if err = m.parseConfig(cfg); err != nil {
		return
	}
	if err = m.register(); err != nil {
		return
	}
	if err = m.startRaftServer(); err != nil {
		return
	}
	if err = m.startMetaManager(); err != nil {
		return
	}
	if err = m.registerAPIHandler(); err != nil {
		return
	}
	if err = m.startServer(); err != nil {
		return
	}
	exporter.Init(m.clusterId, cfg.GetString("role"), cfg)
	return
}

func (m *MetaNode) onShutdown() {
	// shutdown node and release the resource
	m.stopServer()
	m.stopMetaManager()
	m.stopRaftServer()
}

// Sync blocks the invoker's goroutine until the meta node shuts down.
func (m *MetaNode) Sync() {
	if atomic.LoadUint32(&m.state) == StateRunning {
		m.wg.Wait()
	}
}

func (m *MetaNode) parseConfig(cfg *config.Config) (err error) {
	if cfg == nil {
		err = errors.New("invalid configuration")
		return
	}
	m.localAddr = cfg.GetString(cfgLocalIP)
	m.listen = cfg.GetString(cfgListen)
	m.metadataDir = cfg.GetString(cfgMetadataDir)
	m.raftDir = cfg.GetString(cfgRaftDir)
	m.raftHeartbeatPort = cfg.GetString(cfgRaftHeartbeatPort)
	m.raftReplicatePort = cfg.GetString(cfgRaftReplicaPort)
	configTotalMem, _ = strconv.ParseUint(cfg.GetString(cfgTotalMem), 10, 64)
	if configTotalMem != 0 && configTotalMem <= util.GB {
		configTotalMem = util.GB
	}

	total, _, err := util.GetMemInfo()
	if err == nil && configTotalMem == 0 {
		configTotalMem = total
	}

	if configTotalMem > total {
		configTotalMem = total
	}
	if m.metadataDir == "" {
		return fmt.Errorf("bad metadataDir config")
	}
	if m.listen == "" {
		return fmt.Errorf("bad listen config")
	}
	if m.raftDir == "" {
		return fmt.Errorf("bad raftDir config")
	}
	if m.raftHeartbeatPort == "" {
		return fmt.Errorf("bad raftHeartbeatPort config")
	}
	if m.raftReplicatePort == "" {
		return fmt.Errorf("bad cfgRaftReplicaPort config")
	}

	log.LogInfof("[parseConfig] load localAddr[%v].", m.localAddr)
	log.LogInfof("[parseConfig] load listen[%v].", m.listen)
	log.LogInfof("[parseConfig] load metadataDir[%v].", m.metadataDir)
	log.LogInfof("[parseConfig] load raftDir[%v].", m.raftDir)
	log.LogInfof("[parseConfig] load raftHeartbeatPort[%v].", m.raftHeartbeatPort)
	log.LogInfof("[parseConfig] load raftReplicatePort[%v].", m.raftReplicatePort)

	addrs := cfg.GetArray(cfgMasterAddrs)
	masterHelper = util.NewMasterHelper()
	for _, addr := range addrs {
		masterHelper.AddNode(addr.(string))
	}
	err = m.validConfig()
	return
}

func (m *MetaNode) validConfig() (err error) {
	if len(strings.TrimSpace(m.listen)) == 0 {
		err = errors.New("illegal listen")
		return
	}
	if m.metadataDir == "" {
		m.metadataDir = defaultMetadataDir
	}
	if m.raftDir == "" {
		m.raftDir = defaultRaftDir
	}
	if len(masterHelper.Nodes()) == 0 {
		err = errors.New("master address list is empty")
		return
	}
	return
}

func (m *MetaNode) startMetaManager() (err error) {
	if _, err = os.Stat(m.metadataDir); err != nil {
		if err = os.MkdirAll(m.metadataDir, 0755); err != nil {
			return
		}
	}
	// load metadataManager
	conf := MetadataManagerConfig{
		NodeID:    m.nodeId,
		RootDir:   m.metadataDir,
		RaftStore: m.raftStore,
	}
	m.metadataManager = NewMetadataManager(conf)
	if err = m.metadataManager.Start(); err == nil {
		log.LogInfof("[startMetaManager] manager start finish.")
	}
	return
}

func (m *MetaNode) stopMetaManager() {
	if m.metadataManager != nil {
		m.metadataManager.Stop()
	}
}

func (m *MetaNode) register() (err error) {
	step := 0
	reqParam := make(map[string]string)
	for {
		if step < 1 {
			clusterInfo, err = getClusterInfo()
			if err != nil {
				log.LogErrorf("[register] %s", err.Error())
				continue
			}
			if m.localAddr == "" {
				m.localAddr = clusterInfo.Ip
			}
			m.clusterId = clusterInfo.Cluster
			reqParam["addr"] = m.localAddr + ":" + m.listen
			step++
		}
		var respBody []byte
		respBody, err = masterHelper.Request("POST", proto.AddMetaNode, reqParam, nil)
		if err != nil {
			log.LogErrorf("[register] %s", err.Error())
			time.Sleep(3 * time.Second)
			continue
		}
		nodeIDStr := strings.TrimSpace(string(respBody))
		if nodeIDStr == "" {
			log.LogErrorf("[register] master respond empty body")
			time.Sleep(3 * time.Second)
			continue
		}
		m.nodeId, err = strconv.ParseUint(nodeIDStr, 10, 64)
		if err != nil {
			log.LogErrorf("[register] parse to nodeID: %s", err.Error())
			time.Sleep(3 * time.Second)
			continue
		}
		return
	}
}

// NewServer creates a new meta node instance.
func NewServer() *MetaNode {
	return &MetaNode{}
}

func getClusterInfo() (*proto.ClusterInfo, error) {
	respBody, err := masterHelper.Request("GET", proto.AdminGetIP, nil, nil)
	if err != nil {
		return nil, err
	}
	cInfo := &proto.ClusterInfo{}
	if err = json.Unmarshal(respBody, cInfo); err != nil {
		return nil, err
	}
	return cInfo, nil
}

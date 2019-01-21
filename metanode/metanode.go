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

package metanode

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/exporter"
	"github.com/tiglabs/containerfs/util/log"
	"strconv"
)

var (
	clusterInfo  *proto.ClusterInfo
	masterHelper util.MasterHelper
)

// The MetaNode manage Dentry and Inode information in multiple metaPartition, and
// through the RaftStore algorithm and other MetaNodes in the RageGroup for reliable
// data synchronization to maintain data consistency within the MetaGroup.
type MetaNode struct {
	nodeId            uint64
	listen            string
	metaDir           string //metaNode store root dir
	raftDir           string //raftStore log store base dir
	metaManager       MetaManager
	localAddr         string
	clusterId         string
	raftStore         raftstore.RaftStore
	raftHeartbeatPort string
	raftReplicatePort string
	httpStopC         chan uint8
	state             uint32
	wg                sync.WaitGroup
}

// Start this MeteNode with specified configuration.
//  1. Start and load each meta partition from snapshot.
//  2. Restore raftStore fsm of each meta range.
//  3. Start server and accept connection from master and clients.
func (m *MetaNode) Start(cfg *config.Config) (err error) {
	// Parallel safe.
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

// Shutdown stop this MetaNode.
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
	// Shutdown node and release resource.
	m.stopServer()
	m.stopMetaManager()
	m.stopRaftServer()
}

// Sync will block invoker goroutine until this MetaNode shutdown.
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
	m.listen = cfg.GetString(cfgListen)
	m.metaDir = cfg.GetString(cfgMetaDir)
	m.raftDir = cfg.GetString(cfgRaftDir)
	m.raftHeartbeatPort = cfg.GetString(cfgRaftHeartbeatPort)
	m.raftReplicatePort = cfg.GetString(cfgRaftReplicatePort)

	log.LogDebugf("action[parseConfig] load listen[%v].", m.listen)
	log.LogDebugf("action[parseConfig] load metaDir[%v].", m.metaDir)
	log.LogDebugf("action[parseConfig] load raftDir[%v].", m.raftDir)
	log.LogDebugf("action[parseConfig] load raftHeartbeatPort[%v].", m.raftHeartbeatPort)
	log.LogDebugf("action[parseConfig] load raftReplicatePort[%v].", m.raftReplicatePort)

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
	if m.metaDir == "" {
		m.metaDir = defaultMetaDir
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
	if _, err = os.Stat(m.metaDir); err != nil {
		if err = os.MkdirAll(m.metaDir, 0755); err != nil {
			return
		}
	}
	// Load metaManager
	conf := MetaManagerConfig{
		NodeID:    m.nodeId,
		RootDir:   m.metaDir,
		RaftStore: m.raftStore,
	}
	m.metaManager = NewMetaManager(conf)
	err = m.metaManager.Start()
	log.LogDebugf("[startMetaManager] manager start finish.")
	return
}

func (m *MetaNode) stopMetaManager() {
	if m.metaManager != nil {
		m.metaManager.Stop()
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
			m.localAddr = clusterInfo.Ip
			m.clusterId = clusterInfo.Cluster
			reqParam["addr"] = m.localAddr + ":" + m.listen
			step++
		}
		var (
			respBody []byte
			reply    = proto.HTTPReply{
				Data: &m.nodeId,
			}
		)
		respBody, err = masterHelper.Request("POST", proto.AddMetaNode, reqParam, nil)
		if err != nil {
			goto CONTINUE
		}
		if err = json.Unmarshal(respBody, &reply); err != nil {
			goto CONTINUE
		}
		if reply.Code != proto.ErrCodeSuccess {
			err = fmt.Errorf("[register] %s", reply.Msg)
			goto CONTINUE
		} else {
			goto END
		}
	CONTINUE:
		log.LogErrorf("[register] parse to nodeID: %s", err.Error())
		time.Sleep(3 * time.Second)
		continue
	END:
		return
	}
}

// NewServer create an new MetaNode instance.
func NewServer() *MetaNode {
	return &MetaNode{}
}

// 获取集群信息
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

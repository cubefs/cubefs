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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/ump"
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
	retryCount        int
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
	if err = m.startUMP(); err != nil {
		return
	}
	if err = m.registerHandler(); err != nil {
		return
	}
	if err = m.startServer(); err != nil {
		return
	}
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
	for _, addr := range addrs {
		masterAddrs = append(masterAddrs, addr.(string))
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
	if len(masterAddrs) == 0 {
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
	for {
		m.localAddr, err = getLocalIP()
		if err != nil {
			log.LogErrorf("[register] %s", err.Error())
			continue
		}
		err = m.postNodeID()
		if err != nil {
			log.LogErrorf("[register] %s", err.Error())
			time.Sleep(3 * time.Second)
			continue
		}
		return
	}
}

func (m *MetaNode) postNodeID() (err error) {
	reqPath := fmt.Sprintf("%s?addr=%s:%s", metaNodeURL, m.localAddr, m.listen)
	msg, err := postToMaster("POST", reqPath, nil)
	if err != nil {
		err = errors.Errorf("[postNodeID] %s", err.Error())
		return
	}
	nodeIDStr := strings.TrimSpace(string(msg))
	if nodeIDStr == "" {
		err = errors.Errorf("[postNodeID] master respond empty body")
		return
	}
	m.nodeId, err = strconv.ParseUint(nodeIDStr, 10, 64)
	return
}

func postToMaster(method string, reqPath string, body []byte) (msg []byte,
	err error) {
	var (
		req  *http.Request
		resp *http.Response
	)
	client := &http.Client{Timeout: 2 * time.Second}
	for _, maddr := range masterAddrs {
		if curMasterAddr == "" {
			curMasterAddr = maddr
		}
		reqURL := fmt.Sprintf("http://%s%s", curMasterAddr, reqPath)
		reqBody := bytes.NewBuffer(body)
		req, err = http.NewRequest(method, reqURL, reqBody)
		if err != nil {
			log.LogErrorf("[postToMaster] construction NewRequest url=%s: %s",
				reqURL, err.Error())
			curMasterAddr = ""
			continue
		}
		req.Header.Set("Connection", "close")
		resp, err = client.Do(req)
		if err != nil {
			log.LogErrorf("[postToMaster] connect master url=%s: %s",
				reqURL, err.Error())
			curMasterAddr = ""
			continue
		}
		msg, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.LogErrorf("[postToMaster] read body url=%s: %s",
				reqURL, err.Error())
			curMasterAddr = ""
			continue
		}
		if resp.StatusCode == http.StatusOK {
			return
		}
		if resp.StatusCode == http.StatusForbidden {
			curMasterAddr = strings.TrimSpace(string(msg))
			err = errors.Errorf("[postToMaster] master response ")
			continue
		}
		curMasterAddr = ""
		err = errors.Errorf("[postToMaster] master response url=%s,"+
			" status_code=%d, msg: %v", reqURL, resp.StatusCode, string(msg))
		log.LogErrorf(err.Error())
	}
	return
}

func (m *MetaNode) startUMP() (err error) {
	defaultTimeout := http.DefaultClient.Timeout
	defer func() {
		http.DefaultClient.Timeout = defaultTimeout
	}()
	// Get cluster name from master
	http.DefaultClient.Timeout = 2 * time.Second
	resp, err := http.Get(fmt.Sprintf("http://%s%s", curMasterAddr, metaNodeGetName))
	if err != nil {
		err = errors.Errorf("[startUMP]: %s", err.Error())
		return
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = errors.Errorf("[startUMP]: %s", err.Error())
		return
	}
	req := &proto.ClusterInfo{}
	if err = json.Unmarshal(data, req); err != nil {
		err = errors.Errorf("[startUMP]: %s", err.Error())
		return
	}
	UMPKey = req.Cluster + "_metaNode"
	ump.InitUmp(UMPKey)
	return
}

// NewServer create an new MetaNode instance.
func NewServer() *MetaNode {
	return &MetaNode{}
}

func getLocalIP() (string, error) {
	respBody, err := postToMaster("GET", "/admin/getIp", nil)
	if err != nil {
		return "", err
	}
	cInfo := proto.ClusterInfo{}
	if err = json.Unmarshal(respBody, &cInfo); err != nil {
		return "", err
	}
	return cInfo.Ip, nil
}

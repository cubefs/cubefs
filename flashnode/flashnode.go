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

package flashnode

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/cache_engine"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/memory"
	"github.com/cubefs/cubefs/util/statinfo"
	"github.com/cubefs/cubefs/util/statistics"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The FlashNode manages the inode block cache to speed the file reading.
type FlashNode struct {
	nodeId          uint64
	listen          string
	profPort        string
	cacheEngine     *cache_engine.CacheEngine
	localAddr       string
	clusterId       string
	tmpPath         string
	zoneName        string
	total           uint64
	monitorData     []*statistics.MonitorData
	stopTcpServerC  chan uint8
	processStatInfo *statinfo.ProcessStatInfo
	connPool        *connpool.ConnectPool
	readSource      *ReadSource
	stopCh          chan bool
	netListener     net.Listener
	currentCtx      context.Context
	statistics      sync.Map // volume(string) -> []*statistics.MonitorData
	control         common.Control
	sync.RWMutex
}

var (
	clusterInfo  *proto.ClusterInfo
	masterClient *masterSDK.MasterClient
)

// Start starts up the flash node with the specified configuration.
//  1. Start and load each flash partition from the snapshot.
//  2. Restore raftStore fsm of each flash node range.
//  3. Start server and accept connection from the master and clients.
func (f *FlashNode) Start(cfg *config.Config) (err error) {
	return f.control.Start(f, cfg, doStart)
}

// Shutdown stops the flash node.
func (f *FlashNode) Shutdown() {
	f.control.Shutdown(f, doShutdown)
}

// Sync blocks the invoker's goroutine until the flash node shuts down.
func (f *FlashNode) Sync() {
	f.control.Sync()
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	f, ok := s.(*FlashNode)
	if !ok {
		return errors.New("Invalid Node Type!")
	}
	if err = f.parseConfig(cfg); err != nil {
		return
	}
	f.stopCh = make(chan bool, 0)
	f.tmpPath = TmpfsPath
	if err = f.register(); err != nil {
		return
	}
	f.connPool = connpool.NewConnectPoolWithTimeout(ConnectPoolIdleConnTimeoutSec, CacheReqConnectionTimeoutMilliSec)
	if err = f.registerAPIHandler(); err != nil {
		return
	}
	go f.startUpdateNodeInfo()

	exporter.Init(f.clusterId, moduleName, f.zoneName, cfg)
	if err = f.startTcpServer(); err != nil {
		return
	}
	f.readSource = NewReadSource()
	go f.contextMaker()
	if err = f.startCacheEngine(); err != nil {
		return
	}
	statistics.InitStatistics(cfg, f.clusterId, statistics.ModelFlashNode, f.localAddr, f.reportSummary)
	return
}

func doShutdown(s common.Server) {
	f, ok := s.(*FlashNode)
	if !ok {
		return
	}
	close(f.stopCh)
	// shutdown node and release the resource
	f.stopServer()
	f.stopCacheEngine()
}

func (f *FlashNode) parseConfig(cfg *config.Config) (err error) {
	if cfg == nil {
		err = errors.New("invalid configuration")
		return
	}
	f.localAddr = cfg.GetString(cfgLocalIP)
	f.listen = cfg.GetString(cfgListen)
	f.profPort = cfg.GetString(cfgProfPort)
	f.zoneName = cfg.GetString(cfgZoneName)
	f.total, err = strconv.ParseUint(cfg.GetString(cfgTotalMem), 10, 64)
	if err != nil {
		return err
	}
	if f.total == 0 {
		return fmt.Errorf("bad totalMem config,Recommended to be configured as 80 percent of physical machine memory")
	}
	total, _, err := memory.GetMemInfo()
	if err == nil && f.total > uint64(float64(total)*0.8) {
		return fmt.Errorf("bad totalMem config,Recommended to be configured as 80 percent of physical machine memory")
	}
	if f.listen == "" {
		return fmt.Errorf("bad listen config")
	}
	log.LogInfof("[parseConfig] load localAddr[%v].", f.localAddr)
	log.LogInfof("[parseConfig] load listen[%v].", f.listen)
	log.LogInfof("[parseConfig] load zoneName[%v].", f.zoneName)

	addrs := cfg.GetSlice(cfgMasterAddr)
	masters := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		masters = append(masters, addr.(string))
	}
	masterClient = masterSDK.NewMasterClient(masters, false)
	err = f.validConfig()
	return
}

func (f *FlashNode) validConfig() (err error) {
	if len(strings.TrimSpace(f.listen)) == 0 {
		err = errors.New("illegal listen")
		return
	}
	if len(masterClient.Nodes()) == 0 {
		err = errors.New("master address list is empty")
		return
	}
	return
}

func (f *FlashNode) stopCacheEngine() {
	if f.cacheEngine != nil {
		err := f.cacheEngine.Stop()
		if err != nil {
			log.LogErrorf("stopCacheEngine: err:%v", err)
		}
	}
}

func (f *FlashNode) startCacheEngine() (err error) {
	if f.cacheEngine, err = cache_engine.NewCacheEngine(f.tmpPath, int64(f.total), cache_engine.DefaultCacheMaxUsedRatio, LruCacheDefaultCapacity, time.Hour, f.readSource.ReadExtentData, f.recordMonitorAction); err != nil {
		log.LogErrorf("start CacheEngine failed: %v", err)
		return
	}
	f.cacheEngine.Start()
	return
}

func (f *FlashNode) register() (err error) {
	step := 0
	var nodeAddress string
	for {
		if step < 1 {
			clusterInfo, err = masterClient.AdminAPI().GetClusterInfo()
			if err != nil {
				log.LogErrorf("[register] %s", err.Error())
				continue
			}
			if f.localAddr == "" {
				f.localAddr = clusterInfo.Ip
			}
			f.clusterId = clusterInfo.Cluster
			nodeAddress = f.localAddr + ":" + f.listen
			step++
		}
		var nodeID uint64
		if nodeID, err = masterClient.NodeAPI().AddFlashNode(nodeAddress, f.zoneName, NodeLatestVersion); err != nil {
			log.LogErrorf("register: register to master fail: address(%v) err(%s)", nodeAddress, err)
			time.Sleep(3 * time.Second)
			continue
		}
		f.nodeId = nodeID
		return
	}
}

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
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/flashnode/cachengine"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
)

//TODO: remove this later.
//go:generate golangci-lint run --issues-exit-code=1 -D errcheck -E bodyclose ./...

const (
	TmpfsPath = "/cfs/tmpfs"

	moduleName = "flashNode"
	volumePara = "volume"

	LruCacheDefaultCapacity     = 400000
	UpdateRateLimitInfoInterval = 60 * time.Second
	ServerTimeout               = 60 * 5
	ConnectPoolIdleConnTimeout  = 60 * time.Second
	DefaultBurst                = 512

	ExtentReadMaxRetry      = 3
	ExtentReadTimeoutSec    = 3
	ExtentReadSleepInterval = 100 * time.Millisecond
	IdleConnTimeoutData     = 30 * time.Second
)

// Configuration keys
const (
	cfgLocalIP  = "localIP"
	cfgTotalMem = "totalMem"
	cfgZoneName = "zoneName"
)

// The FlashNode manages the inode block cache to speed the file reading.
type FlashNode struct {
	// from configuration
	listen   string
	zoneName string
	total    uint64
	tmpPath  string // const with TmpfsPath
	mc       *master.MasterClient

	// load from master
	localAddr string
	clusterID string
	nodeID    uint64

	nodeLimit     uint64
	nodeLimiter   *rate.Limiter
	volLimitMap   map[string]uint64        // volume -> limit
	volLimiterMap map[string]*rate.Limiter // volume -> *Limiter

	control  common.Control
	stopOnce sync.Once
	stopCh   chan struct{}

	connPool    *util.ConnectPool
	netListener net.Listener
	readSource  *ReadSource
	cacheEngine *cachengine.CacheEngine
}

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
	f.stopCh = make(chan struct{})
	f.tmpPath = TmpfsPath
	if err = f.register(); err != nil {
		return
	}
	f.initLimiter()
	f.connPool = util.NewConnectPoolWithTimeout(ConnectPoolIdleConnTimeout, 1)
	f.registerAPIHandler()
	go f.startUpdateScheduler()

	if err = f.startTcpServer(); err != nil {
		return
	}
	f.readSource = NewReadSource()
	if err = f.startCacheEngine(); err != nil {
		return
	}

	exporter.Init(moduleName, cfg)
	exporter.RegistConsul(f.clusterID, moduleName, cfg)
	return
}

func doShutdown(s common.Server) {
	f, ok := s.(*FlashNode)
	if !ok {
		return
	}
	f.stopOnce.Do(func() {
		close(f.stopCh)
	})
	// shutdown node and release the resource
	f.stopServer()
	f.stopCacheEngine()
}

func (f *FlashNode) parseConfig(cfg *config.Config) (err error) {
	if cfg == nil {
		return errors.New("invalid configuration")
	}
	f.localAddr = cfg.GetString(cfgLocalIP)
	f.listen = strings.TrimSpace(cfg.GetString(proto.ListenPort))
	if f.listen == "" {
		return errors.New("bad listen config")
	}
	f.zoneName = cfg.GetString(cfgZoneName)
	f.total, err = strconv.ParseUint(cfg.GetString(cfgTotalMem), 10, 64)
	if err != nil {
		return err
	}
	if f.total == 0 {
		return errors.New("recommended to be configured as 80 percent of physical machine memory")
	}
	total, _, err := util.GetMemInfo()
	if err == nil && f.total > uint64(float64(total)*0.8) {
		return errors.New("recommended to be configured as 80 percent of physical machine memory")
	}
	log.LogInfof("[parseConfig] load localAddr[%v].", f.localAddr)
	log.LogInfof("[parseConfig] load listen[%v].", f.listen)
	log.LogInfof("[parseConfig] load zoneName[%v].", f.zoneName)

	f.mc = master.NewMasterClient(cfg.GetStringSlice(proto.MasterAddr), false)
	if len(f.mc.Nodes()) == 0 {
		return errors.New("master addresses is empty")
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
	if f.cacheEngine, err = cachengine.NewCacheEngine(f.tmpPath, int64(f.total), cachengine.DefaultCacheMaxUsedRatio, LruCacheDefaultCapacity, time.Hour, f.readSource.ReadExtentData); err != nil {
		log.LogErrorf("start CacheEngine failed: %v", err)
		return
	}
	f.cacheEngine.Start()
	return
}

func (f *FlashNode) initLimiter() {
	f.nodeLimit = 0
	f.nodeLimiter = rate.NewLimiter(rate.Inf, DefaultBurst)
	f.volLimitMap = make(map[string]uint64)
	f.volLimiterMap = make(map[string]*rate.Limiter)
}

func (f *FlashNode) register() (err error) {
	// TODO: xxx
	// var (
	// 	regInfo = &master.RegNodeInfoReq{
	// 		Role:     proto.RoleFlash,
	// 		ZoneName: f.zoneName,
	// 		Version:  NodeLatestVersion,
	// 		SrvPort:  f.listen,
	// 	}
	// 	regRsp *proto.RegNodeRsp
	// )

	// if regRsp, err = masterClient.RegNodeInfo(proto.AuthFilePath, regInfo); err != nil {
	// 	return
	// }

	// f.nodeID= regRsp.Id
	// ipAddr := strings.Split(regRsp.Addr, ":")[0]
	// f.localAddr = ipAddr
	// f.clusterID = regRsp.Cluster

	f.nodeID = 0
	return
}

// Copyright 2023 The CubeFS Authors.
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
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/flashnode/cachengine"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

// TODO: remove this later.
//go:generate golangci-lint run --issues-exit-code=1 -D errcheck -E bodyclose ./...

const (
	DefaultMemDataPath = "/cfs/tmpfs"

	moduleName = "flashNode"

	_defaultReadBurst                   = 512
	_defaultLRUCapacity                 = 400000
	_defaultLRUFhCapacity               = 10000
	_defaultDiskUnavailableCbErrorCount = 3
	_defaultCacheLoadWorkerNum          = 8
	_tcpServerTimeoutSec                = 60 * 5
	_connPoolIdleTimeout                = 60 // 60s
	_extentReadMaxRetry                 = 3
	_extentReadTimeoutSec               = 3
)

// Configuration keys
const (
	LogDir                         = "logDir"
	Stat                           = "stat"
	cfgMemTotal                    = "memTotal"
	cfgCachePercent                = "cachePercent"
	cfgLruCapacity                 = "lruCapacity"
	cfgLruFhCapacity               = "lruFileHandleCapacity"
	cfgDiskUnavailableCbErrorCount = "diskUnavailableCbErrorCount"
	cfgCacheLoadWorkerNum          = "cacheLoadWorkerNum"
	cfgZoneName                    = "zoneName"
	cfgReadRps                     = "readRps"
	cfgLowerHitRate                = "lowerHitRate"
	cfgDisableTmpfs                = "disableTmpfs"
	cfgMemDataPath                 = "memDataPath"
	cfgDiskDataPath                = "diskDataPath"
)

// The FlashNode manages the inode block cache to speed the file reading.
type FlashNode struct {
	// from configuration
	logDir                      string
	listen                      string
	zoneName                    string
	memTotal                    uint64
	lruCapacity                 int
	lruFhCapacity               int // file handle capacity
	diskUnavailableCbErrorCount int64
	cacheLoadWorkerNum          int
	memDataPath                 string
	disks                       []*cachengine.Disk
	mc                          *master.MasterClient

	// load from master
	localAddr string
	clusterID string
	nodeID    uint64

	control  common.Control
	stopOnce sync.Once
	stopCh   chan struct{}

	connPool    *util.ConnectPool
	tcpListener net.Listener
	cacheEngine *cachengine.CacheEngine

	readRps      int
	readLimiter  *rate.Limiter
	lowerHitRate float64
	enableTmpfs  bool
	metrics      *FlashNodeMetrics

	handleReadTimeout int
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
	if err = f.start(cfg); err != nil {
		return
	}
	f.registerMetrics()
	exporter.RegistConsul(f.clusterID, moduleName, cfg)
	f.startMetrics()
	return
}

func doShutdown(s common.Server) {
	f, ok := s.(*FlashNode)
	if !ok {
		return
	}
	f.shutdown()
}

func (f *FlashNode) start(cfg *config.Config) (err error) {
	if err = f.parseConfig(cfg); err != nil {
		return
	}
	f.stopCh = make(chan struct{})
	if err = f.register(); err != nil {
		return
	}
	f.initLimiter()
	initExtentConnPool()
	f.connPool = util.NewConnectPoolWithTimeout(_connPoolIdleTimeout, 1)
	f.registerAPIHandler()

	if err = f.startCacheEngine(); err != nil {
		return
	}
	if err = f.startTcpServer(); err != nil {
		return
	}

	_, err = stat.NewStatistic(f.logDir, Stat, int64(stat.DefaultStatLogSize),
		stat.DefaultTimeOutUs, true)
	if err != nil {
		return
	}

	return nil
}

func (f *FlashNode) shutdown() {
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
	f.logDir = cfg.GetString(LogDir)
	f.listen = strings.TrimSpace(cfg.GetString(proto.ListenPort))
	if f.listen == "" {
		return errors.New("bad listen config")
	}
	if f.zoneName = cfg.GetString(cfgZoneName); f.zoneName == "" {
		return errors.New("bad zoneName config")
	}
	f.readRps = cfg.GetInt(cfgReadRps)
	if f.readRps < 0 {
		f.readRps = _defaultReadBurst
	}

	f.enableTmpfs = !cfg.GetBool(cfgDisableTmpfs)
	percent := cfg.GetFloat(cfgCachePercent)
	if percent <= 1e-2 || percent > 1.0 {
		percent = 1.0
	}
	lruCapacity := cfg.GetInt(cfgLruCapacity)
	if lruCapacity <= 0 {
		lruCapacity = _defaultLRUCapacity
	}
	f.lruCapacity = lruCapacity
	if f.enableTmpfs {
		f.memDataPath = cfg.GetString(cfgMemDataPath)
		if f.memDataPath == "" {
			f.memDataPath = DefaultMemDataPath
		}
		if err = os.MkdirAll(f.memDataPath, 0o755); err != nil {
			return errors.NewErrorf("mkdir cache directory [%v] err[%v]", f.memDataPath, err)
		}
		memTotal := cfg.GetInt64(cfgMemTotal)
		if memTotal <= 0 {
			total, _, err := util.GetMemInfo()
			if err != nil {
				return errors.NewErrorf("get physical memory %v", err)
			}
			memTotal = int64(float64(total) * percent)
		}
		if memTotal < 32*(1<<20) {
			return errors.NewErrorf("low physical cacheSpace %d", memTotal)
		}
		f.memTotal = uint64(memTotal)
	} else {
		disks := make([]*cachengine.Disk, 0)
		allDiskSpace := int64(0)
		for _, p := range cfg.GetSlice(cfgDiskDataPath) {
			arr := strings.Split(p.(string), ":")
			if len(arr) != 2 {
				return errors.New("invalid disk configuration. Example: PATH:MAX_USED_SIZE")
			}
			path := arr[0]
			if _, err = os.Stat(path); err != nil {
				if !os.IsNotExist(err.(*os.PathError)) {
					return errors.NewErrorf("stat cache directory failed: %s", err.Error())
				}
				if err = os.MkdirAll(path, 0o755); err != nil {
					return errors.NewErrorf("mkdir cache directory [%v] err[%v]", path, err)
				}
			}
			totalSpace, err := strconv.ParseInt(arr[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid disk total space. Error: %s", err.Error())
			}

			if totalSpace <= 0 {
				stat := syscall.Statfs_t{}
				err := syscall.Statfs(path, &stat)
				if err != nil {
					return errors.NewErrorf("get disk size, err:%v", err)
				}
				total := int64(stat.Blocks) * int64(stat.Bsize)
				totalSpace = int64(float64(total) * percent)
			}
			if totalSpace < 32*(1<<20) {
				return errors.NewErrorf("low physical cacheSpace %d", totalSpace)
			}
			allDiskSpace += totalSpace
			disk := new(cachengine.Disk)
			disk.TotalSpace = totalSpace
			disk.Path = path
			disks = append(disks, disk)
		}
		if len(disks) < 1 {
			return errors.NewErrorf("the number of disks configured is less than 1")
		}
		for _, disk := range disks {
			disk.Capacity = int(float64(disk.TotalSpace) / float64(allDiskSpace) * float64(f.lruCapacity))
		}
		f.disks = disks
	}

	lruFhCapacity := cfg.GetInt(cfgLruFhCapacity)
	if lruFhCapacity <= 0 || lruFhCapacity >= 1000000 {
		lruFhCapacity = _defaultLRUFhCapacity
	}
	f.lruFhCapacity = lruFhCapacity
	diskUnavailableCbErrorCount := cfg.GetInt64(cfgDiskUnavailableCbErrorCount)
	if diskUnavailableCbErrorCount <= 0 || diskUnavailableCbErrorCount > 100 {
		diskUnavailableCbErrorCount = _defaultDiskUnavailableCbErrorCount
	}
	f.diskUnavailableCbErrorCount = diskUnavailableCbErrorCount
	cacheLoadWorkerNum := cfg.GetInt(cfgCacheLoadWorkerNum)
	if cacheLoadWorkerNum <= 0 || cacheLoadWorkerNum > 100 {
		cacheLoadWorkerNum = _defaultCacheLoadWorkerNum
	}
	f.cacheLoadWorkerNum = cacheLoadWorkerNum
	f.lowerHitRate = cfg.GetFloat(cfgLowerHitRate)

	log.LogInfof("[parseConfig] load listen[%s].", f.listen)
	log.LogInfof("[parseConfig] load zoneName[%s].", f.zoneName)
	log.LogInfof("[parseConfig] load totalMem[%d].", f.memTotal)
	log.LogInfof("[parseConfig] load lruCapacity[%d].", f.lruCapacity)
	log.LogInfof("[parseConfig] load lruFileHandleCapacity[%d]", f.lruFhCapacity)
	log.LogInfof("[parseConfig] load diskUnavailableCbErrorCount[%d]", f.diskUnavailableCbErrorCount)
	log.LogInfof("[parseConfig] load cacheLoadWorkerNum[%d]", f.cacheLoadWorkerNum)
	log.LogInfof("[parseConfig] load  readRps[%d].", f.readRps)
	log.LogInfof("[parseConfig] load  lowerHitRate[%.2f].", f.lowerHitRate)
	log.LogInfof("[parseConfig] load  enableTmpfs[%v].", f.enableTmpfs)
	log.LogInfof("[parseConfig] load  memDataPath[%v].", f.memDataPath)
	for _, d := range f.disks {
		log.LogInfof("[parseConfig] load diskDataPath[%v] totalSize[%d] capacity[%d]", d.Path, d.TotalSpace, d.Capacity)
	}

	f.mc = master.NewMasterClient(cfg.GetStringSlice(proto.MasterAddr), false)
	if len(f.mc.Nodes()) == 0 {
		return errors.New("master addresses is empty")
	}
	return
}

func (f *FlashNode) stopCacheEngine() {
	if f.cacheEngine != nil {
		if err := f.cacheEngine.Stop(); err != nil {
			log.LogErrorf("stopCacheEngine err:%v", err)
		}
	}
}

func (f *FlashNode) startCacheEngine() (err error) {
	if f.cacheEngine, err = cachengine.NewCacheEngine(f.memDataPath, int64(f.memTotal),
		0, f.disks, f.lruCapacity, f.lruFhCapacity, f.diskUnavailableCbErrorCount, f.cacheLoadWorkerNum, f.mc, time.Hour, ReadExtentData, f.enableTmpfs, f.localAddr); err != nil {
		log.LogErrorf("startCacheEngine failed:%v", err)
		return
	}
	f.SetTimeout(_extentReadTimeoutSec, _extentReadTimeoutSec)
	return f.cacheEngine.Start()
}

func (f *FlashNode) initLimiter() {
	f.readLimiter = rate.NewLimiter(rate.Limit(f.readRps), 2*f.readRps)
}

func (f *FlashNode) register() error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		log.LogInfo("to register flashnode")
		for {
			ci, err := f.mc.AdminAPI().GetClusterInfo()
			if err != nil {
				log.LogErrorf("action[register] cannot get ip from master err(%v)", err)
				break
			}

			localIP := ci.Ip
			if !util.IsIPV4(localIP) {
				log.LogErrorf("action[register] got an invalid local ip(%s) from master", localIP)
				break
			}
			f.clusterID = ci.Cluster
			f.localAddr = fmt.Sprintf("%s:%v", localIP, f.listen)

			nodeID, err := f.mc.NodeAPI().AddFlashNode(f.localAddr, f.zoneName, "")
			if err != nil {
				log.LogErrorf("action[register] cannot register flashnode to master err(%v).", err)
				break
			}
			f.nodeID = nodeID
			log.LogInfof("action[register] flashnode(%d) cluster(%s) localAddr(%s)", f.nodeID, f.clusterID, f.localAddr)
			return nil
		}

		select {
		case <-ticker.C:
		case <-f.stopCh:
			return fmt.Errorf("stopped")
		}
	}
}

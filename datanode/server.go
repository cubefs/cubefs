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

package datanode

import (
	"bytes"
	"errors"
	"fmt"
	syslog "log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/datanode/repl"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/loadutil"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/strutil"

	"github.com/xtaci/smux"
)

var (
	ErrIncorrectStoreType          = errors.New("Incorrect store type")
	ErrNoSpaceToCreatePartition    = errors.New("No disk space to create a data partition")
	ErrNewSpaceManagerFailed       = errors.New("Creater new space manager failed")
	ErrGetMasterDatanodeInfoFailed = errors.New("Failed to get datanode info from master")

	LocalIP   string
	gConnPool = util.NewConnectPool()
	// MasterClient        = masterSDK.NewMasterClient(nil, false)
	MasterClient *masterSDK.MasterCLientWithResolver
)

const (
	DefaultZoneName            = proto.DefaultZoneName
	DefaultRaftDir             = "raft"
	DefaultRaftLogsToRetain    = 10 // Count of raft logs per data partition
	DefaultDiskMaxErr          = 1
	DefaultDiskRetainMin       = 5 * util.GB // GB
	DefaultNameResolveInterval = 1           // minutes

	DefaultDiskUnavailableErrorCount          = 5
	DefaultDiskUnavailablePartitionErrorCount = 3
	DefaultGOGCValue                          = 100
	DefaultExtentCacheTtlByMin                = 30
)

const (
	ModuleName = "dataNode"
)

const (
	ConfigKeyLocalIP       = "localIP"         // string
	ConfigKeyPort          = "port"            // int
	ConfigKeyMasterAddr    = "masterAddr"      // array
	ConfigKeyZone          = "zoneName"        // string
	ConfigKeyDisks         = "disks"           // array
	ConfigKeyRaftDir       = "raftDir"         // string
	ConfigKeyRaftHeartbeat = "raftHeartbeat"   // string
	ConfigKeyRaftReplica   = "raftReplica"     // string
	CfgTickInterval        = "tickInterval"    // int
	CfgRaftRecvBufSize     = "raftRecvBufSize" // int
	ConfigKeyLogDir        = "logDir"          // string

	ConfigKeyDiskPath         = "diskPath"            // string
	configNameResolveInterval = "nameResolveInterval" // int

	/*
	 * Metrics Degrade Level
	 * minus value: turn off metrics collection.
	 * 0 or 1: full metrics.
	 * 2: 1/2 of the metrics will be collected.
	 * 3: 1/3 of the metrics will be collected.
	 * ...
	 */
	CfgMetricsDegrade = "metricsDegrade" // int

	CfgDiskRdonlySpace = "diskRdonlySpace" // int
	// smux Config
	ConfigKeyEnableSmuxClient  = "enableSmuxConnPool" // bool
	ConfigKeySmuxPortShift     = "smuxPortShift"      // int
	ConfigKeySmuxMaxConn       = "smuxMaxConn"        // int
	ConfigKeySmuxStreamPerConn = "smuxStreamPerConn"  // int
	ConfigKeySmuxMaxBuffer     = "smuxMaxBuffer"      // int
	ConfigKeySmuxTotalStream   = "sumxTotalStream"    // int

	// rate limit control enable
	ConfigDiskQosEnable      = "diskQosEnable"      // bool
	ConfigDiskAsyncQosEnable = "diskAsyncQosEnable" // bool
	ConfigDiskReadIocc       = "diskReadIocc"       // int
	ConfigDiskReadIops       = "diskReadIops"       // int
	ConfigDiskReadFlow       = "diskReadFlow"       // int
	ConfigDiskWriteIocc      = "diskWriteIocc"      // int
	ConfigDiskWriteIops      = "diskWriteIops"      // int
	ConfigDiskWriteFlow      = "diskWriteFlow"      // int
	ConfigDiskWQueFactor     = "diskWQueFactor"     // int
	ConfigDiskAsyncReadIocc  = "diskAsyncReadIocc"  // int
	ConfigDiskAsyncReadIops  = "diskAsyncReadIops"  // int
	ConfigDiskAsyncReadFlow  = "diskAsyncReadFlow"  // int
	ConfigDiskAsyncWriteIocc = "diskAsyncWriteIocc" // int
	ConfigDiskAsyncWriteIops = "diskAsyncWriteIops" // int
	ConfigDiskAsyncWriteFlow = "diskAsyncWriteFlow" // int
	ConfigDiskDeleteIocc     = "diskDeleteIocc"     // int
	ConfigDiskDeleteIops     = "diskDeleteIops"     // int
	ConfigDiskDeleteFlow     = "diskDeleteFlow"     // int

	// load/stop dp limit
	ConfigDiskCurrentLoadDpLimit = "diskCurrentLoadDpLimit"
	ConfigDiskCurrentStopDpLimit = "diskCurrentStopDpLimit"
	// disk read extent limit
	ConfigEnableDiskReadExtentLimit = "enableDiskReadRepairExtentLimit" // bool

	ConfigServiceIDKey = "serviceIDKey"

	ConfigEnableGcTimer    = "enableGcTimer"
	ConfigGcRecyclePercent = "gcRecyclePercent"

	// disk status becomes unavailable if disk error partition count reaches this value
	ConfigKeyDiskUnavailablePartitionErrorCount = "diskUnavailablePartitionErrorCount"
	ConfigKeyCacheCap                           = "cacheCap"
	ConfigExtentCacheTtlByMin                   = "extentCacheTtlByMin"

	// storage device media type, for hybrid cloud, in string: SDD or HDD
	ConfigMediaType = "mediaType"
)

const cpuSampleDuration = 1 * time.Second

const (
	gcTimerDuration         = 10 * time.Second
	defaultGcRecyclePercent = 0.90
)

// DataNode defines the structure of a data node.
type DataNode struct {
	space                              *SpaceManager
	port                               string
	zoneName                           string
	clusterID                          string
	bindIp                             bool
	localServerAddr                    string
	nodeID                             uint64
	raftPartitionCanUsingDifferentPort bool
	raftDir                            string
	raftHeartbeat                      string
	raftReplica                        string
	raftStore                          raftstore.RaftStore
	tickInterval                       int
	raftRecvBufSize                    int
	startTime                          int64
	// localIP         string

	tcpListener net.Listener
	stopC       chan bool

	smuxPortShift      int
	enableSmuxConnPool bool
	smuxConnPool       *util.SmuxConnectPool
	smuxListener       net.Listener
	smuxServerConfig   *smux.Config
	smuxConnPoolConfig *util.SmuxConnPoolConfig

	getRepairConnFunc func(target string) (net.Conn, error)
	putRepairConnFunc func(conn net.Conn, forceClose bool)

	metrics        *DataNodeMetrics
	metricsDegrade int64
	metricsCnt     uint64
	volUpdating    sync.Map // map[string]*verOp2Phase

	control common.Control

	diskQosEnable           bool
	diskQosEnableFromMaster bool
	diskAsyncQosEnable      bool
	diskReadIocc            int
	diskReadIops            int
	diskReadFlow            int
	diskWriteIocc           int
	diskWriteIops           int
	diskWriteFlow           int
	diskAsyncReadIocc       int
	diskAsyncReadIops       int
	diskAsyncReadFlow       int
	diskAsyncWriteIocc      int
	diskAsyncWriteIops      int
	diskAsyncWriteFlow      int
	diskDeleteIocc          int
	diskDeleteIops          int
	diskDeleteFlow          int
	diskWQueFactor          int
	dpMaxRepairErrCnt       uint64
	clusterUuid             string
	clusterUuidEnable       bool
	clusterEnableSnapshot   bool

	serviceIDKey   string
	cpuUtil        atomicutil.Float64
	cpuSamplerDone chan struct{}

	useLocalGOGC bool
	gogcValue    int

	enableGcTimer    bool
	gcRecyclePercent float64
	gcTimer          *util.RecycleTimer

	diskUnavailablePartitionErrorCount uint64 // disk status becomes unavailable when disk error partition count reaches this value
	started                            int32
	dpBackupTimeout                    time.Duration
	cacheCap                           int
	mediaType                          uint32              // type of storage hardware medi
	nodeForbidWriteOpOfProtoVer0       bool                // whether forbid by node granularity,
	VolsForbidWriteOpOfProtoVer0       map[string]struct{} // whether forbid by volume granularity,
	DirectReadVols                     map[string]struct{}
	IgnoreTinyRecoverVols              map[string]struct{}
	ExtentCacheTtlByMin                int
}

type verOp2Phase struct {
	verSeq     uint64
	verPrepare uint64
	status     uint32
	step       uint32
	// op         uint8
	sync.Mutex
}

func NewServer() *DataNode {
	return &DataNode{}
}

func (s *DataNode) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	return s.control.Start(s, cfg, doStart)
}

// Shutdown shuts down the current data node.
func (s *DataNode) Shutdown() {
	s.control.Shutdown(s, doShutdown)
}

// Sync keeps data node in sync.
func (s *DataNode) Sync() {
	s.control.Sync()
}

// Workflow of starting up a data node.
func doStart(server common.Server, cfg *config.Config) (err error) {
	s, ok := server.(*DataNode)
	if !ok {
		return errors.New("Invalid node Type!")
	}
	s.stopC = make(chan bool)
	s.gogcValue = DefaultGOGCValue
	// parse the config file
	if err = s.parseConfig(cfg); err != nil {
		return
	}
	if err = s.parseRaftConfig(cfg); err != nil {
		return
	}

	s.registerMetrics()

	if err = s.register(cfg); err != nil {
		return
	}

	// parse the smux config
	if err = s.parseSmuxConfig(cfg); err != nil {
		return
	}

	s.startStat(cfg)

	// connection pool must be created before initSpaceManager
	s.initConnPool()

	// init limit
	initRepairLimit()

	// start the raft server
	if err = s.startRaftServer(cfg); err != nil {
		return
	}

	if err = s.newSpaceManager(cfg); err != nil {
		return
	}

	// tcp listening & tcp connection pool
	if err = s.startTCPService(); err != nil {
		return
	}

	// smux listening & smux connection pool
	if err = s.startSmuxService(cfg); err != nil {
		return
	}

	// create space manager (disk, partition, etc.)
	if err = s.startSpaceManager(cfg); err != nil {
		return
	}

	// check local partition compare with master ,if lack,then not start
	if _, err = s.checkLocalPartitionMatchWithMaster(); err != nil {
		log.LogError(err)
		exporter.Warning(err.Error())
		return
	}

	go s.registerHandler()

	s.scheduleTask()

	// start metrics (LackDpCount, etc.)
	s.startMetrics()

	// start cpu sampler
	s.startCpuSample()

	s.startGcTimer()

	s.setStart()

	return
}

func (s *DataNode) setStart() {
	atomic.StoreInt32(&s.started, statusStarted)
	log.LogWarnf("setStart: datanode start success, set start status")
}

func (s *DataNode) setStop() {
	atomic.StoreInt32(&s.started, statusStopped)
	log.LogWarnf("setStop: datanode start stop, set stop status")
}

func (s *DataNode) HasStarted() bool {
	return atomic.LoadInt32(&s.started) == statusStarted
}

func doShutdown(server common.Server) {
	begin := time.Now()
	defer func() {
		msg := fmt.Sprintf("[doShutdown] stop datanode using time(%v)", time.Since(begin))
		log.LogInfo(msg)
		syslog.Print(msg)
	}()
	s, ok := server.(*DataNode)
	if !ok {
		return
	}
	s.setStop()
	s.closeMetrics()
	close(s.stopC)
	s.space.Stop()
	s.stopUpdateNodeInfo()
	s.stopTCPService()
	s.stopRaftServer()
	s.stopSmuxService()
	s.closeSmuxConnPool()
	MasterClient.Stop()
	// stop cpu sample
	close(s.cpuSamplerDone)
	if s.gcTimer != nil {
		s.gcTimer.Stop()
	}
	s.closeStat()
}

func (s *DataNode) parseConfig(cfg *config.Config) (err error) {
	var (
		port       string
		regexpPort *regexp.Regexp
	)
	LocalIP = cfg.GetString(ConfigKeyLocalIP)
	port = cfg.GetString(proto.ListenPort)
	s.bindIp = cfg.GetBool(proto.BindIpKey)
	if regexpPort, err = regexp.Compile(`^(\d)+$`); err != nil {
		return fmt.Errorf("Err:no port")
	}
	if !regexpPort.MatchString(port) {
		return fmt.Errorf("Err:port must string")
	}
	s.port = port

	s.cacheCap = cfg.GetInt(ConfigKeyCacheCap)
	log.LogWarnf("parseConfig: cache cap size %d", s.cacheCap)

	updateInterval := cfg.GetInt(configNameResolveInterval)
	if updateInterval <= 0 || updateInterval > 60 {
		log.LogWarnf("name resolving interval[1-60] is set to default: %v", DefaultNameResolveInterval)
		updateInterval = DefaultNameResolveInterval
	}

	addrs := cfg.GetSlice(proto.MasterAddr)
	if len(addrs) == 0 {
		return fmt.Errorf("Err:masterAddr unavalid")
	}
	masters := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		masters = append(masters, addr.(string))
	}
	MasterClient = masterSDK.NewMasterCLientWithResolver(masters, false, updateInterval)
	if MasterClient == nil {
		err = fmt.Errorf("parseConfig: masters addrs format err[%v]", masters)
		log.LogErrorf("parseConfig: masters addrs format err[%v]", masters)
		return err
	}
	poolSize := cfg.GetInt64(proto.CfgHttpPoolSize)
	syslog.Printf("parseConfig: http pool size %d", poolSize)
	MasterClient.SetTransport(proto.GetHttpTransporter(&proto.HttpCfg{PoolSize: int(poolSize)}))

	if err = MasterClient.Start(); err != nil {
		return err
	}

	s.zoneName = cfg.GetString(ConfigKeyZone)
	if s.zoneName == "" {
		s.zoneName = DefaultZoneName
	}
	s.metricsDegrade = cfg.GetInt64(CfgMetricsDegrade)

	s.serviceIDKey = cfg.GetString(ConfigServiceIDKey)

	s.enableGcTimer = cfg.GetBoolWithDefault(ConfigEnableGcTimer, false)
	gcRecyclePercentStr := cfg.GetString(ConfigGcRecyclePercent)
	if gcRecyclePercentStr == "" {
		s.gcRecyclePercent = defaultGcRecyclePercent
	} else {
		s.gcRecyclePercent, err = strconv.ParseFloat(gcRecyclePercentStr, 64)
		if err != nil {
			err = fmt.Errorf("parseConfig: parse configKey[%v] err: %v", ConfigGcRecyclePercent, err.Error())
			log.LogError(err.Error())
			return err
		}
	}

	if s.gcRecyclePercent <= 0 || s.gcRecyclePercent > 1 {
		s.gcRecyclePercent = defaultGcRecyclePercent
	}

	diskUnavailablePartitionErrorCount := cfg.GetInt64(ConfigKeyDiskUnavailablePartitionErrorCount)
	if diskUnavailablePartitionErrorCount <= 0 || diskUnavailablePartitionErrorCount > 100 {
		diskUnavailablePartitionErrorCount = DefaultDiskUnavailablePartitionErrorCount
		log.LogDebugf("action[parseConfig] ConfigKeyDiskUnavailablePartitionErrorCount(%v) out of range, set as default(%v)",
			diskUnavailablePartitionErrorCount, DefaultDiskUnavailablePartitionErrorCount)
	}
	s.diskUnavailablePartitionErrorCount = uint64(diskUnavailablePartitionErrorCount)
	log.LogDebugf("action[parseConfig] load diskUnavailablePartitionErrorCount(%v)", s.diskUnavailablePartitionErrorCount)

	var mediaType uint32
	if !cfg.HasKey(ConfigMediaType) {
		err = fmt.Errorf("parseConfig: configKey[%v] not set", ConfigMediaType)
		return err
	}
	if err, mediaType = cfg.GetUint32(ConfigMediaType); err != nil {
		err = fmt.Errorf("parseConfig: parse configKey[%v] err: %v", ConfigMediaType, err.Error())
		log.LogError(err.Error())
		return err
	}
	if !proto.IsValidMediaType(mediaType) {
		err = fmt.Errorf("parseConfig: invalid mediaType(%v)", mediaType)
		log.LogError(err.Error())
		return err
	}
	s.mediaType = mediaType

	s.ExtentCacheTtlByMin = cfg.GetIntWithDefault(ConfigExtentCacheTtlByMin, DefaultExtentCacheTtlByMin)

	log.LogDebugf("action[parseConfig] load masterAddrs(%v).", MasterClient.Nodes())
	log.LogDebugf("action[parseConfig] load port(%v).", s.port)
	log.LogDebugf("action[parseConfig] load zoneName(%v).", s.zoneName)
	log.LogDebugf("action[parseConfig] load mediaType(%v).", s.mediaType)
	return
}

func (s *DataNode) initQosLimit(cfg *config.Config) {
	dn := s.space.dataNode
	dn.diskQosEnable = cfg.GetBoolWithDefault(ConfigDiskQosEnable, true)
	dn.diskAsyncQosEnable = cfg.GetBoolWithDefault(ConfigDiskAsyncQosEnable, true)
	dn.diskReadIocc = cfg.GetInt(ConfigDiskReadIocc)
	dn.diskReadIops = cfg.GetInt(ConfigDiskReadIops)
	dn.diskReadFlow = cfg.GetInt(ConfigDiskReadFlow)
	dn.diskWriteIocc = cfg.GetInt(ConfigDiskWriteIocc)
	dn.diskWriteIops = cfg.GetInt(ConfigDiskWriteIops)
	dn.diskWriteFlow = cfg.GetInt(ConfigDiskWriteFlow)
	dn.diskAsyncReadIocc = cfg.GetIntWithDefault(ConfigDiskAsyncReadIocc, 100)
	dn.diskAsyncReadIops = cfg.GetInt(ConfigDiskAsyncReadIops)
	dn.diskAsyncReadFlow = cfg.GetInt(ConfigDiskAsyncReadFlow)
	dn.diskAsyncWriteIocc = cfg.GetIntWithDefault(ConfigDiskAsyncWriteIocc, 100)
	dn.diskAsyncWriteIops = cfg.GetInt(ConfigDiskAsyncWriteIops)
	dn.diskAsyncWriteFlow = cfg.GetInt(ConfigDiskAsyncWriteFlow)
	dn.diskDeleteIocc = cfg.GetInt(ConfigDiskDeleteIocc)
	dn.diskDeleteFlow = cfg.GetInt(ConfigDiskDeleteFlow)
	dn.diskDeleteIops = cfg.GetInt(ConfigDiskDeleteIops)
	log.LogWarnf("action[initQosLimit] set qos [normal %v async %v], rWriteiocc:normal %d async %d, iops:%d async %d, flow:normal %d async %d) write(iocc:%d async %d,iops:%d async %d, flow:%d async %d) delete(iocc:%d flow:%d iops: %d)",
		dn.diskQosEnable, dn.diskAsyncQosEnable, dn.diskReadIocc, dn.diskAsyncReadIocc, dn.diskReadIops, dn.diskAsyncReadIops, dn.diskReadFlow, dn.diskAsyncReadFlow, dn.diskWriteIocc, dn.diskAsyncWriteIocc,
		dn.diskWriteIops, dn.diskAsyncWriteIops, dn.diskWriteFlow, dn.diskAsyncWriteFlow, dn.diskDeleteIocc, dn.diskDeleteFlow, dn.diskDeleteIops)
}

func (s *DataNode) updateQosLimit() {
	for _, disk := range s.space.disks {
		disk.updateQosLimiter()
	}
}

func (s *DataNode) newSpaceManager(cfg *config.Config) (err error) {
	s.startTime = time.Now().Unix()
	s.space = NewSpaceManager(s)
	if len(strings.TrimSpace(s.port)) == 0 {
		err = ErrNewSpaceManagerFailed
		return
	}

	s.space.SetRaftStore(s.raftStore)
	s.space.SetNodeID(s.nodeID)
	s.space.SetClusterID(s.clusterID)
	s.initQosLimit(cfg)

	loadLimit := cfg.GetInt(ConfigDiskCurrentLoadDpLimit)
	stopLimit := cfg.GetInt(ConfigDiskCurrentStopDpLimit)
	s.space.SetCurrentLoadDpLimit(loadLimit)
	s.space.SetCurrentStopDpLimit(stopLimit)

	return
}

func (s *DataNode) getDisks() (disks map[string]interface{}, brokenDisks map[string]interface{}, err error) {
	var dataNode *proto.DataNodeInfo
	disks = make(map[string]interface{})
	brokenDisks = make(map[string]interface{})
	for i := 0; i < 3; i++ {
		dataNode, err = MasterClient.NodeAPI().GetDataNode(s.localServerAddr)
		if err == nil {
			break
		}
		log.LogErrorf("action[getDisks]: getDataNode error %v", err)
	}

	if err != nil {
		log.LogErrorf("action[getDisks]: failed to get datanode(%v), err(%v)", s.localServerAddr, err)
		err = fmt.Errorf("failed to get datanode %v", s.localServerAddr)
		return
	}
	log.LogInfof("[getDisks] data node(%v) disks(%v) broken disks(%v)", dataNode.Addr, dataNode.AllDisks, dataNode.BadDisks)
	for _, disk := range dataNode.BadDisks {
		brokenDisks[disk] = struct{}{}
	}
	for _, disk := range dataNode.AllDisks {
		disks[disk] = struct{}{}
	}

	return
}

func (s *DataNode) startSpaceManager(cfg *config.Config) (err error) {
	diskRdonlySpace := uint64(cfg.GetInt64(CfgDiskRdonlySpace))
	if diskRdonlySpace < DefaultDiskRetainMin {
		diskRdonlySpace = DefaultDiskRetainMin
	}
	diskEnableReadRepairExtentLimit := cfg.GetBoolWithDefault(ConfigEnableDiskReadExtentLimit, false)
	log.LogInfof("startSpaceManager preReserveSpace %d", diskRdonlySpace)

	paths := make([]string, 0)
	diskPath := cfg.GetString(ConfigKeyDiskPath)
	if diskPath != "" {
		paths, err = parseDiskPath(diskPath)
		if err != nil {
			log.LogErrorf("parse diskpath failed, path %s, err %s", diskPath, err.Error())
			return err
		}
	} else {
		for _, p := range cfg.GetSlice(ConfigKeyDisks) {
			paths = append(paths, p.(string))
		}
	}

	disks, brokenDisks, err := s.getDisks()
	if err != nil {
		log.LogErrorf("[startSpaceManager] failed to get disks, err(%v)", err)
		return
	}
	log.LogInfof("[startSpaceManager] disks(%v) brokenDisks(%v)", disks, brokenDisks)

	var wg sync.WaitGroup
	diskReservedSpace := make(map[string]uint64)
	for _, d := range paths {
		log.LogDebugf("action[startSpaceManager] load disk raw config(%v).", d)

		// format "PATH:RESET_SIZE
		arr := strings.Split(d, ":")
		if len(arr) != 2 {
			return errors.New("invalid disk configuration. Example: PATH:RESERVE_SIZE")
		}

		isBroken := false
		path := arr[0]
		fileInfo, err := os.Stat(path)
		if err != nil {
			log.LogErrorf("Stat disk path [%v] error: [%s]", path, err)
			if IsDiskErr(err.Error()) {
				isBroken = true
			} else {
				continue
			}
		} else {
			if !fileInfo.IsDir() {
				return errors.New("Disk path is not dir")
			}
			if s.clusterUuidEnable {
				if err = config.CheckOrStoreClusterUuid(path, s.clusterUuid, false); err != nil {
					log.LogErrorf("CheckOrStoreClusterUuid failed: %v", err)
					return fmt.Errorf("checkOrStoreClusterUuid failed: %v", err.Error())
				}
			}
		}

		reservedSpace, err := strconv.ParseUint(arr[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid disk reserved space. Error: %s", err.Error())
		}

		if reservedSpace < DefaultDiskRetainMin {
			reservedSpace = DefaultDiskRetainMin
		}

		diskReservedSpace[path] = reservedSpace

		wg.Add(1)
		go func(wg *sync.WaitGroup, path string, reservedSpace uint64, isBroken bool) {
			defer wg.Done()
			if isBroken {
				log.LogErrorf("[startSpaceManager] disk %v is broken", path)
				s.space.LoadBrokenDisk(path, reservedSpace, diskRdonlySpace, DefaultDiskMaxErr, diskEnableReadRepairExtentLimit)
				return
			}

			err := s.space.LoadDisk(path, reservedSpace, diskRdonlySpace, DefaultDiskMaxErr, diskEnableReadRepairExtentLimit)
			if err != nil {
				log.LogErrorf("[startSpaceManager] load disk %v failed: %v", path, err)
				return
			}
			if _, found := brokenDisks[path]; found {
				disk, _ := s.space.GetDisk(path)
				disk.doDiskError()
				log.LogErrorf("[startSpaceManager] disk %v is broken", path)
				return
			}
		}(&wg, path, reservedSpace, isBroken)
	}
	wg.Wait()

	for diskPath := range disks {
		if _, ok := diskReservedSpace[diskPath]; !ok {
			log.LogErrorf("[startSpaceManager] diskPath %v in config is missing", diskPath)
			continue
		}
		_, err = s.space.GetDisk(diskPath)
		if err != nil {
			log.LogErrorf("[startSpaceManager] disk %v is lost", diskPath)
			disk := NewLostDisk(diskPath, diskReservedSpace[diskPath], diskRdonlySpace, DefaultDiskMaxErr, s.space, diskEnableReadRepairExtentLimit)
			s.space.putDisk(disk)
		}
	}

	// start check disk lost
	s.space.StartCheckDiskLost()

	// start async sample
	s.space.StartDiskSample()
	s.updateQosLimit() // load from config
	s.markAllDiskLoaded()

	go s.space.StartEvictExtentCache()

	return nil
}

func (s *DataNode) markAllDiskLoaded() {
	s.space.diskMutex.Lock()
	defer s.space.diskMutex.Unlock()
	s.space.allDisksLoaded = true
}

func (s *DataNode) checkAllDiskLoaded() bool {
	s.space.diskMutex.RLock()
	defer s.space.diskMutex.RUnlock()
	return s.space.allDisksLoaded
}

// execute shell to find all paths
// out: like, /disk1:1024, /disk2:1024
func parseDiskPath(pathStr string) (disks []string, err error) {
	log.LogInfof("parse diskpath, %s", pathStr)

	arr := strings.Split(pathStr, ":")
	if len(arr) != 2 {
		return disks, fmt.Errorf("diskPath cfg should be diskPathPrefix:RESERVE_SIZE")
	}

	shell := fmt.Sprintf("mount | grep %s | awk '{print $3}'", arr[0])
	cmd := exec.Command("/bin/sh", "-c", shell)
	log.LogWarnf("execute diskPath shell, %s", shell)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return disks, fmt.Errorf("execute shell failed, %s", err.Error())
	}

	disks = make([]string, 0)
	lines := bytes.Split(out, []byte("\n"))
	for _, line := range lines {
		str := strings.TrimSpace(string(line))
		if str == "" {
			continue
		}

		disks = append(disks, fmt.Sprintf("%s:%s", string(line), arr[1]))
	}

	return disks, nil
}

// registers the data node on the master to report the information such as IsIPV4 address.
// The startup of a data node will be blocked until the registration succeeds.
func (s *DataNode) register(cfg *config.Config) (err error) {
	var nodeForbidWriteOpVerMsg string
	var volsForbidWriteOpVerMsg string

	timer := time.NewTimer(0)

	// get the IsIPV4 address, cluster ID and node ID from the master
	for {
		select {
		case <-timer.C:
			var ci *proto.ClusterInfo
			if ci, err = MasterClient.AdminAPI().GetClusterInfo(); err != nil {
				log.LogErrorf("action[registerToMaster] cannot get ip from master(%v) err(%v).",
					MasterClient.Leader(), err)
				timer.Reset(2 * time.Second)
				continue
			}
			masterAddr := MasterClient.Leader()
			s.clusterUuid = ci.ClusterUuid
			s.clusterUuidEnable = ci.ClusterUuidEnable
			s.clusterEnableSnapshot = ci.ClusterEnableSnapshot
			s.clusterID = ci.Cluster
			s.raftPartitionCanUsingDifferentPort = ci.RaftPartitionCanUsingDifferentPort
			if LocalIP == "" {
				LocalIP = string(ci.Ip)
			}

			s.localServerAddr = fmt.Sprintf("%s:%v", LocalIP, s.port)
			if !util.IsIPV4(LocalIP) {
				log.LogErrorf("action[registerToMaster] got an invalid local ip(%v) from master(%v).",
					LocalIP, masterAddr)
				timer.Reset(2 * time.Second)
				continue
			}

			volListForbidWriteOpOfProtoVer0 := make([]string, 0)
			var settingsForbidFromMaster *proto.UpgradeCompatibleSettings
			if settingsForbidFromMaster, err = MasterClient.AdminAPI().GetUpgradeCompatibleSettings(); err != nil {
				if strings.Contains(err.Error(), proto.KeyWordInHttpApiNotSupportErr) {
					// master may be lower version and has no this API
					volsForbidWriteOpVerMsg = "[registerToMaster] master version has no api GetUpgradeCompatibleSettings, ues default value"
				} else {
					log.LogErrorf("[registerToMaster] GetUpgradeCompatibleSettings from master(%v) err: %v",
						MasterClient.Leader(), err)
					timer.Reset(2 * time.Second)
					continue
				}
			} else {
				s.nodeForbidWriteOpOfProtoVer0 = settingsForbidFromMaster.ClusterForbidWriteOpOfProtoVer0
				nodeForbidWriteOpVerMsg = fmt.Sprintf("action[registerToMaster] from master, cluster node forbid write Operate Of proto version-0: %v",
					s.nodeForbidWriteOpOfProtoVer0)

				volListForbidWriteOpOfProtoVer0 = settingsForbidFromMaster.VolsForbidWriteOpOfProtoVer0
				volsForbidWriteOpVerMsg = fmt.Sprintf("[registerToMaster] from master, volumes forbid write operate of proto version-0: %v",
					volListForbidWriteOpOfProtoVer0)
			}
			volMapForbidWriteOpOfProtoVer0 := make(map[string]struct{})
			for _, vol := range volListForbidWriteOpOfProtoVer0 {
				if _, ok := volMapForbidWriteOpOfProtoVer0[vol]; !ok {
					volMapForbidWriteOpOfProtoVer0[vol] = struct{}{}
				}
			}
			s.VolsForbidWriteOpOfProtoVer0 = volMapForbidWriteOpOfProtoVer0

			// register this data node on the master
			var nodeID uint64
			if nodeID, err = MasterClient.NodeAPI().AddDataNodeWithAuthNode(fmt.Sprintf("%s:%v", LocalIP, s.port), s.raftHeartbeat, s.raftReplica,
				s.zoneName, s.serviceIDKey, s.mediaType); err != nil {
				if strings.Contains(err.Error(), proto.ErrDataNodeAdd.Error()) {
					failMsg := fmt.Sprintf("[register] register to master[%v] failed: %v",
						masterAddr, err)
					log.LogError(failMsg)
					syslog.Printf("%v\n", failMsg)
					return err
				}

				log.LogErrorf("action[registerToMaster] cannot register this node to master[%v] err(%v), keep retry",
					masterAddr, err)
				timer.Reset(2 * time.Second)
				continue
			}
			exporter.RegistConsul(s.clusterID, ModuleName, cfg)
			s.nodeID = nodeID
			log.LogDebugf("register: register DataNode: nodeID(%v)", s.nodeID)
			syslog.Printf("register: register DataNode: nodeID(%v) %v \n", s.nodeID, s.localServerAddr)

			log.LogInfo(nodeForbidWriteOpVerMsg)
			syslog.Printf("%v\n", nodeForbidWriteOpVerMsg)
			log.LogInfo(volsForbidWriteOpVerMsg)
			syslog.Printf("%v\n", volsForbidWriteOpVerMsg)

			return
		case <-s.stopC:
			timer.Stop()
			return
		}
	}
}

type DataNodeInfo struct {
	Addr                                  string
	PersistenceDataPartitions             []uint64
	PersistenceDataPartitionsWithDiskPath []proto.DataPartitionDiskInfo
}

func (s *DataNode) checkLocalPartitionMatchWithMaster() (lackPartitions []uint64, err error) {
	convert := func(node *proto.DataNodeInfo) *DataNodeInfo {
		result := &DataNodeInfo{}
		result.Addr = node.Addr
		result.PersistenceDataPartitions = node.PersistenceDataPartitions
		return result
	}
	var dataNode *proto.DataNodeInfo
	for i := 0; i < 3; i++ {
		if dataNode, err = MasterClient.NodeAPI().GetDataNode(s.localServerAddr); err != nil {
			log.LogErrorf("checkLocalPartitionMatchWithMaster error %v", err)
			time.Sleep(10 * time.Second)
			continue
		}
		break
	}
	if dataNode == nil {
		err = ErrGetMasterDatanodeInfoFailed
		return
	}
	dinfo := convert(dataNode)
	if len(dinfo.PersistenceDataPartitions) == 0 {
		return
	}

	for _, partitionID := range dinfo.PersistenceDataPartitions {
		dp := s.space.Partition(partitionID)
		if dp == nil {
			lackPartitions = append(lackPartitions, partitionID)
		}
	}

	if len(lackPartitions) == 0 {
		log.LogInfo("checkLocalPartitionMatchWithMaster no lack")
	} else {
		log.LogErrorf("checkLocalPartitionMatchWithMaster lack ids [%v]", lackPartitions)
	}
	return
}

func (s *DataNode) checkPartitionInMemoryMatchWithInDisk() (lackPartitions []uint64) {
	s.space.partitionMutex.RLock()
	partitions := make([]*DataPartition, 0)
	for _, dp := range s.space.partitions {
		partitions = append(partitions, dp)
	}
	s.space.partitionMutex.RUnlock()

	for _, dp := range partitions {
		stat, err := os.Stat(dp.path)
		if err != nil {
			lackPartitions = append(lackPartitions, dp.partitionID)
			log.LogErrorf("action[checkPartitionInMemoryMatchWithInDisk] stat dataPartition[%v] fail, path[%v], err[%v]", dp.partitionID, dp.Path(), err)
			continue
		}
		if !stat.IsDir() {
			lackPartitions = append(lackPartitions, dp.partitionID)
			log.LogErrorf("action[checkPartitionInMemoryMatchWithInDisk] dataPartition[%v] is not directory, path[%v]", dp.partitionID, dp.Path())
			continue
		}
	}
	return
}

func (s *DataNode) registerHandler() {
	http.HandleFunc("/disks", s.getDiskAPI)
	http.HandleFunc("/partitions", s.getPartitionsAPI)
	http.HandleFunc("/partition", s.getPartitionAPI)
	http.HandleFunc("/extent", s.getExtentAPI)
	http.HandleFunc("/block", s.getBlockCrcAPI)
	http.HandleFunc("/stats", s.getStatAPI)
	http.HandleFunc("/raftStatus", s.getRaftStatus)
	http.HandleFunc("/setAutoRepairStatus", s.setAutoRepairStatus)
	http.HandleFunc("/getTinyDeleted", s.getTinyDeleted)
	http.HandleFunc("/getNormalDeleted", s.getNormalDeleted)
	http.HandleFunc("/getSmuxPoolStat", s.getSmuxPoolStat())
	http.HandleFunc("/setMetricsDegrade", s.setMetricsDegrade)
	http.HandleFunc("/getMetricsDegrade", s.getMetricsDegrade)
	http.HandleFunc("/qosEnable", s.setQosEnable())
	http.HandleFunc("/genClusterVersionFile", s.genClusterVersionFile)
	http.HandleFunc("/setDiskBad", s.setDiskBadAPI)
	http.HandleFunc("/setDiskQos", s.setDiskQos)
	http.HandleFunc("/getDiskQos", s.getDiskQos)
	http.HandleFunc("/reloadDataPartition", s.reloadDataPartition)
	http.HandleFunc("/setDiskExtentReadLimitStatus", s.setDiskExtentReadLimitStatus)
	http.HandleFunc("/queryDiskExtentReadLimitStatus", s.queryDiskExtentReadLimitStatus)
	http.HandleFunc("/detachDataPartition", s.detachDataPartition)
	http.HandleFunc("/loadDataPartition", s.loadDataPartition)
	http.HandleFunc("/releaseDiskExtentReadLimitToken", s.releaseDiskExtentReadLimitToken)
	http.HandleFunc("/markDataPartitionBroken", s.markDataPartitionBroken)
	http.HandleFunc("/markDiskBroken", s.markDiskBroken)
	http.HandleFunc("/getAllExtent", s.getAllExtent)
	http.HandleFunc("/setOpLog", s.setOpLog)
	http.HandleFunc("/getOpLog", s.getOpLog)
	http.HandleFunc(exporter.SetEnablePidPath, exporter.SetEnablePid)
	http.HandleFunc("/getRaftPeers", s.getRaftPeers)
	http.HandleFunc("/setGOGC", s.setGOGC)
	http.HandleFunc("/getGOGC", s.getGOGC)
	http.HandleFunc("/triggerRaftLogRotate", s.triggerRaftLogRotate)
}

func (s *DataNode) startTCPService() (err error) {
	log.LogInfo("Start: startTCPService")
	addr := fmt.Sprintf(":%v", s.port)
	if s.bindIp {
		addr = fmt.Sprintf("%s:%v", LocalIP, s.port)
	}
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
		begin := time.Now()
		defer func() {
			msg := fmt.Sprintf("[stopTCPService] stop tcp service using time(%v)", time.Since(begin))
			log.LogInfo(msg)
			syslog.Print(msg)
		}()
		s.tcpListener.Close()
		log.LogDebugf("action[stopTCPService] stop tcp service.")
	}
	return
}

func (s *DataNode) serveConn(conn net.Conn) {
	space := s.space
	space.Stats().AddConnection()
	c, _ := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	packetProcessor := repl.NewReplProtocol(conn, s.Prepare, s.OperatePacket, s.Post)
	packetProcessor.ServerConn()
	space.Stats().RemoveConnection()
}

func (s *DataNode) startSmuxService(cfg *config.Config) (err error) {
	log.LogInfo("Start: startSmuxService")
	addr := fmt.Sprintf(":%v", s.port)
	if s.bindIp {
		addr = fmt.Sprintf("%s:%v", LocalIP, s.port)
	}
	addr = util.ShiftAddrPort(addr, s.smuxPortShift)
	log.LogInfof("SmuxListenAddr: (%v)", addr)

	// server
	l, err := net.Listen(NetworkProtocol, addr)
	log.LogDebugf("action[startSmuxService] listen %v address(%v).", NetworkProtocol, addr)
	if err != nil {
		log.LogError("failed to listen smux addr, err:", err)
		return
	}
	s.smuxListener = l
	go func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.LogErrorf("action[startSmuxService] failed to accept, err:%s", err.Error())
				break
			}
			log.LogDebugf("action[startSmuxService] accept connection from %s.", conn.RemoteAddr().String())
			go s.serveSmuxConn(conn)
		}
	}(l)
	return
}

func (s *DataNode) stopSmuxService() (err error) {
	if s.smuxListener != nil {
		begin := time.Now()
		defer func() {
			msg := fmt.Sprintf("[stopSmuxService] stop smux service uing time(%v)", time.Since(begin))
			syslog.Print(msg)
			log.LogInfo(msg)
		}()
		s.smuxListener.Close()
		log.LogDebugf("action[stopSmuxService] stop smux service.")
	}
	return
}

func (s *DataNode) serveSmuxConn(conn net.Conn) {
	space := s.space
	space.Stats().AddConnection()
	c, _ := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)
	var sess *smux.Session
	var err error
	sess, err = smux.Server(conn, s.smuxServerConfig)
	if err != nil {
		log.LogErrorf("action[serveSmuxConn] failed to serve smux connection, addr(%v), err(%v)", c.RemoteAddr(), err)
		return
	}
	defer func() {
		sess.Close()
		space.Stats().RemoveConnection()
	}()
	for {
		stream, err := sess.AcceptStream()
		if err != nil {
			if util.FilterSmuxAcceptError(err) != nil {
				log.LogErrorf("action[startSmuxService] failed to accept, err: %s", err)
			} else {
				log.LogInfof("action[startSmuxService] accept done, err: %s", err)
			}
			break
		}
		go s.serveSmuxStream(stream)
	}
}

func (s *DataNode) serveSmuxStream(stream *smux.Stream) {
	packetProcessor := repl.NewReplProtocol(stream, s.Prepare, s.OperatePacket, s.Post)
	if s.enableSmuxConnPool {
		packetProcessor.SetSmux(s.getRepairConnFunc, s.putRepairConnFunc)
	}
	packetProcessor.ServerConn()
}

func (s *DataNode) parseSmuxConfig(cfg *config.Config) error {
	s.enableSmuxConnPool = cfg.GetBool(ConfigKeyEnableSmuxClient)
	s.smuxPortShift = cfg.GetInt(ConfigKeySmuxPortShift)
	if s.smuxPortShift == 0 {
		s.smuxPortShift = util.DefaultSmuxPortShift
	}
	// smux server cfg
	s.smuxServerConfig = util.DefaultSmuxConfig()
	maxBuffer := cfg.GetInt(ConfigKeySmuxMaxBuffer)
	if maxBuffer > 0 {
		s.smuxServerConfig.MaxReceiveBuffer = maxBuffer
		if s.smuxServerConfig.MaxStreamBuffer > maxBuffer {
			s.smuxServerConfig.MaxStreamBuffer = maxBuffer
		}
		if err := smux.VerifyConfig(s.smuxServerConfig); err != nil {
			return err
		}
	}

	// smux conn pool config
	if s.enableSmuxConnPool {
		s.smuxConnPoolConfig = util.DefaultSmuxConnPoolConfig()
		if maxBuffer > 0 {
			s.smuxConnPoolConfig.MaxReceiveBuffer = maxBuffer
			if s.smuxConnPoolConfig.MaxStreamBuffer > maxBuffer {
				s.smuxConnPoolConfig.MaxStreamBuffer = maxBuffer
			}
		}
		maxConn := cfg.GetInt(ConfigKeySmuxMaxConn)
		if maxConn > 0 {
			if s.smuxConnPoolConfig.ConnsPerAddr < maxConn {
				s.smuxConnPoolConfig.ConnsPerAddr = maxConn
			}
		}
		maxStreamPerConn := cfg.GetInt(ConfigKeySmuxStreamPerConn)
		if maxStreamPerConn > 0 {
			s.smuxConnPoolConfig.StreamsPerConn = maxStreamPerConn
		}
		totalStreams := cfg.GetInt(ConfigKeySmuxTotalStream)
		if totalStreams > 0 {
			s.smuxConnPoolConfig.TotalStreams = totalStreams
		}
		if err := util.VerifySmuxPoolConfig(s.smuxConnPoolConfig); err != nil {
			return err
		}
	}
	log.LogDebugf("[parseSmuxConfig] load smuxPortShift(%v).", s.smuxPortShift)
	log.LogDebugf("[parseSmuxConfig] load enableSmuxConnPool(%v).", s.enableSmuxConnPool)
	log.LogDebugf("[parseSmuxConfig] load smuxServerConfig(%v).", s.smuxServerConfig)
	log.LogDebugf("[parseSmuxConfig] load smuxConnPoolConfig(%v).", s.smuxConnPoolConfig)
	return nil
}

func (s *DataNode) initConnPool() {
	if s.enableSmuxConnPool {
		log.LogInfof("Start: init smux conn pool")
		s.smuxConnPool = util.NewSmuxConnectPool(s.smuxConnPoolConfig)
		s.getRepairConnFunc = func(target string) (net.Conn, error) {
			addr := util.ShiftAddrPort(target, s.smuxPortShift)
			log.LogDebugf("[dataNode.getRepairConnFunc] get smux conn, addr(%v)", addr)
			return s.smuxConnPool.GetConnect(addr)
		}
		s.putRepairConnFunc = func(conn net.Conn, forceClose bool) {
			log.LogDebugf("[dataNode.putRepairConnFunc] put smux conn, addr(%v), forceClose(%v)", conn.RemoteAddr().String(), forceClose)
			s.smuxConnPool.PutConnect(conn.(*smux.Stream), forceClose)
		}
	} else {
		s.getRepairConnFunc = func(target string) (conn net.Conn, err error) {
			log.LogDebugf("[dataNode.getRepairConnFunc] get tcp conn, addr(%v)", target)
			return gConnPool.GetConnect(target)
		}
		s.putRepairConnFunc = func(conn net.Conn, forceClose bool) {
			log.LogDebugf("[dataNode.putRepairConnFunc] put tcp conn, addr(%v), forceClose(%v)", conn.RemoteAddr().String(), forceClose)
			gConnPool.PutConnect(conn.(*net.TCPConn), forceClose)
		}
	}
}

func (s *DataNode) closeSmuxConnPool() {
	if s.smuxConnPool != nil {
		begin := time.Now()
		defer func() {
			msg := fmt.Sprintf("[closeSmuxConnPool] close smux conn pool using time(%v)", time.Since(begin))
			log.LogInfo(msg)
			syslog.Print(msg)
		}()
		s.smuxConnPool.Close()
		log.LogDebugf("action[stopSmuxService] stop smux conn pool")
	}
}

func (s *DataNode) shallDegrade() bool {
	level := atomic.LoadInt64(&s.metricsDegrade)
	if level < 0 {
		return true
	}
	if level == 0 {
		return false
	}
	cnt := atomic.LoadUint64(&s.metricsCnt)
	return cnt%uint64(level) != 0
}

func (s *DataNode) scheduleTask() {
	go s.startUpdateNodeInfo()
	s.scheduleToCheckLackPartitions()
}

func (s *DataNode) startCpuSample() {
	s.cpuSamplerDone = make(chan struct{})
	go func() {
		for {
			select {
			case <-s.cpuSamplerDone:
				return
			default:
				// this function will sleep cpuSampleDuration
				used, err := loadutil.GetCpuUtilPercent(cpuSampleDuration)
				if err == nil {
					s.cpuUtil.Store(used)
				}
			}
		}
	}()
}

func (s *DataNode) startGcTimer() {
	if !s.enableGcTimer {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("[startGcTimer] panic(%v)", r)
		}
	}()

	enable, err := loadutil.IsEnableSwapMemory()
	if err != nil {
		log.LogErrorf("[startGcTimer] failed to get swap memory info, err(%v)", err)
		return
	}
	if enable {
		log.LogWarnf("[startGcTimer] swap memory is enable")
		return
	}
	s.gcTimer, err = util.NewRecycleTimer(gcTimerDuration, s.gcRecyclePercent, 1*util.GB)
	if err != nil {
		log.LogErrorf("[startGcTimer] failed to start gc timer, err(%v)", err)
		return
	}

	s.gcTimer.SetPanicHook(func(r interface{}) {
		log.LogErrorf("[startGcTimer] gc timer panic, err(%v)", r)
	})
	s.gcTimer.SetStatHook(func(totalPercent float64, currentProcess, currentGoHeap uint64) {
		log.LogWarnf("[startGcTimer] host use too many memory, percent(%v), current process(%v), current process go heap(%v)", strutil.FormatPercent(totalPercent), strutil.FormatSize(currentProcess), strutil.FormatSize(currentGoHeap))
	})
}

func (s *DataNode) scheduleToCheckLackPartitions() {
	go func() {
		for {
			lackPartitionsInMem, err := s.checkLocalPartitionMatchWithMaster()
			if err != nil {
				log.LogError(err)
			}
			if len(lackPartitionsInMem) > 0 {
				err = fmt.Errorf("action[scheduleToLackDataPartitions] lackPartitions %v in datanode %v memory",
					lackPartitionsInMem, s.localServerAddr)
				log.LogErrorf(err.Error())
			}
			s.space.stats.updateMetricLackPartitionsInMem(uint64(len(lackPartitionsInMem)))

			lackPartitionsInDisk := s.checkPartitionInMemoryMatchWithInDisk()
			if len(lackPartitionsInDisk) > 0 {
				err = fmt.Errorf("action[scheduleToLackDataPartitions] lackPartitions %v in datanode %v disk",
					lackPartitionsInDisk, s.localServerAddr)
				log.LogErrorf(err.Error())
			}
			s.space.stats.updateMetricLackPartitionsInDisk(uint64(len(lackPartitionsInDisk)))

			time.Sleep(1 * time.Minute)
		}
	}()
}

func IsDiskErr(errMsg string) bool {
	return strings.Contains(errMsg, syscall.EIO.Error()) ||
		strings.Contains(errMsg, syscall.EROFS.Error())
}

func (s *DataNode) IopsStatus() (status proto.IopsStatus) {
	status.ReadIops = s.diskReadIops
	status.WriteIops = s.diskWriteIops
	status.AsyncReadIops = s.diskAsyncReadIops
	status.AsyncWriteIops = s.diskAsyncWriteIops
	status.DeleteIops = s.diskDeleteIops
	return
}

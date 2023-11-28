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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util/fetchtopology"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/repl"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/async"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/cpu"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/multirate"
	"github.com/cubefs/cubefs/util/statinfo"
	"github.com/cubefs/cubefs/util/statistics"
	"github.com/cubefs/cubefs/util/unit"
)

var (
	ErrIncorrectStoreType       = errors.New("Incorrect store type")
	ErrNoSpaceToCreatePartition = errors.New("No disk space to create a data partition")
	ErrNewSpaceManagerFailed    = errors.New("Creater new space manager failed")
	ErrPartitionNil             = errors.New("partition is nil")
	LocalIP                     string
	LocalServerPort             string
	gConnPool                   = connpool.NewConnectPool()
	MasterClient                = masterSDK.NewMasterClient(nil, false)
	MasterDomainClient          = masterSDK.NewMasterClient(nil, false)
	gHasLoadDataPartition       bool
	gHasFinishedLoadDisks       bool

	maybeServerFaultOccurred bool // 是否判定当前节点大概率出现过系统断电
)

const (
	DefaultZoneName          = proto.DefaultZoneName
	DefaultRaftLogsToRetain  = 1000 // Count of raft logs per data partition
	DefaultDiskMaxErr        = 1
	DefaultDiskReservedSpace = 5 * unit.GB // GB
	DefaultDiskUsableRatio   = float64(0.90)
	DefaultDiskReservedRatio = 0.1
)

const (
	ModuleName          = "dataNode"
	SystemStartTimeFile = "SYS_START_TIME"
	MAX_OFFSET_OF_TIME  = 5
)

const (
	ConfigKeyLocalIP        = "localIP"        // string
	ConfigKeyPort           = "port"           // int
	ConfigKeyMasterAddr     = "masterAddr"     // array
	ConfigKeyZone           = "zoneName"       // string
	ConfigKeyDisks          = "disks"          // array
	ConfigKeyRaftDir        = "raftDir"        // string
	ConfigKeyRaftHeartbeat  = "raftHeartbeat"  // string
	ConfigKeyRaftReplica    = "raftReplica"    // string
	cfgTickIntervalMs       = "tickIntervalMs" // int
	ConfigKeyMasterDomain   = "masterDomain"
	ConfigKeyEnableRootDisk = "enableRootDisk"
)

// DataNode defines the structure of a data node.
type DataNode struct {
	space                    *SpaceManager
	port                     string
	httpPort                 string
	zoneName                 string
	clusterID                string
	localIP                  string
	localServerAddr          string
	nodeID                   uint64
	raftDir                  string
	raftHeartbeat            string
	raftReplica              string
	raftStore                raftstore.RaftStore
	tickInterval             int
	tcpListener              net.Listener
	stopC                    chan bool
	fixTinyDeleteRecordLimit uint64
	control                  common.Control
	processStatInfo          *statinfo.ProcessStatInfo
	fetchTopoManager         *fetchtopology.FetchTopologyManager
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
		return errors.New("Invalid Node Type!")
	}
	s.stopC = make(chan bool, 0)
	if err = s.parseSysStartTime(); err != nil {
		return
	}
	// parse the config file
	if err = s.parseConfig(cfg); err != nil {
		return
	}
	repl.SetConnectPool(gConnPool)
	s.register(cfg)
	exporter.Init(exporter.NewOptionFromConfig(cfg).WithCluster(s.clusterID).WithModule(ModuleName).WithZone(s.zoneName))

	_, err = multirate.InitLimiterManager(multirate.ModuleDataNode, s.zoneName, MasterClient.AdminAPI().GetLimitInfo)
	if err != nil {
		return err
	}
	s.fetchTopoManager = fetchtopology.NewFetchTopoManager(time.Minute*5, MasterClient, MasterDomainClient,
		true, false)
	if err = s.fetchTopoManager.Start(); err != nil {
		return
	}

	// start the raft server
	if err = s.startRaftServer(cfg); err != nil {
		return
	}

	// create space manager (disk, partition, etc.)
	if err = s.startSpaceManager(cfg); err != nil {
		exporter.Warning(err.Error())
		return
	}

	// start tcp listening
	if err = s.startTCPService(); err != nil {
		return
	}

	log.LogErrorf("doStart startTCPService finish")

	// Start all loaded data partitions which managed by space manager,
	// this operation will start raft partitions belong to data partitions.
	s.space.StartPartitions()

	async.RunWorker(s.space.AsyncLoadExtent)
	async.RunWorker(s.registerHandler)
	async.RunWorker(s.startUpdateNodeInfo)
	async.RunWorker(s.startUpdateProcessStatInfo)

	statistics.InitStatistics(cfg, s.clusterID, statistics.ModelDataNode, LocalIP, s.rangeMonitorData)

	return
}

func doShutdown(server common.Server) {
	s, ok := server.(*DataNode)
	if !ok {
		return
	}
	close(s.stopC)
	s.space.Stop()
	s.stopUpdateNodeInfo()
	s.stopTCPService()
	s.stopRaftServer()
	if gHasFinishedLoadDisks {
		deleteSysStartTimeFile()
	}
}

func (s *DataNode) parseConfig(cfg *config.Config) (err error) {
	var (
		port       string
		regexpPort *regexp.Regexp
	)
	LocalIP = cfg.GetString(ConfigKeyLocalIP)
	port = cfg.GetString(proto.ListenPort)
	LocalServerPort = port
	if regexpPort, err = regexp.Compile("^(\\d)+$"); err != nil {
		return fmt.Errorf("Err:no port")
	}
	if !regexpPort.MatchString(port) {
		return fmt.Errorf("Err:port must string")
	}
	s.port = port
	s.httpPort = cfg.GetString(proto.HttpPort)
	if len(cfg.GetSlice(proto.MasterAddr)) == 0 {
		return fmt.Errorf("Err:masterAddr unavalid")
	}
	for _, ip := range cfg.GetSlice(proto.MasterAddr) {
		MasterClient.AddNode(ip.(string))
	}
	masterDomain := cfg.GetString(ConfigKeyMasterDomain)
	if masterDomain != "" {
		MasterDomainClient.AddNode(masterDomain)
	}
	s.zoneName = cfg.GetString(ConfigKeyZone)
	if s.zoneName == "" {
		s.zoneName = DefaultZoneName
	}

	s.tickInterval = int(cfg.GetFloat(cfgTickIntervalMs))
	if s.tickInterval <= 300 {
		log.LogWarnf("DataNode: get config %s(%v) less than 300 so set it to 500 ", cfgTickIntervalMs, cfg.GetString(cfgTickIntervalMs))
		s.tickInterval = 500
	}

	log.LogInfof("DataNode: parse config: masterAddrs %v ", MasterClient.Nodes())
	log.LogInfof("DataNode: parse config: port %v", s.port)
	log.LogInfof("DataNode: parse config: zoneName %v ", s.zoneName)
	return
}

// parseSysStartTime maybeServerFaultOccurred is set true only in these two occasions:
// system power off, then restart
// kill -9 the program, then reboot or power off, then restart
func (s *DataNode) parseSysStartTime() (err error) {
	baseDir := getBasePath()
	sysStartFile := path.Join(baseDir, SystemStartTimeFile)
	if _, err = os.Stat(sysStartFile); err != nil {
		if !os.IsNotExist(err) {
			return
		}
		maybeServerFaultOccurred = false
		if err = initSysStartTimeFile(); err != nil {
			log.LogErrorf("parseSysStartTime set system start time has err:%v", err)
		}
	} else {
		bs, err := ioutil.ReadFile(sysStartFile)
		if err != nil {
			return err
		}
		if len(bs) == 0 {
			maybeServerFaultOccurred = false
			if err = initSysStartTimeFile(); err != nil {
				log.LogErrorf("parseSysStartTime set system start time has err:%v", err)
			}
			return err
		}
		localSysStart, err := strconv.ParseInt(strings.TrimSpace(string(bs)), 10, 64)
		if err != nil {
			return err
		}
		newSysStart, err := cpu.SysStartTime()
		if err != nil {
			return err
		}
		log.LogInfof("DataNode: load sys start time: record %d, current %d", localSysStart, newSysStart)

		if maybeServerFaultOccurred = newSysStart-localSysStart > MAX_OFFSET_OF_TIME; maybeServerFaultOccurred {
			log.LogWarnf("DataNode: the program may be started after power off, record %d, current %d", localSysStart, newSysStart)
		}
	}
	return
}

func (s *DataNode) startSpaceManager(cfg *config.Config) (err error) {
	s.space = NewSpaceManager(s)
	if len(strings.TrimSpace(s.port)) == 0 {
		err = ErrNewSpaceManagerFailed
		return
	}

	s.space.SetRaftStore(s.raftStore)
	s.space.SetNodeID(s.nodeID)
	s.space.SetClusterID(s.clusterID)

	var startTime = time.Now()

	// Prepare and validate disk config
	var getDeviceID = func(path string) (devID uint64, err error) {
		var stat = new(syscall.Stat_t)
		if err = syscall.Stat(path, stat); err != nil {
			return
		}
		devID = stat.Dev
		return
	}
	var getDeviceCapacity = func(path string) (capacity uint64, err error) {
		var statfs = new(syscall.Statfs_t)
		if err = syscall.Statfs(path, statfs); err != nil {
			return
		}
		capacity = statfs.Blocks * uint64(statfs.Bsize)
		return
	}

	var rootDevID uint64
	if rootDevID, err = getDeviceID("/"); err != nil {
		return
	}
	log.LogInfof("root device: / (%v)", rootDevID)

	var diskPaths = make(map[uint64]*DiskPath) // DevID -> DiskPath
	for _, d := range cfg.GetSlice(ConfigKeyDisks) {
		var diskPath, ok = ParseDiskPath(d.(string))
		if !ok {
			err = fmt.Errorf("invalid disks configuration: %v", d)
			return
		}
		var devID uint64
		if devID, err = getDeviceID(diskPath.Path()); err != nil {
			return
		}
		if !cfg.GetBool(ConfigKeyEnableRootDisk) && devID == rootDevID {
			err = fmt.Errorf("root device in disks configuration: %v (%v), ", d, devID)
			return
		}
		if p, exists := diskPaths[devID]; exists {
			err = fmt.Errorf("dependent device in disks configuration: [%v,%v]", d, p.Path())
			return
		}
		var capacity uint64
		if capacity, err = getDeviceCapacity(diskPath.Path()); err != nil {
			return
		}
		diskPath.SetReserved(uint64(math.Max(float64(capacity)*unit.NewRatio(DefaultDiskReservedRatio).Float64(), float64(diskPath.Reserved()))))
		log.LogInfof("disk device: %v, path %v, device %v, capacity %v, reserved %v", d, diskPath.Path(), devID, capacity, diskPath.Reserved())
		diskPaths[devID] = diskPath
	}

	var checkExpired CheckExpired
	var requires, fetchErr = s.fetchPersistPartitionIDsFromMaster()
	if fetchErr == nil {
		checkExpired = func(id uint64) bool {
			if len(requires) == 0 {
				return true
			}
			for _, existId := range requires {
				if existId == id {
					return false
				}
			}
			return true
		}
	}

	var futures []*async.Future
	for devID, diskPath := range diskPaths {
		var future = async.NewFuture()
		go func(path *DiskPath, future *async.Future) {
			if log.IsInfoEnabled() {
				log.LogInfof("SPCMGR: loading disk: devID=%v, path=%v", devID, diskPath)
			}
			var err = s.space.LoadDisk(path, checkExpired)
			future.Respond(nil, err)
		}(diskPath, future)
		futures = append(futures, future)
	}
	for _, future := range futures {
		if _, err = future.Response(); err != nil {
			return
		}
	}

	// Check missed partitions
	var misses = make([]uint64, 0)
	for _, id := range requires {
		if dp := s.space.Partition(id); dp == nil {
			misses = append(misses, id)
		}
	}
	if len(misses) > 0 {
		err = fmt.Errorf("lack partitions: %v", misses)
		return
	}

	gHasFinishedLoadDisks = true
	log.LogInfof("SPCMGR: loaded all %v disks elapsed %v", len(diskPaths), time.Since(startTime))
	return nil
}

func (s *DataNode) fetchPersistPartitionIDsFromMaster() (ids []uint64, err error) {
	var dataNode *proto.DataNodeInfo
	for i := 0; i < 3; i++ {
		dataNode, err = MasterClient.NodeAPI().GetDataNode(s.localServerAddr)
		if err != nil {
			log.LogErrorf("DataNode: fetch node info from master failed: %v", err)
			continue
		}
		break
	}
	if err != nil {
		return
	}
	ids = dataNode.PersistenceDataPartitions
	return
}

// registers the data node on the master to report the information such as IsIPV4 address.
// The startup of a data node will be blocked until the registration succeeds.
func (s *DataNode) register(cfg *config.Config) {
	var (
		err error
	)

	timer := time.NewTimer(0)

	// get the IsIPV4 address, cluster ID and node ID from the master
	for {
		select {
		case <-timer.C:
			var ci *proto.ClusterInfo
			if ci, err = MasterClient.AdminAPI().GetClusterInfo(); err != nil {
				log.LogErrorf("DataNode: cannot get ip from master %v: %v",
					MasterClient.Leader(), err)
				timer.Reset(2 * time.Second)
				continue
			}
			masterAddr := MasterClient.Leader()
			s.clusterID = ci.Cluster
			if LocalIP == "" {
				LocalIP = string(ci.Ip)
			}
			s.localServerAddr = fmt.Sprintf("%s:%v", LocalIP, s.port)
			if !unit.IsIPV4(LocalIP) {
				log.LogErrorf("DataNode: got an invalid local ip %v from master %v",
					LocalIP, masterAddr)
				timer.Reset(2 * time.Second)
				continue
			}

			// register this data node on the master
			var nodeID uint64
			if nodeID, err = MasterClient.NodeAPI().AddDataNode(fmt.Sprintf("%s:%v", LocalIP, s.port), s.zoneName, DataNodeLatestVersion); err != nil {
				log.LogErrorf("action[registerToMaster] cannot register this node to master(%v) err(%v).",
					masterAddr, err)
				timer.Reset(2 * time.Second)
				continue
			}
			s.nodeID = nodeID
			log.LogDebugf("DataNode: register success, nodeID %v", s.nodeID)
			return
		case <-s.stopC:
			timer.Stop()
			return
		}
	}
}

type DataNodeInfo struct {
	Addr                      string
	PersistenceDataPartitions []uint64
}

func (s *DataNode) registerHandler() {
	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("DataNode")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})
	http.HandleFunc("/disks", s.getDiskAPI)
	http.HandleFunc("/partitions", s.getPartitionsAPI)
	http.HandleFunc("/partition", s.getPartitionAPI)
	http.HandleFunc("/partitionSimple", s.getPartitionSimpleAPI)
	http.HandleFunc("/partitionRaftHardState", s.getPartitionRaftHardStateAPI)
	http.HandleFunc("/extent", s.getExtentAPI)
	http.HandleFunc("/block", s.getBlockCrcAPI)
	http.HandleFunc("/stats", s.getStatAPI)
	http.HandleFunc("/raftStatus", s.getRaftStatus)
	http.HandleFunc("/setAutoRepairStatus", s.setAutoRepairStatus)
	http.HandleFunc("/releasePartitions", s.releasePartitions)
	http.HandleFunc("/restorePartitions", s.restorePartitions)
	http.HandleFunc("/computeExtentMd5", s.getExtentMd5Sum)
	http.HandleFunc("/stat/info", s.getStatInfo)
	http.HandleFunc("/getReplBufferDetail", s.getReplProtocalBufferDetail)
	http.HandleFunc("/tinyExtentHoleInfo", s.getTinyExtentHoleInfo)
	http.HandleFunc("/playbackTinyExtentMarkDelete", s.playbackPartitionTinyDelete)
	http.HandleFunc("/stopPartition", s.stopPartition)
	http.HandleFunc("/reloadPartition", s.reloadPartition)
	http.HandleFunc("/moveExtent", s.moveExtentFile)
	http.HandleFunc("/moveExtentBatch", s.moveExtentFileBatch)
	http.HandleFunc("/repairExtent", s.repairExtent)
	http.HandleFunc("/repairExtentBatch", s.repairExtentBatch)
	http.HandleFunc("/extentCrc", s.getExtentCrc)
	http.HandleFunc("/fingerprint", s.getFingerprint)
	http.HandleFunc("/resetFaultOccurredCheckLevel", s.resetFaultOccurredCheckLevel)
	http.HandleFunc("/sfxStatus", s.getSfxStatus)
	http.HandleFunc("/getExtentLockInfo", s.getExtentLockInfo)
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
				time.Sleep(time.Second * 5)
				continue
			}
			log.LogDebugf("action[startTCPService] accept connection from %s.", conn.RemoteAddr().String())
			go s.serveConn(conn)
		}
	}(l)
	return
}

func (s *DataNode) stopTCPService() (err error) {
	if s.tcpListener != nil {

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
	if !IsDiskErr(err) {
		return
	}
	if flag == WriteFlag {
		d.incWriteErrCnt()
	} else if flag == ReadFlag {
		d.incReadErrCnt()
	}
}

var (
	staticReflectedErrnoType = reflect.TypeOf(syscall.Errno(0))
)

func IsSysErr(err error) (is bool) {
	if err == nil {
		return
	}
	return reflect.TypeOf(err) == staticReflectedErrnoType
}

func IsDiskErr(err error) bool {
	return err != nil &&
		(strings.Contains(err.Error(), syscall.EIO.Error()) ||
			strings.Contains(err.Error(), syscall.EROFS.Error()))
}

func (s *DataNode) rangeMonitorData(deal func(data *statistics.MonitorData, vol, path string, pid uint64)) {
	s.space.WalkDisks(func(disk *Disk) bool {
		for _, md := range disk.monitorData {
			deal(md, "", disk.Path, 0)
		}
		return true
	})

	s.space.WalkPartitions(func(partition *DataPartition) bool {
		for _, md := range partition.monitorData {
			deal(md, partition.volumeID, partition.Disk().Path, partition.partitionID)
		}
		return true
	})
}

func getBasePath() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}
	return dir
}

func initSysStartTimeFile() (err error) {
	baseDir := getBasePath()
	sysStartTime, err := cpu.SysStartTime()
	if err != nil {
		return
	}
	if err = ioutil.WriteFile(path.Join(baseDir, SystemStartTimeFile), []byte(strconv.FormatUint(uint64(sysStartTime), 10)), os.ModePerm); err != nil {
		return err
	}
	return
}

func deleteSysStartTimeFile() {
	baseDir := getBasePath()
	os.Remove(path.Join(baseDir, SystemStartTimeFile))
}

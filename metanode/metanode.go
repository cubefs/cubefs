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

package metanode

import (
	"fmt"
	syslog "log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/depends/xtaci/smux"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/diskmon"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/fileutil"
	"github.com/cubefs/cubefs/util/log"
)

const (
	RocksdbModeMetaFile = ".rocksdbMode"
)

var (
	gClusterInfo *proto.ClusterInfo
	// masterClient   *masterSDK.MasterClient
	masterClient              *masterSDK.MasterCLientWithResolver
	configTotalMem            uint64
	serverPort                string
	smuxPortShift             int
	smuxPool                  *util.SmuxConnectPool
	smuxPoolCfg               = util.DefaultSmuxConnPoolConfig()
	clusterEnableSnapshot     bool
	legacyReplicaStorageClass uint32 // for compatibility when older version upgrade to hybrid cloud
)

const (
	readDirIops uint32 = 0x01
)

// only used for mp check
func SetLegacyType(t uint32) {
	legacyReplicaStorageClass = t
}

// The MetaNode manages the dentry and inode information of the meta partitions on a meta node.
// The data consistency is ensured by Raft.
type MetaNode struct {
	nodeId                             uint64
	raftPartitionCanUsingDifferentPort bool
	listen                             string
	bindIp                             bool
	metadataDir                        string // root dir of the metaNode
	raftDir                            string // root dir of the raftStore log
	metadataManager                    MetadataManager
	localAddr                          string
	clusterId                          string
	raftStore                          raftstore.RaftStore
	raftHeartbeatPort                  string
	raftReplicatePort                  string
	raftRetainLogs                     uint64
	raftSyncSnapFormatVersion          uint32 // format version of snapshot that raft leader sent to follower
	zoneName                           string
	rack                               string
	region                             string
	httpStopC                          chan uint8
	smuxStopC                          chan uint8
	metrics                            *MetaNodeMetrics
	tickInterval                       int
	raftRecvBufSize                    int
	connectionCnt                      int64
	clusterUuid                        string
	clusterUuidEnable                  bool
	clusterEnableSnapshot              bool
	serviceIDKey                       string
	nodeForbidWriteOpOfProtoVer0       bool                // whether forbid by node granularity,
	VolsForbidWriteOpOfProtoVer0       map[string]struct{} // whether forbid by volume granularity,
	qosEnable                          bool
	readDirIops                        int
	opLimiter                          *OpLimiter

	control common.Control

	rocksDirs          []string
	rocksdbManager     RocksdbManager
	diskStopCh         chan struct{}
	disks              map[string]*diskmon.FsCapMon
	diskReservedSpace  uint64
	rocksdbEnableStats bool
	rocksdbKeyNumMax   uint64
	rocksdbs           []*RocksdbOperator
}

// Start starts up the meta node with the specified configuration.
//  1. Start and load each meta partition from the snapshot.
//  2. Restore raftStore fsm of each meta node range.
//  3. Start server and accept connection from the master and clients.
func (m *MetaNode) Start(cfg *config.Config) (err error) {
	return m.control.Start(m, cfg, doStart)
}

// Shutdown stops the meta node.
func (m *MetaNode) Shutdown() {
	m.control.Shutdown(m, doShutdown)
}

func (m *MetaNode) checkLocalPartitionMatchWithMaster() (err error) {
	var metaNodeInfo *proto.MetaNodeInfo
	for i := 0; i < 3; i++ {
		if metaNodeInfo, err = masterClient.NodeAPI().GetMetaNode(fmt.Sprintf("%s:%s", m.localAddr, m.listen)); err != nil {
			log.LogErrorf("checkLocalPartitionMatchWithMaster: get MetaNode info fail: err(%v)", err)
			continue
		}
		break
	}

	if err != nil {
		return
	}

	if len(metaNodeInfo.PersistenceMetaPartitions) == 0 {
		return
	}

	lackPartitions := make([]uint64, 0, len(metaNodeInfo.PersistenceMetaPartitions))
	for _, partitionID := range metaNodeInfo.PersistenceMetaPartitions {
		if _, err := m.metadataManager.GetPartition(partitionID); err != nil {
			lackPartitions = append(lackPartitions, partitionID)
		}
	}

	if len(lackPartitions) == 0 {
		return
	}

	nodeAddr := m.localAddr + ":" + m.listen
	m.metrics.MetricMetaFailedPartition.SetWithLabels(float64(1), map[string]string{
		"partids": fmt.Sprintf("%v", lackPartitions),
		"node":    nodeAddr,
		"nodeid":  fmt.Sprintf("%d", m.nodeId),
	})
	log.LogErrorf("LackPartitions %v on metanode %v, please deal quickly", lackPartitions, nodeAddr)
	return
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	m, ok := s.(*MetaNode)
	if !ok {
		return errors.New("Invalid node Type!")
	}

	// Parse configuration
	if err = m.parseConfig(cfg); err != nil {
		return
	}

	// Init metrics early so startup-time checks can emit alerts.
	m.startStat()
	defer func() {
		if err != nil {
			m.stopStat()
		}
	}()

	// Initialize components
	if err = m.newRocksdbManager(cfg); err != nil {
		return
	}
	if err = m.startDiskStat(); err != nil {
		return
	}
	if err = m.register(); err != nil {
		return
	}

	// Start services
	if err = m.startRaftServer(cfg); err != nil {
		return
	}
	if err = m.newMetaManager(cfg); err != nil {
		return
	}

	if err = m.startServer(); err != nil {
		return
	}
	if err = m.startSmuxServer(); err != nil {
		return
	}
	if err = m.startMetaManager(); err != nil {
		return
	}
	if err = m.registerAPIHandler(); err != nil {
		return
	}

	go m.startUpdateNodeInfo()

	exporter.RegistConsul(m.clusterId, cfg.GetString("role"), cfg)

	// Check local partition compare with master, if lack, then not start
	if err = m.checkLocalPartitionMatchWithMaster(); err != nil {
		syslog.Println(err)
		exporter.Warning(err.Error())
		return
	}

	return
}

func doShutdown(s common.Server) {
	m, ok := s.(*MetaNode)
	if !ok {
		return
	}
	m.stopRocksdbManager()
	m.stopDiskStat()
	m.stopUpdateNodeInfo()
	// shutdown node and release the resource
	m.stopStat()
	m.stopServer()
	m.stopSmuxServer()
	m.stopMetaManager()
	m.stopRaftServer()
	masterClient.Stop()
}

// Sync blocks the invoker's goroutine until the meta node shuts down.
func (m *MetaNode) Sync() {
	m.control.Sync()
}

func (m *MetaNode) parseConfig(cfg *config.Config) (err error) {
	if cfg == nil {
		return errors.New("invalid configuration")
	}

	// Parse basic configuration
	m.localAddr = cfg.GetString(cfgLocalIP)
	m.listen = cfg.GetString(proto.ListenPort)
	m.bindIp = cfg.GetBool(proto.BindIpKey)
	serverPort = m.listen
	m.metadataDir = cfg.GetString(cfgMetadataDir)
	m.raftDir = cfg.GetString(cfgRaftDir)
	m.raftHeartbeatPort = cfg.GetString(cfgRaftHeartbeatPort)
	m.raftReplicatePort = cfg.GetString(cfgRaftReplicaPort)
	m.tickInterval = int(cfg.GetFloat(cfgTickInterval))
	m.raftRecvBufSize = int(cfg.GetInt(cfgRaftRecvBufSize))
	m.zoneName = cfg.GetString(cfgZoneName)
	m.rack = cfg.GetString(cfgRack)
	m.region = cfg.GetString(cfgRegion)
	if m.region == "" {
		m.region = "default"
	}

	// Parse delete batch count
	if deleteBatchCount := cfg.GetInt64(cfgDeleteBatchCount); deleteBatchCount > 1 {
		updateDeleteBatchCount(uint64(deleteBatchCount))
	}

	m.serviceIDKey = cfg.GetString(cfgServiceIDKey)

	// Parse memory configuration
	if err = m.parseMemoryConfig(cfg); err != nil {
		return
	}

	// Validate required configurations
	if err = m.validateRequiredConfigs(); err != nil {
		return
	}

	// Parse QoS configuration
	m.qosEnable = cfg.GetBoolWithDefault(cfsQosEnable, true)
	m.readDirIops = cfg.GetInt(cfgReadDirIops)
	if m.readDirIops <= 0 {
		m.readDirIops = defaultReadDirIops
	}
	syslog.Printf("conf qosEnable=%v readDirIops=%v", m.qosEnable, m.readDirIops)
	log.LogInfof("[parseConfig] qosEnable[%v] readDirIops[%v]", m.qosEnable, m.readDirIops)

	// Parse raft configuration
	if err = m.parseRaftConfig(cfg); err != nil {
		return
	}

	// Parse rocksdb configuration
	if err = m.parseRocksdbConfig(cfg); err != nil {
		return
	}

	// Parse smux configuration
	if err = m.parseSmuxConfig(cfg); err != nil {
		return fmt.Errorf("parseSmuxConfig fail err %v", err)
	}
	log.LogInfof("Start: init smux conn pool (%v).", smuxPoolCfg)
	smuxPool = util.NewSmuxConnectPool(smuxPoolCfg)

	// Parse master configuration
	if err = m.parseMasterConfig(cfg); err != nil {
		return
	}

	return m.validConfig()
}

func (m *MetaNode) parseMemoryConfig(cfg *config.Config) error {
	total, _, err := util.GetMemInfo()
	if err != nil {
		log.LogErrorf("get total mem failed, err %s", err.Error())
	}

	ratioStr := cfg.GetString(cfgMemRatio)
	if err == nil && ratioStr != "" {
		ratio, _ := strconv.Atoi(ratioStr)
		if ratio <= 0 || ratio >= 100 {
			return fmt.Errorf("cfgMemRatio is not legal, should be between 1-100, now %s", ratioStr)
		}

		configTotalMem = total * uint64(ratio) / 100
		log.LogInfof("configTotalMem by ratio is: mem [%d], ratio[%d]", configTotalMem, ratio)
	} else {
		configTotalMem, _ = strconv.ParseUint(cfg.GetString(cfgTotalMem), 10, 64)
		if configTotalMem == 0 {
			return fmt.Errorf("bad totalMem config, recommended to be configured as 80 percent of physical machine memory")
		}
	}

	if err == nil && configTotalMem > total-util.GB {
		return fmt.Errorf("bad totalMem config, recommended to be configured as 80 percent of physical machine memory")
	}

	return nil
}

func (m *MetaNode) validateRequiredConfigs() error {
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
	return nil
}

func (m *MetaNode) parseRaftConfig(cfg *config.Config) error {
	m.opLimiter = newOpLimiter()
	// Load operation rate limiter configuration
	if err := m.loadOpLimiterConfig(cfg); err != nil {
		return err
	}

	syslog.Printf("conf qosEnable=%v readDirIops=%v", m.qosEnable, m.readDirIops)
	log.LogInfof("[parseConfig] qosEnable[%v] readDirIops[%v]", m.qosEnable, m.readDirIops)

	raftRetainLogs := cfg.GetString(cfgRetainLogs)
	if raftRetainLogs != "" {
		var err error
		if m.raftRetainLogs, err = strconv.ParseUint(raftRetainLogs, 10, 64); err != nil {
			return fmt.Errorf("%v, err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	if m.raftRetainLogs <= 0 {
		m.raftRetainLogs = DefaultRaftNumOfLogsToRetain
	}
	syslog.Println("conf raftRetainLogs=", m.raftRetainLogs)
	log.LogInfof("[parseConfig] raftRetainLogs[%v]", m.raftRetainLogs)

	// Parse raft sync snap format version
	if cfg.HasKey(cfgRaftSyncSnapFormatVersion) {
		raftSyncSnapFormatVersion := uint32(cfg.GetInt64(cfgRaftSyncSnapFormatVersion))
		if raftSyncSnapFormatVersion > SnapFormatVersion_1 {
			m.raftSyncSnapFormatVersion = SnapFormatVersion_1
			log.LogInfof("invalid config raftSyncSnapFormatVersion, using default[%v]", m.raftSyncSnapFormatVersion)
		} else {
			m.raftSyncSnapFormatVersion = raftSyncSnapFormatVersion
			log.LogInfof("by config raftSyncSnapFormatVersion:[%v]", m.raftSyncSnapFormatVersion)
		}
	} else {
		m.raftSyncSnapFormatVersion = SnapFormatVersion_1
		log.LogInfof("using default raftSyncSnapFormatVersion[%v]", m.raftSyncSnapFormatVersion)
	}
	syslog.Println("conf raftSyncSnapFormatVersion=", m.raftSyncSnapFormatVersion)
	log.LogInfof("[parseConfig] raftSyncSnapFormatVersion[%v]", m.raftSyncSnapFormatVersion)

	return nil
}

func (m *MetaNode) parseRocksdbConfig(cfg *config.Config) error {
	m.rocksDirs = cfg.GetStringSlice(cfgRocksDirs)

	// Create db directories
	for _, dbDir := range m.rocksDirs {
		if !fileutil.ExistDir(dbDir) {
			if err := os.MkdirAll(dbDir, 0o755); err != nil {
				log.LogErrorf("[parseConfig] failed to create rocksdb db dir(%v), err(%v)", dbDir, err)
				return err
			}
		}
	}

	m.diskReservedSpace, _ = strconv.ParseUint(cfg.GetString(cfgDiskReservedSpace), 10, 64)
	if m.diskReservedSpace == 0 || m.diskReservedSpace < defaultDiskReservedSpace {
		m.diskReservedSpace = defaultDiskReservedSpace
	}

	// Check and store constant configuration
	constCfg := config.ConstConfig{
		Listen:           m.listen,
		RaftHeartbetPort: m.raftHeartbeatPort,
		RaftReplicaPort:  m.raftReplicatePort,
	}
	ok, err := config.CheckOrStoreConstCfg(m.metadataDir, config.DefaultConstConfigFile, &constCfg)
	if !ok {
		log.LogErrorf("constCfg check failed %v %v %v %v", m.metadataDir, config.DefaultConstConfigFile, constCfg, err)
		return fmt.Errorf("constCfg check failed %v %v %v %v", m.metadataDir, config.DefaultConstConfigFile, constCfg, err)
	}

	log.LogInfof("[parseConfig] load localAddr[%v].", m.localAddr)
	log.LogInfof("[parseConfig] load listen[%v].", m.listen)
	log.LogInfof("[parseConfig] load metadataDir[%v].", m.metadataDir)
	log.LogInfof("[parseConfig] load raftDir[%v].", m.raftDir)
	log.LogInfof("[parseConfig] load raftHeartbeatPort[%v].", m.raftHeartbeatPort)
	log.LogInfof("[parseConfig] load raftReplicatePort[%v].", m.raftReplicatePort)
	log.LogInfof("[parseConfig] load zoneName[%v], rack[%v].", m.zoneName, m.rack)

	return nil
}

func (m *MetaNode) parseMasterConfig(cfg *config.Config) error {
	addrs := cfg.GetSlice(proto.MasterAddr)
	masters := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		masters = append(masters, addr.(string))
	}

	updateInterval := cfg.GetInt(configNameResolveInterval)
	if updateInterval <= 0 || updateInterval > 60 {
		log.LogWarnf("name resolving interval[1-60] is set to default: %v", DefaultNameResolveInterval)
		updateInterval = DefaultNameResolveInterval
	}

	masterClient = masterSDK.NewMasterCLientWithResolver(masters, false, updateInterval)
	if masterClient == nil {
		err := fmt.Errorf("parseConfig: masters addrs format err[%v]", masters)
		log.LogErrorf("parseConfig: masters addrs format err[%v]", masters)
		return err
	}

	poolSize := cfg.GetInt64(proto.CfgHttpPoolSize)
	syslog.Printf("parseConfig: http pool size %d", poolSize)
	masterClient.SetTransport(proto.GetHttpTransporter(&proto.HttpCfg{PoolSize: int(poolSize)}))

	return masterClient.Start()
}

func (m *MetaNode) parseSmuxConfig(cfg *config.Config) error {
	// SMux port
	smuxPortShift = cfg.GetInt(cfgSmuxPortShift)
	if smuxPortShift == 0 {
		smuxPortShift = util.DefaultSmuxPortShift
	}

	// SMux buffer
	maxBuffer := cfg.GetInt(cfgSmuxMaxBuffer)
	if maxBuffer > 0 {
		smuxPoolCfg.MaxReceiveBuffer = maxBuffer
		if smuxPoolCfg.MaxStreamBuffer > maxBuffer {
			smuxPoolCfg.MaxStreamBuffer = maxBuffer
		}

		if err := smux.VerifyConfig(smuxPoolCfg.Config); err != nil {
			return err
		}
	}

	maxConn := cfg.GetInt(cfgSmuxMaxConn)
	if maxConn > 0 {
		smuxPoolCfg.ConnsPerAddr = maxConn
	}

	maxStreamPerConn := cfg.GetInt(cfgSmuxStreamPerConn)
	if maxStreamPerConn > 0 {
		smuxPoolCfg.StreamsPerConn = maxStreamPerConn
	}

	if err := util.VerifySmuxPoolConfig(smuxPoolCfg); err != nil {
		return err
	}

	log.LogDebugf("[parseSmuxConfig] cfg %v.", smuxPoolCfg)
	return nil
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
	if len(masterClient.Nodes()) == 0 {
		err = errors.New("master address list is empty")
		return
	}
	return
}

func (m *MetaNode) newMetaManager(cfg *config.Config) (err error) {
	if _, err = os.Stat(m.metadataDir); err != nil {
		if err = os.MkdirAll(m.metadataDir, 0o755); err != nil {
			return
		}
	}

	if m.clusterUuidEnable {
		if err = config.CheckOrStoreClusterUuid(m.metadataDir, m.clusterUuid, false); err != nil {
			log.LogErrorf("CheckOrStoreClusterUuid failed: %v", err)
			return fmt.Errorf("CheckOrStoreClusterUuid failed: %v", err)
		}
	}

	constCfg := config.ConstConfig{
		Listen:           m.listen,
		RaftHeartbetPort: m.raftHeartbeatPort,
		RaftReplicaPort:  m.raftReplicatePort,
	}
	ok := false
	if ok, err = config.CheckOrStoreConstCfg(m.metadataDir, config.DefaultConstConfigFile, &constCfg); !ok {
		log.LogErrorf("constCfg check failed %v %v %v %v", m.metadataDir, config.DefaultConstConfigFile, constCfg, err)
		return fmt.Errorf("constCfg check failed %v %v %v %v", m.metadataDir, config.DefaultConstConfigFile, constCfg, err)
	}

	// Parse GC recycle percent
	gcRecyclePercent, err := m.parseGcRecyclePercent(cfg)
	if err != nil {
		return err
	}

	// Parse rocksdb disk usage threshold
	rocksDBDiskUsageThreshold, err := m.parseRocksDBDiskUsageThreshold(cfg)
	if err != nil {
		return err
	}

	// Load metadataManager
	conf := MetadataManagerConfig{
		NodeID:                    m.nodeId,
		RootDir:                   m.metadataDir,
		RaftStore:                 m.raftStore,
		ZoneName:                  m.zoneName,
		Rack:                      m.rack,
		EnableGcTimer:             cfg.GetBoolWithDefault(cfgEnableGcTimer, false),
		GcRecyclePercent:          gcRecyclePercent,
		RocksDBDiskUsageThreshold: rocksDBDiskUsageThreshold,
	}
	m.metadataManager = NewMetadataManager(conf, m)
	return
}

func (m *MetaNode) parseGcRecyclePercent(cfg *config.Config) (float64, error) {
	gcRecyclePercentStr := cfg.GetString(CfgGcRecyclePercent)
	if gcRecyclePercentStr == "" {
		return defaultGcRecyclePercent, nil
	}

	gcRecyclePercent, err := strconv.ParseFloat(gcRecyclePercentStr, 64)
	if err != nil {
		err = fmt.Errorf("parse configKey[%v] failed: %v", CfgGcRecyclePercent, err.Error())
		log.LogError(err.Error())
		return 0, err
	}
	return gcRecyclePercent, nil
}

func (m *MetaNode) parseRocksDBDiskUsageThreshold(cfg *config.Config) (float64, error) {
	rocksDBDiskUsageThresholdStr := cfg.GetString(CfgRocksDBDiskUsageThreshold)
	if rocksDBDiskUsageThresholdStr == "" {
		return 0.8, nil
	}

	rocksDBDiskUsageThreshold, err := strconv.ParseFloat(rocksDBDiskUsageThresholdStr, 64)
	if err != nil {
		err = fmt.Errorf("parse configKey[%v] failed: %v", CfgRocksDBDiskUsageThreshold, err.Error())
		log.LogError(err.Error())
		return 0, err
	}
	return rocksDBDiskUsageThreshold, nil
}

func (m *MetaNode) startMetaManager() (err error) {
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
	tryCnt := 0
	var nodeAddress string
	var volsForbidWriteOpVerMsg string
	var nodeForbidWriteOpOfProtoVerMsg string
	var legacyReplicaStorageClassMsg string

	for {
		tryCnt++
		gClusterInfo, err = getClusterInfo()
		if err != nil {
			log.LogErrorf("[register] tryCnt(%v), getClusterInfo err: %s", tryCnt, err.Error())
			time.Sleep(3 * time.Second)
			continue
		}
		if m.localAddr == "" {
			m.localAddr = gClusterInfo.Ip
		}
		m.clusterUuid = gClusterInfo.ClusterUuid
		m.clusterUuidEnable = gClusterInfo.ClusterUuidEnable
		m.clusterEnableSnapshot = gClusterInfo.ClusterEnableSnapshot
		clusterEnableSnapshot = m.clusterEnableSnapshot
		m.clusterId = gClusterInfo.Cluster
		m.raftPartitionCanUsingDifferentPort = gClusterInfo.RaftPartitionCanUsingDifferentPort
		nodeAddress = m.localAddr + ":" + m.listen

		var settingsFromMaster *proto.UpgradeCompatibleSettings
		if settingsFromMaster, err = getUpgradeCompatibleSettings(); err != nil {
			if strings.Contains(err.Error(), proto.KeyWordInHttpApiNotSupportErr) {
				// master may be lower version and has no this API
				err = fmt.Errorf("getUpgradeCompatibleSettings from master failed for current master version not support this API")
				volsForbidWriteOpVerMsg = "[register] master version has no api GetUpgradeCompatibleSettings, ues default value"
				log.LogError(volsForbidWriteOpVerMsg)
				syslog.Printf("%v \n", volsForbidWriteOpVerMsg)
				return err
			}
			log.LogErrorf("[register] tryCnt(%v), GetUpgradeCompatibleSettings from master err: %v", tryCnt, err)
			time.Sleep(3 * time.Second)
			continue
		}

		if !settingsFromMaster.DataMediaTypeVaild {
			err = fmt.Errorf("getUpgradeCompatibleSettings from master: master not set vaild mediaType, cfg %v", settingsFromMaster)
			log.LogError(err)
			return
		}

		volListForbidWriteOpOfProtoVer0 := settingsFromMaster.VolsForbidWriteOpOfProtoVer0
		volsForbidWriteOpVerMsg = fmt.Sprintf("[register] from master, volumes forbid write operate of proto version-0: %v",
			volListForbidWriteOpOfProtoVer0)

		m.nodeForbidWriteOpOfProtoVer0 = settingsFromMaster.ClusterForbidWriteOpOfProtoVer0
		nodeForbidWriteOpOfProtoVerMsg = fmt.Sprintf("[register] from master, cluster node forbid write Operate Of proto version-0: %v",
			m.nodeForbidWriteOpOfProtoVer0)

		legacyReplicaStorageClass = proto.GetMediaTypeByStorageClass(settingsFromMaster.LegacyDataMediaType)

		volMapForbidWriteOpOfProtoVer0 := make(map[string]struct{})
		for _, vol := range volListForbidWriteOpOfProtoVer0 {
			if _, ok := volMapForbidWriteOpOfProtoVer0[vol]; !ok {
				volMapForbidWriteOpOfProtoVer0[vol] = struct{}{}
			}
		}
		m.VolsForbidWriteOpOfProtoVer0 = volMapForbidWriteOpOfProtoVer0

		var nodeID uint64
		if nodeID, err = masterClient.NodeAPI().AddMetaNodeWithAuthNode(nodeAddress, m.raftHeartbeatPort, m.raftReplicatePort, m.rack, m.zoneName, m.serviceIDKey, m.region); err != nil {
			log.LogErrorf("[register] tryCnt(%v), register to master fail: address(%v) err(%s)", tryCnt, nodeAddress, err)
			time.Sleep(3 * time.Second)
			continue
		}
		m.nodeId = nodeID

		if proto.IsStorageClassReplica(legacyReplicaStorageClass) {
			legacyReplicaStorageClassMsg = fmt.Sprintf("[register] from master, legacyReplicaStorageClass(%v)",
				proto.StorageClassString(legacyReplicaStorageClass))
			log.LogInfo(legacyReplicaStorageClassMsg)
		} else {
			legacyReplicaStorageClassMsg = fmt.Sprintf("[register] from master, invalid legacyReplicaStorageClass(%v)",
				settingsFromMaster.LegacyDataMediaType)
			legacyReplicaStorageClass = proto.StorageClass_Unspecified
			log.LogWarn(legacyReplicaStorageClassMsg)
		}
		syslog.Printf("%v \n", legacyReplicaStorageClassMsg)

		log.LogInfo(nodeForbidWriteOpOfProtoVerMsg)
		syslog.Printf("%v \n", nodeForbidWriteOpOfProtoVerMsg)
		log.LogInfo(volsForbidWriteOpVerMsg)
		return
	}
}

// NewServer creates a new meta node instance.
func NewServer() *MetaNode {
	return &MetaNode{}
}

func getClusterInfo() (ci *proto.ClusterInfo, err error) {
	ci, err = masterClient.AdminAPI().GetClusterInfo()
	return
}

// AddConnection adds a connection.
func (m *MetaNode) AddConnection() {
	atomic.AddInt64(&m.connectionCnt, 1)
}

// RemoveConnection removes a connection.
func (m *MetaNode) RemoveConnection() {
	atomic.AddInt64(&m.connectionCnt, -1)
}

// loadOpLimiterConfig loads operation rate limiter configuration from config file
func (m *MetaNode) loadOpLimiterConfig(cfg *config.Config) error {
	supportedOps := make(map[string]bool)
	for opName := range proto.GOpInfo {
		supportedOps[opName] = true
	}

	configuredCount := 0

	for opName := range supportedOps {
		customLimit := cfg.GetInt(fmt.Sprintf("opLimit_%s", opName))
		customTimeout := cfg.GetInt(fmt.Sprintf("opTimeout_%s", opName))

		if customLimit > 0 {
			timeout := uint32(0)
			if customTimeout > 0 {
				timeout = uint32(customTimeout)
			}

			if err := m.opLimiter.SetLimiter(opName, uint32(customLimit), timeout); err != nil {
				log.LogWarnf("failed to set op limiter for %s: %v", opName, err)
			} else {
				log.LogInfof("set op limiter: %s limit=%d timeout=%d", opName, customLimit, timeout)
				configuredCount++
			}
		}
	}

	if configuredCount > 0 {
		log.LogInfof("op limiter loaded with %d configured operations", configuredCount)
	} else {
		log.LogInfof("op limiter loaded with no configured operations - all operations unlimited")
	}

	return nil
}

func getUpgradeCompatibleSettings() (volListForbidWriteOpOfProtoVer0 *proto.UpgradeCompatibleSettings, err error) {
	volListForbidWriteOpOfProtoVer0, err = masterClient.AdminAPI().GetUpgradeCompatibleSettings()
	return
}

func (m *MetaNode) hasPartitions() (ok bool, err error) {
	dentries, err := os.ReadDir(m.metadataDir)
	if err != nil {
		return
	}
	for _, dentry := range dentries {
		if strings.HasPrefix(dentry.Name(), rocksdbMpPrefix) {
			ok = true
			return
		}
	}
	return
}

func (m *MetaNode) persistRocksdbMode(mode string) (err error) {
	file := path.Join(m.metadataDir, RocksdbModeMetaFile)
	tmpFile := file + ".tmp"
	err = os.WriteFile(tmpFile, []byte(mode), 0o644)
	if err != nil {
		return
	}
	err = os.Rename(tmpFile, file)
	return
}

func (m *MetaNode) readRocksdbMode() (mode string, err error) {
	file := path.Join(m.metadataDir, RocksdbModeMetaFile)
	v, err := os.ReadFile(file)
	if err != nil {
		return
	}
	mode = string(v)
	return
}

func (m *MetaNode) newRocksdbManager(cfg *config.Config) (err error) {
	writeBufferSize := cfg.GetInt(cfgRocksdbWriteBufferSize)
	blockCacheSize := cfg.GetInt64(cfgRocksdbBlockCacheSize)
	writeBufferNum := cfg.GetInt(cfgRocksdbWriteBufferNum)
	minWriteBufferToMerge := cfg.GetInt(cfgRocksdbMinWriteBufferToMerge)
	maxSubCompactions := cfg.GetInt(cfgRocksdbMaxSubCompactions)
	mode := cfg.GetString(cfgRocksdbMode)
	if mode == "" {
		mode = defaultRocksdMode
	}
	rocksdbMode := ParseRocksdbMode(mode)
	rocksdbEnableStats := cfg.GetBoolWithDefault(cfgRocksdbEnableStats, true)
	m.rocksdbEnableStats = rocksdbEnableStats
	rocksdbKeyNumMax := cfg.GetInt64(cfgRocksdbKeyNumMax)
	if rocksdbKeyNumMax <= 0 {
		rocksdbKeyNumMax = defaultRocksdbKeyNumMax
	}
	m.rocksdbKeyNumMax = uint64(rocksdbKeyNumMax)
	bytesPerSync := cfg.GetInt64(cfgRocksdbBytesPerSync)
	if bytesPerSync < 0 {
		bytesPerSync = 0
	}
	parallelism := cfg.GetInt(cfgRocksdbParallelism)
	maxBackgroundCompactions := cfg.GetInt(cfgRocksdbMaxBackgroundCompactions)
	maxBackgroundFlushes := cfg.GetInt(cfgRocksdbMaxBackgroundFlushes)
	softCompactionLimit := cfg.GetInt64(cfgRocksdbSoftCompactionLimit)
	if softCompactionLimit < 0 {
		softCompactionLimit = 0
	}
	hardCompactionLimit := cfg.GetInt64(cfgRocksdbHardCompactionLimit)
	if hardCompactionLimit < 0 {
		hardCompactionLimit = 0
	}
	periodicCompactSec := cfg.GetInt64WithDefault(cfsRocksdbPeriodicCompactSecond, defaultPeriodicCompactSec)

	rocksdbModeFile := path.Join(m.metadataDir, RocksdbModeMetaFile)
	if fileutil.Exist(rocksdbModeFile) {
		var tmp string
		if tmp, err = m.readRocksdbMode(); err != nil {
			return
		}
		// NOTE: check rocksdb mode consistence
		if mode != tmp {
			log.LogWarnf("[newRocksdbManager] inconsistent rocksdb config meta file(%v) persist(%v) config(%v)", rocksdbModeFile, tmp, mode)
			var hasMp bool
			hasMp, err = m.hasPartitions()
			if err != nil {
				log.LogErrorf("[newRocksdbManager] failed to get persist mp cnt, err(%v)", err)
				return
			}
			if hasMp {
				log.LogErrorf("[newRocksdbManager] failed to change rocksdb mode(%v) to new(%v), has meta partitions on disk", tmp, mode)
				return errors.NewErrorf("cannot init rocksdb manager, inconsistent rocksdb mode")
			}
			log.LogWarnf("[newRocksdbManager] change rocksdb mode(%v) to new(%v)", tmp, mode)
		}
		rocksdbMode = ParseRocksdbMode(mode)
	}
	// NOTE: persist rocksdb mode to disk
	err = m.persistRocksdbMode(mode)
	if err != nil {
		log.LogErrorf("[newRocksdbManager] failed to persist rocksdb mode(%v) to meta file(%v), err(%v)", mode, rocksdbModeFile, err)
		return
	}

	config := &RocksdbManagerConfig{
		WriteBufferSize:          writeBufferSize,
		WriteBufferNum:           writeBufferNum,
		MinWriteBuffToMerge:      minWriteBufferToMerge,
		MaxSubCompactions:        maxSubCompactions,
		BlockCacheSize:           uint64(blockCacheSize),
		EnableStats:              rocksdbEnableStats,
		BytesPerSync:             uint64(bytesPerSync),
		Parallelism:              parallelism,
		MaxBackgroundCompactions: maxBackgroundCompactions,
		MaxBackgroundFlushes:     maxBackgroundFlushes,
		SoftCompactionLimit:      uint64(softCompactionLimit),
		HardCompactionLimit:      uint64(hardCompactionLimit),
		PeriodicCompactSec:       periodicCompactSec,
	}
	config = AdjustRocksdbOptions(config)
	if rocksdbMode == PerDiskRocksdbMode {
		m.rocksdbManager = NewPerDiskRocksdbManager(config)
	} else {
		m.rocksdbManager = NewPerPartitionRocksdbManager(config)
	}
	m.rocksdbs = make([]*RocksdbOperator, len(m.rocksDirs))

	for i, dbPath := range m.rocksDirs {
		m.warnIfNotNvmeDevice(dbPath)

		err = m.rocksdbManager.Register(dbPath)
		if err != nil {
			log.LogErrorf("[newRocksdbManager] failed to register rocksdb(%v), err(%v)", dbPath, err)
			m.stopRocksdbManager()
			return
		}
		m.rocksdbs[i], err = m.rocksdbManager.OpenRocksdb(dbPath, 0)
		if err != nil {
			log.LogErrorf("[newRocksdbManager] failed to open rocksdb(%v), err(%v)", dbPath, err)
			m.stopRocksdbManager()
			return
		}
	}
	return
}

func (m *MetaNode) stopRocksdbManager() {
	for _, db := range m.rocksdbs {
		if db != nil {
			m.rocksdbManager.CloseRocksdb(db)
		}
	}
	m.rocksdbs = nil
}

func AdjustRocksdbOptions(config *RocksdbManagerConfig) *RocksdbManagerConfig {
	if config.BlockCacheSize == 0 {
		rocksdbBlockCacheSize := configTotalMem / 5
		if rocksdbBlockCacheSize < DefaultCacheSize {
			config.BlockCacheSize = DefaultCacheSize
		} else {
			config.BlockCacheSize = rocksdbBlockCacheSize
		}
	}

	return config
}

func (m *MetaNode) warnIfNotNvmeDevice(dbPath string) {
	// Check disk type for rocksdbDir: alert if not NVMe.
	if m.metrics != nil && m.metrics.RocksdbNonNvmeDisk != nil {
		// Default to healthy; will be overwritten to 1 on non-NVMe.
		m.metrics.RocksdbNonNvmeDisk.SetWithLabelValues(0, dbPath)
	}
	if ok, dev, chkErr := isNvmeDisk(dbPath); chkErr != nil {
		log.LogWarnf("[newRocksdbManager] failed to detect disk type for rocksdbDir(%v): err(%v)", dbPath, chkErr)
	} else if !ok {
		log.LogWarnf("[newRocksdbManager] rocksdbDir(%v) is not on NVMe device(%v)", dbPath, dev)
		if m.metrics != nil && m.metrics.RocksdbNonNvmeDisk != nil {
			m.metrics.RocksdbNonNvmeDisk.SetWithLabelValues(1, dbPath)
		}
	}
}

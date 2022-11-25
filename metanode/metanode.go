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
	"fmt"
	"github.com/chubaofs/chubaofs/util/statinfo"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/raftstore"
	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/diskusage"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/memory"
	"github.com/chubaofs/chubaofs/util/statistics"
	"github.com/chubaofs/chubaofs/util/unit"
)

var (
	clusterInfo    *proto.ClusterInfo
	masterClient   *masterSDK.MasterClient
	configTotalMem uint64
	serverPort     string
)

// The MetaNode manages the dentry and inode information of the meta partitions on a meta node.
type MetaNode struct {
	nodeId            uint64
	listen            string
	profPort          string
	metadataDir       string // root dir of the metaNode
	raftDir           string // root dir of the raftStore log
	metadataManager   MetadataManager
	localAddr         string
	clusterId         string
	raftStore         raftstore.RaftStore
	raftHeartbeatPort string
	raftReplicatePort string
	tickInterval      int
	zoneName          string
	httpStopC         chan uint8
	processStatInfo   *statinfo.ProcessStatInfo
	diskReservedSpace uint64
	rocksDirs         []string
	diskStopCh        chan struct{}
	disks             map[string]*diskusage.FsCapMon
	control           common.Control
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

	if metaNodeInfo == nil || len(metaNodeInfo.PersistenceMetaPartitions) == 0 {
		return
	}
	lackPartitions := make([]uint64, 0)
	for _, partitionID := range metaNodeInfo.PersistenceMetaPartitions {
		_, err := m.metadataManager.GetPartition(partitionID)
		if err != nil {
			lackPartitions = append(lackPartitions, partitionID)
		}
	}
	if len(lackPartitions) == 0 {
		return
	}
	err = fmt.Errorf("LackPartitions %v on metanode %v,metanode cannot start", lackPartitions, m.localAddr+":"+m.listen)
	log.LogErrorf(err.Error())
	return
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	m, ok := s.(*MetaNode)
	if !ok {
		return errors.New("Invalid Node Type!")
	}
	if err = m.parseConfig(cfg); err != nil {
		return
	}
	if err = m.startDiskStat(); err != nil {
		return
	}
	if err = m.register(); err != nil {
		return
	}

	if err = m.startRaftServer(); err != nil {
		return
	}
	if err = m.loadMetaPartitions(); err != nil {
		return
	}
	if err = m.registerAPIHandler(); err != nil {
		return
	}

	go m.startUpdateNodeInfo()

	exporter.Init(m.clusterId, cfg.GetString("role"), cfg)

	// check local partition compare with master ,if lack,then not start
	if err = m.checkLocalPartitionMatchWithMaster(); err != nil {
		fmt.Println(err)
		exporter.Warning(err.Error())
		return
	}

	if err = m.startServer(); err != nil {
		return
	}

	if err = m.startMetaPartitions(); err != nil {
		return
	}

	exporter.RegistConsul(cfg)

	statistics.InitStatistics(cfg, m.clusterId, statistics.ModelMetaNode, m.localAddr, m.metadataManager.SummaryMonitorData)

	go m.startUpdateProcessStatInfo()

	return
}

func doShutdown(s common.Server) {
	m, ok := s.(*MetaNode)
	if !ok {
		return
	}
	m.stopUpdateNodeInfo()
	// shutdown node and release the resource
	m.failOverLeaderMp()
	m.stopServer()
	m.stopMetaManager()
	m.stopRaftServer()
	m.stopDiskStat()
}

// Sync blocks the invoker's goroutine until the meta node shuts down.
func (m *MetaNode) Sync() {
	m.control.Sync()
}

func (m *MetaNode) parseConfig(cfg *config.Config) (err error) {
	if cfg == nil {
		err = errors.New("invalid configuration")
		return
	}
	m.localAddr = cfg.GetString(cfgLocalIP)
	m.listen = cfg.GetString(proto.ListenPort)
	serverPort = m.listen
	m.profPort = cfg.GetString(cfgProfPort)
	m.metadataDir = cfg.GetString(cfgMetadataDir)
	m.raftDir = cfg.GetString(cfgRaftDir)
	m.raftHeartbeatPort = cfg.GetString(cfgRaftHeartbeatPort)
	m.raftReplicatePort = cfg.GetString(cfgRaftReplicaPort)
	m.zoneName = cfg.GetString(cfgZoneName)
	configTotalMem, _ = strconv.ParseUint(cfg.GetString(cfgTotalMem), 10, 64)
	m.diskReservedSpace, _ = strconv.ParseUint(cfg.GetString(cfgDiskReservedSpace), 10, 64)
	if m.diskReservedSpace == 0 || m.diskReservedSpace < defaultDiskReservedSpace {
		m.diskReservedSpace = defaultDiskReservedSpace
	}

	m.tickInterval = int(cfg.GetFloat(cfgTickIntervalMs))
	if m.tickInterval <= 300 {
		log.LogWarnf("get config [%s]:[%v] less than 300 so set it to 500 ", cfgTickIntervalMs, cfg.GetString(cfgTickIntervalMs))
		m.tickInterval = 500
	}

	if configTotalMem == 0 {
		return fmt.Errorf("bad totalMem config,Recommended to be configured as 80 percent of physical machine memory")
	}

	deleteBatchCount := cfg.GetInt64(cfgDeleteBatchCount)
	if deleteBatchCount > 1 {
		updateDeleteBatchCount(uint64(deleteBatchCount))
	}

	m.rocksDirs = cfg.GetStringSlice(cfgRocksDirs)

	total, _, err := memory.GetMemInfo()
	if err == nil && configTotalMem > total-unit.GB {
		return fmt.Errorf("bad totalMem config,Recommended to be configured as 80 percent of physical machine memory")
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
	if len(m.rocksDirs) == 0 {
		log.LogInfof("conf do not have rocks db dir, now use meta data dir")
		m.rocksDirs = append(m.rocksDirs, m.metadataDir)
	}

	constCfg := config.ConstConfig{
		Listen:           m.listen,
		RaftHeartbetPort: m.raftHeartbeatPort,
		RaftReplicaPort:  m.raftReplicatePort,
	}
	var ok = false
	if ok, err = config.CheckOrStoreConstCfg(m.metadataDir, config.DefaultConstConfigFile, &constCfg); !ok {
		log.LogErrorf("constCfg check failed %v %v %v %v", m.metadataDir, config.DefaultConstConfigFile, constCfg, err)
		return fmt.Errorf("constCfg check failed %v %v %v %v", m.metadataDir, config.DefaultConstConfigFile, constCfg, err)
	}

	log.LogInfof("[parseConfig] load localAddr[%v].", m.localAddr)
	log.LogInfof("[parseConfig] load listen[%v].", m.listen)
	log.LogInfof("[parseConfig] load metadataDir[%v].", m.metadataDir)
	log.LogInfof("[parseConfig] load raftDir[%v].", m.raftDir)
	log.LogInfof("[parseConfig] load raftHeartbeatPort[%v].", m.raftHeartbeatPort)
	log.LogInfof("[parseConfig] load raftReplicatePort[%v].", m.raftReplicatePort)
	log.LogInfof("[parseConfig] load zoneName[%v].", m.zoneName)
	log.LogInfof("[parseConfig] load rocksdb dirs[%v].", m.rocksDirs)

	addrs := cfg.GetSlice(proto.MasterAddr)
	masters := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		masters = append(masters, addr.(string))
	}
	masterClient = masterSDK.NewMasterClient(masters, false)
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
	if len(masterClient.Nodes()) == 0 {
		err = errors.New("master address list is empty")
		return
	}
	return
}

func (m *MetaNode) loadMetaPartitions() (err error) {
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
		ZoneName:  m.zoneName,
	}
	if m.metadataManager, err = NewMetadataManager(conf, m); err != nil {
		log.LogErrorf("load metadata manager failed: %v", err)
	}
	log.LogInfof("load metadata manager finish.")
	return
}

func (m *MetaNode) failOverLeaderMp() {
	if m.metadataManager != nil {
		m.metadataManager.FailOverLeaderMp()
	}
}

func (m *MetaNode) stopMetaManager() {
	if m.metadataManager != nil {
		m.metadataManager.Stop()
	}
}

func (m *MetaNode) register() (err error) {
	step := 0
	var nodeAddress string
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
			nodeAddress = m.localAddr + ":" + m.listen
			step++
		}
		var nodeID uint64
		if nodeID, err = masterClient.NodeAPI().AddMetaNode(nodeAddress, m.zoneName, MetaNodeLatestVersion); err != nil {
			log.LogErrorf("register: register to master fail: address(%v) err(%s)", nodeAddress, err)
			time.Sleep(3 * time.Second)
			continue
		}
		m.nodeId = nodeID
		return
	}
}

func (m *MetaNode) startMetaPartitions() error {
	return m.metadataManager.Start()
}

func (m *MetaNode) startUpdateProcessStatInfo() {
	m.processStatInfo = statinfo.NewProcessStatInfo()
	m.processStatInfo.ProcessStartTime = time.Now().Format("2006-01-02 15:04:05")
	go m.processStatInfo.UpdateStatInfoSchedule()
}

// NewServer creates a new meta node instance.
func NewServer() *MetaNode {
	return &MetaNode{}
}

func getClusterInfo() (ci *proto.ClusterInfo, err error) {
	ci, err = masterClient.AdminAPI().GetClusterInfo()
	return
}

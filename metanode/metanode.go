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
	syslog "log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xtaci/smux"

	masterSDK "github.com/cubefs/cubefs/sdk/master"

	"fmt"
	"strconv"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

var (
	clusterInfo    *proto.ClusterInfo
	masterClient   *masterSDK.MasterClient
	configTotalMem uint64
	serverPort     string
	smuxPortShift  int
	smuxPool       *util.SmuxConnectPool
	smuxPoolCfg    = util.DefaultSmuxConnPoolConfig()
)

// The MetaNode manages the dentry and inode information of the meta partitions on a meta node.
// The data consistency is ensured by Raft.
type MetaNode struct {
	nodeId            uint64
	listen            string
	bindIp            bool
	metadataDir       string // root dir of the metaNode
	raftDir           string // root dir of the raftStore log
	metadataManager   MetadataManager
	localAddr         string
	clusterId         string
	raftStore         raftstore.RaftStore
	raftHeartbeatPort string
	raftReplicatePort string
	zoneName          string
	httpStopC         chan uint8
	smuxStopC         chan uint8
	metrics           *MetaNodeMetrics
	tickInterval      int
	raftRecvBufSize   int
	connectionCnt     int64
	clusterUuid       string
	clusterUuidEnable bool

	control common.Control
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

	if len(metaNodeInfo.PersistenceMetaPartitions) == 0 {
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
		return errors.New("Invalid node Type!")
	}
	if err = m.parseConfig(cfg); err != nil {
		return
	}
	if err = m.register(); err != nil {
		return
	}

	if err = m.startRaftServer(cfg); err != nil {
		return
	}
	if err = m.newMetaManager(); err != nil {
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

	exporter.Init(cfg.GetString("role"), cfg)
	m.startStat()

	// check local partition compare with master ,if lack,then not start
	if err = m.checkLocalPartitionMatchWithMaster(); err != nil {
		syslog.Println(err)
		exporter.Warning(err.Error())
		return
	}

	exporter.RegistConsul(m.clusterId, cfg.GetString("role"), cfg)
	return
}

func doShutdown(s common.Server) {
	m, ok := s.(*MetaNode)
	if !ok {
		return
	}
	m.stopUpdateNodeInfo()
	// shutdown node and release the resource
	m.stopStat()
	m.stopServer()
	m.stopSmuxServer()
	m.stopMetaManager()
	m.stopRaftServer()
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
	m.bindIp = cfg.GetBool(proto.BindIpKey)
	serverPort = m.listen
	m.metadataDir = cfg.GetString(cfgMetadataDir)
	m.raftDir = cfg.GetString(cfgRaftDir)
	m.raftHeartbeatPort = cfg.GetString(cfgRaftHeartbeatPort)
	m.raftReplicatePort = cfg.GetString(cfgRaftReplicaPort)
	m.tickInterval = int(cfg.GetFloat(cfgTickInterval))
	m.raftRecvBufSize = int(cfg.GetInt(cfgRaftRecvBufSize))
	m.zoneName = cfg.GetString(cfgZoneName)

	deleteBatchCount := cfg.GetInt64(cfgDeleteBatchCount)
	if deleteBatchCount > 1 {
		updateDeleteBatchCount(uint64(deleteBatchCount))
	}

	total, _, err := util.GetMemInfo()
	if err != nil {
		log.LogErrorf("get total mem failed, err %s", err.Error())
	}

	ratioStr := cfg.GetString(cfgMemRatio)
	if err == nil && ratioStr != "" {
		ratio, _ := strconv.Atoi(ratioStr)
		if ratio <= 0 || ratio >= 100 {
			return fmt.Errorf("cfgMemRatio is not legal, shoule beteen 1-100, now %s", ratioStr)
		}

		configTotalMem = total * uint64(ratio) / 100
		log.LogInfof("configTotalMem by ratio is: mem [%d], ratio[%d]", configTotalMem, ratio)
	} else {
		configTotalMem, _ = strconv.ParseUint(cfg.GetString(cfgTotalMem), 10, 64)
		if configTotalMem == 0 {
			return fmt.Errorf("bad totalMem config,Recommended to be configured as 80 percent of physical machine memory")
		}
	}

	if err == nil && configTotalMem > total-util.GB {
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

	log.LogInfof("[parseConfig] load localAddr[%v].", m.localAddr)
	log.LogInfof("[parseConfig] load listen[%v].", m.listen)
	log.LogInfof("[parseConfig] load metadataDir[%v].", m.metadataDir)
	log.LogInfof("[parseConfig] load raftDir[%v].", m.raftDir)
	log.LogInfof("[parseConfig] load raftHeartbeatPort[%v].", m.raftHeartbeatPort)
	log.LogInfof("[parseConfig] load raftReplicatePort[%v].", m.raftReplicatePort)
	log.LogInfof("[parseConfig] load zoneName[%v].", m.zoneName)

	if err = m.parseSmuxConfig(cfg); err != nil {
		return fmt.Errorf("parseSmuxConfig fail err %v", err)
	} else {
		log.LogInfof("Start: init smux conn pool (%v).", smuxPoolCfg)
		smuxPool = util.NewSmuxConnectPool(smuxPoolCfg)
	}

	addrs := cfg.GetSlice(proto.MasterAddr)
	masters := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		masters = append(masters, addr.(string))
	}
	masterClient = masterSDK.NewMasterClient(masters, false)
	err = m.validConfig()
	return
}

func (m *MetaNode) parseSmuxConfig(cfg *config.Config) error {
	// SMux port
	smuxPortShift = int(cfg.GetInt64(cfgSmuxPortShift))
	if smuxPortShift == 0 {
		smuxPortShift = util.DefaultSmuxPortShift
	}

	// SMux buffer
	var maxBuffer = cfg.GetInt64(cfgSmuxMaxBuffer)
	if maxBuffer > 0 {
		smuxPoolCfg.MaxReceiveBuffer = int(maxBuffer)
		if smuxPoolCfg.MaxStreamBuffer > int(maxBuffer) {
			smuxPoolCfg.MaxStreamBuffer = int(maxBuffer)
		}

		if err := smux.VerifyConfig(smuxPoolCfg.Config); err != nil {
			return err
		}
	}

	maxConn := cfg.GetInt64(cfgSmuxMaxConn)
	if maxConn > 0 {
		smuxPoolCfg.ConnsPerAddr = int(maxConn)
	}

	maxStreamPerConn := cfg.GetInt64(cfgSmuxStreamPerConn)
	if maxStreamPerConn > 0 {
		smuxPoolCfg.StreamsPerConn = int(maxStreamPerConn)
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

func (m *MetaNode) newMetaManager() (err error) {
	if _, err = os.Stat(m.metadataDir); err != nil {
		if err = os.MkdirAll(m.metadataDir, 0755); err != nil {
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
	var ok = false
	if ok, err = config.CheckOrStoreConstCfg(m.metadataDir, config.DefaultConstConfigFile, &constCfg); !ok {
		log.LogErrorf("constCfg check failed %v %v %v %v", m.metadataDir, config.DefaultConstConfigFile, constCfg, err)
		return fmt.Errorf("constCfg check failed %v %v %v %v", m.metadataDir, config.DefaultConstConfigFile, constCfg, err)
	}

	// load metadataManager
	conf := MetadataManagerConfig{
		NodeID:    m.nodeId,
		RootDir:   m.metadataDir,
		RaftStore: m.raftStore,
		ZoneName:  m.zoneName,
	}
	m.metadataManager = NewMetadataManager(conf, m)
	return
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
			m.clusterUuid = clusterInfo.ClusterUuid
			m.clusterUuidEnable = clusterInfo.ClusterUuidEnable
			m.clusterId = clusterInfo.Cluster
			nodeAddress = m.localAddr + ":" + m.listen
			step++
		}
		var nodeID uint64
		if nodeID, err = masterClient.NodeAPI().AddMetaNode(nodeAddress, m.zoneName); err != nil {
			log.LogErrorf("register: register to master fail: address(%v) err(%s)", nodeAddress, err)
			time.Sleep(3 * time.Second)
			continue
		}
		m.nodeId = nodeID
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

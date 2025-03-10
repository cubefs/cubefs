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

package master

import (
	"context"
	"fmt"
	syslog "log"
	"net/http"
	"net/http/httputil"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/raftstore/raftstore_db"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

// configuration keys
const (
	ClusterName              = "clusterName"
	ID                       = "id"
	IP                       = "ip"
	Port                     = "port"
	LogLevel                 = "logLevel"
	LogDir                   = "logDir"
	WalDir                   = "walDir"
	StoreDir                 = "storeDir"
	EbsAddrKey               = "ebsAddr"
	BStoreAddrKey            = "bStoreAddr"
	EbsServicePathKey        = "ebsServicePath"
	BStoreServicePathKey     = "bStoreServicePath"
	GroupID                  = 1
	ModuleName               = "master"
	CfgRetainLogs            = "retainLogs"
	DefaultRetainLogs        = 20000
	cfgTickInterval          = "tickInterval"
	cfgRaftRecvBufSize       = "raftRecvBufSize"
	cfgElectionTick          = "electionTick"
	SecretKey                = "masterServiceKey"
	cfgEnableDirectDeleteVol = "enableDirectDeleteVol"
	Stat                     = "stat"
	Authenticate             = "authenticate"
	AuthNodeHost             = "authNodeHost"
	AuthNodeEnableHTTPS      = "authNodeEnableHTTPS"
	AuthNodeCertFile         = "authNodeCertFile"
)

var (
	// regexps for data validation
	volNameRegexp = regexp.MustCompile("^[a-zA-Z0-9][a-zA-Z0-9_.-]{1,61}[a-zA-Z0-9]$")
	ownerRegexp   = regexp.MustCompile("^[A-Za-z][A-Za-z0-9_]{0,20}$")

	useConnPool           = true // for test
	enableDirectDeleteVol = true
	gConfig               *clusterConfig
)

var overSoldFactor = defaultOverSoldFactor

func overSoldLimit() bool {
	if overSoldFactor <= 0 {
		return false
	}

	return true
}

func overSoldCap(cap uint64) uint64 {
	if overSoldFactor <= 0 {
		return cap
	}

	return uint64(float32(cap) * overSoldFactor)
}

func setOverSoldFactor(factor float32) {
	if factor != overSoldFactor {
		overSoldFactor = factor
	}
}

var volNameErr = errors.New("name can only start and end with number or letters, and len can't less than 3")

// Server represents the server in a cluster
type Server struct {
	id              uint64
	clusterName     string
	ip              string
	bindIp          bool
	port            string
	logDir          string
	walDir          string
	storeDir        string
	bStoreAddr      string
	servicePath     string
	retainLogs      uint64
	tickInterval    int
	raftRecvBufSize int
	electionTick    int
	leaderInfo      *LeaderInfo
	config          *clusterConfig
	cluster         *Cluster
	user            *User
	rocksDBStore    *raftstore_db.RocksDBStore
	raftStore       raftstore.RaftStore
	fsm             *MetadataFsm
	partition       raftstore.Partition
	wg              sync.WaitGroup
	reverseProxy    *httputil.ReverseProxy
	metaReady       bool
	apiServer       *http.Server
	cliMgr          *ClientMgr
}

// NewServer creates a new server
func NewServer() *Server {
	return &Server{}
}

// Start starts a server
func (m *Server) Start(cfg *config.Config) (err error) {
	m.config = newClusterConfig()
	gConfig = m.config
	m.leaderInfo = &LeaderInfo{}

	if err = m.checkConfig(cfg); err != nil {
		log.LogError(errors.Stack(err))
		return
	}
	m.reverseProxy = m.newReverseProxy()
	m.cliMgr = newClientMgr()

	if m.rocksDBStore, err = raftstore_db.NewRocksDBStoreAndRecovery(m.storeDir, LRUCacheSize, WriteBufferSize); err != nil {
		return
	}

	if err = m.createRaftServer(cfg); err != nil {
		log.LogError(errors.Stack(err))
		return
	}

	m.initCluster()
	m.initUser()
	m.cluster.partition = m.partition
	m.cluster.idAlloc.partition = m.partition
	MasterSecretKey := cfg.GetString(SecretKey)
	if m.cluster.MasterSecretKey, err = cryptoutil.Base64Decode(MasterSecretKey); err != nil {
		return fmt.Errorf("action[Start] failed %v, err: master service Key invalid = %s", proto.ErrInvalidCfg, MasterSecretKey)
	}

	m.cluster.authenticate = cfg.GetBool(Authenticate)
	if m.cluster.authenticate {
		m.cluster.initAuthentication(cfg)
	}
	WarnMetrics = newWarningMetrics(m.cluster)
	m.cluster.scheduleTask()
	m.startHTTPService(ModuleName, cfg)
	exporter.RegistConsul(m.clusterName, ModuleName, cfg)
	metricsService := newMonitorMetrics(m.cluster)
	metricsService.start()

	_, err = stat.NewStatistic(m.logDir, Stat, int64(stat.DefaultStatLogSize),
		stat.DefaultTimeOutUs, true)

	// only set buckets 1ms from master module.
	exporter.SetBuckets([]float64{1000})

	m.wg.Add(1)
	return nil
}

var closeStopcOnce sync.Once

// Shutdown closes the server
func (m *Server) Shutdown() {
	var err error
	if !atomic.CompareAndSwapInt32(&m.cluster.stopFlag, 0, 1) {
		log.LogWarnf("action[Shutdown] cluster already stopped!")
		return
	}
	if m.cluster.stopc != nil {
		close(m.cluster.stopc)
		m.cluster.wg.Wait()
		log.LogWarnf("action[Shutdown] cluster stopped!")
	}

	if m.apiServer != nil {
		if err = m.apiServer.Shutdown(context.Background()); err != nil {
			log.LogErrorf("action[Shutdown] failed, err: %v", err)
		}
	}
	stat.CloseStat()

	// stop raftServer first
	if m.fsm != nil {
		m.fsm.Stop()
	}

	// NOTE: wait 10 second for background goroutines to exit
	time.Sleep(10 * time.Second)
	// NOTE: close rocksdb
	if m.rocksDBStore != nil {
		m.rocksDBStore.Close()
	}

	m.wg.Done()
}

// Sync waits for the execution termination of the server
func (m *Server) Sync() {
	m.wg.Wait()
}

func (m *Server) checkConfig(cfg *config.Config) (err error) {
	m.clusterName = cfg.GetString(ClusterName)
	m.ip = cfg.GetString(IP)
	m.bindIp = cfg.GetBool(proto.BindIpKey)
	m.port = cfg.GetString(proto.ListenPort)
	m.logDir = cfg.GetString(LogDir)
	m.walDir = cfg.GetString(WalDir)
	m.storeDir = cfg.GetString(StoreDir)
	m.bStoreAddr = cfg.GetString(BStoreAddrKey)
	if m.bStoreAddr == "" {
		m.bStoreAddr = cfg.GetString(EbsAddrKey)
	}
	m.servicePath = cfg.GetString(BStoreServicePathKey)
	if m.servicePath == "" {
		m.servicePath = cfg.GetString(EbsServicePathKey)
	}
	peerAddrs := cfg.GetString(cfgPeers)
	if m.port == "" || m.walDir == "" || m.storeDir == "" || m.clusterName == "" || peerAddrs == "" {
		return fmt.Errorf("%v,err:%v,%v,%v,%v,%v,%v", proto.ErrInvalidCfg, "one of (listen,walDir,storeDir,clusterName) is null",
			m.port, m.walDir, m.storeDir, m.clusterName, peerAddrs)
	}

	if m.id, err = strconv.ParseUint(cfg.GetString(ID), 10, 64); err != nil {
		return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
	}

	m.config.DisableAutoCreate = cfg.GetBoolWithDefault(disableAutoCreate, false)
	syslog.Printf("get disableAutoCreate cfg %v", m.config.DisableAutoCreate)

	m.config.EnableFollowerCache = cfg.GetBoolWithDefault(enableFollowerCache, true)
	syslog.Printf("get enableFollowerCache cfg %v", m.config.EnableFollowerCache)

	m.config.EnableSnapshot = cfg.GetBoolWithDefault(enableSnapshot, false)
	syslog.Printf("get enableSnapshot cfg %v", m.config.EnableSnapshot)

	m.config.EnableFollowerCache = cfg.GetBoolWithDefault(enableFollowerCache, true)
	syslog.Printf("get enableFollowerCache cfg %v", m.config.EnableFollowerCache)

	m.config.faultDomain = cfg.GetBoolWithDefault(faultDomain, false)
	m.config.heartbeatPort = cfg.GetInt64(heartbeatPortKey)
	m.config.replicaPort = cfg.GetInt64(replicaPortKey)
	if m.config.heartbeatPort <= 1024 {
		m.config.heartbeatPort = raftstore.DefaultHeartbeatPort
	}
	if m.config.replicaPort <= 1024 {
		m.config.replicaPort = raftstore.DefaultReplicaPort
	}
	syslog.Printf("heartbeatPort[%v],replicaPort[%v]\n", m.config.heartbeatPort, m.config.replicaPort)
	if err = m.config.parsePeers(peerAddrs); err != nil {
		return
	}
	nodeSetCapacity := cfg.GetString(nodeSetCapacity)
	if nodeSetCapacity != "" {
		if m.config.nodeSetCapacity, err = strconv.Atoi(nodeSetCapacity); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	if m.config.nodeSetCapacity < 3 {
		m.config.nodeSetCapacity = defaultNodeSetCapacity
	}

	m.config.httpProxyPoolSize = uint64(cfg.GetInt64(cfgHttpReversePoolSize))
	if m.config.httpProxyPoolSize < defaultHttpReversePoolSize {
		m.config.httpProxyPoolSize = defaultHttpReversePoolSize
	}
	m.config.httpPoolSize = uint64(cfg.GetInt64(proto.CfgHttpPoolSize))

	syslog.Printf("http reverse pool size %d, http pool size %d\n", m.config.httpProxyPoolSize, m.config.httpPoolSize)

	m.config.DefaultNormalZoneCnt = defaultNodeSetGrpBatchCnt
	m.config.DomainBuildAsPossible = cfg.GetBoolWithDefault(cfgDomainBuildAsPossible, false)
	domainBatchGrpCnt := cfg.GetString(cfgDomainBatchGrpCnt)
	if domainBatchGrpCnt != "" {
		if m.config.DefaultNormalZoneCnt, err = strconv.Atoi(domainBatchGrpCnt); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}

	metaNodeReservedMemory := cfg.GetString(cfgMetaNodeReservedMem)
	if metaNodeReservedMemory != "" {
		if m.config.metaNodeReservedMem, err = strconv.ParseUint(metaNodeReservedMemory, 10, 64); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	if m.config.metaNodeReservedMem < 32*1024*1024 {
		m.config.metaNodeReservedMem = defaultMetaNodeReservedMem
	}

	retainLogs := cfg.GetString(CfgRetainLogs)
	if retainLogs != "" {
		if m.retainLogs, err = strconv.ParseUint(retainLogs, 10, 64); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	if m.retainLogs <= 0 {
		m.retainLogs = DefaultRetainLogs
	}
	syslog.Println("retainLogs=", m.retainLogs)

	missingDataPartitionInterval := cfg.GetString(missingDataPartitionInterval)
	if missingDataPartitionInterval != "" {
		if m.config.MissingDataPartitionInterval, err = strconv.ParseInt(missingDataPartitionInterval, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}

	dpNoLeaderReportInterval := cfg.GetString(cfgDpNoLeaderReportIntervalSec)
	if dpNoLeaderReportInterval != "" {
		if m.config.DpNoLeaderReportIntervalSec, err = strconv.ParseInt(dpNoLeaderReportInterval, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}

	mpNoLeaderReportInterval := cfg.GetString(cfgMpNoLeaderReportIntervalSec)
	if mpNoLeaderReportInterval != "" {
		if m.config.MpNoLeaderReportIntervalSec, err = strconv.ParseInt(mpNoLeaderReportInterval, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}

	dataPartitionTimeOutSec := cfg.GetString(dataPartitionTimeOutSec)
	if dataPartitionTimeOutSec != "" {
		if m.config.DataPartitionTimeOutSec, err = strconv.ParseInt(dataPartitionTimeOutSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}

	numberOfDataPartitionsToLoad := cfg.GetString(NumberOfDataPartitionsToLoad)
	if numberOfDataPartitionsToLoad != "" {
		if m.config.numberOfDataPartitionsToLoad, err = strconv.Atoi(numberOfDataPartitionsToLoad); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	if m.config.numberOfDataPartitionsToLoad <= 40 {
		m.config.numberOfDataPartitionsToLoad = 40
	}
	if secondsToFreeDP := cfg.GetString(secondsToFreeDataPartitionAfterLoad); secondsToFreeDP != "" {
		if m.config.secondsToFreeDataPartitionAfterLoad, err = strconv.ParseInt(secondsToFreeDP, 10, 64); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}

	if cfg.GetInt(cfgStartLcScanTime) >= 1 && cfg.GetInt(cfgStartLcScanTime) <= 14 {
		m.config.StartLcScanTime = cfg.GetInt(cfgStartLcScanTime)
	}

	m.tickInterval = int(cfg.GetFloat(cfgTickInterval))
	m.raftRecvBufSize = int(cfg.GetInt(cfgRaftRecvBufSize))
	m.electionTick = int(cfg.GetFloat(cfgElectionTick))
	if m.tickInterval <= 300 {
		m.tickInterval = 500
	}
	if m.electionTick <= 3 {
		m.electionTick = 5
	}

	maxQuotaNumPerVol := cfg.GetString(cfgMaxQuotaNumPerVol)
	if maxQuotaNumPerVol != "" {
		if m.config.MaxQuotaNumPerVol, err = strconv.Atoi(maxQuotaNumPerVol); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}

	m.config.MonitorPushAddr = cfg.GetString(cfgMonitorPushAddr)

	m.config.volForceDeletion = cfg.GetBoolWithDefault(cfgVolForceDeletion, true)

	threshold := cfg.GetInt64WithDefault(cfgVolDeletionDentryThreshold, 0)
	if threshold < 0 {
		return fmt.Errorf("volDeletionDentryThreshold can't be less than 0 ! ")
	}
	m.config.volDeletionDentryThreshold = uint64(threshold)

	enableDirectDeleteVol = cfg.GetBoolWithDefault(cfgEnableDirectDeleteVol, true)

	m.config.cfgDataMediaType = uint32(cfg.GetInt64(cfgLegacyDataMediaType))
	if m.config.cfgDataMediaType != 0 && !proto.IsValidMediaType(m.config.cfgDataMediaType) {
		return fmt.Errorf("legacy media type is not vaild, type %d", m.config.cfgDataMediaType)
	}
	syslog.Printf("config mediaType %v", m.config.cfgDataMediaType)
	return
}

func (m *Server) createRaftServer(cfg *config.Config) (err error) {
	raftCfg := &raftstore.Config{
		NodeID:            m.id,
		RaftPath:          m.walDir,
		IPAddr:            cfg.GetString(IP),
		NumOfLogsToRetain: m.retainLogs,
		HeartbeatPort:     int(m.config.heartbeatPort),
		ReplicaPort:       int(m.config.replicaPort),
		TickInterval:      m.tickInterval,
		ElectionTick:      m.electionTick,
		RecvBufSize:       m.raftRecvBufSize,
	}
	if m.raftStore, err = raftstore.NewRaftStore(raftCfg, cfg); err != nil {
		return errors.Trace(err, "NewRaftStore failed! id[%v] walPath[%v]", m.id, m.walDir)
	}
	syslog.Printf("peers[%v],tickInterval[%v],electionTick[%v]\n", m.config.peers, m.tickInterval, m.electionTick)
	m.initFsm()
	partitionCfg := &raftstore.PartitionConfig{
		ID:      GroupID,
		Peers:   m.config.peers,
		Applied: m.fsm.applied,
		SM:      m.fsm,
	}
	if m.partition, err = m.raftStore.CreatePartition(partitionCfg); err != nil {
		return errors.Trace(err, "CreatePartition failed")
	}
	return
}

func (m *Server) initFsm() {
	m.fsm = newMetadataFsm(m.rocksDBStore, m.retainLogs, m.raftStore.RaftServer())
	m.fsm.registerLeaderChangeHandler(m.handleLeaderChange)
	m.fsm.registerPeerChangeHandler(m.handlePeerChange)

	// register the handlers for the interfaces defined in the Raft library
	m.fsm.registerApplySnapshotHandler(m.handleApplySnapshot)
	m.fsm.registerRaftUserCmdApplyHandler(m.handleRaftUserCmd)
	m.fsm.restore()
}

func (m *Server) initCluster() {
	log.LogInfo("action[initCluster] begin")
	m.cluster = newCluster(m.clusterName, m.leaderInfo, m.fsm, m.partition, m.config, m)
	m.cluster.retainLogs = m.retainLogs
	log.LogInfo("action[initCluster] end")

	// in case any limiter on follower
	log.LogInfo("action[loadApiLimiterInfo] begin")
	m.cluster.loadApiLimiterInfo()
	log.LogInfo("action[loadApiLimiterInfo] end")
}

func (m *Server) initUser() {
	log.LogInfo("action[initUser] begin")
	m.user = newUser(m.fsm, m.partition)
	log.LogInfo("action[initUser] end")
}

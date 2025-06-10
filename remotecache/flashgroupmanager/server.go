package flashgroupmanager

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/raftstore"
	syslog "log"
	"net/http"
	"net/http/httputil"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore/raftstore_db"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

const (
	ModuleName = "flashGroupManager"
)

const (
	ClusterName         = "clusterName"
	IP                  = "ip"
	Stat                = "stat"
	LogDir              = "logDir"
	StoreDir            = "storeDir"
	HttpReversePoolSize = "httpReversePoolSize"
	ID                  = "id"
	WalDir              = "walDir"
	RetainLogs          = "retainLogs"
	HeartbeatPort       = "heartbeatPort"
	ReplicaPort         = "replicaPort"
	Peers               = "peers"
	TickInterval        = "tickInterval"
	RaftRecvBufSize     = "raftRecvBufSize"
	ElectionTick        = "electionTick"
	GroupID             = 1
)

const (
	useConnPool = true
)

type FlashGroupManager struct {
	wg              sync.WaitGroup
	clusterName     string
	config          *clusterConfig
	ip              string
	bindIp          bool
	port            string
	apiServer       *http.Server
	cluster         *Cluster
	logDir          string
	metaReady       bool
	leaderInfo      *LeaderInfo
	rocksDBStore    *raftstore_db.RocksDBStore
	storeDir        string
	reverseProxy    *httputil.ReverseProxy
	id              uint64
	walDir          string
	retainLogs      uint64
	tickInterval    int
	raftRecvBufSize int
	electionTick    int
	raftStore       raftstore.RaftStore
	fsm             *MetadataFsm
	partition       raftstore.Partition
}

func NewFlashGroupManager() *FlashGroupManager {
	return &FlashGroupManager{}
}

func (m *FlashGroupManager) Start(cfg *config.Config) (err error) {
	m.config = newClusterConfig()
	if err = m.checkConfig(cfg); err != nil {
		log.LogError(errors.Stack(err))
		return
	}

	m.leaderInfo = &LeaderInfo{}

	if m.rocksDBStore, err = raftstore_db.NewRocksDBStoreAndRecovery(m.storeDir, LRUCacheSize, WriteBufferSize); err != nil {
		return
	}

	m.reverseProxy = m.newReverseProxy()
	if err = m.createRaftServer(cfg); err != nil {
		log.LogError(errors.Stack(err))
		return
	}

	m.initCluster()

	// TODO
	// WarnMetrics = newWarningMetrics(m.cluster)

	m.cluster.scheduleTask()

	m.startHTTPService(ModuleName, cfg)
	exporter.RegistConsul(m.clusterName, ModuleName, cfg)
	exporter.SetBuckets([]float64{1000})
	// TODO
	// newMonitorMetrics

	_, err = stat.NewStatistic(m.logDir, Stat, int64(stat.DefaultStatLogSize),
		stat.DefaultTimeOutUs, true)

	m.wg.Add(1)
	return
}

func (m *FlashGroupManager) Shutdown() {
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

	if m.fsm != nil {
		m.fsm.Stop()
	}

	// NOTE: wait 10 second for background goroutines to exit
	time.Sleep(10 * time.Second)
	if m.rocksDBStore != nil {
		m.rocksDBStore.Close()
	}

	m.wg.Done()
}

func (m *FlashGroupManager) Sync() {
	m.wg.Wait()
}

func (m *FlashGroupManager) checkConfig(cfg *config.Config) (err error) {
	m.clusterName = cfg.GetString(ClusterName)
	m.ip = cfg.GetString(IP)
	m.bindIp = cfg.GetBool(proto.BindIpKey)
	m.port = cfg.GetString(proto.ListenPort)
	m.logDir = cfg.GetString(LogDir)
	m.walDir = cfg.GetString(WalDir)
	peerAddrs := cfg.GetString(Peers)

	if m.port == "" || m.walDir == "" || m.clusterName == "" || peerAddrs == "" {
		return fmt.Errorf("%v,err:%v,%v,%v,%v,%v", proto.ErrInvalidCfg, "one of (listen,walDir,clusterName) is null",
			m.ip, m.clusterName, m.walDir, peerAddrs)
	}

	m.config.heartbeatPort = cfg.GetInt64(HeartbeatPort)
	m.config.replicaPort = cfg.GetInt64(ReplicaPort)
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

	m.storeDir = cfg.GetString(StoreDir)
	if m.storeDir == "" {
		return fmt.Errorf("store dir is empty")
	}

	retainLogs := cfg.GetString(RetainLogs)
	if retainLogs != "" {
		if m.retainLogs, err = strconv.ParseUint(retainLogs, 10, 64); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	if m.retainLogs <= 0 {
		m.retainLogs = defaultRetainLogs
	}
	syslog.Println("retainLogs=", m.retainLogs)

	if m.id, err = strconv.ParseUint(cfg.GetString(ID), 10, 64); err != nil {
		return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
	}

	m.config.httpProxyPoolSize = uint64(cfg.GetInt64(HttpReversePoolSize))
	if m.config.httpProxyPoolSize < defaultHttpReversePoolSize {
		m.config.httpProxyPoolSize = defaultHttpReversePoolSize
	}
	syslog.Printf("http reverse pool size %d \n", m.config.httpProxyPoolSize)

	m.tickInterval = int(cfg.GetFloat(TickInterval))
	m.raftRecvBufSize = int(cfg.GetInt(RaftRecvBufSize))
	m.electionTick = int(cfg.GetFloat(ElectionTick))
	if m.tickInterval <= 300 {
		m.tickInterval = 500
	}
	if m.electionTick <= 3 {
		m.electionTick = 5
	}
	return nil
}

func (m *FlashGroupManager) initCluster() {
	log.LogInfo("action[initCluster] begin")
	m.cluster = newCluster(m.clusterName, m.config, m.leaderInfo, m.fsm, m.partition)
	log.LogInfo("action[initCluster] end")
	// in case any limiter on follower
	log.LogInfo("action[loadApiLimiterInfo] begin")
	// TODO
	//m.cluster.loadApiLimiterInfo()
	log.LogInfo("action[loadApiLimiterInfo] end")
}

func (m *FlashGroupManager) createRaftServer(cfg *config.Config) (err error) {
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

func (m *FlashGroupManager) initFsm() {
	m.fsm = newMetadataFsm(m.rocksDBStore, m.retainLogs, m.raftStore.RaftServer())
	m.fsm.registerLeaderChangeHandler(m.handleLeaderChange)
	m.fsm.registerPeerChangeHandler(m.handlePeerChange)

	// register the handlers for the interfaces defined in the Raft library
	m.fsm.registerApplySnapshotHandler(m.handleApplySnapshot)
	m.fsm.registerRaftUserCmdApplyHandler(m.handleRaftUserCmd)
	m.fsm.restore()
}

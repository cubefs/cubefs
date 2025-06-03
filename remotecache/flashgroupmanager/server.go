package flashgroupmanager

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
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
	ClusterName = "clusterName"
	IP          = "ip"
	Stat        = "stat"
	LogDir      = "logDir"
)

const (
	useConnPool = true
)

type FlashGroupManager struct {
	wg          sync.WaitGroup
	clusterName string
	config      *clusterConfig
	ip          string
	bindIp      bool
	port        string
	apiServer   *http.Server
	cluster     *Cluster
	logDir      string
	metaReady   bool //TODO leader change
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

	// TODO
	// m.leaderInfo = &LeaderInfo{}

	// TODO
	// if m.rocksDBStore, err = raftstore_db.NewRocksDBStoreAndRecovery(m.storeDir, LRUCacheSize, WriteBufferSize); err != nil {
	// TODO
	// if err = m.createRaftServer(cfg); err != nil {

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

	// TODO
	//if m.fsm != nil {
	//	m.fsm.Stop()
	//}

	// NOTE: wait 10 second for background goroutines to exit
	time.Sleep(10 * time.Second)
	// TODO
	//if m.rocksDBStore != nil {
	//	m.rocksDBStore.Close()
	//}

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

	if m.port == "" || m.clusterName == "" {
		return fmt.Errorf("%v,err:%v,%v,%v", proto.ErrInvalidCfg, "one of (listen,walDir,clusterName) is null",
			m.ip, m.clusterName)
	}
	return
}

func (m *FlashGroupManager) initCluster() {
	log.LogInfo("action[initCluster] begin")
	m.cluster = newCluster(m.clusterName)
	log.LogInfo("action[initCluster] end")

	// in case any limiter on follower
	log.LogInfo("action[loadApiLimiterInfo] begin")
	// TODO
	//m.cluster.loadApiLimiterInfo()
	log.LogInfo("action[loadApiLimiterInfo] end")
}

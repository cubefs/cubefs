package flashgroupmaster

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

const (
	ModuleName = "flashGroupMaster"
)

const (
	ClusterName = "clusterName"
	IP          = "ip"
)

type FlashGroupMaster struct {
	wg          sync.WaitGroup
	clusterName string
	config      *clusterConfig
	ip          string
	bindIp      bool
	port        string
	apiServer   *http.Server
}

func NewFlashGroupMaster() *FlashGroupMaster {
	return &FlashGroupMaster{}
}

func (m *FlashGroupMaster) Start(cfg *config.Config) (err error) {
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

	// TODO
	// initCluster

	// TODO:need?
	// m.cluster.initAuthentication(cfg)

	// TODO
	// WarnMetrics = newWarningMetrics(m.cluster)

	// TODO
	// scheduleTask

	m.startHTTPService(ModuleName, cfg)
	exporter.RegistConsul(m.clusterName, ModuleName, cfg)
	exporter.SetBuckets([]float64{1000})
	// TODO
	// newMonitorMetrics

	// TODO
	// stat.NewStatistic

	m.wg.Add(1)
	return
}

func (m *FlashGroupMaster) Shutdown() {

	m.wg.Done()
}

func (m *FlashGroupMaster) Sync() {
	m.wg.Wait()
}

func (m *FlashGroupMaster) checkConfig(cfg *config.Config) (err error) {
	m.clusterName = cfg.GetString(ClusterName)
	m.ip = cfg.GetString(IP)
	m.bindIp = cfg.GetBool(proto.BindIpKey)
	m.port = cfg.GetString(proto.ListenPort)

	if m.port == "" || m.clusterName == "" {
		return fmt.Errorf("%v,err:%v,%v,%v", proto.ErrInvalidCfg, "one of (listen,walDir,clusterName) is null",
			m.ip, m.clusterName)
	}
	return
}

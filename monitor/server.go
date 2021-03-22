package monitor

import (
	"context"
	"fmt"
	"net/http"
	"regexp"

	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
	"github.com/gorilla/mux"
	"github.com/tsuna/gohbase"
)

var (
	// Regular expression used to verify the configuration of the service listening listenPort.
	// A valid service listening listenPort configuration is a string containing only numbers.
	regexpListen = regexp.MustCompile("^(\\d)+$")
)

type Monitor struct {
	cluster    string
	port       string
	zkQuorum   string
	zkRoot     string
	namespace  string
	queryIP    string
	hbase      *HBaseClient
	apiServer  *http.Server
	jmqConfig  *JMQConfig
	mqProducer *MQProducer
	stopC      chan bool
	control    common.Control
}

func NewServer() *Monitor {
	return &Monitor{}
}

func (m *Monitor) Start(cfg *config.Config) error {
	return m.control.Start(m, cfg, doStart)
}

func (m *Monitor) Shutdown() {
	m.control.Shutdown(m, doShutdown)
}

func (m *Monitor) Sync() {
	m.control.Sync()
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	m, ok := s.(*Monitor)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}
	m.stopC = make(chan bool)
	// parse config
	if err = m.parseConfig(cfg); err != nil {
		return
	}
	// init HBase
	m.initHBaseClient()
	// create table
	if err = m.initHBaseTable(); err != nil {
		return
	}
	if m.mqProducer, err = initMQProducer(m.cluster, m.jmqConfig); err != nil {
		return
	}
	// start http service
	m.startHTTPService()

	return
}

func doShutdown(s common.Server) {
	m, ok := s.(*Monitor)
	if !ok {
		return
	}
	// 1.http Server
	if m.apiServer != nil {
		if err := m.apiServer.Shutdown(context.Background()); err != nil {
			log.LogErrorf("action[Shutdown] failed, err: %v", err)
		}
	}
	// 2. MQProducer
	m.mqProducer.closeMQProducer()
	close(m.stopC)
	return
}

func (m *Monitor) parseConfig(cfg *config.Config) (err error) {
	m.cluster = cfg.GetString(ConfigCluster)
	listen := cfg.GetString(ConfigListenPort)
	if !regexpListen.MatchString(listen) {
		return fmt.Errorf("Port must be a string only contains numbers.")
	}
	m.port = listen

	zkQuorum := cfg.GetString(ConfigHBaseZK)
	if zkQuorum != "" {
		m.zkQuorum = zkQuorum
	} else {
		log.LogErrorf("Can not creat HBaseClient if no zkQuorum in conf, plz set")
		return
	}

	zkRoot := cfg.GetString(ConfigZKRoot)
	if zkRoot != "" {
		m.zkRoot = zkRoot
	} else {
		m.zkRoot = defaultZkRoot
	}
	namespace := cfg.GetString(ConfigNamespace)
	if namespace == "" {
		namespace = defaultNamespace
	}
	m.namespace = namespace
	queryIP := cfg.GetString(ConfigQueryIP)
	if queryIP == "" {
		queryIP = defaultQueryIP
	}
	m.queryIP = queryIP

	m.jmqConfig = &JMQConfig{}
	m.jmqConfig.topic = cfg.GetString(ConfigTopic)
	m.jmqConfig.address = cfg.GetString(ConfigJMQAddress)
	m.jmqConfig.clientID = cfg.GetString(ConfigJMQClientID)

	log.LogDebugf("action[parseConfig] load listen port(%v).", m.port)
	log.LogDebugf("action[parseConfig] load cluster name(%v).", m.cluster)
	log.LogDebugf("action[parseConfig] load query ip(%v).", m.queryIP)
	log.LogDebugf("action[parseConfig] load zk quorum(%v).", m.zkQuorum)
	log.LogDebugf("action[parseConfig] load zk root(%v).", m.zkRoot)
	return
}

func (m *Monitor) initHBaseClient() {
	m.hbase = &HBaseClient{}
	m.hbase.namespace = m.namespace
	m.hbase.cluster = m.cluster
	m.hbase.adminClient = gohbase.NewAdminClient(m.zkQuorum, gohbase.ZookeeperRoot(m.zkRoot))
	m.hbase.client = gohbase.NewClient(m.zkQuorum, gohbase.ZookeeperRoot(m.zkRoot))
}

func (m *Monitor) registerAPIRoutes(router *mux.Router) {
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorCollect).
		HandlerFunc(m.collect)

	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorClusterTopIP).
		HandlerFunc(m.getClusterTopIP)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorClusterTopVol).
		HandlerFunc(m.getClusterTopVol)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorClusterTopPartition).
		HandlerFunc(m.getClusterTopPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorOpTopIP).
		HandlerFunc(m.getOpTopIP)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorOpTopVol).
		HandlerFunc(m.getOpTopVol)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorOpTopPartition).
		HandlerFunc(m.getOpTopPartition)

	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorIPTopPartition).
		HandlerFunc(m.getIPTopPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorTopPartitionOp).
		HandlerFunc(m.getTopPartitionOp)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorTopVol).
		HandlerFunc(m.getTopVol)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorTopVolOp).
		HandlerFunc(m.getTopVolOp)

}

func (m *Monitor) startHTTPService() {
	router := mux.NewRouter().SkipClean(true)
	m.registerAPIRoutes(router)
	var server = &http.Server{
		Addr:    colonSplit + m.port,
		Handler: router,
	}
	if err := server.ListenAndServe(); err != nil {
		log.LogErrorf("serveAPI: serve http server failed: err(%v)", err)
		return
	}
	log.LogDebugf("startHTTPService successfully: port(%v)", m.port)
	m.apiServer = server
	return
}

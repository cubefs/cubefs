package monitor

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
	"github.com/gorilla/mux"
)

var (
	// Regular expression used to verify the configuration of the service listening listenPort.
	// A valid service listening listenPort configuration is a string containing only numbers.
	regexpListen = regexp.MustCompile("^(\\d)+$")
)

type Monitor struct {
	port             string
	thriftAddr       string
	namespace        string
	queryIP          string
	clusters         []string
	splitRegionRules map[string]int64 	// clusterName:regionNum
	apiServer        *http.Server
	jmqConfig        *JMQConfig
	mqProducer       *MQProducer
	stopC            chan bool
	control          common.Control
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
	// create table
	if err = m.initHBaseTable(); err != nil {
		return
	}
	if m.mqProducer, err = initMQProducer(m.jmqConfig); err != nil {
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
	if m.mqProducer != nil {
		m.mqProducer.closeMQProducer()
	}
	close(m.stopC)
	return
}

func (m *Monitor) parseConfig(cfg *config.Config) (err error) {
	clusters := cfg.GetString(ConfigCluster)
	m.clusters = strings.Split(clusters, ",")

	listen := cfg.GetString(ConfigListenPort)
	if !regexpListen.MatchString(listen) {
		return fmt.Errorf("Port must be a string only contains numbers.")
	}
	m.port = listen

	thriftAddr := cfg.GetString(ConfigThriftAddr)
	if thriftAddr == "" {
		thriftAddr = defaultThriftAddr
	}
	m.thriftAddr = thriftAddr

	namespace := cfg.GetString(ConfigNamespace)
	if namespace == "" {
		namespace = defaultNamespace
	}
	m.namespace = namespace

	tableExpiredDay := cfg.GetInt64(ConfigExpiredDay)
	if tableExpiredDay > 0 {
		TableClearTime = time.Duration(tableExpiredDay) * 24 * time.Hour
	}

	queryIP := cfg.GetString(ConfigQueryIP)
	if queryIP == "" {
		queryIP = defaultQueryIP
	}
	m.queryIP = queryIP

	m.jmqConfig = &JMQConfig{}
	topic := cfg.GetString(ConfigTopic)
	m.jmqConfig.topic = strings.Split(topic, ",")
	m.jmqConfig.address = cfg.GetString(ConfigJMQAddress)
	m.jmqConfig.clientID = cfg.GetString(ConfigJMQClientID)
	m.jmqConfig.produceNum = cfg.GetInt64(ConfigProducerNum)
	if m.jmqConfig.produceNum <= 0 {
		m.jmqConfig.produceNum = defaultProducerNum
	}

	m.splitRegionRules = getSplitRules(cfg.GetStringSlice(ConfigSplitRegion))

	log.LogInfof("action[parseConfig] load listen port(%v).", m.port)
	log.LogInfof("action[parseConfig] load cluster name(%v).", m.clusters)
	log.LogInfof("action[parseConfig] load table expired time(%v).", TableClearTime)
	log.LogInfof("action[parseConfig] load query ip(%v).", m.queryIP)
	log.LogInfof("action[parseConfig] load thrift server address(%v).", m.thriftAddr)
	log.LogInfof("action[parseConfig] load producer num(%v).", m.jmqConfig.produceNum)
	log.LogInfof("action[parseConfig] load splitRegionRules(%v).", m.splitRegionRules)
	return
}

func (m *Monitor) registerAPIRoutes(router *mux.Router) {
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorCollect).
		HandlerFunc(m.collect)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorCluster).
		HandlerFunc(m.setCluster)

	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorClusterTopIP).
		HandlerFunc(m.getClusterTopIP)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorClusterTopVol).
		HandlerFunc(m.getClusterTopVol)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorOpTopIP).
		HandlerFunc(m.getOpTopIP)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorOpTopVol).
		HandlerFunc(m.getOpTopVol)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorTopPartition).
		HandlerFunc(m.getTopPartition)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorTopOp).
		HandlerFunc(m.getTopOp)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorTopIP).
		HandlerFunc(m.getTopIP)
}

func (m *Monitor) startHTTPService() {
	router := mux.NewRouter().SkipClean(true)
	m.registerAPIRoutes(router)
	var server = &http.Server{
		Addr:    colonSplit + m.port,
		Handler: router,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.LogErrorf("serveAPI: serve http server failed: err(%v)", err)
			return
		}
	}()
	log.LogDebugf("startHTTPService successfully: port(%v)", m.port)
	m.apiServer = server
	return
}

package monitor

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/statistics"
	"github.com/gorilla/mux"
	"net/http"
	"regexp"
	"strings"
)

var (
	// Regular expression used to verify the configuration of the service listening listenPort.
	// A valid service listening listenPort configuration is a string containing only numbers.
	regexpListen = regexp.MustCompile("^(\\d)+$")
)

type Monitor struct {
	port             string
	clusters         []string
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

	if m.jmqConfig != nil {
		if m.mqProducer, err = initMQProducer(m.jmqConfig); err != nil {
			return
		}
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

	var (
		jmqTopic    = cfg.GetString(ConfigTopic)
		jmqAddress  = cfg.GetString(ConfigJMQAddress)
		jmqClientID = cfg.GetString(ConfigJMQClientID)
		producerNum = cfg.GetInt64(ConfigProducerNum)
	)
	if jmqTopic != "" && jmqAddress != "" && jmqClientID != "" {
		m.jmqConfig = &JMQConfig{
			topic:    strings.Split(jmqTopic, ","),
			address:  jmqAddress,
			clientID: jmqClientID,
			produceNum: func() int64 {
				if producerNum <= 0 {
					return defaultProducerNum
				}
				return producerNum
			}(),
		}
	}

	log.LogInfof("action[parseConfig] load listen port(%v).", m.port)
	log.LogInfof("action[parseConfig] load cluster name(%v).", m.clusters)
	if m.jmqConfig != nil {
		log.LogInfof("action[parseConfig] load JMQ topics(%v).", m.jmqConfig.topic)
		log.LogInfof("action[parseConfig] load JMQ address(%v).", m.jmqConfig.address)
		log.LogInfof("action[parseConfig] load JMQ clientID(%v).", m.jmqConfig.clientID)
		log.LogInfof("action[parseConfig] load producer num(%v).", m.jmqConfig.produceNum)
	}
	return
}

func (m *Monitor) registerAPIRoutes(router *mux.Router) {
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorCollect).
		HandlerFunc(m.collect)

	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorClusterSet).
		HandlerFunc(m.setCluster)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorClusterGet).
		HandlerFunc(m.getCluster)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorClusterAdd).
		HandlerFunc(m.addCluster)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorClusterDel).
		HandlerFunc(m.delCluster)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(statistics.MonitorTopicSet).
		HandlerFunc(m.setTopic)
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

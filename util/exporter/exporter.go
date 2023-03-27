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

package exporter

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	PromHandlerPattern      = "/metrics"       // prometheus handler
	AppName                 = "cfs"            //app name
	ConfigKeyExporterEnable = "exporterEnable" //exporter enable
	ConfigKeyExporterPort   = "exporterPort"   //exporter port
	ConfigKeyConsulAddr     = "consulAddr"     //consul addr
	ChSize                  = 1024 * 10        //collect chan size
)

var (
	inited            bool
	namespace         string
	clustername       string
	modulename        string
	exporterPort      int64
	enabledPrometheus = false
	replacer          = strings.NewReplacer("-", "_", ".", "_", " ", "_", ",", "_", ":", "_")
	stopC             = make(chan struct{})
	wg                sync.WaitGroup
)

func metricsName(name string) string {
	if len(namespace) > 0 {
		return replacer.Replace(fmt.Sprintf("%s_%s", namespace, name))
	}
	return name
}

// Init initializes the exporter.
func Init(cluster, role string, cfg *config.Config) {
	defer func() {
		inited = true
		log.LogInfof("exporter [cluster: %v, role: %v, exporterPort: %v] inited.", clustername, modulename, exporterPort)
	}()

	clustername = cluster
	modulename = role

	if !cfg.GetBoolWithDefault(ConfigKeyExporterEnable, true) {
		log.LogInfof("%v exporter disabled", role)
		return
	}
	port := cfg.GetInt64(ConfigKeyExporterPort)
	if port == 0 {
		log.LogInfof("%v exporter port not set", port)
		return
	}

	exporterPort = port
	enabledPrometheus = true
	http.Handle(PromHandlerPattern, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
		Timeout: 5 * time.Second,
	}))
	namespace = AppName + "_" + role
	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{Addr: addr}
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := server.ListenAndServe()
		if err != nil {
			log.LogError("exporter http serve error: ", err)
		}
	}()
	go func() {
		defer wg.Done()
		<-stopC
		server.Shutdown(context.Background())
	}()

	collect()

	m := NewGauge("start_time")
	m.Set(time.Now().Unix() * 1000)
}

// Init initializes the exporter.
func InitWithRouter(role string, cfg *config.Config, router *mux.Router, exPort string) {
	modulename = role
	if !cfg.GetBoolWithDefault(ConfigKeyExporterEnable, true) {
		log.LogInfof("%v metrics exporter disabled", role)
		return
	}
	exporterPort, _ = strconv.ParseInt(exPort, 10, 64)
	enabledPrometheus = true
	router.NewRoute().Name("metrics").
		Methods(http.MethodGet).
		Path(PromHandlerPattern).
		Handler(promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
			Timeout: 5 * time.Second,
		}))
	namespace = AppName + "_" + role

	collect()

	m := NewGauge("start_time")
	m.Set(time.Now().Unix() * 1000)

	log.LogInfof("exporter Start: %v %v", exporterPort, m)
}

func InitRole(cluster string, role string) {
	clustername = replacer.Replace(cluster)
	modulename = role
}

// is prometheus enabled
func IsEnabled() bool {
	return enabledPrometheus
}

func RegistConsul(cfg *config.Config) {
	if !inited || !enabledPrometheus {
		log.LogInfof("skip consul registration cause exporter not inited or prometheus not enabled.")
		return
	}

	consulAddr := cfg.GetString(ConfigKeyConsulAddr)

	if len(consulAddr) == 0 {
		log.LogInfof("skip consul registration cause configured consul address is illegal.")
		return
	}

	if exporterPort == int64(0) {
		exporterPort = cfg.GetInt64(ConfigKeyExporterPort)
	}

	if exporterPort <= 0 {
		log.LogInfof("skip consul registration cause configured export port is illegal.")
		return
	}

	if exporterPort != int64(0) && len(consulAddr) > 0 {
		if ok := strings.HasPrefix(consulAddr, "http"); !ok {
			consulAddr = "http://" + consulAddr
		}
		wg.Add(1)
		go DoConsulRegisterProc(consulAddr, AppName, modulename, clustername, exporterPort)
		log.LogInfof("consul registered [addr %v, app: %v, role: %v, cluster: %v, port: %v]",
			consulAddr, AppName, modulename, clustername, exporterPort)
	}
}

func collect() {
	if !enabledPrometheus {
		return
	}
	wg.Add(4)
	go collectCounter()
	go collectGauge()
	go collectTP()
	go collectAlarm()
}

func Stop() {
	close(stopC)
	wg.Wait()
}

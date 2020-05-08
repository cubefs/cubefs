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

package exporter

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
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
	namespace         string
	clustername       string
	modulename        string
	exporterPort      int64
	enabledPrometheus = false
	replacer          = strings.NewReplacer("-", "_", ".", "_", " ", "_", ",", "_", ":", "_")
)

func metricsName(name string) string {
	return replacer.Replace(fmt.Sprintf("%s_%s", namespace, name))
}

// Init initializes the exporter.
func Init(role string, cfg *config.Config) {
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
	go func() {
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.LogError("exporter http serve error: ", err)
		}
	}()

	collect()

	m := NewGauge("start_time")
	m.Set(time.Now().Unix() * 1000)

	log.LogInfof("exporter Start: %v", addr)
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

func RegistConsul(cluster string, role string, cfg *config.Config) {
	clustername = replacer.Replace(cluster)
	consulAddr := cfg.GetString(ConfigKeyConsulAddr)
	if exporterPort == int64(0) {
		exporterPort = cfg.GetInt64(ConfigKeyExporterPort)
	}
	if exporterPort != int64(0) && len(consulAddr) > 0 {
		DoConsulRegisterProc(consulAddr, AppName, role, cluster, exporterPort)
	}
}

func collect() {
	if !enabledPrometheus {
		return
	}
	go collectCounter()
	go collectGauge()
	go collectTP()
	go collectAlarm()
}

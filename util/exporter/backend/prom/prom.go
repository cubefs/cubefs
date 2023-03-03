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

package prom

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"github.com/cubefs/cubefs/util/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	clustername  string
	modulename   string
	exporterPort int64

	stopC = make(chan struct{})
	wg    sync.WaitGroup
)

const (
	defaultHandlerPattern = "/metrics"
	chSize                = 1024 * 10 //collect chan size
	appName               = "cfs"
)

type ConsulRegisterInfo struct {
	ConsulAddress string
	ExporterPort  int64
	Module        string
	Cluster       string
}

type LabelValue struct {
	Label string
	Value string
}

func stringMD5(str string) string {
	h := md5.New()
	_, err := io.WriteString(h, str)
	if err != nil {
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// InitProm initializes the exporter.
func InitProm(cluster, role string, port int64) {

	clustername = cluster
	modulename = role
	exporterPort = port

	http.Handle(defaultHandlerPattern, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
		Timeout: 5 * time.Second,
	}))
	addr := fmt.Sprintf(":%d", exporterPort)
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
	log.LogInfof("prometheus exporter [cluster: %v, role: %v, exporterPort: %v] inited.", clustername, modulename, exporterPort)
}

// Init initializes the exporter.
func InitPromWithRouter(cluster, role string, port int64, router *mux.Router) {

	if cluster == "" || role == "" || router == nil {
		return
	}

	clustername = cluster
	modulename = role
	exporterPort = port

	router.NewRoute().Name("metrics").
		Methods(http.MethodGet).
		Path(defaultHandlerPattern).
		Handler(promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
			Timeout: 5 * time.Second,
		}))

	collect()
	log.LogInfof("prometheus exporter [cluster: %v, role: %v, exporterPort: %v] inited.", clustername, modulename, exporterPort)
}

func RegisterConsul(consulAddress string) {

	if ok := strings.HasPrefix(consulAddress, "http"); !ok {
		consulAddress = "http://" + consulAddress
	}
	wg.Add(1)
	go consoleRegisterWorker(consulAddress, appName, modulename, clustername, exporterPort)
	log.LogInfof("registered exporter to consul[%v], module[%v], cluster[%v], exporterPort[%v]",
		consulAddress, modulename, clustername, exporterPort)
}

func collect() {
	wg.Add(3)
	go collectCounter()
	go collectGauge()
	go collectorSummary()
}

func Stop() {
	close(stopC)
	wg.Wait()
	prometheus.WaitGoroutinesFinish()
}

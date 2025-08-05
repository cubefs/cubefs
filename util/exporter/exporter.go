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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
)

const (
	PromHandlerPattern      = "/metrics"       // prometheus handler
	AppName                 = "cfs"            // app name
	ConfigKeyExporterEnable = "exporterEnable" // exporter enable
	ConfigKeyExporterPort   = "exporterPort"   // exporter port
	ConfigKeyConsulAddr     = "consulAddr"     // consul addr
	ConfigKeyConsulMeta     = "consulMeta"     // consul meta
	ConfigKeyIpFilter       = "ipFilter"       // add ip filter
	ConfigKeyEnablePid      = "enablePid"      // enable report partition id
	SetEnablePidPath        = "/setEnablePid"  // set enable report partition id
	ConfigKeyPushAddr       = "pushAddr"       // enable push data to gateway
	ChSize                  = 1024 * 10        // collect chan size
	ConfigKeySubDir         = "subdir"

	// monitor label name
	Vol       = "vol"
	Disk      = "disk"
	PartId    = "partid"
	Op        = "op"
	Type      = "type"
	Err       = "err"
	Version   = "version"
	FlashNode = "flashnode"
)

var (
	namespace         string
	clustername       string
	clusterNameLk     sync.RWMutex
	modulename        string
	pushAddr          string
	exporterPort      int64
	enabledPrometheus = false
	enablePush        = false
	EnablePid         = false
	replacer          = strings.NewReplacer("-", "_", ".", "_", " ", "_", ",", "_", ":", "_")
	registry          = prometheus.NewRegistry()
)

func metricsName(name string) string {
	return replacer.Replace(fmt.Sprintf("%s_%s", namespace, name))
}

func getClusterName() string {
	clusterNameLk.RLock()
	defer clusterNameLk.RUnlock()
	return clustername
}

// Init initializes the exporter.
func Init(role string, cfg *config.Config) {
	modulename = role
	if !cfg.GetBoolWithDefault(ConfigKeyExporterEnable, true) {
		log.LogInfof("%v exporter disabled", role)
		return
	}

	EnablePid = cfg.GetBoolWithDefault(ConfigKeyEnablePid, false)
	log.LogInfo("enable report partition id info? ", EnablePid)

	port := cfg.GetInt64(ConfigKeyExporterPort)

	if port < 0 {
		log.LogInfof("%v exporter port set random default", port)
	}

	exporterPort = port
	enabledPrometheus = true

	pushAddr = cfg.GetString(ConfigKeyPushAddr)
	log.LogInfof("pushAddr %v ", pushAddr)
	if pushAddr != "" {
		enablePush = true
	}

	http.Handle(PromHandlerPattern, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
		Timeout: 60 * time.Second,
	}))

	namespace = AppName + "_" + role
	addr := fmt.Sprintf(":%d", port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.LogError("exporter tcp listen error: ", err)
		return
	}

	exporterPort = int64(l.Addr().(*net.TCPAddr).Port)

	go func() {
		err = http.Serve(l, nil)
		if err != nil {
			log.LogError("exporter http serve error: ", err)
			return
		}
	}()

	collect()

	m := NewGauge("start_time")
	m.Set(float64(time.Now().Unix() * 1000))

	log.LogInfof("exporter Start: %v", exporterPort)
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
	m.Set(float64(time.Now().Unix() * 1000))

	log.LogInfof("exporter Start: %v %v", exporterPort, m)
}

func SetEnablePid(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form error: %v", err)
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	EnablePidStr, err := strconv.ParseBool(r.FormValue("enablePid"))
	if err != nil {
		err = fmt.Errorf("parse param %v failL: %v", r.FormValue("enablePid"), err)
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	EnablePid = EnablePidStr
	buildSuccessResp(w, fmt.Sprintf("set enablePid %v success", EnablePid))
}

func buildSuccessResp(w http.ResponseWriter, data interface{}) {
	buildJSONResp(w, http.StatusOK, data, "")
}

func buildFailureResp(w http.ResponseWriter, code int, msg string) {
	buildJSONResp(w, code, nil, msg)
}

// Create response for the API request.
func buildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
		Msg  string      `json:"msg"`
	}{
		Code: code,
		Data: data,
		Msg:  msg,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	w.Write(jsonBody)
}

func RegistConsul(cluster string, role string, cfg *config.Config) {
	ipFilter := cfg.GetString(ConfigKeyIpFilter)
	host, err := GetLocalIpAddr(ipFilter)
	if err != nil {
		log.LogErrorf("get local ip error, %v", err.Error())
		return
	}

	InitRecoderWithCluster(cluster)

	rawmnt := cfg.GetString(ConfigKeySubDir)
	if rawmnt == "" {
		rawmnt = "/"
	}
	mountPoint, _ := filepath.Abs(rawmnt)
	log.LogWarnf("RegistConsul:%v, subdir %s", enablePush, mountPoint)
	if enablePush {
		log.LogWarnf("[RegisterConsul] use auto push data strategy, not register consul")
		autoPush(pushAddr, role, cluster, host, mountPoint)
		return
	}

	clusterNameLk.Lock()
	clustername = replacer.Replace(cluster)
	clusterNameLk.Unlock()

	consulAddr := cfg.GetString(ConfigKeyConsulAddr)
	consulMeta := cfg.GetString(ConfigKeyConsulMeta)

	if exporterPort == int64(0) {
		exporterPort = cfg.GetInt64(ConfigKeyExporterPort)
	}

	if exporterPort == 0 {
		log.LogInfo("config export port is 0, use default 17510")
		exporterPort = 17510
	}

	if exporterPort != int64(0) && len(consulAddr) > 0 {
		if ok := strings.HasPrefix(consulAddr, "http"); !ok {
			consulAddr = "http://" + consulAddr
		}
		go DoConsulRegisterProc(consulAddr, AppName, role, cluster, consulMeta, host, exporterPort)
	}
}

func autoPush(pushAddr, role, cluster, ip, mountPoint string) {
	pid := os.Getpid()

	client := &http.Client{
		Timeout: time.Second * 10,
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.LogWarnf("get host name failed %v", err)
	}

	pusher := push.New(pushAddr, "cbfs").
		Client(client).
		Gatherer(registry).
		Grouping("cip", ip).
		Grouping("role", role).
		Grouping("cluster", cluster).
		Grouping("pid", strconv.Itoa(pid)).
		Grouping("commit", proto.CommitID).
		Grouping("app", AppName).
		Grouping("mountPoint", mountPoint).
		Grouping("hostName", hostname)

	log.LogInfof("start push data, ip %s, addr %s, role %s, cluster %s, mountPoint %s, hostName %s",
		ip, pushAddr, role, cluster, mountPoint, hostname)

	ticker := time.NewTicker(time.Second * 15)
	go func() {
		for range ticker.C {
			if err := pusher.Push(); err != nil {
				log.LogWarnf("push monitor data to %s err, %s", pushAddr, err.Error())
			}
		}
	}()
}

func collect() {
	if !enabledPrometheus {
		return
	}
	go collectCounter()
	go collectGauge()
	go collectHistogram()
	go collectAlarm()
}

var recoder *prometheus.HistogramVec

func InitRecoderWithCluster(cluster string) {
	recoder = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "request_const_us",
			Help:        "recode cost time by us",
			ConstLabels: prometheus.Labels{"cluster": cluster},
			Buckets:     []float64{50, 100, 200, 500, 1000, 2000, 5000, 10000, 200000, 500000},
		},
		[]string{"api"},
	)
	prometheus.Register(recoder)
}

var (
	obMap = map[string]prometheus.Observer{}
	obLk  = sync.RWMutex{}
)

func RecodCost(api string, costUs int64) {
	obLk.RLock()
	ob := obMap[api]
	obLk.RUnlock()
	if ob != nil {
		ob.Observe(float64(costUs))
		return
	}

	obLk.Lock()
	defer obLk.Unlock()

	ob = obMap[api]
	if ob != nil {
		ob.Observe(float64(costUs))
		return
	}

	ob = recoder.WithLabelValues(api)
	obMap[api] = ob
	ob.Observe(float64(costUs))
}

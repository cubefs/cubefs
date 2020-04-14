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
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/gorilla/mux"
)

const (
	AppName                 = "cfs"            //app name
	ConfigKeyAppName        = "appName"        //exporter enable
	ConfigKeyExporterEnable = "exporterEnable" //exporter enable
)

var (
	namespace        string //namespace for metric
	clustername      string //metric cluster name
	role             string
	hostIP           string
	exporterDisabled = true
	appName          = AppName
)

func init() {
	hostIP, _ = GetLocalIpAddr()
}

// Init initializes the exporter.
func Init(role string, cfg *config.Config) {
	if appName = cfg.GetString(ConfigKeyAppName); appName == "" {
		appName = AppName
	}

	SetRole(role)

	if !cfg.GetBoolWithDefault(ConfigKeyExporterEnable, true) {
		log.LogInfof("%v metrics exporter disabled", cfg)
		exporterDisabled = true
		return
	}

	//register backend by config
	if promCfg := ParsePromConfig(cfg); promCfg != nil && promCfg.enabled {
		exporterDisabled = false
		b := NewPromBackend(promCfg)
		CollectorInstance().RegisterBackend(PromBackendName, b)
	}

	if conf := ParseTSDBConfig(cfg); conf != nil {
		exporterDisabled = false
		b := NewTSDBBackend(conf)
		CollectorInstance().RegisterBackend(TSDBConfigKey, b)
	}

	// start collector
	CollectorInstance().Start()
}

// Init initializes the exporter.
func InitWithRouter(role string, cfg *config.Config, router *mux.Router, exPort string) {
	if appName = cfg.GetString(ConfigKeyAppName); appName == "" {
		appName = AppName
	}

	SetRole(role)

	if !cfg.GetBoolWithDefault(ConfigKeyExporterEnable, true) {
		log.LogInfof("exporter: %v metrics exporter disabled", cfg)
		exporterDisabled = true
		return
	}

	if promCfg := ParsePromConfigWithRouter(cfg, router, exPort); promCfg != nil && promCfg.enabled {
		exporterDisabled = false
		b := NewPromBackend(promCfg)
		CollectorInstance().RegisterBackend(PromBackendName, b)
	}

	if conf := ParseTSDBConfig(cfg); conf != nil {
		exporterDisabled = false
		b := NewTSDBBackend(conf)
		CollectorInstance().RegisterBackend(TSDBConfigKey, b)
	}

	CollectorInstance().Start()
}

// stop exporter
func Stop() {
	CollectorInstance().Stop()
	log.LogInfo("exporter stopped")
}

func IsEnabled() bool {
	return !exporterDisabled
}

func SetNamespace(role string) {
	namespace = appName + "_" + role
}

func Namespace() string {
	return namespace
}

func SetClusterName(cluster string) {
	clustername = replacer.Replace(cluster)
}

func ClusterName() string {
	return clustername
}

func SetRole(r string) {
	role = r
	SetNamespace(role)
}

func Role() string {
	return role
}

func HostIP() string {
	return hostIP
}

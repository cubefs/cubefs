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
	"strings"
	"sync"

	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter/backend/prom"
	"github.com/cubefs/cubefs/util/exporter/backend/ump"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
)

type UMPCollectMethod = ump.CollectMethod

const (
	UMPCollectMethodUnknown = ump.CollectMethodUnknown
	UMPCollectMethodFile    = ump.CollectMethodFile
	UMPCollectMethodJMTP    = ump.CollectMethodJMTP
)

const (
	PromHandlerPattern = "/metrics" // prometheus handler

	ConfigKeyExporterEnable = "exporterEnable" //exporter enable
	ConfigKeyExporterPort   = "exporterPort"   //exporter port
	ConfigKeyConsulAddr     = "consulAddr"     //consul addr

	umpAppName = "jdos_chubaofs-node"
)

type LabelValue = prom.LabelValue

var (
	clusterName string
	moduleName  string
	zoneName    string

	replacer = strings.NewReplacer("-", "_", ".", "_", " ", "_", ",", "_", ":", "_")
	wg       sync.WaitGroup

	umpEnabled, promEnabled bool
)

// Init initializes the exporter.
func Init(cluster, module, zone string, cfg *config.Config) {

	clusterName = replaceSpace(cluster)
	moduleName = replaceSpace(module)
	zoneName = replaceSpace(zone)

	if err := ump.InitUmp(module, umpAppName); err == nil {
		umpEnabled = true
	}

	if cfg == nil {
		log.LogInfof("skip init prometheus exporter cause no valid extend configure have been specified")
		return
	}

	port := cfg.GetInt64(ConfigKeyExporterPort)
	if port == 0 {
		log.LogInfof("skip init prometheus exporter cause exporter port is not configured")
		return
	}

	prom.InitProm(cluster, module, port)
	promEnabled = true

	consulAddress := cfg.GetString(ConfigKeyConsulAddr)

	if len(consulAddress) == 0 {
		log.LogInfof("skip consul registration cause configured consul address is illegal.")
		return
	}

	prom.RegisterConsul(consulAddress)
}

// Init initializes the exporter.
func InitWithRouter(cluster, module string, cfg *config.Config, router *mux.Router, exporterPort int64) {
	clusterName = cluster
	moduleName = module

	if err := ump.InitUmp(module, umpAppName); err == nil {
		umpEnabled = true
	}

	prom.InitPromWithRouter(cluster, module, exporterPort, router)
	promEnabled = true

	if cfg == nil {
		log.LogInfof("skip init prometheus exporter cause no valid extend configure have been specified")
		return
	}

	consulAddress := cfg.GetString(ConfigKeyConsulAddr)

	if len(consulAddress) == 0 {
		log.LogInfof("skip consul registration cause configured consul address is illegal.")
		return
	}

	prom.RegisterConsul(consulAddress)
}

func SetCluster(cluster string) {
	if clean := replaceSpace(cluster); cluster != clean {
		clusterName = clean
	}
}

func SetModule(module string) {
	if clean := replaceSpace(module); moduleName != clean {
		moduleName = clean
	}
}

func SetZone(zone string) {
	if clean := replaceSpace(zone); zoneName != clean {
		zoneName = clean
	}
}

func GetUmpCollectMethod() UMPCollectMethod {
	return ump.GetUmpCollectMethod()
}

func SetUMPCollectMethod(method UMPCollectMethod) {
	ump.SetUmpCollectMethod(method)
}

func SetUMPJMTPAddress(address string) {
	ump.SetUmpJmtpAddr(address)
}

func SetUmpJMTPBatch(batch uint) {
	ump.SetUmpJmtpBatch(batch)
}

func Stop() {
	if umpEnabled {
		ump.StopUmp()
	}
	if promEnabled {
		prom.Stop()
	}
}

func replaceSpace(src string) string {
	return strings.Replace(src, " ", "", -1)
}

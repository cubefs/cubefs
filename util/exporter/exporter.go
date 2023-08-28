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
	"fmt"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter/backend/prom"
	"github.com/cubefs/cubefs/util/exporter/backend/ump"
	"github.com/cubefs/cubefs/util/log"
)

type UMPCollectMethod = ump.CollectMethod

const (
	UMPCollectMethodUnknown = ump.CollectMethodUnknown
	UMPCollectMethodFile    = ump.CollectMethodFile
	UMPCollectMethodJMTP    = ump.CollectMethodJMTP
)

const (
	ConfigKeyExporterPort = "exporterPort" //exporter port
	ConfigKeyConsulAddr   = "consulAddr"   //consul addr

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

type Option struct {
	Cluster       string
	Module        string
	Zone          string
	UmpFilePrefix string
	ExporterPort  int64
	ConsulAddr    string
}

func (o *Option) WithCluster(cluster string) *Option {
	o.Cluster = cluster
	return o
}

func (o *Option) WithModule(module string) *Option {
	o.Module = module
	return o
}

func (o *Option) WithZone(zone string) *Option {
	o.Zone = zone
	return o
}

func (o *Option) WithUmpFilePrefix(prefix string) *Option {
	o.UmpFilePrefix = prefix
	return o
}

func (o *Option) WithExporterPort(port int64) *Option {
	o.ExporterPort = port
	return o
}

func (o *Option) WithConsulAddr(addr string) *Option {
	o.ConsulAddr = addr
	return o
}

func (o *Option) String() string {
	return fmt.Sprintf("cluster = %v, module = %v, zone = %v, umpfileprefix = %v, exporterport = %v, consuladdr = %v",
		o.Cluster, o.Module, o.Zone, o.UmpFilePrefix, o.ExporterPort, o.ConsulAddr)
}

func NewOption() *Option {
	return new(Option)
}

func NewOptionFromConfig(cfg *config.Config) *Option {
	var opt = NewOption()
	if cfg != nil {
		opt.ExporterPort = cfg.GetInt64(ConfigKeyExporterPort)
		opt.ConsulAddr = cfg.GetString(ConfigKeyConsulAddr)
	}
	return opt
}

// Init initializes the exporter.
func Init(opt *Option) {

	defer func() {
		log.LogInfof("exporter inited with option(%v)", opt)
	}()

	clusterName = replaceSpace(opt.Cluster)
	moduleName = replaceSpace(opt.Module)
	zoneName = replaceSpace(opt.Zone)

	var umpFilePrefix string
	if umpFilePrefix = replaceSpace(opt.UmpFilePrefix); umpFilePrefix == "" {
		umpFilePrefix = moduleName
	}
	if err := ump.InitUmp(umpFilePrefix, umpAppName); err == nil {
		umpEnabled = true
	}

	if opt.ExporterPort == 0 {
		log.LogInfof("skip init prometheus exporter cause exporter port is not configured")
		return
	}

	prom.InitProm(clusterName, moduleName, opt.ExporterPort)
	promEnabled = true

	if len(opt.ConsulAddr) == 0 {
		log.LogInfof("skip consul registration cause configured consul address is illegal.")
		return
	}

	prom.RegisterConsul(opt.ConsulAddr)
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

// Copyright 2019 The ChubaoFS Authors.
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

package objectnode

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/statistics"

	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/exporter"

	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/gorilla/mux"
)

// Configuration items that act on the ObjectNode.
const (
	// String type configuration type, used to configure the listening port number of the service.
	// Example:
	//		{
	//			"listen": "80"
	//		}
	configListen = proto.ListenPort

	// String array configuration item, used to configure the hostname or IP address of the cluster master node.
	// The ObjectNode needs to communicate with the Master during the startup and running process to update the
	// cluster, user and volume information.
	// Example:
	//		{
	//			"masterAddr":[
	//				"master1.chubao.io",
	//				"master2.chubao.io",
	//				"master3.chubao.io"
	//			]
	//		}
	configMasterAddr = proto.MasterAddr

	// A bool type configuration is used to ensure that the topology information is consistent with the cluster
	// in real time during the compatibility test. If true, the object node will not cache user information and
	// volume topology. This configuration will cause a drastic decrease in performance after being turned on,
	// and can only be turned on for protocol compatibility testing.
	// Example:
	//		{
	//			"strict": true
	//		}
	configStrict = "strict"

	// The character creation array configuration item is used to configure the domain name bound to the object
	// storage interface. You can bind multiple. ObjectNode uses this configuration to implement automatic
	// resolution of pan-domain names.
	// Example:
	//		{
	//			"domains": [
	//				"object.chubao.io"
	//			]
	//		}
	// The configuration in the example will allow ObjectNode to automatically resolve "* .object.chubao.io".
	configDomains = "domains"

	disabledActions               = "disabledActions"
	configSignatureIgnoredActions = "signatureIgnoredActions"
)

// Default of configuration value
const (
	defaultListen = "80"
)

var (
	// Regular expression used to verify the configuration of the service listening port.
	// A valid service listening port configuration is a string containing only numbers.
	regexpListen = regexp.MustCompile("^(\\d)+$")
)

type ObjectNode struct {
	domains    []string
	wildcards  Wildcards
	listen     string
	region     string
	httpServer *http.Server
	vm         *VolumeManager
	mc         *master.MasterClient
	state      uint32
	wg         sync.WaitGroup
	userStore  UserInfoStore

	signatureIgnoredActions proto.Actions // signature ignored actions
	disabledActions         proto.Actions // disabled actions

	encodedRegion []byte

	statistics sync.Map // volume(string) -> []*statistics.MonitorData

	control common.Control
}

func (o *ObjectNode) Start(cfg *config.Config) (err error) {
	return o.control.Start(o, cfg, handleStart)
}

func (o *ObjectNode) Shutdown() {
	o.control.Shutdown(o, handleShutdown)
}

func (o *ObjectNode) Sync() {
	o.control.Sync()
}

func (o *ObjectNode) loadConfig(cfg *config.Config) (err error) {
	// parse listen
	listen := cfg.GetString(configListen)
	if len(listen) == 0 {
		listen = defaultListen
	}
	if match := regexpListen.MatchString(listen); !match {
		err = errors.New("invalid listen configuration")
		return
	}
	o.listen = listen
	log.LogInfof("loadConfig: setup config: %v(%v)", configListen, listen)

	// parse domain
	domains := cfg.GetStringSlice(configDomains)
	o.domains = domains
	if o.wildcards, err = NewWildcards(domains); err != nil {
		return
	}
	log.LogInfof("loadConfig: setup config: %v(%v)", configDomains, domains)

	// parse master config
	masters := cfg.GetStringSlice(configMasterAddr)
	if len(masters) == 0 {
		return config.NewIllegalConfigError(configMasterAddr)
	}
	log.LogInfof("loadConfig: setup config: %v(%v)", configMasterAddr, strings.Join(masters, ","))

	// parse signature ignored actions
	signatureIgnoredActionNames := cfg.GetStringSlice(configSignatureIgnoredActions)
	for _, actionName := range signatureIgnoredActionNames {
		action := proto.ParseAction(actionName)
		if !action.IsNone() {
			o.signatureIgnoredActions = append(o.signatureIgnoredActions, action)
			log.LogInfof("loadConfig: signature ignored action: %v", action)
		}
	}

	// parse disabled actions
	disabledActions := cfg.GetStringSlice(disabledActions)
	for _, actionName := range disabledActions {
		action := proto.ParseAction(actionName)
		if !action.IsNone() {
			o.disabledActions = append(o.disabledActions, action)
			log.LogInfof("loadConfig: disabled action: %v", action)
		}
	}

	// parse strict config
	strict := cfg.GetBool(configStrict)
	log.LogInfof("loadConfig: strict: %v", strict)

	o.mc = master.NewMasterClient(masters, false)
	o.vm = NewVolumeManager(masters, strict)
	o.userStore = NewUserInfoStore(masters, strict)

	return
}

func (o *ObjectNode) updateRegion(region string) {
	o.region = region
	o.encodedRegion =
		[]byte(fmt.Sprintf(fmt.Sprintf("<LocationConstraint>%s</LocationConstraint>", o.region)))
}

func handleStart(s common.Server, cfg *config.Config) (err error) {
	o, ok := s.(*ObjectNode)
	if !ok {
		return errors.New("Invalid Node Type!")
	}
	// parse config
	if err = o.loadConfig(cfg); err != nil {
		return
	}

	// Get cluster info from master
	var ci *proto.ClusterInfo
	for {
		if ci, err = o.mc.AdminAPI().GetClusterInfo(); err != nil {
			log.LogErrorf("fetch cluster info from master service failed and will retry after 1s, error message: %v", err)
			time.Sleep(time.Second * 1)
			continue
		}
		break
	}
	o.updateRegion(ci.Cluster)
	log.LogInfof("handleStart: get cluster information: region(%v)", o.region)

	// start rest api
	if err = o.startMuxRestAPI(); err != nil {
		log.LogInfof("handleStart: start rest api fail: err(%v)", err)
		return
	}

	exporter.Init(ci.Cluster, cfg.GetString("role"), cfg)
	exporter.RegistConsul(cfg)

	// Init SRE monitor
	statistics.InitStatistics(cfg, ci.Cluster, statistics.ModelObjectNode, ci.Ip, o.reportSummary)

	log.LogInfo("object subsystem start success")
	return
}

func handleShutdown(s common.Server) {
	o, ok := s.(*ObjectNode)
	if !ok {
		return
	}
	o.shutdownRestAPI()
}

func (o *ObjectNode) startMuxRestAPI() (err error) {
	router := mux.NewRouter().SkipClean(true)
	o.registerApiRouters(router)
	router.Use(
		o.expectMiddleware,
		o.corsMiddleware,
		o.traceMiddleware,
		o.authMiddleware,
		o.policyCheckMiddleware,
		o.contentMiddleware,
	)

	var server = &http.Server{
		Addr:    ":" + o.listen,
		Handler: router,
	}

	go func() {
		if err = server.ListenAndServe(); err != nil {
			log.LogErrorf("startMuxRestAPI: start http server fail, err(%v)", err)
			return
		}
	}()
	o.httpServer = server
	return
}

func (o *ObjectNode) shutdownRestAPI() {
	if o.httpServer != nil {
		_ = o.httpServer.Shutdown(context.Background())
		o.httpServer = nil
	}
}

func NewServer() *ObjectNode {
	return &ObjectNode{}
}

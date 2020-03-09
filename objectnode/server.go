// Copyright 2018 The ChubaoFS Authors.
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
	"net/http"
	"regexp"
	"sync"

	"github.com/chubaofs/chubaofs/util/config"

	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/gorilla/mux"
)

// Configuration keys
const (
	configListen                  = "listen"
	configDomains                 = "domains"
	configEnableHTTPS             = "enableHTTPS"
	configSignatureIgnoredActions = "signatureIgnoredActions"
)

// Default of configuration value
const (
	defaultListen = ":80"
)

var (
	regexpListen = regexp.MustCompile("^(([0-9]{1,3}.){3}([0-9]{1,3}))?:(\\d)+$")
)

type ObjectNode struct {
	domains    []string
	wildcards  Wildcards
	listen     string
	region     string
	httpServer *http.Server
	vm         VolumeManager
	mc         *master.MasterClient
	state      uint32
	wg         sync.WaitGroup
	userStore  *UserStore

	signatureIgnoredActions Actions // signature ignored actions

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
	listen := cfg.GetString(proto.ListenPort)
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
	enableHTTPS := cfg.GetBool(configEnableHTTPS)
	masters := cfg.GetStringSlice(proto.MasterAddr)
	if len(masters) == 0 {
		return config.NewIllegalConfigError(proto.MasterAddr)
	}
	log.LogInfof("loadConfig: setup config: %v(%v)", proto.MasterAddr, masters)

	// parse signature ignored actions
	signatureIgnoredActionNames := cfg.GetStringSlice(configSignatureIgnoredActions)
	for _, actionName := range signatureIgnoredActionNames {
		action := ActionFromString(actionName)
		if action.IsKnown() {
			o.signatureIgnoredActions = append(o.signatureIgnoredActions, action)
			log.LogInfof("loadConfig: signature ignored action: %v", action)
		}
	}

	o.mc = master.NewMasterClient(masters, false)
	o.vm = NewVolumeManager(masters)
	o.vm.InitStore(new(xattrStore))
	o.vm.InitMasterClient(masters, enableHTTPS)
	o.userStore = o.newUserStore()

	return
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
	if ci, err = o.mc.AdminAPI().GetClusterInfo(); err != nil {
		return
	}
	o.region = ci.Cluster
	log.LogInfof("handleStart: get cluster information: region(%v)", o.region)

	// start rest api
	if err = o.startMuxRestAPI(); err != nil {
		log.LogInfof("handleStart: start rest api fail: err(%v)", err)
		return
	}
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
		o.traceMiddleware,
		o.authMiddleware,
		o.policyCheckMiddleware,
		o.contentMiddleware,
	)

	var server = &http.Server{
		Addr:    o.listen,
		Handler: router,
	}

	go func() {
		if err = server.ListenAndServe(); err != nil {
			log.LogErrorf("startMuxRestAPI: start http server fail, err(%o)", err)
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

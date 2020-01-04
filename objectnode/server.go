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
	"net/http"
	"regexp"
	"sync"
	"sync/atomic"

	authSDK "github.com/chubaofs/chubaofs/sdk/auth"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/gorilla/mux"
)

// The status of the s3 server
const (
	Standby uint32 = iota
	Start
	Running
	Shutdown
	Stopped
)

// Configuration keys
const (
	configListen      = "listen"
	configDomains     = "domains"
	configMasters     = "masters"
	configRegion      = "region"
	configAuthnodes   = "authNodes"
	configAuthkey     = "authKey"
	configEnableHTTPS = "enableHTTPS"
	configCertFile    = "certFile"
)

// Default of configuration value
const (
	defaultListen = ":80"
	defaultRegion = "cfs_default"
)

var (
	regexpListen = regexp.MustCompile("^(([0-9]{1,3}.){3}([0-9]{1,3}))?:(\\d)+$")
)

type ObjectNode struct {
	domains    []string
	listen     string
	region     string
	httpServer *http.Server
	vm         VolumeManager
	state      uint32
	wg         sync.WaitGroup
	authKey    string
	authClient *authSDK.AuthClient
}

func (o *ObjectNode) Start(cfg *config.Config) (err error) {
	if atomic.CompareAndSwapUint32(&o.state, Standby, Start) {
		defer func() {
			if err != nil {
				atomic.StoreUint32(&o.state, Standby)
			} else {
				atomic.StoreUint32(&o.state, Running)
			}
		}()
		if err = o.handleStart(cfg); err != nil {
			return
		}
		o.wg.Add(1)
	}
	return
}

func (o *ObjectNode) Shutdown() {
	if atomic.CompareAndSwapUint32(&o.state, Running, Shutdown) {
		o.handleShutdown()
		o.wg.Done()
		atomic.StoreUint32(&o.state, Stopped)
	}
}

func (o *ObjectNode) Sync() {
	if atomic.LoadUint32(&o.state) == Running {
		o.wg.Wait()
	}
}

func (o *ObjectNode) parseConfig(cfg *config.Config) (err error) {
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

	// parse domain
	domainCfgs := cfg.GetArray(configDomains)
	domains := make([]string, len(domainCfgs))
	for i, domainCfg := range domainCfgs {
		domains[i] = domainCfg.(string)
	}
	o.domains = domains

	// parse master config
	masterCfgs := cfg.GetArray(configMasters)
	masters := make([]string, len(masterCfgs))
	for i, masterCfg := range masterCfgs {
		masters[i] = masterCfg.(string)
	}
	o.vm = NewVolumeManager(masters)
	o.vm.InitStore(new(xattrStore))

	// parse region
	region := cfg.GetString(configRegion)
	if len(region) == 0 {
		region = defaultRegion
	}
	o.region = region

	//parse authnode info
	authNodes := cfg.GetString(configAuthnodes)
	enableHTTPS := cfg.GetBool(configEnableHTTPS)
	certFile := cfg.GetString(configCertFile)
	o.authKey = cfg.GetString(configAuthkey)
	o.authClient = authSDK.NewAuthClient(authNodes, enableHTTPS, certFile)
	return
}

func (o *ObjectNode) handleStart(cfg *config.Config) (err error) {
	// parse config
	if err = o.parseConfig(cfg); err != nil {
		return
	}

	// start rest api
	if err = o.startMuxRestAPI(); err != nil {
		log.LogInfof("handleStart: start mux rest api fail, err(%v)", err)
		return
	}
	log.LogInfo("s3node start success")
	return
}

func (o *ObjectNode) handleShutdown() {
	o.shutdownRestAPI()
}

func (o *ObjectNode) startMuxRestAPI() (err error) {
	router := mux.NewRouter().SkipClean(true)
	o.registerApiRouters(router)
	router.Use(
		o.traceMiddleware,
		o.authMiddleware,
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

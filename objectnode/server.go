// Copyright 2019 The CubeFS Authors.
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
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blockcache/bcache"
	"github.com/cubefs/cubefs/sdk/data/blobstore"
	"github.com/hashicorp/consul/api"
	"net/http"
	"path"
	"regexp"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
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

	//ObjMetaCache takes each path hierarchy of the path-like S3 object key as the cache key,
	//and map it to the corresponding posix-compatible inode
	// when enabled, the maxDentryCacheNum must at least be the minimum of defaultMaxDentryCacheNum
	// Example:
	//		{
	//			"enableObjMetaCache": true
	//		}
	configObjMetaCache = "enableObjMetaCache"
	// Example:
	//		{
	//			"cacheRefreshInterval": 600
	//			"maxDentryCacheNum": 10000000
	//			"maxInodeAttrCacheNum": 10000000
	//		}
	configCacheRefreshInterval = "cacheRefreshInterval"
	configMaxDentryCacheNum    = "maxDentryCacheNum"
	configMaxInodeAttrCacheNum = "maxInodeAttrCacheNum"

	//enable block cache when reading data in cold volume
	enableBcache = "enableBcache"
	//define thread numbers for writing and reading ebs
	ebsWriteThreads = "bStoreWriteThreads"
	ebsReadThreads  = "bStoreReadThreads"
)

// Default of configuration value
const (
	defaultListen               = "80"
	defaultCacheRefreshInterval = 10 * 60
	defaultMaxDentryCacheNum    = 10000000
	defaultMaxInodeAttrCacheNum = 10000000
	//ebs
	MaxSizePutOnce = int64(1) << 23
)

var (
	// Regular expression used to verify the configuration of the service listening port.
	// A valid service listening port configuration is a string containing only numbers.
	regexpListen     = regexp.MustCompile("^(\\d)+$")
	objMetaCache     *ObjMetaCache
	blockCache       *bcache.BcacheClient
	ebsClient        *blobstore.BlobStoreClient
	writeThreads     int = 4
	readThreads      int = 4
	enableBlockcache bool
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

	// parse inode cache
	cacheEnable := cfg.GetBool(configObjMetaCache)
	if cacheEnable {

		cacheRefreshInterval := uint64(cfg.GetInt64(configCacheRefreshInterval))
		if cacheRefreshInterval <= 0 {
			cacheRefreshInterval = defaultCacheRefreshInterval
		}

		maxDentryCacheNum := cfg.GetInt64(configMaxDentryCacheNum)
		if maxDentryCacheNum < defaultMaxDentryCacheNum {
			maxDentryCacheNum = defaultMaxDentryCacheNum
		}

		maxInodeAttrCacheNum := cfg.GetInt64(configMaxInodeAttrCacheNum)
		if maxInodeAttrCacheNum < defaultMaxInodeAttrCacheNum {
			maxInodeAttrCacheNum = defaultMaxInodeAttrCacheNum
		}
		objMetaCache = NewObjMetaCache(maxDentryCacheNum, maxInodeAttrCacheNum, cacheRefreshInterval)
		log.LogInfof("loadConfig: enableObjMetaCache: %v, maxDentryCacheNum: %v, maxInodeAttrCacheNum: %v"+
			", cacheRefreshInterval: %v", cacheEnable, maxDentryCacheNum, maxInodeAttrCacheNum, cacheRefreshInterval)
	}

	enableBlockcache = cfg.GetBool(enableBcache)
	if enableBlockcache {
		blockCache = bcache.NewBcacheClient()
	}

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
		return errors.New("Invalid node Type!")
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
	o.updateRegion(ci.Cluster)
	log.LogInfof("handleStart: get cluster information: region(%v)", o.region)
	ebsClient, err = blobstore.NewEbsClient(access.Config{
		ConnMode: access.NoLimitConnMode,
		Consul: api.Config{
			Address: ci.EbsAddr,
		},
		//ServicePath:    ci.ServicePath,
		MaxSizePutOnce: MaxSizePutOnce,
		Logger: &access.Logger{
			Filename: path.Join(cfg.GetString("logDir"), "ebs.log"),
		},
	})

	if err != nil {
		wt := cfg.GetInt(ebsWriteThreads)
		if wt != 0 {
			writeThreads = wt
		}
		rt := cfg.GetInt(ebsReadThreads)
		if rt != 0 {
			readThreads = rt
		}
	}

	// start rest api
	if err = o.startMuxRestAPI(); err != nil {
		log.LogInfof("handleStart: start rest api fail: err(%v)", err)
		return
	}

	exporter.Init(cfg.GetString("role"), cfg)
	exporter.RegistConsul(ci.Cluster, cfg.GetString("role"), cfg)

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

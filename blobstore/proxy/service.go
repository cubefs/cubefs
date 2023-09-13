// Copyright 2022 The CubeFS Authors.
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

package proxy

import (
	"context"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/proxy/cacher"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	// register service
	defaultHeartbeatIntervalS = uint32(3)
	defaultHeartbeatTicks     = uint32(30)
	defaultExpiresTicks       = uint32(60)
	defaultTimeoutMS          = int64(1000)
)

var (
	service *Service
	conf    Config
)

type Config struct {
	cmd.Config
	cacher.ConfigCache

	Idc                string            `json:"idc"`
	Host               string            `json:"host"`
	ClusterID          proto.ClusterID   `json:"cluster_id"`
	HeartbeatIntervalS uint32            `json:"heartbeat_interval_s"` // proxy heartbeat interval to ClusterManager
	HeartbeatTicks     uint32            `json:"heartbeat_ticks"`
	ExpiresTicks       uint32            `json:"expires_ticks"`
	Clustermgr         clustermgr.Config `json:"clustermgr"`
}

type Service struct {
	Config

	// cacher
	cacher cacher.Cacher
}

func init() {
	mod := &cmd.Module{
		Name:       proto.ServiceNameProxy,
		InitConfig: initConfig,
		SetUp:      setUp,
		TearDown:   tearDown,
	}
	cmd.RegisterGracefulModule(mod)
}

func initConfig(args []string) (*cmd.Config, error) {
	config.Init("f", "", "proxy.conf")
	if err := config.Load(&conf); err != nil {
		return nil, err
	}
	return &conf.Config, nil
}

func setUp() (*rpc.Router, []rpc.ProgressHandler) {
	cmcli := clustermgr.New(&conf.Clustermgr)
	service = New(conf, cmcli)
	return NewHandler(service), nil
}

func tearDown() {}

func New(cfg Config, cmcli clustermgr.APIProxy) *Service {
	if err := cfg.checkAndFix(); err != nil {
		log.Fatalf("init proxy failed, err: %s", err.Error())
	}

	cacher, err := cacher.New(cfg.ClusterID, cfg.ConfigCache, cmcli)
	if err != nil {
		log.Fatalf("fail to new cacher, error: %s", err.Error())
	}

	// register to clustermgr
	node := clustermgr.ServiceNode{
		ClusterID: uint64(cfg.ClusterID),
		Name:      proto.ServiceNameProxy,
		Host:      cfg.Host,
		Idc:       cfg.Idc,
	}
	if err := cmcli.RegisterService(context.Background(), node,
		cfg.HeartbeatIntervalS, cfg.HeartbeatTicks, cfg.ExpiresTicks); err != nil {
		log.Fatalf("proxy register to clustermgr error:%v", err)
	}

	return &Service{
		Config: cfg,
		cacher: cacher,
	}
}

func NewHandler(service *Service) *rpc.Router {
	router := rpc.New()
	rpc.RegisterArgsParser(&clustermgr.CacheVolumeArgs{}, "json")
	rpc.RegisterArgsParser(&clustermgr.CacheDiskArgs{}, "json")

	// GET /cache/volume/{vid}?flush={flush}&version={version}
	// response body: json
	router.Handle(http.MethodGet, "/cache/volume/:vid", service.GetCacheVolume, rpc.OptArgsURI(), rpc.OptArgsQuery())
	// GET /cache/disk/{disk_id}?flush={flush}
	// response body: json
	router.Handle(http.MethodGet, "/cache/disk/:disk_id", service.GetCacheDisk, rpc.OptArgsURI(), rpc.OptArgsQuery())
	router.Handle(http.MethodDelete, "/cache/erase/:key", service.EraseCache, rpc.OptArgsURI())

	return router
}

func (c *Config) checkAndFix() (err error) {
	defaulter.Equal(&c.HeartbeatIntervalS, defaultHeartbeatIntervalS)
	defaulter.Equal(&c.HeartbeatTicks, defaultHeartbeatTicks)
	defaulter.Equal(&c.ExpiresTicks, defaultExpiresTicks)
	defaulter.LessOrEqual(&c.Clustermgr.Config.ClientTimeoutMs, defaultTimeoutMS)
	return nil
}

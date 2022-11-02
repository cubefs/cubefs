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
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	alloc "github.com/cubefs/cubefs/blobstore/proxy/allocator"
	"github.com/cubefs/cubefs/blobstore/proxy/cacher"
	"github.com/cubefs/cubefs/blobstore/proxy/mq"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
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

	// ErrIllegalTopic illegal topic
	ErrIllegalTopic = errors.New("illegal topic")
)

// MQConfig is mq config
type MQConfig struct {
	BlobDeleteTopic          string            `json:"blob_delete_topic"`
	ShardRepairTopic         string            `json:"shard_repair_topic"`
	ShardRepairPriorityTopic string            `json:"shard_repair_priority_topic"`
	MsgSender                kafka.ProducerCfg `json:"msg_sender"`
}

type Config struct {
	cmd.Config

	alloc.BlobConfig
	alloc.VolConfig
	cacher.ConfigCache

	HeartbeatIntervalS uint32            `json:"heartbeat_interval_s"` // proxy heartbeat interval to ClusterManager
	HeartbeatTicks     uint32            `json:"heartbeat_ticks"`
	ExpiresTicks       uint32            `json:"expires_ticks"`
	Clustermgr         clustermgr.Config `json:"clustermgr"`
	MQ                 MQConfig          `json:"mq"`
}

func (c *Config) blobDeleteCfg() mq.BlobDeleteConfig {
	return mq.BlobDeleteConfig{
		Topic:        c.MQ.BlobDeleteTopic,
		MsgSenderCfg: c.MQ.MsgSender,
	}
}

func (c *Config) shardRepairCfg() mq.ShardRepairConfig {
	return mq.ShardRepairConfig{
		Topic:         c.MQ.ShardRepairTopic,
		PriorityTopic: c.MQ.ShardRepairPriorityTopic,
		MsgSenderCfg:  c.MQ.MsgSender,
	}
}

type Service struct {
	Config

	// mq
	shardRepairMgr mq.ShardRepairHandler
	blobDeleteMgr  mq.BlobDeleteHandler
	// allocator
	volumeMgr alloc.VolumeMgr
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

func tearDown() {
	service.volumeMgr.Close()
}

func New(cfg Config, cmcli clustermgr.APIProxy) *Service {
	if err := cfg.checkAndFix(); err != nil {
		log.Fatalf("init proxy failed, err: %s", err.Error())
	}

	// mq
	blobDeleteMgr, err := mq.NewBlobDeleteMgr(cfg.blobDeleteCfg())
	if err != nil {
		log.Fatalf("fail to new blobDeleteMgr, error: %s", err.Error())
	}
	shardRepairMgr, err := mq.NewShardRepairMgr(cfg.shardRepairCfg())
	if err != nil {
		log.Fatalf("fail to new shardRepairMgr, error: %s", err.Error())
	}

	// allocator
	volumeMgr, err := alloc.NewVolumeMgr(context.Background(), cfg.BlobConfig, cfg.VolConfig, cmcli)
	if err != nil {
		log.Fatalf("fail to new volumeMgr, error: %s", err.Error())
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
		Config:         cfg,
		volumeMgr:      volumeMgr,
		cacher:         cacher,
		shardRepairMgr: shardRepairMgr,
		blobDeleteMgr:  blobDeleteMgr,
	}
}

func NewHandler(service *Service) *rpc.Router {
	router := rpc.New()
	rpc.RegisterArgsParser(&proxy.ListVolsArgs{}, "json")
	rpc.RegisterArgsParser(&proxy.CacheVolumeArgs{}, "json")

	// POST /volume/alloc
	// request  body:  json
	// response body:  json
	router.Handle(http.MethodPost, "/volume/alloc", service.Alloc, rpc.OptArgsBody())

	// GET /volume/list?code_mode={code_mode}
	router.Handle(http.MethodGet, "/volume/list", service.List, rpc.OptArgsQuery())

	// POST /repairmsg
	// request body: json
	router.Handle(http.MethodPost, "/repairmsg", service.SendRepairMessage, rpc.OptArgsBody())

	// POST /deletemsg
	// request body: json
	router.Handle(http.MethodPost, "/deletemsg", service.SendDeleteMessage, rpc.OptArgsBody())

	// GET /cache/volume/{vid}?flush={flush}&version={version}
	// response body: json
	router.Handle(http.MethodGet, "/cache/volume/:vid", service.GetCacheVolume, rpc.OptArgsURI(), rpc.OptArgsQuery())

	return router
}

func (c *Config) checkAndFix() (err error) {
	// check topic cfg
	if c.MQ.BlobDeleteTopic == "" || c.MQ.ShardRepairTopic == "" || c.MQ.ShardRepairPriorityTopic == "" {
		return ErrIllegalTopic
	}

	if c.MQ.BlobDeleteTopic == c.MQ.ShardRepairTopic || c.MQ.BlobDeleteTopic == c.MQ.ShardRepairPriorityTopic {
		return ErrIllegalTopic
	}
	defaulter.Equal(&c.HeartbeatIntervalS, defaultHeartbeatIntervalS)
	defaulter.Equal(&c.HeartbeatTicks, defaultHeartbeatTicks)
	defaulter.Equal(&c.ExpiresTicks, defaultExpiresTicks)
	defaulter.LessOrEqual(&c.Clustermgr.Config.ClientTimeoutMs, defaultTimeoutMS)
	defaulter.LessOrEqual(&c.MQ.MsgSender.TimeoutMs, defaultTimeoutMS)
	return nil
}

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

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	httpproxy "net/http/httputil"
	"net/url"
	"time"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	localHost = "127.0.0.1"
	scheme    = "http://"
)

var (
	errIllegalClusterID = errors.New("illegal cluster_id")
	errInvalidHourRange = errors.New("invalid hour range")
	errInvalidMembers   = errors.New("invalid members")
	errInvalidLeader    = errors.New("invalid leader")
	errInvalidNodeID    = errors.New("invalid node_id")
	errInvalidKafka     = errors.New("invalid kafka")
)

var (
	service *Service
	conf    Config
)

func init() {
	mod := &cmd.Module{
		Name:       proto.ServiceNameScheduler,
		InitConfig: initConfig,
		SetUp:      setUp,
		TearDown:   tearDown,
	}
	cmd.RegisterModule(mod)
}

func initConfig(args []string) (*cmd.Config, error) {
	config.Init("f", "", "scheduler.conf")

	if err := config.Load(&conf); err != nil {
		return nil, err
	}

	return &conf.Config, nil
}

func setUp() (*rpc.Router, []rpc.ProgressHandler) {
	var err error
	service, err = NewService(&conf)
	if err != nil {
		log.Panicf("new service failed, err: %v", err)
	}
	return NewHandler(service), []rpc.ProgressHandler{service}
}

func tearDown() {
	// close record file safety
	service.Close()
}

// NewService returns scheduler service
func NewService(conf *Config) (svr *Service, err error) {
	if err := conf.fixConfig(); err != nil {
		log.Errorf("service config check failed: err[%v]", err)
		return nil, err
	}

	svr = &Service{
		ClusterID:     conf.ClusterID,
		leader:        conf.IsLeader(),
		leaderHost:    conf.Leader(),
		followerHosts: conf.Follower(),
		kafkaMonitors: make([]*base.KafkaTopicMonitor, 0),
	}

	clusterMgrCli := client.NewClusterMgrClient(&conf.ClusterMgr)

	blobnodeCli := client.NewBlobnodeClient(&conf.Blobnode)
	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)
	volumeUpdater := client.NewVolumeUpdater(&conf.Scheduler, scheme+localHost+conf.BindAddr)

	topoConf := &clusterTopologyConfig{
		ClusterID:               conf.ClusterID,
		Leader:                  conf.IsLeader(),
		UpdateInterval:          time.Duration(conf.TopologyUpdateIntervalMin) * time.Minute,
		VolumeUpdateInterval:    time.Duration(conf.VolumeCacheUpdateIntervalS) * time.Second,
		FreeChunkCounterBuckets: conf.FreeChunkCounterBuckets,
	}
	topologyMgr := NewClusterTopologyMgr(clusterMgrCli, topoConf)

	kafkaClient := base.NewKafkaConsumer(conf.Kafka.BrokerList)
	shardRepairMgr, err := NewShardRepairMgr(&conf.ShardRepair, topologyMgr, switchMgr, blobnodeCli, clusterMgrCli, kafkaClient)
	if err != nil {
		log.Errorf("new shard repair mgr: cfg[%+v], err[%w]", conf.ShardRepair, err)
		return nil, err
	}

	deleteMgr, err := NewBlobDeleteMgr(&conf.BlobDelete, topologyMgr, switchMgr, blobnodeCli, kafkaClient)
	if err != nil {
		log.Errorf("new blob delete mgr: cfg[%+v], err[%w]", conf.BlobDelete, err)
		return nil, err
	}

	svr.shardRepairMgr = shardRepairMgr
	svr.blobDeleteMgr = deleteMgr
	svr.clusterTopology = topologyMgr
	svr.volumeUpdater = volumeUpdater
	svr.clusterMgrCli = clusterMgrCli

	if err = svr.register(conf.ServiceRegister); err != nil {
		return nil, fmt.Errorf("service register: err:[%w]", err)
	}

	if err = svr.RunTask(); err != nil {
		return nil, err
	}

	if !svr.leader {
		return
	}

	err = svr.NewKafkaMonitor(conf.ClusterID)
	if err != nil {
		log.Errorf("run kafka monitor failed: err[%w]", err)
		return nil, err
	}

	// //===========blobnode module migrate manager===============
	taskLogger, err := recordlog.NewEncoder(&conf.TaskLog)
	if err != nil {
		return nil, err
	}

	balanceTaskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeBalance.String())
	if err != nil {
		return nil, err
	}
	balanceMgr := NewBalanceMgr(clusterMgrCli, volumeUpdater, balanceTaskSwitch, topologyMgr, taskLogger, &conf.Balance)

	diskDropTaskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeDiskDrop.String())
	if err != nil {
		return nil, err
	}
	diskDropMgr := NewDiskDropMgr(clusterMgrCli, volumeUpdater, diskDropTaskSwitch, taskLogger, &conf.DiskDrop, topologyMgr)

	// new disk repair manager
	diskRepairTaskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeDiskRepair.String())
	if err != nil {
		return nil, err
	}

	diskRepairMgr := NewDiskRepairMgr(clusterMgrCli, diskRepairTaskSwitch, taskLogger, &conf.DiskRepair)

	manualMigMgr := NewManualMigrateMgr(clusterMgrCli, volumeUpdater, taskLogger, &conf.ManualMigrate)

	mqProxy := client.NewProxyClient(&conf.Proxy, cmapi.New(&conf.ClusterMgr), conf.ClusterID)
	inspectorTaskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeVolumeInspect.String())
	if err != nil {
		return nil, err
	}
	inspectMgr := NewVolumeInspectMgr(clusterMgrCli, mqProxy, inspectorTaskSwitch, &conf.VolumeInspect)

	//===========shard module migrate manager===============
	// new shard disk repair manager
	shardDiskRepairTaskSwitch, err := switchMgr.AddSwitch(proto.TaskTypeShardDiskRepair.String())
	if err != nil {
		return nil, err
	}
	shardDiskRepairMgr := NewShardDiskRepairMgr(&conf.ShardDiskRepair, clusterMgrCli, shardDiskRepairTaskSwitch)

	svr.balanceMgr = balanceMgr
	svr.diskDropMgr = diskDropMgr
	svr.manualMigMgr = manualMigMgr
	svr.diskRepairMgr = diskRepairMgr
	svr.inspectMgr = inspectMgr
	svr.shardDiskRepairMgr = shardDiskRepairMgr

	err = svr.waitAndLoad()
	if err != nil {
		log.Errorf("load task from database failed: err[%+v]", err)
		return nil, err
	}

	go svr.Run()
	return svr, nil
}

func (svr *Service) waitAndLoad() error {
	//why:service stop a task lease period to make sure all worker release task
	//so there will not a task run on multiple worker
	log.Infof("start waitAndLoad")
	time.Sleep(proto.TaskLeaseExpiredS * time.Second)
	return svr.load()
}

func (svr *Service) load() (err error) {
	if err = svr.diskRepairMgr.Load(); err != nil {
		return
	}
	if err = svr.balanceMgr.Load(); err != nil {
		return
	}
	if err = svr.diskDropMgr.Load(); err != nil {
		return
	}
	if err = svr.manualMigMgr.Load(); err != nil {
		return
	}
	if err = svr.shardDiskRepairMgr.Load(); err != nil {
		return
	}

	return
}

func (svr *Service) register(cfg ServiceRegisterConfig) error {
	info := client.RegisterInfo{
		ClusterID:          uint64(svr.ClusterID),
		Name:               proto.ServiceNameScheduler,
		Host:               cfg.Host,
		Idc:                cfg.Idc,
		HeartbeatIntervalS: cfg.TickInterval,
		HeartbeatTicks:     cfg.HeartbeatTicks,
		ExpiresTicks:       cfg.ExpiresTicks,
	}
	return svr.clusterMgrCli.Register(context.Background(), info)
}

// Run task
func (svr *Service) Run() {
	svr.diskRepairMgr.Run()
	svr.balanceMgr.Run()
	svr.diskDropMgr.Run()
	svr.manualMigMgr.Run()
	svr.inspectMgr.Run()
	svr.shardDiskRepairMgr.Run()
}

// RunTask run shard repair and blob delete tasks
func (svr *Service) RunTask() error {
	if err := svr.LoadVolInfo(); err != nil {
		log.Errorf("load volume info failed: err[%+v]", err)
		return err
	}
	svr.blobDeleteMgr.Run()
	svr.shardRepairMgr.Run()
	return nil
}

func (svr *Service) NewKafkaMonitor(clusterID proto.ClusterID) error {
	// blob delete
	brokerList := conf.Kafka.BrokerList
	if err := svr.newMonitor(proto.TaskTypeBlobDelete, clusterID, conf.BlobDelete.topics(), brokerList); err != nil {
		return err
	}

	// shard repair
	return svr.newMonitor(proto.TaskTypeShardRepair, clusterID, conf.ShardRepair.topics(), brokerList)
}

func (svr *Service) newMonitor(taskType proto.TaskType, clusterID proto.ClusterID, topics []string, brokerList []string) error {
	for _, topic := range topics {
		cfg := &base.KafkaConfig{BrokerList: brokerList, Topic: topic}
		m, err := base.NewKafkaTopicMonitor(taskType, clusterID, cfg)
		if err != nil {
			log.Errorf("new kafka topic monitor topic failed: topic[%s], err[%+v]", topic, err)
			return err
		}
		svr.kafkaMonitors = append(svr.kafkaMonitors, m)
	}
	return nil
}

func (svr *Service) CloseKafkaMonitors() {
	for _, monitor := range svr.kafkaMonitors {
		monitor.Close()
	}
}

// LoadVolInfo load volume info
func (svr *Service) LoadVolInfo() error {
	return svr.clusterTopology.LoadVolumes()
}

func (svr *Service) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	if svr.needForwardToLeader(req) {
		svr.forwardToLeader(w, req)
		return
	}
	f(w, req)
}

func (svr *Service) needForwardToLeader(req *http.Request) bool {
	if !svr.leader {
		switch req.URL.Path {
		case api.PathUpdateVolume, api.PathStats:
			return false
		default:
			return true
		}
	}
	return false
}

// forwardToLeader will forward http request to leader
func (svr *Service) forwardToLeader(w http.ResponseWriter, req *http.Request) {
	url, err := url.Parse(scheme + req.RequestURI)
	if err != nil {
		panic("parse leader host url failed: " + err.Error())
	}
	url.Host = svr.leaderHost

	proxy := httpproxy.ReverseProxy{
		Director: func(request *http.Request) {
			request.URL = url
		},
	}

	proxy.ServeHTTP(w, req)
}

// Close close service safe
func (svr *Service) Close() {
	log.Infof("stop scheduler service")
	svr.blobDeleteMgr.Close()
	svr.shardRepairMgr.Close()
	if !svr.leader {
		return
	}
	svr.CloseKafkaMonitors()
	svr.balanceMgr.Close()
	svr.diskRepairMgr.Close()
	svr.diskDropMgr.Close()
	svr.manualMigMgr.Close()
	svr.inspectMgr.Close()
	svr.shardDiskRepairMgr.Close()
}

// NewHandler returns app server handler
func NewHandler(service *Service) *rpc.Router {
	rpc.RegisterArgsParser(&api.AcquireArgs{}, "json")
	rpc.RegisterArgsParser(&api.DiskMigratingStatsArgs{}, "json")
	rpc.RegisterArgsParser(&api.MigrateTaskDetailArgs{}, "json")

	// rpc http svr interface
	rpc.GET(api.PathTaskAcquire, service.HTTPTaskAcquire, rpc.OptArgsQuery())
	rpc.POST(api.PathTaskReclaim, service.HTTPTaskReclaim, rpc.OptArgsBody())
	rpc.POST(api.PathTaskCancel, service.HTTPTaskCancel, rpc.OptArgsBody())
	rpc.POST(api.PathTaskComplete, service.HTTPTaskComplete, rpc.OptArgsBody())
	rpc.POST(api.PathManualMigrateTaskAdd, service.HTTPManualMigrateTaskAdd, rpc.OptArgsBody())

	rpc.GET(api.PathInspectAcquire, service.HTTPInspectAcquire)
	rpc.POST(api.PathInspectComplete, service.HTTPInspectComplete, rpc.OptArgsBody())

	rpc.POST(api.PathTaskReport, service.HTTPTaskReport, rpc.OptArgsBody())
	rpc.POST(api.PathTaskRenewal, service.HTTPTaskRenewal, rpc.OptArgsBody())

	rpc.GET(api.PathTaskDetailURI, service.HTTPTaskDetail, rpc.OptArgsURI())
	rpc.GET(api.PathStats, service.HTTPStats, rpc.OptArgsQuery())
	rpc.GET(api.PathStatsLeader, service.HTTPStats, rpc.OptArgsQuery())
	rpc.GET(api.PathStatsDiskMigrating, service.HTTPDiskMigratingStats, rpc.OptArgsQuery())

	rpc.POST(api.PathUpdateVolume, service.HTTPUpdateVolume, rpc.OptArgsBody())

	return rpc.DefaultRouter
}

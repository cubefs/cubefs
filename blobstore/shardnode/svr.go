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

// nolint
package shardnode

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	shardnodeapi "github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/cmd"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/security"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/shardnode/blobdeleter"
	"github.com/cubefs/cubefs/blobstore/shardnode/catalog"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/store"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

var (
	globalService *service
	conf          Config
)

// singleton service instance control
var serviceOnce sync.Once

const defaultTaskPoolSize = 64

type Config struct {
	cmd.Config
	CmConfig    cmapi.Config `json:"cm_config"`
	RegionMagic string       `json:"region_magic"`

	DisksConfig struct {
		Disks           []string `json:"disks"`
		CheckMountPoint bool     `json:"check_mount_point"`
	} `json:"disks_config"`

	StoreConfig     store.Config            `json:"store_config"`
	RaftConfig      raft.Config             `json:"raft_config"`
	ShardBaseConfig storage.ShardBaseConfig `json:"shard_base_config"`
	NodeConfig      cmapi.ShardNodeInfo     `json:"node_config"`

	AllocVolConfig struct {
		BidAllocNums         uint64  `json:"bid_alloc_nums"`
		RetainIntervalS      int64   `json:"retain_interval_s"`
		DefaultAllocVolsNum  int     `json:"default_alloc_vols_num"`
		InitVolumeNum        int     `json:"init_volume_num"`
		TotalThresholdRatio  float64 `json:"total_threshold_ratio"`
		RetainVolumeBatchNum int     `json:"retain_volume_batch_num"`
		RetainBatchIntervalS int64   `json:"retain_batch_interval_s"`
	} `json:"alloc_vol_config"`
	HandleIOError                func(ctx context.Context)
	HeartBeatIntervalS           int64 `json:"heart_beat_interval_s"`
	ReportIntervalS              int64 `json:"report_interval_s"`
	RouteUpdateIntervalS         int64 `json:"route_update_interval_s"`
	CheckPointIntervalM          int64 `json:"check_point_interval_m"`
	WaitRepairCloseDiskIntervalS int64 `json:"wait_repair_close_disk_interval_s"`
	WaitReOpenDiskIntervalS      int64 `json:"wait_re_open_disk_interval_s"`
	ShardCheckAndClearIntervalH  int64 `json:"shard_check_and_clear_interval_h"`

	DeleteBlobCfg blobdeleter.BlobDelCfg `json:"blob_delete_cfg"`
}

// newService returns the singleton service instance
func newService(cfg *Config) *service {
	serviceOnce.Do(func() {
		globalService = createService(cfg)
	})
	return globalService
}

// createService creates a new service instance
func createService(cfg *Config) *service {
	span, ctx := trace.StartSpanFromContext(context.Background(), "NewShardNodeService")

	security.InitWithRegionMagic(cfg.RegionMagic)
	initServiceConfig(cfg)
	cmClient := cmapi.New(&cfg.CmConfig)
	if err := cmapi.LoadExtendCodemode(context.Background(), cmClient); err != nil {
		span.Fatalf("load extend codemod failed: %s", err)
	}

	snClient := shardnodeapi.New(rpc2.Client{RetryOn: func(err error) bool {
		return rpc2.DetectStatusCode(err) < apierr.CodeShardNodeNotLeader
	}})
	transport := base.NewTransport(base.TransportConfig{
		CMClient: cmClient,
		SNClient: snClient,
		BNClient: bnapi.New(&bnapi.Config{}),
		Self:     &cfg.NodeConfig,
	})
	cfg.ShardBaseConfig.Transport = transport

	// set raft config
	resolver := &storage.AddressResolver{Transport: transport}
	cfg.RaftConfig.TransportConfig.Resolver = resolver
	cfg.RaftConfig.Transport = raft.NewTransport(&cfg.RaftConfig.TransportConfig)

	// register node
	if err := transport.Register(ctx); err != nil {
		span.Fatalf("register shard server failed: %s", err)
	}

	svr := &service{
		cfg:       *cfg,
		transport: transport,
		taskPool:  taskpool.New(defaultTaskPoolSize, defaultTaskPoolSize),
		closer:    closer.New(),
		disks:     make(map[proto.DiskID]*storage.Disk),
	}

	// load disks
	err := svr.initDisks(ctx)
	if err != nil {
		span.Fatalf("init shard node disks failed: %s", err)
	}

	shards := make([]storage.ShardHandler, 0)
	shardReports := make([]cmapi.ShardUnitInfo, 0)

	/* do shardReport here to sync route version to avoid
	shard load old route version from storage because shard info
	did not sync, which may cause client get old route version by shard stat api */
	if err = svr.shardReports(ctx, shards, shardReports, true, proto.ShardTaskTypeSyncRouteVersion); err != nil {
		span.Fatalf("sync shard node route failed: %s", err)
	}

	c := catalog.NewCatalog(ctx, &catalog.Config{
		ClusterID:   cfg.NodeConfig.ClusterID,
		Transport:   transport,
		ShardGetter: svr,
		AllocCfg: catalog.AllocCfg{
			BidAllocNums:         cfg.AllocVolConfig.BidAllocNums,
			RetainIntervalS:      cfg.AllocVolConfig.RetainIntervalS,
			DefaultAllocVolsNum:  cfg.AllocVolConfig.DefaultAllocVolsNum,
			InitVolumeNum:        cfg.AllocVolConfig.InitVolumeNum,
			TotalThresholdRatio:  cfg.AllocVolConfig.TotalThresholdRatio,
			RetainVolumeBatchNum: cfg.AllocVolConfig.RetainVolumeBatchNum,
			RetainBatchIntervalS: cfg.AllocVolConfig.RetainBatchIntervalS,
		},
	})
	svr.catalog = c

	cfg.DeleteBlobCfg.ClusterID = cfg.NodeConfig.ClusterID
	taskSwitchMgr := taskswitch.NewSwitchMgr(cmClient)
	dm, err := blobdeleter.NewBlobDeleteMgr(&blobdeleter.BlobDelMgrConfig{
		TaskSwitchMgr: taskSwitchMgr,
		ShardGetter:   svr,
		Transport:     transport,
		VolCache:      base.NewVolumeCache(transport, 10*time.Second),
		BlobDelCfg:    cfg.DeleteBlobCfg,
	})
	if err != nil {
		span.Fatalf("new blob delete mgr failed, err: %s", err.Error())
	}
	svr.blobDelMgr = dm

	go svr.loop(ctx)
	span.Infof("service started success")

	return svr
}

type service struct {
	catalog   *catalog.Catalog
	disks     map[proto.DiskID]*storage.Disk
	transport base.Transport
	taskPool  taskpool.TaskPool
	groupRun  singleflight.Group

	blobDelMgr *blobdeleter.BlobDeleteMgr

	cfg    Config
	lock   sync.RWMutex
	closer closer.Closer
}

func (s *service) getDisk(diskID proto.DiskID) (*storage.Disk, error) {
	s.lock.RLock()
	disk := s.disks[diskID]
	s.lock.RUnlock()
	if disk == nil {
		return nil, apierr.ErrShardNodeDiskNotFound
	}
	return disk, nil
}

func (s *service) addDisk(disk *storage.Disk) {
	s.lock.Lock()
	s.disks[disk.DiskID()] = disk
	s.lock.Unlock()
}

func (s *service) getAllDisks() []*storage.Disk {
	s.lock.RLock()
	disks := make([]*storage.Disk, 0, len(s.disks))
	for i := range s.disks {
		disks = append(disks, s.disks[i])
	}
	s.lock.RUnlock()

	return disks
}

func (s *service) close() {
	s.closer.Close()
	s.blobDelMgr.Close()
	s.cfg.RaftConfig.Transport.Close()
}

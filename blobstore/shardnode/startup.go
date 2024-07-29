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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func (s *service) initNode(ctx context.Context, cmClient *clustermgr.Client) {
	span := trace.SpanFromContext(ctx)

	transport := base.NewTransport(cmClient, &s.cfg.NodeConfig)
	// register node
	if err := transport.Register(ctx); err != nil {
		span.Fatalf("register shard server failed: %s", err)
	}
}

func (s *service) initDisks(ctx context.Context) error {
	span := trace.SpanFromContext(ctx)

	// load disk from master
	registeredDisks, err := s.transport.ListDisks(ctx)
	if err != nil {
		return err
	}
	registeredDisksMap := make(map[proto.DiskID]struct{})
	// map's key is disk path, and map's value is the disk which has not been repaired yet,
	// disk which not repaired can be replaced for security consider
	registerDiskPathsMap := make(map[string]clustermgr.ShardNodeDiskInfo)
	for _, disk := range registeredDisks {
		registeredDisksMap[disk.DiskID] = struct{}{}
		if disk.Status != proto.DiskStatusRepaired {
			registerDiskPathsMap[disk.Path] = disk
		}
	}

	// load disk from local
	disks := make([]*storage.Disk, 0, len(s.cfg.DisksConfig.Disks))
	for _, diskPath := range s.cfg.DisksConfig.Disks {
		disk := storage.OpenDisk(ctx, storage.DiskConfig{
			ClusterID:       s.cfg.ClusterID,
			NodeID:          s.transport.NodeID(),
			DiskPath:        diskPath,
			CheckMountPoint: s.cfg.DisksConfig.CheckMountPoint,
			StoreConfig:     s.cfg.StoreConfig,
			Transport:       s.transport,
			RaftConfig:      s.cfg.RaftConfig,
			ShardBaseConfig: s.cfg.ShardBaseConfig,
		})
		disks = append(disks, disk)
	}
	// compare local disk and remote disk info, alloc new disk id and register new disk
	// when local disk is not register and local disk is not saved
	newDisks := make([]*storage.Disk, 0)
	for _, disk := range disks {
		// unregister disk and the old path disk device has been repaired, then add new disk
		if !disk.IsRegistered() {
			if unrepairedDisk, ok := registerDiskPathsMap[disk.GetDiskInfo().Path]; ok {
				return errors.Newf("disk device has been replaced but old disk device[%+v] is not repaired", unrepairedDisk)
			}
			newDisks = append(newDisks, disk)
			continue
		}
		// alloc disk id already but didn't register yet, then add new disk
		if _, ok := registeredDisksMap[disk.DiskID()]; !ok {
			newDisks = append(newDisks, disk)
			continue
		}
	}

	// register disk
	for _, disk := range newDisks {
		// alloc new disk id
		if disk.DiskID() == 0 {
			diskID, err := s.transport.AllocDiskID(ctx)
			if err != nil {
				return errors.Newf("alloc disk id failed: %s", err)
			}
			// save disk id
			disk.SetDiskID(diskID)
		}
		// save disk meta
		if err := disk.SaveDiskInfo(); err != nil {
			return errors.Newf("save disk info[%+v] failed: %s", disk, err)
		}
		diskInfo := disk.GetDiskInfo()
		// register disk
		if err := s.transport.RegisterDisk(ctx, &diskInfo); err != nil {
			return errors.Newf("register new disk[%+v] failed: %s", disk, err)
		}
	}

	// load disk concurrently
	wg := sync.WaitGroup{}
	wg.Add(len(disks))
	for i := range disks {
		disk := disks[i]
		go func() {
			defer wg.Done()
			if err := disk.Load(ctx); err != nil {
				span.Fatalf("load disk[%+v] failed[%s]", disk, err)
			}
		}()
	}
	wg.Wait()

	for _, disk := range disks {
		s.addDisk(disk)
	}
	return nil
}

func initConfig(cfg *Config) {
	if cfg.NodeConfig.RaftHost == "" || cfg.NodeConfig.Host == "" {
		log.Panicf("invalid node[%+v] config port", cfg.NodeConfig)
	}

	cfg.StoreConfig.KVOption.CreateIfMissing = true
	cfg.StoreConfig.RaftOption.CreateIfMissing = true

	defaulter.LessOrEqual(&cfg.ShardBaseConfig.TruncateWalLogInterval, uint64(1<<16))
	defaulter.LessOrEqual(&cfg.ShardBaseConfig.RaftSnapTransmitConfig.BatchInflightNum, 64)
	defaulter.LessOrEqual(&cfg.ShardBaseConfig.RaftSnapTransmitConfig.BatchInflightSize, 1<<20)
}

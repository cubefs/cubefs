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
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/store"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	tickInterval   = 1
	heartbeatTicks = 30
	expiresTicks   = 60
)

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
	repairedDiskMap := make(map[string]clustermgr.ShardNodeDiskInfo)
	for _, disk := range registeredDisks {
		registeredDisksMap[disk.DiskID] = struct{}{}
		if disk.Status != proto.DiskStatusRepaired {
			registerDiskPathsMap[disk.Path] = disk
			continue
		}
		repairedDiskMap[disk.Path] = disk
	}

	// load disk from local
	disks := make([]*storage.Disk, 0, len(s.cfg.DisksConfig.Disks))
	for _, diskPath := range s.cfg.DisksConfig.Disks {
		disk, err := storage.OpenDisk(ctx, storage.DiskConfig{
			ClusterID:       s.cfg.NodeConfig.ClusterID,
			NodeID:          s.transport.NodeID(),
			DiskPath:        diskPath,
			CheckMountPoint: s.cfg.DisksConfig.CheckMountPoint,
			StoreConfig:     s.cfg.StoreConfig,
			Transport:       s.transport,
			RaftConfig:      s.cfg.RaftConfig,
			ShardBaseConfig: s.cfg.ShardBaseConfig,
			HandleEIO:       s.handleEIO,
		})
		// open disk failed, check disk status,
		if err != nil {
			if !store.IsEIO(err) {
				span.Fatalf("open disk[%s] failed: %s", diskPath, err)
			}

			_, ok := repairedDiskMap[diskPath]
			if ok {
				span.Errorf("repaired disk[%s] open failed: %s", diskPath, err)
				continue
			}

			registeredDisk, ok := registerDiskPathsMap[diskPath]
			// fatal abort when disk is not registered,
			if !ok {
				span.Fatalf("open disk[%s] failed: %s", diskPath, err)
			}

			// skip open disk when disk path status is not normal
			if registeredDisk.Status != proto.DiskStatusNormal {
				span.Errorf("open disk[%s] failed: %s, diskinfo: %+v", diskPath, err, registeredDisk)
				continue
			}

			// handleEIO when disk path status is normal and err is EIO
			if store.IsEIO(err) {
				span.Errorf("open status normal disk[%s] failed: %s, diskinfo: %+v", diskPath, err, registeredDisk)
				s.transport.SetDiskBroken(ctx, registeredDisk.DiskID)
				continue
			}
			// other situation, do fatal log
			span.Fatalf("open disk[%s] failed: %s", diskPath, err)
		}
		disks = append(disks, disk)
	}
	// compare local disk and remote disk info, alloc new disk id and register new disk
	// when local disk is not register and local disk is not saved
	newDisks := make([]*storage.Disk, 0)
	normalDisks := make([]*storage.Disk, 0)
	for _, disk := range disks {
		// unregister disk and the old path disk device has been repaired, then add new disk
		if !disk.IsRegistered() {
			if unrepairedDisk, ok := registerDiskPathsMap[disk.GetDiskInfo().Path]; ok {
				return errors.Newf("disk device has been replaced but old disk device[%+v] is not repaired", unrepairedDisk)
			}
			newDisks = append(newDisks, disk)
			normalDisks = append(normalDisks, disk)
			continue
		}
		// alloc disk id already but didn't register yet, then add new disk
		_, ok := registeredDisksMap[disk.DiskID()]
		if !ok {
			newDisks = append(newDisks, disk)
			normalDisks = append(normalDisks, disk)
			continue
		}

		path := disk.GetDiskInfo().Path
		registeredInfo, ok := registerDiskPathsMap[path]
		if !ok {
			span.Warnf("skip disk repaired but open success, diskinfo: %+v", disk)
			continue
		}
		if !isDiskInfoMatch(registeredInfo, disk.GetDiskInfo()) {
			return errors.Newf("disk info miss match, registered: %+v, loaded: %+v", registeredInfo, disk.GetDiskInfo())
		}
		if registeredInfo.Status != proto.DiskStatusNormal {
			span.Warnf("disk[%d] status is not normal, diskinfo: %+v", disk.DiskID(), registeredInfo)
			disk.Close()
			continue
		}
		normalDisks = append(normalDisks, disk)
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
		if err := disk.SaveDiskInfo(ctx); err != nil {
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
	wg.Add(len(normalDisks))
	for i := range normalDisks {
		disk := normalDisks[i]
		go func() {
			defer wg.Done()
			if err := disk.Load(ctx); err != nil {
				if store.IsEIO(err) {
					span.Errorf("load disk[%+v] failed[%s]", disk, err)
				} else {
					span.Fatalf("load disk[%+v] failed[%s]", disk, err)
				}
			}
		}()
	}
	wg.Wait()

	for _, disk := range normalDisks {
		s.addDisk(disk)
	}
	return nil
}

func (s *service) handleEIO(ctx context.Context, diskID proto.DiskID, err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Warnf("handle eio from storage layer, disk[%d], err: %s", diskID, err)

	disk, err := s.getDisk(diskID)
	if err != nil {
		span.Warnf("get disk failed: %s, maybe has been removed", err)
		return
	}

	s.groupRun.Do(fmt.Sprintf("disk-%d", diskID), func() (interface{}, error) {
		// Note: there is another goroutine set disk broken,
		// just return and do not do any progress below
		if !disk.SetBroken() {
			return nil, nil
		}

		for {
			err := s.transport.SetDiskBroken(ctx, diskID)
			if err == nil {
				break
			}
			span.Errorf("set Disk[%d] broken to cm failed", diskID)
			time.Sleep(5 * time.Second)
		}

		// wait for disk repairing
		go s.waitRepairCloseDisk(ctx, disk)

		return nil, nil
	})
}

func (s *service) waitRepairCloseDisk(ctx context.Context, disk *storage.Disk) {
	span := trace.SpanFromContextSafe(ctx)

	diskInfo := disk.GetDiskInfo()
	diskID := diskInfo.DiskID

	func() {
		ticker := time.NewTicker(time.Duration(s.cfg.WaitRepairCloseDiskIntervalS) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-s.closer.Done():
				span.Warnf("service is closed. return")
				return
			case <-ticker.C:
			}

			info, err := s.transport.GetDisk(ctx, diskID, false)
			if err != nil {
				span.Errorf("get disk info from clustermgr failed. disk[%d], err:%+v", diskID, err)
				continue
			}

			if info.Status <= proto.DiskStatusRepairing {
				span.Infof("disk:%d path:%s status:%v", diskID, info.Path, info.Status)
				continue
			}

			if disk.GetShardCnt() < 1 {
				break
			}
		}

		// after all disk shard deleted, the handle can be safely removed
		span.Infof("Delete %d from the map table of the service", diskID)

		s.lock.Lock()
		delete(s.disks, diskID)
		s.lock.Unlock()

		disk.Close()
	}()

	go s.waitReOpenDisk(ctx, diskInfo)
}

func (s *service) waitReOpenDisk(ctx context.Context, diskInfo clustermgr.ShardNodeDiskInfo) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start to wait for disk[%+v] reopen", diskInfo)

	diskID := diskInfo.DiskID

	ticker := time.NewTicker(time.Duration(s.cfg.WaitReOpenDiskIntervalS) * time.Second)
	defer ticker.Stop()

	// wait for disk reopen
	for {
		select {
		case <-s.closer.Done():
			span.Warnf("service is closed. return")
			return
		case <-ticker.C:
			// check old path disk has been repaired or not
			info, err := s.transport.GetDisk(ctx, diskID, false)
			if err != nil {
				span.Warnf("get disk from cm failed: %s", err)
				continue
			}
			if info.Status != proto.DiskStatusRepaired {
				span.Warnf("disk[%d] is not repaired", diskID)
				continue
			}

			// check disk path empty or not
			empty, err := storage.IsEmptyDisk(diskInfo.Path)
			if err != nil || !empty {
				span.Errorf("disk path(%s) is not empty. err: %s", diskInfo.Path, err)
				continue
			}

			ok := func() bool {
				success := false

				// register new disk
				disk, err := storage.OpenDisk(ctx, storage.DiskConfig{
					ClusterID:       s.cfg.NodeConfig.ClusterID,
					NodeID:          s.transport.NodeID(),
					DiskPath:        diskInfo.Path,
					CheckMountPoint: s.cfg.DisksConfig.CheckMountPoint,
					StoreConfig:     s.cfg.StoreConfig,
					Transport:       s.transport,
					RaftConfig:      s.cfg.RaftConfig,
					ShardBaseConfig: s.cfg.ShardBaseConfig,
					HandleEIO:       s.handleEIO,
				})
				if err != nil {
					span.Errorf("open disk[%s] failed: %s", diskInfo.Path, err)
					return false
				}

				defer func() {
					if !success {
						disk.Close()
					}
				}()

				// alloc new disk id
				if disk.DiskID() == 0 {
					diskID, err := s.transport.AllocDiskID(ctx)
					if err != nil {
						span.Errorf("alloc disk id failed: %s", err)
						return false
					}
					// save disk id
					disk.SetDiskID(diskID)
				}
				// save disk meta
				if err := disk.SaveDiskInfo(ctx); err != nil {
					span.Errorf("save disk info[%+v] failed: %s", disk, err)
					return false
				}
				diskInfo := disk.GetDiskInfo()
				// register disk
				if err := s.transport.RegisterDisk(ctx, &diskInfo); err != nil {
					span.Errorf("register new disk[%+v] failed: %s", disk, err)
					return false
				}

				if err := disk.Load(ctx); err != nil {
					span.Errorf("load disk failed: %s", err)
					return false
				}

				success = true
				s.addDisk(disk)

				span.Infof("reopen disk[%d] success", diskID)
				return true
			}()

			if ok {
				return
			}
		}
	}
}

func initServiceConfig(cfg *Config) {
	if cfg.NodeConfig.RaftHost == "" || cfg.NodeConfig.Host == "" {
		log.Panicf("invalid node[%+v] config port", cfg)
	}

	cfg.RaftConfig.TransportConfig.Addr = cfg.NodeConfig.RaftHost
	cfg.StoreConfig.KVOption.CreateIfMissing = true
	cfg.StoreConfig.RaftOption.CreateIfMissing = true

	defaulter.LessOrEqual(&cfg.ShardBaseConfig.TruncateWalLogInterval, uint64(1<<16))
	defaulter.LessOrEqual(&cfg.ShardBaseConfig.RaftSnapTransmitConfig.BatchInflightNum, 64)
	defaulter.LessOrEqual(&cfg.ShardBaseConfig.RaftSnapTransmitConfig.BatchInflightSize, 1<<20)
	defaulter.LessOrEqual(&cfg.HeartBeatIntervalS, int64(1))
	defaulter.LessOrEqual(&cfg.ReportIntervalS, int64(60))
	defaulter.LessOrEqual(&cfg.RouteUpdateIntervalS, int64(5))
	defaulter.LessOrEqual(&cfg.CheckPointIntervalM, int64(1))
	defaulter.LessOrEqual(&cfg.WaitRepairCloseDiskIntervalS, int64(30))
	defaulter.LessOrEqual(&cfg.WaitReOpenDiskIntervalS, int64(30))
	defaulter.LessOrEqual(&cfg.ShardCheckAndClearIntervalH, int64(24))
}

func isDiskInfoMatch(a, b clustermgr.ShardNodeDiskInfo) bool {
	if a.DiskID != b.DiskID || a.Path != b.Path ||
		a.NodeID != b.NodeID || a.ClusterID != b.ClusterID {
		return false
	}
	return true
}

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
	"math/rand"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/codemode"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (s *service) addShard(ctx context.Context, req *shardnode.AddShardArgs) error {
	disk, err := s.getDisk(req.DiskID)
	if err != nil {
		return err
	}

	return disk.AddShard(ctx, req.Suid, req.RouteVersion, req.Range, req.Units)
}

// UpdateShard update shard info
func (s *service) updateShard(ctx context.Context, req *shardnode.UpdateShardArgs) error {
	disk, err := s.getDisk(req.DiskID)
	if err != nil {
		return err
	}

	return disk.UpdateShard(ctx, req.Suid, req.ShardUpdateType, req.Unit)
}

// transferShardLeader transfer shard leader
func (s *service) transferShardLeader(ctx context.Context, req *shardnode.TransferShardLeaderArgs) error {
	shard, err := s.GetShard(req.DiskID, req.Suid)
	if err != nil {
		return err
	}

	return shard.TransferLeader(ctx, req.GetDestDiskID())
}

func (s *service) getShardUintInfo(ctx context.Context, diskID proto.DiskID, suid proto.Suid) (ret clustermgr.ShardUnitInfo, err error) {
	shard, err := s.GetShard(diskID, suid)
	if err != nil {
		return
	}

	shardStat, err := shard.Stats(ctx)
	if err != nil {
		return
	}

	return clustermgr.ShardUnitInfo{
		Suid:         suid,
		DiskID:       diskID,
		AppliedIndex: shardStat.AppliedIndex,
		LeaderDiskID: shardStat.LeaderDiskID,
		Range:        shardStat.Range,
		RouteVersion: shardStat.RouteVersion,
	}, nil
}

func (s *service) getShardStats(ctx context.Context, diskID proto.DiskID, suid proto.Suid) (ret shardnode.ShardStats, err error) {
	shard, err := s.GetShard(diskID, suid)
	if err != nil {
		return
	}

	shardStat, err := shard.Stats(ctx)
	if err != nil {
		return
	}

	return shardStat, nil
}

func (s *service) listVolume(cxt context.Context, mode codemode.CodeMode) ([]clustermgr.AllocVolumeInfo, error) {
	return s.catalog.ListVolume(cxt, mode)
}

func (s *service) listShards(ctx context.Context, diskID proto.DiskID, count uint64) (ret []clustermgr.ShardUnitInfo, err error) {
	span := trace.SpanFromContextSafe(ctx)
	disk, err := s.getDisk(diskID)
	if err != nil {
		return
	}
	ret = make([]clustermgr.ShardUnitInfo, 0, count)
	disk.RangeShard(func(shard storage.ShardHandler) bool {
		if count == 0 {
			return false
		}
		stats, err := shard.Stats(ctx)
		if err != nil {
			span.Errorf("get shard stats failed, err:%s", err.Error())
			return false
		}
		ret = append(ret, clustermgr.ShardUnitInfo{
			Suid:         stats.Suid,
			DiskID:       diskID,
			AppliedIndex: stats.AppliedIndex,
			LeaderDiskID: stats.LeaderDiskID,
			Range:        stats.Range,
			RouteVersion: stats.RouteVersion,
		})
		count--
		return true
	})
	return ret, err
}

func (s *service) GetShard(diskID proto.DiskID, suid proto.Suid) (storage.ShardHandler, error) {
	disk, err := s.getDisk(diskID)
	if err != nil {
		return nil, err
	}
	return disk.GetShard(suid)
}

func (s *service) loop(ctx context.Context) {
	heartbeatTicker := time.NewTicker(1 * time.Second)
	reportTicker := time.NewTicker(60 * time.Second)
	routeUpdateTicker := time.NewTicker(5 * time.Second)
	checkpointTicker := time.NewTicker(1 * time.Minute)

	defer func() {
		heartbeatTicker.Stop()
		reportTicker.Stop()
		routeUpdateTicker.Stop()
		checkpointTicker.Stop()
	}()

	var span trace.Span
	diskReports := make([]clustermgr.ShardNodeDiskHeartbeatInfo, 0)
	shardReports := make([]clustermgr.ShardUnitInfo, 0, 1<<10)

	for {
		select {
		case <-heartbeatTicker.C:
			span, ctx = trace.StartSpanFromContext(ctx, "")
			diskReports = diskReports[:0]

			disks := s.getAllDisks()
			for _, disk := range disks {
				diskInfo := disk.GetDiskInfo()
				diskReports = append(diskReports, clustermgr.ShardNodeDiskHeartbeatInfo{
					DiskID:       diskInfo.DiskID,
					Used:         diskInfo.Used,
					Size:         diskInfo.Size,
					UsedShardCnt: int32(disk.GetShardCnt()),
				})
			}
			if err := s.transport.HeartbeatDisks(ctx, diskReports); err != nil {
				span.Warnf("heartbeat to master failed: %s", err)
			}

		case <-reportTicker.C:
			span, ctx = trace.StartSpanFromContext(ctx, "")
			shardReports = shardReports[:0]

			disks := s.getAllDisks()
			for _, disk := range disks {
				disk.RangeShard(func(shard storage.ShardHandler) bool {
					stats, err := shard.Stats(ctx)
					if err != nil {
						span.Errorf("get shard stat err: %s", err)
						return false
					}
					shardReports = append(shardReports, clustermgr.ShardUnitInfo{
						Suid:         stats.Suid,
						DiskID:       disk.DiskID(),
						AppliedIndex: stats.AppliedIndex,
						LeaderDiskID: stats.LeaderDiskID,
						Range:        stats.Range,
						RouteVersion: stats.RouteVersion,
					})
					return true
				})
			}

			tasks, err := s.transport.ShardReport(ctx, shardReports)
			if err != nil {
				span.Errorf("shard report failed: %s", err)
				continue
			}
			for _, task := range tasks {
				if err := s.executeShardTask(ctx, task); err != nil {
					span.Errorf("execute shard task[%+v] failed: %s", task, errors.Detail(err))
					continue
				}
			}
			reportTicker.Reset(time.Duration(60+rand.Intn(10)) * time.Second)
		case <-s.closer.Done():
			return
		}
	}
}

func (s *service) executeShardTask(ctx context.Context, task clustermgr.ShardTask) error {
	span := trace.SpanFromContext(ctx)

	disk, err := s.getDisk(task.DiskID)
	if err != nil {
		return err
	}
	shard, err := disk.GetShard(task.Suid)
	if err != nil {
		return err
	}

	switch task.TaskType {
	case proto.ShardTaskTypeClearShard:
		s.taskPool.Run(func() {
			curVersion := shard.GetRouteVersion()
			if curVersion == task.OldRouteVersion && curVersion < task.RouteVersion {
				err := disk.DeleteShard(ctx, task.Suid, task.RouteVersion)
				if err != nil {
					span.Errorf("delete shard task[%+v] failed: %s", task, err)
				}
			} else {
				span.Errorf("route version not match, current: %d, task old: %d, task new: %d",
					curVersion, task.OldRouteVersion, task.RouteVersion)
			}
		})
	case proto.ShardTaskTypeSyncRouteVersion:
		s.taskPool.Run(func() {
			curVersion := shard.GetRouteVersion()
			if curVersion == task.OldRouteVersion && curVersion < task.RouteVersion {
				err = disk.UpdateShardRouteVersion(ctx, task.Suid, task.RouteVersion)
				if err != nil {
					span.Errorf("update shard routeVersion task[%+v] failed: %s", task, err)
				}
			} else {
				span.Errorf("route version not match, current: %d, task old: %d, task new: %d",
					curVersion, task.OldRouteVersion, task.RouteVersion)
			}
		})
	default:
	}
	return nil
}

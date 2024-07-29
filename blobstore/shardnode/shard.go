package shardnode

import (
	"context"
	"math/rand"
	"time"

	"github.com/cubefs/cubefs/blobstore/shardnode/storage"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func (s *service) AddShard(ctx context.Context, req *shardnode.AddShardRequest) error {
	disk, err := s.getDisk(req.DiskID)
	if err != nil {
		return err
	}

	return disk.AddShard(ctx, req.Suid, req.Epoch, req.Range, req.Units)
}

// UpdateShard update shard info
func (s *service) UpdateShard(ctx context.Context, req *shardnode.UpdateShardRequest) error {
	disk, err := s.getDisk(req.DiskID)
	if err != nil {
		return err
	}

	return disk.UpdateShard(ctx, req.Suid, req.ShardUpdateType, req.Unit)
}

func (s *service) GetShardInfo(ctx context.Context, diskID proto.DiskID, suid proto.Suid) (ret clustermgr.ShardUnitInfo, err error) {
	shard, err := s.GetShard(diskID, suid)
	if err != nil {
		return
	}

	shardStat := shard.Stats()

	return clustermgr.ShardUnitInfo{
		Suid:         suid,
		DiskID:       diskID,
		AppliedIndex: shardStat.AppliedIndex,
		LeaderIdx:    shardStat.LeaderIdx,
		Range:        shardStat.Range,
		Epoch:        shardStat.Epoch,
	}, nil
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
					stats := shard.Stats()
					shardReports = append(shardReports, clustermgr.ShardUnitInfo{
						Suid:         stats.Suid,
						DiskID:       disk.DiskID(),
						AppliedIndex: stats.AppliedIndex,
						LeaderIdx:    stats.LeaderIdx,
						Range:        stats.Range,
						Epoch:        stats.Epoch,
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
			if shard.GetEpoch() == task.Epoch {
				err := disk.DeleteShard(ctx, task.Suid)
				if err != nil {
					span.Errorf("delete shard task[%+v] failed: %s", task, err)
				}
			}
		})
	default:
	}
	return nil
}

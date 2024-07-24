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

func (s *service) GetShardInfo(ctx context.Context, diskID proto.DiskID, suid proto.Suid) (ret clustermgr.Shard, err error) {
	shard, err := s.GetShard(diskID, suid)
	if err != nil {
		return
	}

	shardStat := shard.Stats()
	// transform into external nodes
	units := make([]clustermgr.ShardUnitInfo, 0, len(shardStat.Units))
	for _, node := range shardStat.Units {
		units = append(units, clustermgr.ShardUnitInfo{
			DiskID:  node.DiskID,
			Learner: node.Learner,
		})
	}

	// todo
	return clustermgr.Shard{}, nil
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
			shardReports := s.getAlteredShardReports()
			tasks, err := s.transport.ShardReport(ctx, shardReports)
			if err != nil {
				span.Warnf("shard report failed: %s", err)
				continue
			}
			for _, task := range tasks {
				if err := s.catalog.ExecuteShardTask(ctx, task); err != nil {
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

// getAlteredShardReports get altered shards, optimized the shard report load of master
func (s *service) getAlteredShardReports() []clustermgr.ShardReport {
	ret := make([]clustermgr.ShardReport, 0, 1<<10)

	disks := s.getAllDisks()
	for _, disk := range disks {
		disk.RangeShard(func(shard storage.ShardHandler) bool {
			stats := shard.Stats()
			ret = append(ret, clustermgr.ShardReport{
				DiskID: disk.DiskID(),
				// todo
				Shard: clustermgr.Shard{
					Epoch: stats.Epoch,
				},
			})
			return true
		})
	}

	return ret
}

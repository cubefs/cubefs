package catalog

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (c *Catalog) ExecuteShardTask(ctx context.Context, task clustermgr.ShardTask) error {
	// span := trace.SpanFromContext(ctx)

	shard, err := c.cfg.ShardGetter.GetShard(task.DiskID, task.ShardID)
	if err != nil {
		return errors.Info(err, "get shard failed", task.ShardID)
	}

	switch task.TaskType {
	case proto.ShardTaskTypeClearShard:
		c.taskPool.Run(func() {
			if shard.GetEpoch() == task.Epoch {
				// todo
				/*err := disk.DeleteShard(ctx, task.ShardID)
				if err != nil {
					span.Errorf("delete shard task[%+v] failed: %s", task, err)
				}*/
			}
		})
	default:
	}
	return nil
}

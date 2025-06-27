package client

import (
	"context"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// TaskAPI is the interface for scheduler task
type TaskAPI interface {
	CheckTaskExist(ctx context.Context, typ proto.TaskType, diskID proto.DiskID, vuid proto.Vuid) (bool, error)
	UpdateFollowerVolumeCache(ctx context.Context, host string, vid proto.Vid) (err error)
	UpdateLeaderVolumeCache(ctx context.Context, vid proto.Vid) (err error)
}

// taskClient scheduler task client
type taskClient struct {
	leaderHost string
	api.ITaskInfoNotifier
}

// NewTaskClient return scheduler client
func NewTaskClient(cfg *api.Config, leaderHost string) TaskAPI {
	return &taskClient{
		leaderHost:        leaderHost,
		ITaskInfoNotifier: api.NewTaskInfoNotifier(cfg),
	}
}

func (c *taskClient) CheckTaskExist(ctx context.Context, typ proto.TaskType, diskID proto.DiskID, vuid proto.Vuid) (bool, error) {
	resp, err := c.ITaskInfoNotifier.CheckTaskExist(ctx, &api.CheckTaskExistArgs{
		TaskType: typ,
		DiskID:   diskID,
		Vuid:     vuid,
	})
	if err != nil {
		return false, err
	}
	return resp.Exist, nil
}

func (c *taskClient) UpdateLeaderVolumeCache(ctx context.Context, vid proto.Vid) (err error) {
	return c.UpdateVolume(ctx, c.leaderHost, vid)
}

func (c *taskClient) UpdateFollowerVolumeCache(ctx context.Context, host string, vid proto.Vid) (err error) {
	return c.UpdateVolume(ctx, host, vid)
}

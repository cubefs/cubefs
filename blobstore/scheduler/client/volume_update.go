package client

import (
	"context"

	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// IVolumeUpdater update volume cache
type IVolumeUpdater interface {
	UpdateFollowerVolumeCache(ctx context.Context, host string, vid proto.Vid) (err error)
	UpdateLeaderVolumeCache(ctx context.Context, vid proto.Vid) (err error)
}

type volumeUpdater struct {
	LeaderHost string
	Client     api.IVolumeUpdater
}

func NewVolumeUpdater(cfg *api.Config, host string) IVolumeUpdater {
	return &volumeUpdater{
		LeaderHost: host,
		Client:     api.NewVolumeUpdater(cfg),
	}
}

func (v *volumeUpdater) UpdateLeaderVolumeCache(ctx context.Context, vid proto.Vid) (err error) {
	return v.Client.UpdateVol(ctx, v.LeaderHost, vid)
}

func (v *volumeUpdater) UpdateFollowerVolumeCache(ctx context.Context, host string, vid proto.Vid) (err error) {
	return v.Client.UpdateVol(ctx, host, vid)
}

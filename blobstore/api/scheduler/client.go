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

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/selector"
)

// defined http server path.
const (
	PathStats              = "/stats"
	PathStatsLeader        = "/stats/leader"
	PathStatsDiskMigrating = "/stats/disk/migrating"

	PathTaskAcquire          = "/task/acquire"
	PathTaskReclaim          = "/task/reclaim"
	PathTaskCancel           = "/task/cancel"
	PathTaskComplete         = "/task/complete"
	PathTaskReport           = "/task/report"
	PathTaskRenewal          = "/task/renewal"
	PathInspectComplete      = "/inspect/complete"
	PathInspectAcquire       = "/inspect/acquire"
	PathManualMigrateTaskAdd = "/manual/migrate/task/add"

	PathTaskDetail    = "/task/detail"
	PathTaskDetailURI = PathTaskDetail + "/:type/:id" // "/task/detail/:type/:id"
	PathUpdateVolume  = "/update/vol"
)

const defaultHostSyncIntervalMs = 3600000 // 1 hour

var errNoServiceAvailable = errors.New("no service available")

// IMigrator scheduler migrate task and shard task.
type IMigrator interface {
	AcquireTask(ctx context.Context, args *AcquireArgs) (ret *proto.Task, err error)
	RenewalTask(ctx context.Context, args *TaskRenewalArgs) (ret *TaskRenewalRet, err error)
	BlobnodeMigrator
	ShardMigrator
}

type BlobnodeMigrator interface {
	ReportBlobnodeTask(ctx context.Context, args *BlobnodeTaskReportArgs) (err error)
	ReclaimBlobnodeTask(ctx context.Context, args *BlobnodeTaskArgs) (err error)
	CancelBlobnodeTask(ctx context.Context, args *BlobnodeTaskArgs) (err error)
	CompleteBlobnodeTask(ctx context.Context, args *BlobnodeTaskArgs) (err error)
}

type ShardMigrator interface {
	ReportShardTask(ctx context.Context, args *ShardTaskReportArgs) (err error)
	ReclaimShardTask(ctx context.Context, args *ShardTaskArgs) (err error)
	CancelShardTask(ctx context.Context, args *ShardTaskArgs) (err error)
	CompleteShardTask(ctx context.Context, args *ShardTaskArgs) (err error)
}

// IInspector volume inspect task.
type IInspector interface {
	AcquireInspectTask(ctx context.Context) (ret *proto.VolumeInspectTask, err error)
	CompleteInspectTask(ctx context.Context, args *proto.VolumeInspectRet) (err error)
}

// ISchedulerStatus scheduler status.
type ISchedulerStatus interface {
	DetailMigrateTask(ctx context.Context, args *MigrateTaskDetailArgs) (detail MigrateTaskDetail, err error)
	DiskMigratingStats(ctx context.Context, args *DiskMigratingStatsArgs) (ret *DiskMigratingStats, err error)
	Stats(ctx context.Context, host string) (ret TasksStat, err error)
	LeaderStats(ctx context.Context) (ret TasksStat, err error)
}

// IManualMigrator add manual migrate task.
type IManualMigrator interface {
	AddManualMigrateTask(ctx context.Context, args *AddManualMigrateArgs) (err error)
}

// IVolumeUpdater volume updater.
type IVolumeUpdater interface {
	UpdateVolume(ctx context.Context, host string, vid proto.Vid) (err error)
}

// IScheduler scheduler api interface.
type IScheduler interface {
	IMigrator
	IInspector
	ISchedulerStatus
	IManualMigrator
	IVolumeUpdater
}

// Config scheduler config.
type Config struct {
	HostRetry          int   `json:"host_retry"`
	HostSyncIntervalMs int64 `json:"host_sync_interval_ms"`
	rpc.Config
}

type client struct {
	hostRetry int
	selector  selector.Selector
	rpc.Client
}

// New returns scheduler client.
func New(cfg *Config, service cmapi.APIService, clusterID proto.ClusterID) IScheduler {
	hostGetter := func() ([]string, error) {
		svrInfos, err := service.GetService(context.Background(), cmapi.GetServiceArgs{Name: proto.ServiceNameScheduler})
		if err != nil {
			return nil, err
		}

		var hosts []string
		for _, s := range svrInfos.Nodes {
			if clusterID == proto.ClusterID(s.ClusterID) {
				hosts = append(hosts, s.Host)
			}
		}
		if len(hosts) == 0 {
			return nil, errNoServiceAvailable
		}

		return hosts, nil
	}
	if cfg.HostSyncIntervalMs == 0 {
		cfg.HostSyncIntervalMs = defaultHostSyncIntervalMs
	}
	if cfg.HostRetry == 0 {
		cfg.HostRetry = 1
	}
	return &client{
		hostRetry: cfg.HostRetry,
		selector:  selector.MakeSelector(cfg.HostSyncIntervalMs, hostGetter),
		Client:    rpc.NewClient(&cfg.Config),
	}
}

// NewVolumeUpdater returns volume updater client.
func NewVolumeUpdater(cfg *Config) IVolumeUpdater {
	return &client{Client: rpc.NewClient(&cfg.Config)}
}

// UpdateVolumeArgs argument of volume to update.
type UpdateVolumeArgs struct {
	Vid proto.Vid `json:"vid"`
}

func (c *client) UpdateVolume(ctx context.Context, host string, vid proto.Vid) (err error) {
	return c.PostWith(ctx, hostWithScheme(host)+PathUpdateVolume, nil, UpdateVolumeArgs{Vid: vid})
}

func (c *client) ReportShardTask(ctx context.Context, args *ShardTaskReportArgs) (err error) {
	taskArgs, err := args.TaskArgs()
	if err != nil {
		return err
	}
	return c.ReportTask(ctx, taskArgs)
}

func (c *client) ReclaimShardTask(ctx context.Context, args *ShardTaskArgs) (err error) {
	taskArgs, err := args.TaskArgs()
	if err != nil {
		return err
	}
	return c.ReclaimTask(ctx, taskArgs)
}

func (c *client) CancelShardTask(ctx context.Context, args *ShardTaskArgs) (err error) {
	taskArgs, err := args.TaskArgs()
	if err != nil {
		return err
	}
	return c.CancelTask(ctx, taskArgs)
}

func (c *client) CompleteShardTask(ctx context.Context, args *ShardTaskArgs) (err error) {
	taskArgs, err := args.TaskArgs()
	if err != nil {
		return err
	}
	return c.CompleteTask(ctx, taskArgs)
}

func (c *client) ReportBlobnodeTask(ctx context.Context, args *BlobnodeTaskReportArgs) (err error) {
	taskArgs, err := args.TaskArgs()
	if err != nil {
		return err
	}
	return c.ReportTask(ctx, taskArgs)
}

func (c *client) ReclaimBlobnodeTask(ctx context.Context, args *BlobnodeTaskArgs) (err error) {
	taskArgs, err := args.TaskArgs()
	if err != nil {
		return err
	}
	return c.ReclaimTask(ctx, taskArgs)
}

func (c *client) CancelBlobnodeTask(ctx context.Context, args *BlobnodeTaskArgs) (err error) {
	taskArgs, err := args.TaskArgs()
	if err != nil {
		return err
	}
	return c.CancelTask(ctx, taskArgs)
}

func (c *client) CompleteBlobnodeTask(ctx context.Context, args *BlobnodeTaskArgs) (err error) {
	taskArgs, err := args.TaskArgs()
	if err != nil {
		return err
	}
	return c.CompleteTask(ctx, taskArgs)
}

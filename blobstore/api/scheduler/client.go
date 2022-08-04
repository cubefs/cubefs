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
	PathStats       = "/stats"
	PathLeaderStats = "/leader/stats"

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

var errNoServiceAvailable = errors.New("no service available")

type IScheduler interface {
	AcquireTask(ctx context.Context, args *AcquireArgs) (ret *WorkerTask, err error)
	AcquireInspectTask(ctx context.Context) (ret *WorkerInspectTask, err error)
	// report alive tasks
	RenewalTask(ctx context.Context, args *TaskRenewalArgs) (ret *TaskRenewalRet, err error)
	ReportTask(ctx context.Context, args *TaskReportArgs) (err error)
	ReclaimTask(ctx context.Context, args *OperateTaskArgs) (err error)
	CancelTask(ctx context.Context, args *OperateTaskArgs) (err error)
	CompleteTask(ctx context.Context, args *OperateTaskArgs) (err error)
	CompleteInspect(ctx context.Context, args *CompleteInspectArgs) (err error)

	// stats
	DetailMigrateTask(ctx context.Context, args *MigrateTaskDetailArgs) (detail MigrateTaskDetail, err error)
	Stats(ctx context.Context, host string) (ret TasksStat, err error)
	LeaderStats(ctx context.Context) (ret TasksStat, err error)

	// add manual migrate task
	AddManualMigrateTask(ctx context.Context, args *AddManualMigrateArgs) (err error)
	IVolumeUpdater
}

type IVolumeUpdater interface {
	UpdateVol(ctx context.Context, host string, vid proto.Vid) (err error)
}

// UpdateVolumeArgs argument of volume to update.
type UpdateVolumeArgs struct {
	Vid proto.Vid `json:"vid"`
}

type Config struct {
	HostSyncIntervalMs int64 `json:"host_sync_interval_ms"`
	rpc.Config
}

type client struct {
	selector selector.Selector
	rpc.Client
}

func NewVolumeUpdater(cfg *Config) IVolumeUpdater {
	return &client{
		Client: rpc.NewClient(&cfg.Config),
	}
}

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
		cfg.HostSyncIntervalMs = 1000
	}
	return &client{
		selector: selector.MakeSelector(cfg.HostSyncIntervalMs, hostGetter),
		Client:   rpc.NewClient(&cfg.Config),
	}
}

func (c *client) UpdateVol(ctx context.Context, host string, vid proto.Vid) (err error) {
	return c.PostWith(ctx, host+PathUpdateVolume, nil, UpdateVolumeArgs{Vid: vid})
}

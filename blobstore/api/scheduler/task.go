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
	"fmt"

	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type AcquireArgs struct {
	IDC string `json:"idc"`
}

type WorkerTask struct {
	Task proto.MigrateTask `json:"task"` // migrate task
}

func (t *WorkerTask) TaskType() proto.TaskType {
	return t.Task.TaskType
}

func (t *WorkerTask) IsValid() bool {
	return t.TaskType().Valid() && t.Task.CodeMode.IsValid() &&
		proto.CheckVunitLocations([]proto.VunitLocation{t.Task.Destination}) &&
		proto.CheckVunitLocations(t.Task.Sources)
}

func (c *client) AcquireTask(ctx context.Context, args *AcquireArgs) (ret *WorkerTask, err error) {
	err = c.request(func(host string) error {
		return c.GetWith(ctx, host+PathTaskAcquire+"?idc="+args.IDC, &ret)
	})
	return
}

type WorkerInspectTask struct {
	Task *proto.InspectTask `json:"task"`
}

func (task *WorkerInspectTask) IsValid() bool {
	return task.Task.Mode.IsValid() && proto.CheckVunitLocations(task.Task.Replicas)
}

func (c *client) AcquireInspectTask(ctx context.Context) (*WorkerInspectTask, error) {
	ret := WorkerInspectTask{
		Task: &proto.InspectTask{},
	}
	err := c.request(func(host string) error {
		return c.GetWith(ctx, host+PathInspectAcquire, &ret)
	})
	return &ret, err
}

type TaskRenewalArgs struct {
	IDC string                      `json:"idc"`
	IDs map[proto.TaskType][]string `json:"ids"`
}

type TaskRenewalRet struct {
	Errors map[proto.TaskType]map[string]string `json:"errors,omitempty"`
}

func (c *client) RenewalTask(ctx context.Context, args *TaskRenewalArgs) (ret *TaskRenewalRet, err error) {
	err = c.request(func(host string) error {
		return c.PostWith(ctx, host+PathTaskRenewal, &ret, args)
	})
	return
}

type TaskReportArgs struct {
	TaskType proto.TaskType `json:"task_type"`
	TaskId   string         `json:"task_id"`

	TaskStats            proto.TaskStatistics `json:"task_stats"`
	IncreaseDataSizeByte int                  `json:"increase_data_size_byte"`
	IncreaseShardCnt     int                  `json:"increase_shard_cnt"`
}

func (c *client) ReportTask(ctx context.Context, args *TaskReportArgs) (err error) {
	return c.request(func(host string) error {
		return c.PostWith(ctx, host+PathTaskReport, nil, args)
	})
}

type ReclaimTaskArgs struct {
	TaskId   string                `json:"task_id"`
	IDC      string                `json:"idc"`
	TaskType proto.TaskType        `json:"task_type"`
	Src      []proto.VunitLocation `json:"src"`
	Dest     proto.VunitLocation   `json:"dest"`
	Reason   string                `json:"reason"`
}

func (c *client) ReclaimTask(ctx context.Context, args *ReclaimTaskArgs) (err error) {
	return c.request(func(host string) error {
		return c.PostWith(ctx, host+PathTaskReclaim, nil, args)
	})
}

type CancelTaskArgs struct {
	TaskId   string                `json:"task_id"`
	IDC      string                `json:"idc"`
	TaskType proto.TaskType        `json:"task_type"`
	Src      []proto.VunitLocation `json:"src"`
	Dest     proto.VunitLocation   `json:"dest"`
	Reason   string                `json:"reason"`
}

func (c *client) CancelTask(ctx context.Context, args *CancelTaskArgs) (err error) {
	return c.request(func(host string) error {
		return c.PostWith(ctx, host+PathTaskCancel, nil, args)
	})
}

type CompleteTaskArgs struct {
	TaskId   string                `json:"task_id"`
	IDC      string                `json:"idc"`
	TaskType proto.TaskType        `json:"task_type"`
	Src      []proto.VunitLocation `json:"src"`
	Dest     proto.VunitLocation   `json:"dest"`
}

func (c *client) CompleteTask(ctx context.Context, args *CompleteTaskArgs) (err error) {
	return c.request(func(host string) error {
		return c.PostWith(ctx, host+PathTaskComplete, nil, args)
	})
}

type CompleteInspectArgs struct {
	*proto.InspectRet
}

func (c *client) CompleteInspect(ctx context.Context, args *CompleteInspectArgs) (err error) {
	return c.request(func(host string) error {
		return c.PostWith(ctx, host+PathInspectComplete, nil, args)
	})
}

type AddManualMigrateArgs struct {
	Vuid           proto.Vuid `json:"vuid"`
	DirectDownload bool       `json:"direct_download"`
}

func (args *AddManualMigrateArgs) Valid() bool {
	return args.Vuid.IsValid()
}

func (c *client) AddManualMigrateTask(ctx context.Context, args *AddManualMigrateArgs) (err error) {
	return c.request(func(host string) error {
		return c.PostWith(ctx, host+PathManualMigrateTaskAdd, nil, args)
	})
}

// MigrateTaskDetailArgs migrate task detail args.
type MigrateTaskDetailArgs struct {
	Type proto.TaskType `json:"type"`
	ID   string         `json:"id"`
}

// MigrateTaskDetail migrate task detail.
type MigrateTaskDetail struct {
	Task proto.MigrateTask    `json:"task"`
	Stat proto.TaskStatistics `json:"stat"`
}

type PerMinStats struct {
	FinishedCnt    string `json:"finished_cnt"`
	ShardCnt       string `json:"shard_cnt"`
	DataAmountByte string `json:"data_amount_byte"`
}

type DiskRepairTasksStat struct {
	Enable           bool         `json:"enable"`
	RepairingDiskID  proto.DiskID `json:"repairing_disk_id"`
	TotalTasksCnt    int          `json:"total_tasks_cnt"`
	RepairedTasksCnt int          `json:"repaired_tasks_cnt"`
	MigrateTasksStat
}

type MigrateTasksStat struct {
	PreparingCnt   int         `json:"preparing_cnt"`
	WorkerDoingCnt int         `json:"worker_doing_cnt"`
	FinishingCnt   int         `json:"finishing_cnt"`
	StatsPerMin    PerMinStats `json:"stats_per_min"`
}

type DiskDropTasksStat struct {
	Enable          bool         `json:"enable"`
	DroppingDiskID  proto.DiskID `json:"dropping_disk_id"`
	TotalTasksCnt   int          `json:"total_tasks_cnt"`
	DroppedTasksCnt int          `json:"dropped_tasks_cnt"`
	MigrateTasksStat
}

type BalanceTasksStat struct {
	Enable bool `json:"enable"`
	MigrateTasksStat
}

type ManualMigrateTasksStat struct {
	MigrateTasksStat
}

type VolumeInspectTasksStat struct {
	Enable         bool   `json:"enable"`
	FinishedPerMin string `json:"finished_per_min"`
	TimeOutPerMin  string `json:"time_out_per_min"`
}

// RunnerStat shard repair and blob delete stat
type RunnerStat struct {
	Enable        bool     `json:"enable"`
	SuccessPerMin string   `json:"success_per_min"`
	FailedPerMin  string   `json:"failed_per_min"`
	TotalErrCnt   uint64   `json:"total_err_cnt"`
	ErrStats      []string `json:"err_stats"`
}

type TasksStat struct {
	DiskRepair    *DiskRepairTasksStat    `json:"disk_repair,omitempty"`
	DiskDrop      *DiskDropTasksStat      `json:"disk_drop,omitempty"`
	Balance       *BalanceTasksStat       `json:"balance,omitempty"`
	ManualMigrate *ManualMigrateTasksStat `json:"manual_migrate,omitempty"`
	VolumeInspect *VolumeInspectTasksStat `json:"volume_inspect,omitempty"`
	ShardRepair   *RunnerStat             `json:"shard_repair"`
	BlobDelete    *RunnerStat             `json:"blob_delete"`
}

func (c *client) DetailMigrateTask(ctx context.Context, args *MigrateTaskDetailArgs) (detail MigrateTaskDetail, err error) {
	if args == nil || !args.Type.Valid() {
		err = errcode.ErrIllegalArguments
		return
	}
	err = c.request(func(host string) error {
		path := fmt.Sprintf("%s%s/%s/%s", host, PathTaskDetail, args.Type, args.ID)
		return c.GetWith(ctx, path, &detail)
	})
	return
}

func (c *client) Stats(ctx context.Context, host string) (ret TasksStat, err error) {
	err = c.GetWith(ctx, host+PathStats, &ret)
	return
}

func (c *client) LeaderStats(ctx context.Context) (ret TasksStat, err error) {
	err = c.request(func(host string) error {
		return c.GetWith(ctx, host+PathLeaderStats, &ret)
	})
	return
}

func (c *client) selectHost() (string, error) {
	hosts := c.selector.GetRandomN(1)
	if len(hosts) == 0 {
		return "", errNoServiceAvailable
	}
	return hosts[0], nil
}

func (c *client) request(req func(host string) error) error {
	host, err := c.selectHost()
	if err != nil {
		return err
	}
	return req(host)
}

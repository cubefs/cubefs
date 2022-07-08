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

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type AcquireArgs struct {
	IDC string `json:"idc"`
}

type WorkerTask struct {
	TaskType      string               `json:"task_type"`      // task type
	Repair        *proto.VolRepairTask `json:"repair"`         // repair task
	Balance       *proto.MigrateTask   `json:"balance"`        // balance task
	DiskDrop      *proto.MigrateTask   `json:"disk_drop"`      // disk drop task
	ManualMigrate *proto.MigrateTask   `json:"manual_migrate"` // manual migrate task
}

func (task *WorkerTask) IsValid() bool {
	var (
		mode        codemode.CodeMode
		destination proto.VunitLocation
		srcs        []proto.VunitLocation
	)
	switch task.TaskType {
	case proto.RepairTaskType:
		mode = task.Repair.CodeMode
		destination = task.Repair.Destination
		srcs = task.Repair.Sources
	case proto.BalanceTaskType:
		mode = task.Balance.CodeMode
		destination = task.Balance.Destination
		srcs = task.Balance.Sources
	case proto.DiskDropTaskType:
		mode = task.DiskDrop.CodeMode
		destination = task.DiskDrop.Destination
		srcs = task.DiskDrop.Sources
	case proto.ManualMigrateType:
		mode = task.ManualMigrate.CodeMode
		destination = task.ManualMigrate.Destination
		srcs = task.ManualMigrate.Sources
	default:
		return false
	}

	if !mode.IsValid() {
		return false
	}
	// check destination
	if !proto.CheckVunitLocations([]proto.VunitLocation{destination}) {
		return false
	}
	// check sources
	if !proto.CheckVunitLocations(srcs) {
		return false
	}

	return true
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
	if !task.Task.Mode.IsValid() {
		return false
	}

	if !proto.CheckVunitLocations(task.Task.Replicas) {
		return false
	}

	return true
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
	IDC           string              `json:"idc"`
	Repair        map[string]struct{} `json:"repair"`
	Balance       map[string]struct{} `json:"balance"`
	DiskDrop      map[string]struct{} `json:"disk_drop"`
	ManualMigrate map[string]struct{} `json:"manual_migrate"`
}

type TaskRenewalRet struct {
	Repair        map[string]string `json:"repair"`
	Balance       map[string]string `json:"balance"`
	DiskDrop      map[string]string `json:"disk_drop"`
	ManualMigrate map[string]string `json:"manual_migrate"`
}

func (c *client) RenewalTask(ctx context.Context, args *TaskRenewalArgs) (ret *TaskRenewalRet, err error) {
	err = c.request(func(host string) error {
		return c.PostWith(ctx, host+PathTaskRenewal, &ret, args)
	})
	return
}

type TaskReportArgs struct {
	TaskType string `json:"task_type"`
	TaskId   string `json:"task_id"`

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
	TaskType string                `json:"task_type"`
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
	TaskType string                `json:"task_type"`
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
	TaskType string                `json:"task_type"`
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

// for task stat
type TaskStatArgs struct {
	TaskId string `json:"task_id"`
}

type RepairTaskDetail struct {
	TaskInfo proto.VolRepairTask  `json:"task_info"`
	RunStats proto.TaskStatistics `json:"run_stats"`
}

type MigrateTaskDetail struct {
	TaskInfo proto.MigrateTask    `json:"task_info"`
	RunStats proto.TaskStatistics `json:"run_stats"`
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

func (c *client) DiskRepairTaskDetail(ctx context.Context, args *TaskStatArgs) (ret RepairTaskDetail, err error) {
	err = c.request(func(host string) error {
		return c.PostWith(ctx, host+PathRepairTaskDetail, &ret, args)
	})
	return
}

func (c *client) BalanceTaskDetail(ctx context.Context, args *TaskStatArgs) (ret MigrateTaskDetail, err error) {
	err = c.request(func(host string) error {
		return c.PostWith(ctx, host+PathBalanceTaskDetail, &ret, args)
	})
	return
}

func (c *client) DiskDropTaskDetail(ctx context.Context, args *TaskStatArgs) (ret MigrateTaskDetail, err error) {
	err = c.request(func(host string) error {
		return c.PostWith(ctx, host+PathDropTaskDetail, &ret, args)
	})
	return
}

func (c *client) ManualMigrateTaskDetail(ctx context.Context, args *TaskStatArgs) (ret MigrateTaskDetail, err error) {
	err = c.request(func(host string) error {
		return c.PostWith(ctx, host+PathManualMigrateTaskDetail, &ret, args)
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

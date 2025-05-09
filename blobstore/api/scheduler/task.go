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
	"encoding/json"
	"fmt"
	"net/url"

	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type AcquireArgs struct {
	IDC string `json:"idc"`
}

type TaskArgs struct {
	ModuleType proto.ModuleType `json:"module_type"`
	TaskType   proto.TaskType   `json:"task_type"`
	Data       []byte           `json:"data"`
}

// TaskRet use for query task info
type TaskRet struct {
	TaskType proto.TaskType `json:"task_type"`
	Data     []byte         `json:"data"`
}

func (c *client) AcquireTask(ctx context.Context, args *AcquireArgs) (ret *proto.Task, err error) {
	ret = new(proto.Task)
	err = c.request(func(host string) error {
		return c.GetWith(ctx, host+PathTaskAcquire+"?idc="+args.IDC, ret)
	})
	return
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
	TaskID   string         `json:"task_id"`

	TaskStats            proto.TaskStatistics `json:"task_stats"`
	IncreaseDataSizeByte int                  `json:"increase_data_size_byte"`
	IncreaseShardCnt     int                  `json:"increase_shard_cnt"`
}

func (t *TaskReportArgs) Unmarshal(data []byte) error {
	return json.Unmarshal(data, t)
}

func (t *TaskReportArgs) Marshal() (data []byte, err error) {
	return json.Marshal(t)
}

func (t *TaskReportArgs) TaskArgs() (*TaskArgs, error) {
	ret := new(TaskArgs)
	ret.ModuleType = proto.TypeBlobNode
	ret.TaskType = t.TaskType
	data, err := t.Marshal()
	if err != nil {
		return nil, err
	}
	ret.Data = data
	return ret, nil
}

func (c *client) ReportTask(ctx context.Context, args *TaskArgs) (err error) {
	return c.request(func(host string) error {
		return c.PostWith(ctx, host+PathTaskReport, nil, args)
	})
}

// OperateTaskArgs for blobnode task action.
type OperateTaskArgs struct {
	IDC      string                `json:"idc"`
	TaskID   string                `json:"task_id"`
	TaskType proto.TaskType        `json:"task_type"`
	Src      []proto.VunitLocation `json:"src"`
	Dest     proto.VunitLocation   `json:"dest"`
	Reason   string                `json:"reason"`
}

func (t *OperateTaskArgs) Unmarshal(data []byte) error {
	return json.Unmarshal(data, t)
}

func (t *OperateTaskArgs) Marshal() (data []byte, err error) {
	return json.Marshal(t)
}

func (t *OperateTaskArgs) TaskArgs() (*TaskArgs, error) {
	ret := new(TaskArgs)
	ret.ModuleType = proto.TypeBlobNode
	ret.TaskType = t.TaskType
	data, err := t.Marshal()
	if err != nil {
		return nil, err
	}
	ret.Data = data
	return ret, nil
}

func (c *client) ReclaimTask(ctx context.Context, args *TaskArgs) (err error) {
	return c.request(func(host string) error {
		return c.PostWith(ctx, host+PathTaskReclaim, nil, args)
	})
}

func (c *client) CancelTask(ctx context.Context, args *TaskArgs) (err error) {
	return c.request(func(host string) error {
		return c.PostWith(ctx, host+PathTaskCancel, nil, args)
	})
}

func (c *client) CompleteTask(ctx context.Context, args *TaskArgs) (err error) {
	return c.request(func(host string) error {
		return c.PostWith(ctx, host+PathTaskComplete, nil, args)
	})
}

func (c *client) AcquireInspectTask(ctx context.Context) (ret *proto.VolumeInspectTask, err error) {
	err = c.request(func(host string) error {
		return c.GetWith(ctx, host+PathInspectAcquire, &ret)
	})
	return
}

func (c *client) CompleteInspectTask(ctx context.Context, args *proto.VolumeInspectRet) (err error) {
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

type ShardTaskDetail struct {
	Task proto.ShardMigrateTask `json:"task"`
}

type PerMinStats struct {
	FinishedCnt    string `json:"finished_cnt"`
	ShardCnt       string `json:"shard_cnt"`
	DataAmountByte string `json:"data_amount_byte"`
}

type DiskRepairTasksStat struct {
	Enable           bool           `json:"enable"`
	RepairingDisks   []proto.DiskID `json:"repairing_disks"`
	TotalTasksCnt    int            `json:"total_tasks_cnt"`
	RepairedTasksCnt int            `json:"repaired_tasks_cnt"`
	MigrateTasksStat
}

type MigrateTasksStat struct {
	PreparingCnt   int         `json:"preparing_cnt"`
	WorkerDoingCnt int         `json:"worker_doing_cnt"`
	FinishingCnt   int         `json:"finishing_cnt"`
	StatsPerMin    PerMinStats `json:"stats_per_min"`
}

type ShardTaskStat struct {
	PreparingCnt   int `json:"preparing_cnt"`
	WorkerDoingCnt int `json:"worker_doing_cnt"`
	FinishingCnt   int `json:"finishing_cnt"`
}

type DiskDropTasksStat struct {
	Enable          bool           `json:"enable"`
	DroppingDisks   []proto.DiskID `json:"dropping_disks"`
	TotalTasksCnt   int            `json:"total_tasks_cnt"`
	DroppedTasksCnt int            `json:"dropped_tasks_cnt"`
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

	ShardDiskRepair *ShardTaskStat `json:"shard_disk_repair"`
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
	err = c.GetWith(ctx, hostWithScheme(host)+PathStats, &ret)
	return
}

func (c *client) LeaderStats(ctx context.Context) (ret TasksStat, err error) {
	err = c.request(func(host string) error {
		return c.GetWith(ctx, host+PathStatsLeader, &ret)
	})
	return
}

type DiskMigratingStatsArgs struct {
	TaskType proto.TaskType `json:"task_type"`
	DiskID   proto.DiskID   `json:"disk_id"`
}

type DiskMigratingStats struct {
	TotalTasksCnt    int `json:"total_tasks_cnt"`
	MigratedTasksCnt int `json:"migrated_tasks_cnt"`
}

func (c *client) DiskMigratingStats(ctx context.Context, args *DiskMigratingStatsArgs) (ret *DiskMigratingStats, err error) {
	if args == nil || !args.TaskType.Valid() {
		err = errcode.ErrIllegalArguments
		return
	}
	err = c.request(func(host string) error {
		path := host + PathStatsDiskMigrating + fmt.Sprintf("?task_type=%s&disk_id=%d", args.TaskType, args.DiskID)
		return c.GetWith(ctx, path, &ret)
	})
	return
}

func (c *client) selectHost() ([]string, error) {
	hosts := c.selector.GetRandomN(c.hostRetry)
	if len(hosts) == 0 {
		return nil, errNoServiceAvailable
	}
	return hosts, nil
}

func (c *client) request(req func(host string) error) (err error) {
	var hosts []string
	hosts, err = c.selectHost()
	if err != nil {
		return err
	}

	for _, host := range hosts {
		if err = req(host); err == nil {
			return err
		}
	}
	return err
}

func hostWithScheme(host string) string {
	u, err := url.Parse(host)
	if err == nil && u.Scheme != "" && u.Host != "" {
		return host
	}
	return "http://" + host
}

// ShardTaskArgs for shard node task action.
type ShardTaskArgs struct {
	IDC      string                    `json:"idc"`
	TaskID   string                    `json:"task_id"`
	TaskType proto.TaskType            `json:"task_type"`
	Source   proto.ShardUnitInfoSimple `json:"source"`
	Dest     proto.ShardUnitInfoSimple `json:"dest"`
	Leader   proto.ShardUnitInfoSimple `json:"leader"`
	Learner  bool                      `json:"learner"`
	Reason   string                    `json:"reason"`
}

func (t *ShardTaskArgs) Unmarshal(data []byte) error {
	return json.Unmarshal(data, t)
}

func (t *ShardTaskArgs) Marshal() (data []byte, err error) {
	return json.Marshal(t)
}

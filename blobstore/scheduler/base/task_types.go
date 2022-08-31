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

package base

import (
	"errors"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

const (
	defaultPrepareQueueRetryDelayS = 10
	defaultCancelPunishDurationS   = 20
	defaultFinishQueueRetryDelayS  = 10
	defaultCollectIntervalS        = 5
	defaultCheckTaskIntervalS      = 5

	defaultDiskConcurrency = 1
	defaultWorkQueueSize   = 20
)

const (
	// EmptyDiskID empty diskID
	EmptyDiskID = proto.DiskID(0)
)

// err use for task
var (
	ErrNoTaskInQueue     = errors.New("no task in queue")
	ErrVolNotOnlyOneTask = errors.New("vol not only one task running")
	ErrUpdateVolumeCache = errors.New("update volume cache failed")
)

// TaskCommonConfig task common config
type TaskCommonConfig struct {
	PrepareQueueRetryDelayS int `json:"prepare_queue_retry_delay_s"`
	FinishQueueRetryDelayS  int `json:"finish_queue_retry_delay_s"`
	CancelPunishDurationS   int `json:"cancel_punish_duration_s"`
	WorkQueueSize           int `json:"work_queue_size"`
	CollectTaskIntervalS    int `json:"collect_task_interval_s"`
	CheckTaskIntervalS      int `json:"check_task_interval_s"`
	DiskConcurrency         int `json:"disk_concurrency"`
}

// CheckAndFix check and fix task common config
func (conf *TaskCommonConfig) CheckAndFix() {
	defaulter.LessOrEqual(&conf.PrepareQueueRetryDelayS, defaultPrepareQueueRetryDelayS)
	defaulter.LessOrEqual(&conf.FinishQueueRetryDelayS, defaultFinishQueueRetryDelayS)
	defaulter.LessOrEqual(&conf.CancelPunishDurationS, defaultCancelPunishDurationS)
	defaulter.LessOrEqual(&conf.WorkQueueSize, defaultWorkQueueSize)
	defaulter.LessOrEqual(&conf.CollectTaskIntervalS, defaultCollectIntervalS)
	defaulter.LessOrEqual(&conf.CheckTaskIntervalS, defaultCheckTaskIntervalS)
	defaulter.LessOrEqual(&conf.DiskConcurrency, defaultDiskConcurrency)
}

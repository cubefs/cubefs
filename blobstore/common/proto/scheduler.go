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

package proto

import (
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var (
	ErrTaskPaused = errors.New("task has paused")
	ErrTaskEmpty  = errors.New("no task to run")
)

const (
	// TaskRenewalPeriodS + RenewalTimeoutS < TaskLeaseExpiredS
	TaskRenewalPeriodS = 5  // worker alive tasks  renewal period
	RenewalTimeoutS    = 1  // timeout of worker task renewal
	TaskLeaseExpiredS  = 10 // task lease duration in scheduler
)

type TaskType string

const (
	TaskTypeDiskRepair    TaskType = "disk_repair"
	TaskTypeBalance       TaskType = "balance"
	TaskTypeDiskDrop      TaskType = "disk_drop"
	TaskTypeManualMigrate TaskType = "manual_migrate"
	TaskTypeVolumeInspect TaskType = "volume_inspect"
	TaskTypeShardRepair   TaskType = "shard_repair"
	TaskTypeBlobDelete    TaskType = "blob_delete"
)

func (t TaskType) Valid() bool {
	switch t {
	case TaskTypeDiskRepair, TaskTypeBalance, TaskTypeDiskDrop, TaskTypeManualMigrate,
		TaskTypeVolumeInspect, TaskTypeShardRepair, TaskTypeBlobDelete:
		return true
	default:
		return false
	}
}

func (t TaskType) String() string {
	return string(t)
}

type VunitLocation struct {
	Vuid   Vuid   `json:"vuid" bson:"vuid"`
	Host   string `json:"host" bson:"host"`
	DiskID DiskID `json:"disk_id" bson:"disk_id"`
}

// for task check
func CheckVunitLocations(locations []VunitLocation) bool {
	if len(locations) == 0 {
		return false
	}

	for _, l := range locations {
		if l.Vuid == InvalidVuid || l.Host == "" || l.DiskID == InvalidDiskID {
			return false
		}
	}
	return true
}

type MigrateState uint8

const (
	MigrateStateInited MigrateState = iota + 1
	MigrateStatePrepared
	MigrateStateWorkCompleted
	MigrateStateFinished
	MigrateStateFinishedInAdvance
)

type MigrateTask struct {
	TaskID   string       `json:"task_id"`   // task id
	TaskType TaskType     `json:"task_type"` // task type
	State    MigrateState `json:"state"`     // task state

	SourceIDC    string `json:"source_idc"`     // source idc
	SourceDiskID DiskID `json:"source_disk_id"` // source disk id
	SourceVuid   Vuid   `json:"source_vuid"`    // source volume unit id

	Sources  []VunitLocation   `json:"sources"`   // source volume units location
	CodeMode codemode.CodeMode `json:"code_mode"` // codemode

	Destination VunitLocation `json:"destination"` // destination volume unit location

	Ctime string `json:"ctime"` // create time
	MTime string `json:"mtime"` // modify time

	FinishAdvanceReason string `json:"finish_advance_reason"`
	// task migrate chunk direct download first,if fail will recover chunk by ec repair
	ForbiddenDirectDownload bool `json:"forbidden_direct_download"`

	WorkerRedoCnt uint8 `json:"worker_redo_cnt"` // worker redo task count
}

func (t *MigrateTask) Vid() Vid {
	return t.SourceVuid.Vid()
}

func (t *MigrateTask) GetSources() []VunitLocation {
	return t.Sources
}

func (t *MigrateTask) GetDestination() VunitLocation {
	return t.Destination
}

func (t *MigrateTask) SetDestination(dest VunitLocation) {
	t.Destination = dest
}

func (t *MigrateTask) DestinationDiskID() DiskID {
	return t.Destination.DiskID
}

func (t *MigrateTask) GetSourceDiskID() DiskID {
	return t.SourceDiskID
}

func (t *MigrateTask) Running() bool {
	return t.State == MigrateStatePrepared || t.State == MigrateStateWorkCompleted
}

func (t *MigrateTask) Copy() *MigrateTask {
	task := &MigrateTask{}
	*task = *t
	dst := make([]VunitLocation, len(t.Sources))
	copy(dst, t.Sources)
	task.Sources = dst
	return task
}

type VolumeInspectCheckPoint struct {
	StartVid Vid    `json:"start_vid"` // min vid in current batch volumes
	Ctime    string `json:"ctime"`
}

type VolumeInspectTask struct {
	TaskId   string            `json:"task_id"`
	Mode     codemode.CodeMode `json:"mode"`
	Replicas []VunitLocation   `json:"replicas"`
}

type MissedShard struct {
	Vuid Vuid   `json:"vuid"`
	Bid  BlobID `json:"bid"`
}

type VolumeInspectRet struct {
	TaskID        string         `json:"task_id"`
	InspectErrStr string         `json:"inspect_err_str"` // inspect run success or not
	MissedShards  []*MissedShard `json:"missed_shards"`
}

func (inspect *VolumeInspectRet) Err() error {
	if len(inspect.InspectErrStr) == 0 {
		return nil
	}
	return errors.New(inspect.InspectErrStr)
}

type ShardRepairTask struct {
	Bid      BlobID            `json:"bid"`
	CodeMode codemode.CodeMode `json:"code_mode"`
	Sources  []VunitLocation   `json:"sources"`
	BadIdxs  []uint8           `json:"bad_idxs"` // TODO: BadIdxes
	Reason   string            `json:"reason"`
}

func (task *ShardRepairTask) IsValid() bool {
	return task.CodeMode.IsValid() && CheckVunitLocations(task.Sources)
}

// TaskStatistics thread-unsafe task statistics.
type TaskStatistics struct {
	DoneSize   uint64 `json:"done_size"`
	DoneCount  uint64 `json:"done_count"`
	TotalSize  uint64 `json:"total_size"`
	TotalCount uint64 `json:"total_count"`
	Progress   uint64 `json:"progress"`
}

// TaskProgress migrate task running progress.
type TaskProgress interface {
	Total(size, count uint64) // reset total size and count.
	Do(size, count uint64)    // update progress.
	Done() TaskStatistics     // returns newest statistics.
}

// NewTaskProgress returns thread-safe task progress.
func NewTaskProgress() TaskProgress {
	return &taskProgress{}
}

type taskProgress struct {
	mu sync.Mutex
	st TaskStatistics
}

func (p *taskProgress) Total(size, count uint64) {
	p.mu.Lock()
	st := &p.st
	st.TotalSize = size
	st.TotalCount = count
	if st.TotalSize == 0 {
		st.Progress = 100
	} else {
		st.Progress = (st.DoneSize * 100) / st.TotalSize
	}
	p.mu.Unlock()
}

func (p *taskProgress) Do(size, count uint64) {
	p.mu.Lock()
	st := &p.st
	st.DoneSize += size
	st.DoneCount += count
	if st.TotalSize == 0 {
		st.Progress = 100
	} else {
		st.Progress = (st.DoneSize * 100) / st.TotalSize
	}
	p.mu.Unlock()
}

func (p *taskProgress) Done() TaskStatistics {
	p.mu.Lock()
	st := p.st
	p.mu.Unlock()
	return st
}

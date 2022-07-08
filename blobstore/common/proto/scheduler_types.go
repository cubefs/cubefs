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

const (
	RepairTaskType    = "repair_task"
	BalanceTaskType   = "balance_task"
	DiskDropTaskType  = "disk_drop_task"
	ManualMigrateType = "manual_migrate"
)

var _taskType = map[string]struct{}{
	RepairTaskType:    {},
	BalanceTaskType:   {},
	DiskDropTaskType:  {},
	ManualMigrateType: {},
}

func ValidTaskType(task string) bool {
	_, ok := _taskType[task]
	return ok
}

type RepairState uint8

const (
	RepairStateInited RepairState = iota + 1
	RepairStatePrepared
	RepairStateWorkCompleted
	RepairStateFinished
	RepairStateFinishedInAdvance
)

const (
	BrokenDiskTrigger   = 0
	BrokenStripeTrigger = 1
)

type VolRepairTask struct {
	TaskID        string      `json:"task_id" bson:"_id"`
	State         RepairState `json:"state" bson:"state"`
	WorkerRedoCnt uint8       `json:"worker_redo_cnt" bson:"worker_redo_cnt"`
	RepairDiskID  DiskID      `json:"repair_disk_id" bson:"repair_disk_id"`

	CodeMode    codemode.CodeMode `json:"code_mode" bson:"code_mode"`
	Sources     []VunitLocation   `json:"sources" bson:"sources"` // include all replicas of volumes
	Destination VunitLocation     `json:"destination" bson:"destination"`

	BadVuid Vuid  `json:"bad_vuid"`
	BadIdx  uint8 `json:"bad_idx" bson:"bad_idx"` // index of repair replica in volume replicas

	BrokenDiskIDC string `json:"broken_disk_idc"`

	Ctime string `json:"ctime" bson:"ctime"` // task create time
	MTime string `json:"mtime" bson:"mtime"` // task modify time

	// BrokenDiskTrigger: trigger by broken disk,
	// BrokenStripeTrigger: trigger by stripe which has broken replica
	TriggerBy int `json:"trigger_by" bson:"trigger_by"`
}

func (t *VolRepairTask) GetSrc() []VunitLocation {
	return t.Sources
}

func (t *VolRepairTask) GetDest() VunitLocation {
	return t.Destination
}

func (t *VolRepairTask) SetDest(dst VunitLocation) {
	t.Destination = dst
}

func (t *VolRepairTask) NewDiskId() DiskID {
	return t.Destination.DiskID
}

func (t *VolRepairTask) Vid() Vid {
	return t.BadVuid.Vid()
}

func (t *VolRepairTask) RepairVuid() Vuid {
	return t.BadVuid
}

func (t *VolRepairTask) Finished() bool {
	return t.State == RepairStateFinished || t.State == RepairStateFinishedInAdvance
}

func (t *VolRepairTask) Running() bool {
	return t.State == RepairStatePrepared || t.State == RepairStateWorkCompleted
}

func (t *VolRepairTask) Copy() *VolRepairTask {
	task := &VolRepairTask{}
	*task = *t
	dst := make([]VunitLocation, len(t.Sources))
	copy(dst, t.Sources)
	task.Sources = dst
	return task
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
	TaskID        string       `json:"task_id" bson:"_id"`                     // task id
	State         MigrateState `json:"state" bson:"state"`                     // task state
	WorkerRedoCnt uint8        `json:"worker_redo_cnt" bson:"worker_redo_cnt"` // worker redo task count

	SourceIdc    string `json:"source_idc" bson:"source_idc"`         // source idc
	SourceDiskID DiskID `json:"source_disk_id" bson:"source_disk_id"` // source disk id
	SourceVuid   Vuid   `json:"source_vuid" bson:"source_vuid"`       // source volume unit id

	Sources  []VunitLocation   `json:"sources" bson:"sources"`     // source volume units location
	CodeMode codemode.CodeMode `json:"code_mode" bson:"code_mode"` // codemode

	Destination VunitLocation `json:"destination" bson:"destination"` // destination volume unit location

	Ctime string `json:"ctime" bson:"ctime"` // create time
	MTime string `json:"mtime" bson:"mtime"` // modify time

	FinishAdvanceReason string `json:"finish_advance_reason" bson:"finish_advance_reason"`
	// task migrate chunk direct download first,if fail will recover chunk by ec repair
	ForbiddenDirectDownload bool `json:"forbidden_direct_download" bson:"forbidden_direct_download"`
}

func (t *MigrateTask) GetSrc() []VunitLocation {
	return t.Sources
}

func (t *MigrateTask) GetDest() VunitLocation {
	return t.Destination
}

func (t *MigrateTask) SetDest(dest VunitLocation) {
	t.Destination = dest
}

func (t *MigrateTask) DestinationDiskId() DiskID {
	return t.Destination.DiskID
}

func (t *MigrateTask) Running() bool {
	return t.State == MigrateStatePrepared || t.State == MigrateStateWorkCompleted
}

func (t *MigrateTask) Finished() bool {
	return t.State == MigrateStateFinished || t.State == MigrateStateFinishedInAdvance
}

func (t *MigrateTask) Copy() *MigrateTask {
	task := &MigrateTask{}
	*task = *t
	dst := make([]VunitLocation, len(t.Sources))
	copy(dst, t.Sources)
	task.Sources = dst
	return task
}

func (t *MigrateTask) SrcMigDiskID() DiskID {
	return t.SourceDiskID
}

type InspectCheckPoint struct {
	Id       string `json:"_id" bson:"_id"`
	StartVid Vid    `json:"start_vid" bson:"start_vid"` // min vid in current batch volumes
	Ctime    string `json:"ctime" bson:"ctime"`
}

type InspectTask struct {
	TaskId   string            `json:"task_id"`
	Mode     codemode.CodeMode `json:"mode"`
	Replicas []VunitLocation   `json:"replicas"`
}

type MissedShard struct {
	Vuid Vuid   `json:"vuid"`
	Bid  BlobID `json:"bid"`
}

type InspectRet struct {
	TaskID        string         `json:"task_id"`
	InspectErrStr string         `json:"inspect_err_str"` // inspect run success or not
	MissedShards  []*MissedShard `json:"missed_shards"`
}

func (inspect *InspectRet) Err() error {
	if len(inspect.InspectErrStr) == 0 {
		return nil
	}
	return errors.New(inspect.InspectErrStr)
}

// ArchiveRecord archive record
type ArchiveRecord struct {
	TaskID      string      `bson:"_id"`
	TaskType    string      `bson:"task_type"`
	ArchiveTime string      `bson:"archive_time"`
	Content     interface{} `bson:"content"`
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

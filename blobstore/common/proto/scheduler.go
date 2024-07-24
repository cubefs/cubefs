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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
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

	TaskTypeShardInspect    TaskType = "shard_inspect"
	TaskTypeShardDiskRepair TaskType = "shard_disk_repair"
	TaskTypeShardMigrate    TaskType = "shard_migrate"
	TaskTypeShardDiskDrop   TaskType = "shard_disk_drop"
)

func (t TaskType) Valid() bool {
	switch t {
	case TaskTypeDiskRepair, TaskTypeBalance, TaskTypeDiskDrop, TaskTypeManualMigrate,
		TaskTypeVolumeInspect, TaskTypeShardRepair, TaskTypeBlobDelete,
		TaskTypeShardInspect, TaskTypeShardDiskRepair, TaskTypeShardMigrate, TaskTypeShardDiskDrop:
		return true
	default:
		return false
	}
}

func (t TaskType) String() string {
	return string(t)
}

// VunitLocation volume or shard location
type VunitLocation struct {
	Vuid   Vuid   `json:"vuid" bson:"vuid"`
	Host   string `json:"host" bson:"host"`
	DiskID DiskID `json:"disk_id" bson:"disk_id"`
}

// CheckVunitLocations for task check
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

// SunitLocation shard location
type SunitLocation struct {
	// todo for proto suid
	Suid   Suid   `json:"suid"`
	Host   string `json:"host"`
	DiskID DiskID `json:"disk_id"`
}

// CheckSunitLocation CheckSunitLocations for shard task check
func CheckSunitLocation(location SunitLocation) bool {
	if location.Suid == InvalidSuid || location.Host == "" || location.DiskID == InvalidDiskID {
		return false
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

// MigrateTask for blobnode task
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

func (t *MigrateTask) Unmarshal(data []byte) error {
	return json.Unmarshal(data, t)
}

func (t *MigrateTask) Marshal() (data []byte, err error) {
	return json.Marshal(t)
}

func (t *MigrateTask) Task() (*Task, error) {
	ret := new(Task)
	ret.ModuleType = TypeBlobNode
	ret.TaskType = t.TaskType
	ret.TaskID = t.TaskID
	data, err := t.Marshal()
	if err != nil {
		return nil, err
	}
	ret.Data = data
	return ret, nil
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

func (t *MigrateTask) IsValid() bool {
	return t.TaskType.Valid() && t.CodeMode.IsValid() &&
		CheckVunitLocations(t.Sources) &&
		CheckVunitLocations([]VunitLocation{t.Destination})
}

type ShardTaskState uint8

const (
	ShardTaskStateInited ShardTaskState = iota + 1
	ShardTaskStatePrepared
	ShardTaskStateWorkCompleted
	ShardTaskStateFinished
	ShardTaskStateFinishedInAdvance
)

// ShardMigrateTask for shard node task
type ShardMigrateTask struct {
	TaskID   string         `json:"task_id"`   // task id
	TaskType TaskType       `json:"task_type"` // task type
	State    ShardTaskState `json:"state"`     // task state

	SourceIDC string `json:"source_idc"` // source idc

	Ctime string `json:"ctime"` // create time
	MTime string `json:"mtime"` // modify time

	Source      SunitLocation `json:"source"`      // old shard location
	Leader      SunitLocation `json:"leader"`      // shard leader location
	Destination SunitLocation `json:"destination"` // new shard location
	Learner     bool          `json:"learner"`
}

func (s *ShardMigrateTask) Unmarshal(data []byte) error {
	return json.Unmarshal(data, s)
}

func (s *ShardMigrateTask) Marshal() (data []byte, err error) {
	return json.Marshal(s)
}

func (s *ShardMigrateTask) Task() (*Task, error) {
	ret := new(Task)
	ret.TaskID = s.TaskID
	ret.ModuleType = TypeShardNode
	ret.TaskType = s.TaskType
	data, err := s.Marshal()
	if err != nil {
		return nil, err
	}
	ret.Data = data
	return ret, err
}

func (s *ShardMigrateTask) GetSource() SunitLocation {
	return s.Source
}

func (s *ShardMigrateTask) GetLeader() SunitLocation {
	return s.Leader
}

func (s *ShardMigrateTask) GetDestination() SunitLocation {
	return s.Destination
}

func (s *ShardMigrateTask) SetDestination(dest SunitLocation) {
	s.Destination = dest
}

func (s *ShardMigrateTask) IsValid() bool {
	return CheckSunitLocation(s.Source) && CheckSunitLocation(s.Destination)
}

type VolumeInspectCheckPoint struct {
	StartVid Vid    `json:"start_vid"` // min vid in current batch volumes
	Ctime    string `json:"ctime"`
}

type VolumeInspectTask struct {
	TaskID   string            `json:"task_id"`
	Mode     codemode.CodeMode `json:"mode"`
	Replicas []VunitLocation   `json:"replicas"`
}

func (t *VolumeInspectTask) IsValid() bool {
	return t.Mode.IsValid() && CheckVunitLocations(t.Replicas)
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

type ModuleType uint8

const (
	TypeMin ModuleType = iota
	TypeBlobNode
	TypeShardNode
	TypeMax
)

type Task struct {
	ModuleType ModuleType `json:"module_type"`
	TaskType   TaskType   `json:"task_type"`
	TaskID     string     `json:"task_id"`
	Data       []byte     `json:"data"`
}

func (mt ModuleType) IsValid() bool {
	return mt > TypeMin && mt < TypeMax
}

func (t *Task) Marshal() ([]byte, string, error) {
	buffer := bytes.NewBuffer(nil)
	if err := binary.Write(buffer, binary.BigEndian, t.ModuleType); err != nil {
		return nil, "", err
	}
	taskType := []byte(t.TaskType)
	numTaskType := int32(len(taskType))
	if err := binary.Write(buffer, binary.BigEndian, numTaskType); err != nil {
		return nil, "", err
	}
	if _, err := buffer.Write(taskType); err != nil {
		return nil, "", err
	}
	taskID := []byte(t.TaskID)
	numTaskID := int32(len(taskID))
	if err := binary.Write(buffer, binary.BigEndian, numTaskID); err != nil {
		return nil, "", err
	}
	if _, err := buffer.Write(taskID); err != nil {
		return nil, "", err
	}

	numData := int32(len(t.Data))
	if err := binary.Write(buffer, binary.BigEndian, numData); err != nil {
		return nil, "", err
	}
	if _, err := buffer.Write(t.Data); err != nil {
		return nil, "", err
	}
	return buffer.Bytes(), rpc.MIMEStream, nil
}

func (t *Task) UnmarshalFrom(body io.Reader) (err error) {
	if err = binary.Read(body, binary.BigEndian, &t.ModuleType); err != nil {
		return
	}

	numTaskType := int32(0)
	if err = binary.Read(body, binary.BigEndian, &numTaskType); err != nil {
		return
	}
	taskType := make([]byte, numTaskType)
	if _, err = io.ReadFull(body, taskType); err != nil {
		return err
	}

	numTaskID := int32(0)
	if err = binary.Read(body, binary.BigEndian, &numTaskID); err != nil {
		return
	}
	taskID := make([]byte, numTaskID)
	if _, err = io.ReadFull(body, taskID); err != nil {
		return err
	}

	numData := int32(0)
	if err = binary.Read(body, binary.BigEndian, &numData); err != nil {
		return
	}
	data := make([]byte, numData)
	if _, err = io.ReadFull(body, data); err != nil {
		return err
	}
	t.Data = data
	t.TaskType = TaskType(taskType)
	t.TaskID = string(taskID)
	return nil
}

func (t *Task) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	reader := bytes.NewReader(data)
	return t.UnmarshalFrom(reader)
}

// Copyright 2023 The CubeFS Authors.
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
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	RuleEnabled  string = "Enabled"
	RuleDisabled string = "Disabled"

	OpTypeDelete          = "DELETE"
	OpTypeStorageClassHDD = "STANDARD_IA"
	OpTypeStorageClassEBS = "GLACIER"
)

func OpTypeToStorageType(op string) uint32 {
	switch op {
	case OpTypeStorageClassHDD:
		return StorageClass_Replica_HDD
	case OpTypeStorageClassEBS:
		return StorageClass_BlobStore
	}
	return 0
}

type LcConfiguration struct {
	VolName string
	Rules   []*Rule
}

type Rule struct {
	ID          string        `json:"ID" xml:"ID" bson:"ID"`
	Status      string        `json:"Status" xml:"Status" bson:"Status"`
	Filter      *Filter       `json:"Filter,omitempty" xml:"Filter,omitempty" bson:"Filter,omitempty"`
	Expiration  *Expiration   `json:"Expiration,omitempty" xml:"Expiration,omitempty" bson:"Expiration,omitempty"`
	Transitions []*Transition `json:"Transition,omitempty" xml:"Transition,omitempty" bson:"Transition,omitempty"`
}

type Expiration struct {
	Date *time.Time `json:"Date,omitempty" xml:"Date,omitempty" bson:"Date,omitempty"`
	Days *int       `json:"Days,omitempty" xml:"Days,omitempty" bson:"Days,omitempty"`
}

type Filter struct {
	Prefix string `json:"Prefix,omitempty" xml:"Prefix,omitempty" bson:"Prefix,omitempty"`
}

type Transition struct {
	Date         *time.Time `json:"Date,omitempty" xml:"Date,omitempty" bson:"Date,omitempty"`
	Days         *int       `json:"Days,omitempty" xml:"Days,omitempty" bson:"Days,omitempty"`
	StorageClass string     `json:"StorageClass,omitempty" xml:"StorageClass,omitempty" bson:"StorageClass,omitempty"`
}

func (lcConf *LcConfiguration) GenEnabledRuleTasks() []*RuleTask {
	tasks := make([]*RuleTask, 0)
	for _, r := range lcConf.Rules {
		if r.Status != RuleEnabled {
			log.LogDebugf("GenEnabledRuleTasks: skip disabled rule(%v) in volume(%v)", r.ID, lcConf.VolName)
			continue
		}
		task := &RuleTask{
			Id:      fmt.Sprintf("%s:%s", lcConf.VolName, r.ID),
			VolName: lcConf.VolName,
			Rule:    r,
		}
		tasks = append(tasks, task)
		log.LogDebugf("GenEnabledRuleTasks: RuleTask(%v) generated from rule(%v) in volume(%v)", *task, r.ID, lcConf.VolName)
	}
	return tasks
}

// ----------------------------------------------
// lcnode <-> master
// LcNodeRuleTask

type LcNodeRuleTaskRequest struct {
	MasterAddr string
	LcNodeAddr string
	Task       *RuleTask
}

type RuleTask struct {
	Id      string
	VolName string
	Rule    *Rule
}

type LcNodeRuleTaskResponse struct {
	ID         string
	LcNode     string
	StartTime  *time.Time
	EndTime    *time.Time
	UpdateTime *time.Time
	Done       bool
	Status     uint8
	Result     string
	LcNodeRuleTaskStatistics
}

type LcNodeRuleTaskStatistics struct {
	Volume               string
	RuleId               string
	TotalInodeScannedNum int64
	FileScannedNum       int64
	DirScannedNum        int64
	ExpiredNum           int64
	MigrateToHddNum      int64
	MigrateToEbsNum      int64
	MigrateToHddBytes    int64
	MigrateToEbsBytes    int64
	ErrorSkippedNum      int64
}

// ----------------------------------
// lcnode <-> meta

type ScanDentry struct {
	ParentId     uint64 `json:"pid"`   // FileID value of the parent inode.
	Inode        uint64 `json:"inode"` // FileID value of the current inode.
	Name         string `json:"name"`  // Name of the current dentry.
	Path         string `json:"path"`  // Path of the current dentry.
	Type         uint32 `json:"type"`  // Type of the current dentry.
	Op           string `json:"op"`    // to delete or migrate
	Size         uint64 `json:"size"`  // for migrate: size of the current dentry
	StorageClass uint32 `json:"sc"`    // for migrate: storage class of the current dentry
	WriteGen     uint64 `json:"gen"`   // for migrate: used to determine whether a file is modified
}

type BatchDentries struct {
	sync.RWMutex
	dentries map[uint64]*ScanDentry
}

func NewBatchDentries() *BatchDentries {
	return &BatchDentries{
		dentries: make(map[uint64]*ScanDentry, 0),
	}
}

func (f *BatchDentries) Append(dentry *ScanDentry) {
	f.Lock()
	defer f.Unlock()
	f.dentries[dentry.Inode] = dentry
}

func (f *BatchDentries) Len() int {
	f.RLock()
	defer f.RUnlock()
	return len(f.dentries)
}

func (f *BatchDentries) BatchGetAndClear() (map[uint64]*ScanDentry, []uint64) {
	f.Lock()
	defer f.Unlock()
	var dentries = f.dentries
	var inodes []uint64
	for i := range f.dentries {
		inodes = append(inodes, i)
	}
	f.dentries = make(map[uint64]*ScanDentry, 0)
	return dentries, inodes
}

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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	RuleEnabled   string = "Enabled"
	RuleDisabled  string = "Disabled"
	RuleMaxCounts        = 1000
	MaxIdLength          = 255

	OpTypeDelete          = "DELETE"
	OpTypeStorageClassHDD = "HDD"
	OpTypeStorageClassEBS = "BLOBSTORE"
)

func OpTypeToStorageType(op string) uint32 {
	switch op {
	case OpTypeStorageClassHDD:
		return StorageClass_Replica_HDD
	case OpTypeStorageClassEBS:
		return StorageClass_BlobStore
	}
	return StorageClass_Unspecified
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

var (
	LifeCycleErrTooManyRules   = errors.New("Rules number should not exceed allowed limit of 1000")
	LifeCycleErrMissingRules   = errors.New("No Lifecycle Rules found in request")
	LifeCycleErrMissingActions = errors.New("At least one action needs to be specified in a rule")
	LifeCycleErrMissingRuleID  = errors.New("No Lifecycle Rule ID in request")
	LifeCycleErrTooLongRuleID  = errors.New("ID length should not exceed allowed limit of 255")
	LifeCycleErrSameRuleID     = errors.New("Rule ID must be unique. Found same ID for more than one rule")
	LifeCycleErrDateType       = errors.New("'Date' must be at midnight GMT")
	LifeCycleErrDaysType       = errors.New("'Days' for Expiration action must be a positive integer")
	LifeCycleErrStorageClass   = errors.New("'StorageClass' must be different for 'Transition' actions in same 'Rule'")
	LifeCycleErrMalformedXML   = errors.New("The XML you provided was not well-formed or did not validate against our published schema")
)

func ValidRules(Rules []*Rule) error {
	if len(Rules) > RuleMaxCounts {
		return LifeCycleErrTooManyRules
	}
	if len(Rules) <= 0 {
		return LifeCycleErrMissingRules
	}

	isRuleIdExist := make(map[string]bool)
	for _, rule := range Rules {
		_, ok := isRuleIdExist[rule.ID]
		if !ok {
			isRuleIdExist[rule.ID] = true
		} else {
			return LifeCycleErrSameRuleID
		}
		if err := validRule(rule); err != nil {
			return err
		}
	}

	return nil
}

func validRule(r *Rule) error {
	if len(r.ID) == 0 {
		return LifeCycleErrMissingRuleID
	}
	if len(r.ID) > MaxIdLength {
		return LifeCycleErrTooLongRuleID
	}
	if r.Status != RuleEnabled && r.Status != RuleDisabled {
		return LifeCycleErrMalformedXML
	}

	if r.Expiration == nil && r.Transitions == nil {
		return LifeCycleErrMissingActions
	}

	if r.Expiration != nil {
		if err := validExpiration(r.Expiration); err != nil {
			return err
		}
	}

	if r.Transitions != nil {
		daysMap := make(map[string]int)
		dateMap := make(map[string]*time.Time)
		singleMap := make(map[string]int)
		for _, transition := range r.Transitions {
			singleMap[transition.StorageClass]++
			if singleMap[transition.StorageClass] > 1 {
				return LifeCycleErrStorageClass
			}
			if err := validTransition(transition, dateMap, daysMap); err != nil {
				return err
			}
		}

		if err := validTransitions(dateMap, daysMap, r.Expiration); err != nil {
			return err
		}
	}

	return nil
}

func validExpiration(e *Expiration) error {
	// Date and Days cannot be set at the same time
	if e.Date != nil && e.Days != nil {
		return LifeCycleErrMalformedXML
	}
	// Date and Days cannot both be nil
	if e.Date == nil && e.Days == nil {
		return LifeCycleErrMalformedXML
	}
	// Date must be midnight UTC
	if e.Date != nil {
		date := e.Date.In(time.UTC)
		if !(date.Hour() == 0 && date.Minute() == 0 && date.Second() == 0 && date.Nanosecond() == 0) {
			return LifeCycleErrDateType
		}
	} else if e.Days != nil {
		// Days must be greater than 0
		if *e.Days <= 0 {
			return LifeCycleErrDaysType
		}
	}

	return nil
}

func validTransition(t *Transition, dateMap map[string]*time.Time, daysMap map[string]int) error {
	// Date and Days cannot be set at the same time
	if t.Date != nil && t.Days != nil {
		return LifeCycleErrMalformedXML
	}
	// Date and Days cannot both be nil
	if t.Date == nil && t.Days == nil {
		return LifeCycleErrMalformedXML
	}
	// StorageClass must be the specified
	if t.StorageClass != OpTypeStorageClassHDD && t.StorageClass != OpTypeStorageClassEBS {
		return LifeCycleErrMalformedXML
	}
	// Date must be midnight UTC
	if t.Date != nil {
		date := t.Date.In(time.UTC)
		if !(date.Hour() == 0 && date.Minute() == 0 && date.Second() == 0 && date.Nanosecond() == 0) {
			//return LifeCycleErrDateType
		}
		dateMap[t.StorageClass] = t.Date
	} else if t.Days != nil {
		// Days must be greater than 0
		if *t.Days <= 0 {
			return LifeCycleErrDaysType
		}
		daysMap[t.StorageClass] = *t.Days
	}

	return nil
}

func validTransitions(dateMap map[string]*time.Time, daysMap map[string]int, expiration *Expiration) error {
	// transitions and expiration must be all in date form or all in days form
	if len(dateMap) > 0 && len(daysMap) > 0 {
		return LifeCycleErrMalformedXML
	}

	if len(dateMap) > 0 {
		var s []*time.Time
		if c, ok := dateMap[OpTypeStorageClassHDD]; ok {
			s = append(s, c)
		}
		if c, ok := dateMap[OpTypeStorageClassEBS]; ok {
			s = append(s, c)
		}
		for i := 0; i < len(s)-1; i++ {
			if !s[i+1].After(*s[i]) {
				return LifeCycleErrMalformedXML
			}
		}
		if expiration != nil {
			if expiration.Days != nil || !expiration.Date.After(*s[len(s)-1]) {
				return LifeCycleErrMalformedXML
			}
		}
	}

	if len(daysMap) > 0 {
		var s []int
		if c, ok := daysMap[OpTypeStorageClassHDD]; ok {
			s = append(s, c)
		}
		if c, ok := daysMap[OpTypeStorageClassEBS]; ok {
			s = append(s, c)
		}
		for i := 0; i < len(s)-1; i++ {
			if s[i+1] <= s[i] {
				return LifeCycleErrMalformedXML
			}
		}
		if expiration != nil {
			if expiration.Date != nil || *expiration.Days <= s[len(s)-1] {
				return LifeCycleErrMalformedXML
			}
		}
	}

	return nil
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
	HasMek       bool   `json:"mek"`   // for migrate: if HasMek, call DeleteMigrationExtentKey instead of migrating
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

type ExtentsLocalTransitionRequest struct {
	Hosts      []string
	SrcDp      uint64
	DstDp      uint64
	SrcExtents []ExtentKey
	DstExtents []ExtentKey
}

type ExtentsLocalTransitionResponse ExtentsLocalTransitionRequest

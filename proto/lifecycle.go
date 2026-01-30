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
	"regexp"
	"strings"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	RuleEnabled   string = "Enabled"
	RuleDisabled  string = "Disabled"
	RuleMaxCounts        = 1000
	MaxIdLength          = 255

	ScanByDir uint8 = 0
	ScanByMp  uint8 = 1

	OpTypeDelete          = "DELETE"
	OpTypeStorageClassHDD = "HDD"
	OpTypeStorageClassEBS = "BLOBSTORE"

	// DelayDelMinute constraints
	MinDelayDelMinute = 60     // Minimum delay delete time: 1 hour
	MaxDelayDelMinute = 525600 // Maximum delay delete time: 1 year (365 days * 24 hours * 60 minutes)
)

func OpTypeToStorageType(op string) uint32 {
	switch op {
	case OpTypeStorageClassHDD:
		return StorageClass_Replica_HDD
	case OpTypeStorageClassEBS:
		return StorageClass_BlobStore
	default:
		return StorageClass_Unspecified
	}
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
	Prefix  string `json:"Prefix" xml:"Prefix" bson:"Prefix"`
	MinSize uint64 `json:"MinSize" xml:"MinSize" bson:"MinSize"`
	ByMp    uint8  `json:"ByMp" xml:"ByMp" bson:"ByMp"`
}

type Transition struct {
	Date           *time.Time `json:"Date,omitempty" xml:"Date,omitempty" bson:"Date,omitempty"`
	Days           *int       `json:"Days,omitempty" xml:"Days,omitempty" bson:"Days,omitempty"`
	StorageClass   string     `json:"StorageClass,omitempty" xml:"StorageClass,omitempty" bson:"StorageClass,omitempty"`
	FromPoolId     uint8      `json:"FromPoolId" xml:"FromPoolId" bson:"FromPoolId"`
	ToPoolId       uint8      `json:"ToPoolId" xml:"ToPoolId" bson:"ToPoolId"`
	DelayDelMinute *uint64    `json:"DelayDelMinute" xml:"DelayDelMinute" bson:"DelayDelMinute"` // Delay delete in minutes after migration (1 hour to 1 year)
}

var (
	LifeCycleErrTooManyRules    = errors.New("Rules number should not exceed allowed limit of 1000")
	LifeCycleErrMissingRules    = errors.New("No Lifecycle Rules found in request")
	LifeCycleErrMissingActions  = errors.New("At least one action needs to be specified in a rule")
	LifeCycleErrMissingRuleID   = errors.New("No Lifecycle Rule ID in request")
	LifeCycleErrTooLongRuleID   = errors.New("ID length should not exceed allowed limit of 255")
	LifeCycleErrInvalidRuleID   = errors.New("Invalid Rule ID")
	LifeCycleErrSameRuleID      = errors.New("Rule ID must be unique. Found same ID for more than one rule")
	LifeCycleErrDateType        = errors.New("'Date' must be at midnight GMT")
	LifeCycleErrDaysType        = errors.New("'Days' for Expiration action must be a positive integer")
	LifeCycleErrStorageClass    = errors.New("'StorageClass' must be different for 'Transition' actions in same 'Rule'")
	LifeCycleErrPoolId          = errors.New("'FromPoolId' and 'ToPoolId' must be specified for 'Transition' actions")
	LifeCycleErrMalformedXML    = errors.New("The XML you provided was not well-formed or did not validate against our published schema")
	LifeCycleErrByMpAndPrefix   = errors.New("'ByMp' and 'Prefix' cannot be specified at the same time")
	LifeCycleErrConflictRules   = errors.New("Conflicting rule prefix")
	LifeCycleErrRulePrefix      = errors.New("Rule prefix cannot start with '/'")
	LifeCycleErrTransitionCycle = errors.New("Circular dependency detected in transition rules")
	LifeCycleErrDelayDelMinute  = errors.New("'DelayDelMinute' must be between MinDelayDelMinute (1 hour) and MaxDelayDelMinute (1 year) minutes")
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

	if err := ValidRulePrefix(Rules); err != nil {
		return err
	}

	// Check for circular dependencies in transition rules
	if err := validateTransitionCycles(Rules); err != nil {
		return err
	}

	return nil
}

func ValidRulePrefix(Rules []*Rule) error {
	if len(Rules) == 1 {
		if strings.HasPrefix(Rules[0].GetPrefix(), "/") {
			return LifeCycleErrRulePrefix
		}
		return nil
	}

	// Collect fromPoolIds for each rule
	type ruleInfo struct {
		prefix      string
		fromPoolIds map[uint8]bool
	}
	var ruleInfos []ruleInfo

	for _, rule := range Rules {
		if rule.Filter == nil {
			return LifeCycleErrConflictRules
		}

		if strings.HasPrefix(rule.GetPrefix(), "/") {
			return LifeCycleErrRulePrefix
		}

		if rule.Filter.ByMp == ScanByMp && rule.Filter.Prefix != "" {
			return LifeCycleErrByMpAndPrefix
		}

		// Collect all fromPoolIds from transitions
		fromPoolIds := make(map[uint8]bool)
		if rule.Transitions != nil {
			for _, transition := range rule.Transitions {
				if transition.FromPoolId != 0 {
					fromPoolIds[transition.FromPoolId] = true
				}
			}
		}

		ruleInfos = append(ruleInfos, ruleInfo{
			prefix:      rule.GetPrefix(),
			fromPoolIds: fromPoolIds,
		})
	}

	// Check prefix conflicts only for rules with same fromPoolId
	for i, r1 := range ruleInfos {
		for j, r2 := range ruleInfos {
			if i == j {
				continue
			}

			// Check if rules have common fromPoolId
			hasCommonFromPoolId := false
			if len(r1.fromPoolIds) > 0 && len(r2.fromPoolIds) > 0 {
				for poolId := range r1.fromPoolIds {
					if r2.fromPoolIds[poolId] {
						hasCommonFromPoolId = true
						break
					}
				}
			}

			// Only check prefix conflict if rules have same fromPoolId
			if hasCommonFromPoolId {
				if r1.prefix == "" || r2.prefix == "" {
					return LifeCycleErrConflictRules
				}

				if strings.HasPrefix(r1.prefix, r2.prefix) {
					return LifeCycleErrConflictRules
				}
				if strings.HasPrefix(r2.prefix, r1.prefix) {
					return LifeCycleErrConflictRules
				}
			}
			// If no common fromPoolId, allow prefix overlap
		}
	}

	return nil
}

func (r *Rule) GetPrefix() string {
	var prefix string
	if r.Filter != nil {
		prefix = r.Filter.Prefix
	}
	return prefix
}

func (r *Rule) MinSize() uint64 {
	if r.Filter != nil {
		return r.Filter.MinSize
	}
	return 0
}

var regexRuleId = regexp.MustCompile(`^[A-Za-z0-9.-]+$`)

var ExpirationEnabled bool

func validRule(r *Rule) error {
	if len(r.ID) == 0 {
		return LifeCycleErrMissingRuleID
	}
	if len(r.ID) > MaxIdLength {
		return LifeCycleErrTooLongRuleID
	}
	if !regexRuleId.MatchString(r.ID) {
		return LifeCycleErrInvalidRuleID
	}

	if r.Status != RuleEnabled && r.Status != RuleDisabled {
		return LifeCycleErrMalformedXML
	}

	if r.Expiration == nil && len(r.Transitions) == 0 {
		return LifeCycleErrMissingActions
	}

	// expiration is temporarily disabled, remove this code to enable expiration
	if r.Expiration != nil && !ExpirationEnabled {
		return errors.New("expiration is temporarily disabled")
	}

	if r.Expiration != nil {
		if err := validExpiration(r.Expiration); err != nil {
			return err
		}
	}

	if r.Transitions != nil {
		daysMap := make(map[string]int)
		dateMap := make(map[string]*time.Time)
		for _, transition := range r.Transitions {
			if err := validTransition(transition, dateMap, daysMap); err != nil {
				return err
			}
		}

		// if err := validTransitions(dateMap, daysMap, r.Expiration); err != nil {
		// 	return err
		// }
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

	// FromPoolId and ToPoolId must be specified
	if t.FromPoolId == 0 || t.ToPoolId == 0 {
		return LifeCycleErrPoolId
	}

	// Validate DelayDelMinute if specified (1 hour to 1 year)
	if t.DelayDelMinute != nil {
		if *t.DelayDelMinute < MinDelayDelMinute || *t.DelayDelMinute > MaxDelayDelMinute {
			return LifeCycleErrDelayDelMinute
		}
	}

	// Date must be midnight UTC
	if t.Date != nil {
		date := t.Date.In(time.UTC)
		if !(date.Hour() == 0 && date.Minute() == 0 && date.Second() == 0 && date.Nanosecond() == 0) {
			return LifeCycleErrDateType
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

// func validTransitions(dateMap map[string]*time.Time, daysMap map[string]int, expiration *Expiration) error {
// 	// transitions and expiration must be all in date form or all in days form
// 	if len(dateMap) > 0 && len(daysMap) > 0 {
// 		return LifeCycleErrMalformedXML
// 	}

// 	if len(dateMap) > 0 {
// 		var s []*time.Time
// 		if c, ok := dateMap[OpTypeStorageClassHDD]; ok {
// 			s = append(s, c)
// 		}
// 		if c, ok := dateMap[OpTypeStorageClassEBS]; ok {
// 			s = append(s, c)
// 		}
// 		for i := 0; i < len(s)-1; i++ {
// 			if !s[i+1].After(*s[i]) {
// 				return LifeCycleErrMalformedXML
// 			}
// 		}
// 		if expiration != nil {
// 			if expiration.Days != nil || !expiration.Date.After(*s[len(s)-1]) {
// 				return LifeCycleErrMalformedXML
// 			}
// 		}
// 	}

// 	if len(daysMap) > 0 {
// 		var s []int
// 		if c, ok := daysMap[OpTypeStorageClassHDD]; ok {
// 			s = append(s, c)
// 		}
// 		if c, ok := daysMap[OpTypeStorageClassEBS]; ok {
// 			s = append(s, c)
// 		}
// 		for i := 0; i < len(s)-1; i++ {
// 			if s[i+1] <= s[i] {
// 				return LifeCycleErrMalformedXML
// 			}
// 		}
// 		if expiration != nil {
// 			if expiration.Date != nil || *expiration.Days <= s[len(s)-1] {
// 				return LifeCycleErrMalformedXML
// 			}
// 		}
// 	}

// 	return nil
// }

// validateTransitionCycles checks for circular dependencies in transition rules
// It builds a directed graph from all transitions and uses DFS to detect cycles
func validateTransitionCycles(rules []*Rule) error {
	// Build adjacency list: map[fromPoolId] -> []toPoolId
	graph := make(map[uint8][]uint8)
	allPools := make(map[uint8]bool)

	// Collect all transitions from all rules
	for _, rule := range rules {
		if rule.Transitions == nil {
			continue
		}
		for _, transition := range rule.Transitions {
			// Only check transitions with both FromPoolId and ToPoolId specified
			if transition.FromPoolId == 0 || transition.ToPoolId == 0 {
				continue
			}
			// Skip self-loops (from same pool to same pool)
			if transition.FromPoolId == transition.ToPoolId {
				continue
			}
			fromPool := transition.FromPoolId
			toPool := transition.ToPoolId
			graph[fromPool] = append(graph[fromPool], toPool)
			allPools[fromPool] = true
			allPools[toPool] = true
		}
	}

	// Use DFS to detect cycles
	visited := make(map[uint8]bool)
	recStack := make(map[uint8]bool)

	var dfs func(uint8) bool
	dfs = func(poolId uint8) bool {
		visited[poolId] = true
		recStack[poolId] = true

		// Check all neighbors
		for _, neighbor := range graph[poolId] {
			if !visited[neighbor] {
				if dfs(neighbor) {
					return true
				}
			} else if recStack[neighbor] {
				// Found a back edge, cycle detected
				return true
			}
		}

		recStack[poolId] = false
		return false
	}

	// Check all pools for cycles
	for poolId := range allPools {
		if !visited[poolId] {
			if dfs(poolId) {
				return LifeCycleErrTransitionCycle
			}
		}
	}

	return nil
}

func (lcConf *LcConfiguration) GenEnabledRuleTasks() []*RuleTask {
	tasks := make([]*RuleTask, 0)
	for _, r := range lcConf.Rules {

		// expiration is temporarily disabled, remove this code to enable expiration
		if r.Expiration != nil {
			log.LogWarnf("GenEnabledRuleTasks: expiration is temporarily disabled, skip ruleid: %v", r.ID)
			continue
		}

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
	StartErr   string
	Volume     string
	RcvStop    bool
	Rule       *Rule
	LcNodeRuleTaskStatistics
}

type LcNodeRuleTaskStatistics struct {
	TotalFileScannedNum int64
	TotalFileExpiredNum int64
	TotalDirScannedNum  int64

	ExpiredDeleteNum         int64
	ExpiredMToHddNum         int64
	ExpiredMToHddBytes       int64
	ExpiredMToBlobstoreNum   int64
	ExpiredMToBlobstoreBytes int64
	ExpiredSkipNum           int64
	ExpiredMNum              int64
	ExpiredMBytes            int64

	ErrorDeleteNum       int64
	ErrorMNum            int64
	ErrorMToHddNum       int64
	ErrorMToBlobstoreNum int64
	ErrorReadDirNum      int64
}

// ----------------------------------
// lcnode <-> meta

type ScanDentry struct {
	ParentId       uint64     `json:"pid"`            // FileID value of the parent inode.
	Inode          uint64     `json:"inode"`          // FileID value of the current inode.
	Name           string     `json:"name"`           // Name of the current dentry.
	Path           string     `json:"path"`           // Path of the current dentry.
	Type           uint32     `json:"type"`           // Type of the current dentry.
	Op             string     `json:"op"`             // to delete or migrate
	Size           uint64     `json:"size"`           // for migrate: size of the current dentry
	StorageClass   uint32     `json:"sc"`             // for migrate: storage class of the current dentry
	SrcPoolId      uint8      `json:"srcPoolId"`      // for migrate: pool id of the current dentry
	DstPoolId      uint8      `json:"dstPoolId"`      // for migrate: pool id of the destination dentry
	LeaseExpire    uint64     `json:"leaseExpire"`    // for migrate: used to determine whether a file is modified
	HasMek         bool       `json:"mek"`            // for migrate: if HasMek, call DeleteMigrationExtentKey instead of migrating
	HasInodeInfo   bool       `json:"hasInodeInfo"`   // indicates whether inode info was successfully retrieved
	InodeInfo      *InodeInfo `json:"inodeInfo"`      // inode info of the current dentry
	DelayDelMinute uint64     `json:"delayDelMinute"` // delay delete in minutes after migration
}

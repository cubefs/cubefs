// Copyright 2018 The Chubao Authors.
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
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

//master
type LcConfiguration struct {
	VolName string
	Rules   []*Rule
}

func (lcConf *LcConfiguration) GenRuleTasks() []*RuleTask {
	tasks := make([]*RuleTask, 0)
	for _, r := range lcConf.Rules {
		var err error
		var encoded []byte
		if encoded, err = json.Marshal(r); err != nil {
			continue
		}
		var idSlice = []byte(lcConf.VolName)
		idSlice = append(idSlice, encoded...)

		task := &RuleTask{
			Id:      fmt.Sprintf("%x", md5.Sum(idSlice)),
			VolName: lcConf.VolName,
			Rule:    r,
		}
		tasks = append(tasks, task)
	}
	return tasks
}

func (lcConf *LcConfiguration) GenEnabledRuleTasks() []*RuleTask {
	tasks := make([]*RuleTask, 0)
	for _, r := range lcConf.Rules {
		if r.Status != RuleEnabled {
			log.LogDebugf("GenEnabledRuleTasks: skip disabled rule(%v) in volume(%v)", r.ID, lcConf.VolName)
			continue
		}
		var err error
		var encoded []byte
		if encoded, err = json.Marshal(r); err != nil {
			continue
		}
		var idSlice = []byte(lcConf.VolName)
		idSlice = append(idSlice, encoded...)

		task := &RuleTask{
			Id:      fmt.Sprintf("%x", md5.Sum(idSlice)),
			VolName: lcConf.VolName,
			Rule:    r,
		}
		tasks = append(tasks, task)
		log.LogDebugf("GenEnabledRuleTasks: RuleTask(%v) generated from rule(%v) in volume(%v)", *task, r.ID, lcConf.VolName)
	}
	return tasks
}

//lcnode

type ScanDentry struct {
	DelInode bool   `json:"delino"` //if Type is file, and DelInode is true, then Inode and Dentry(ParentId, Name) is to be deleted
	ParentId uint64 `json:"pid"`    // FileID value of the parent inode.
	Name     string `json:"name"`   // Name of the current dentry.
	Inode    uint64 `json:"inode"`  // FileID value of the current inode.
	Type     uint32 `json:"type"`
}

//meta

type InodeExpireCondition struct {
	ExpirationInfo        *Expiration
	ObjectSizeGreaterThan int64 `json:"sizegt"`
	ObjectSizeLessThan    int64 `json:"sizelt"`
	Tags                  []*TagConfig
}

// BatchInodeGetExpirationRequest defines the request to get the inode in batch.
type BatchInodeGetExpirationRequest struct {
	VolName     string                `json:"vol"`
	PartitionID uint64                `json:"pid"`
	Dentries    []*ScanDentry         `json:"dentries"`
	Cond        *InodeExpireCondition `json:"cond"`
}

type ExpireInfo struct {
	Dentry  *ScanDentry `json:"dentry"`
	Expired bool        `json:"expired"`
}

// BatchInodeGetExpirationResponse defines the response to the request of getting the inode in batch.
type BatchInodeGetExpirationResponse struct {
	ExpirationResults []*ExpireInfo `json:"rsts"`
}

//rules
var (
	RuleEnabled  string = "Enabled"
	RuleDisabled string = "Disabled"
)

type RuleTask struct {
	Id      string
	VolName string
	Rule    *Rule
}

type RuleTaskRequest struct {
	MasterAddr string
	RoutineID  int64
	Task       *RuleTask
}

type RuleTaskResponse struct {
	TotalInodeScannedNum          int64
	FileScannedNum                int64
	DirScannedNum                 int64
	ExpiredNum                    int64
	ErrorSkippedNum               int64
	AbortedIncompleteMultipartNum int64
	StartTime                     *time.Time
	EndTime                       *time.Time
	Status                        uint8
	Result                        string
}

type AbortIncompleteMultipartUpload struct {
	DaysAfterInitiation int
}

type Expiration struct {
	Date *time.Time
	Days int
}

type AndOpr struct {
	ObjectSizeGreaterThan int64
	ObjectSizeLessThan    int64
	Prefix                string
	Tags                  []*TagConfig
}

type TagConfig struct {
	Key   string
	Value string
}

type FilterConfig struct {
	And                   *AndOpr
	ObjectSizeGreaterThan int64
	ObjectSizeLessThan    int64
	Prefix                string
	Tag                   *TagConfig
}

type Rule struct {
	AbortMultipartUpload *AbortIncompleteMultipartUpload
	Expire               *Expiration
	Filter               *FilterConfig
	ID                   string
	Prefix               string
	Status               string
}

type SetBucketLifecycleRequest struct {
	VolName string
	Rules   []*Rule
}

type ScanFilter struct {
	ExpireCond *InodeExpireCondition
	Prefix     string
}

type AbortIncompleteMultiPartFilter struct {
	DaysAfterInitiation int
	Prefix              string
}

func (r *Rule) GetAbortIncompleteMultiPartFilter() *AbortIncompleteMultiPartFilter {
	if valid, _ := r.Validate(); !valid {
		return nil
	}
	if r.AbortMultipartUpload != nil {
		if r.AbortMultipartUpload.DaysAfterInitiation > 0 {
			return &AbortIncompleteMultiPartFilter{
				DaysAfterInitiation: r.AbortMultipartUpload.DaysAfterInitiation,
			}
		}
	}
	return nil
}

func (r *Rule) GetScanFilter() (filter *ScanFilter, abortFilter *AbortIncompleteMultiPartFilter) {
	if valid, _ := r.Validate(); !valid {
		return nil, nil
	}
	filter = &ScanFilter{
		ExpireCond: &InodeExpireCondition{
			ExpirationInfo: r.Expire,
			Tags:           make([]*TagConfig, 0),
		},
	}

	if r.Filter == nil {
		filter.Prefix = r.Prefix
	} else {
		if r.Prefix != "" {
			filter.Prefix = r.Prefix
		} else if r.Filter.Prefix != "" {
			filter.Prefix = r.Filter.Prefix
		} else {
			filter.Prefix = r.Filter.And.Prefix
		}

		if r.Filter.Tag != nil {
			filter.ExpireCond.Tags = append(filter.ExpireCond.Tags, r.Filter.Tag)
		} else {
			if r.Filter.And != nil {
				filter.ExpireCond.Tags = r.Filter.And.Tags
			}

		}

		if r.Filter.ObjectSizeGreaterThan > 0 {
			filter.ExpireCond.ObjectSizeGreaterThan = r.Filter.ObjectSizeGreaterThan
		} else {
			if r.Filter.And != nil {
				filter.ExpireCond.ObjectSizeGreaterThan = r.Filter.And.ObjectSizeGreaterThan
			}

		}

		if r.Filter.ObjectSizeLessThan > 0 {
			filter.ExpireCond.ObjectSizeLessThan = r.Filter.ObjectSizeLessThan
		} else {
			if r.Filter.And != nil {
				filter.ExpireCond.ObjectSizeLessThan = r.Filter.And.ObjectSizeLessThan
			}

		}
	}

	if r.AbortMultipartUpload != nil {
		if r.AbortMultipartUpload.DaysAfterInitiation > 0 {
			abortFilter = &AbortIncompleteMultiPartFilter{
				DaysAfterInitiation: r.AbortMultipartUpload.DaysAfterInitiation,
				Prefix:              filter.Prefix,
			}
		}
	}

	return filter, abortFilter
}

func (r *Rule) Validate() (valid bool, err error) {
	//https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html
	if r.Expire == nil {
		return false, errors.New("Expiration can not be empty!")
	} else {
		if r.Expire.Date != nil && r.Expire.Days > 0 || r.Expire.Days < 0 {
			return false, errors.New("Expiration days or date error!")
		}
	}

	if r.Filter != nil {
		if r.Prefix != "" && r.Filter.Prefix != "" {
			return false, errors.New("Prefix error!")
		}

		if r.Filter.ObjectSizeLessThan > 0 && r.Filter.ObjectSizeGreaterThan > 0 {
			if r.Filter.ObjectSizeLessThan < r.Filter.ObjectSizeGreaterThan {
				return false, errors.New("ObjectSize filter error!")
			}
		}

		if r.Filter.And != nil {
			//"When using more than one filter, you must wrap the filters in an <And> element. "
			if r.Prefix != "" || r.Filter.Prefix != "" || r.Filter.Tag != nil || r.Filter.ObjectSizeGreaterThan > 0 || r.Filter.ObjectSizeLessThan > 0 {
				return false, errors.New("And operator conflicts!")
			}

			if r.Filter.And.ObjectSizeLessThan > 0 && r.Filter.And.ObjectSizeGreaterThan > 0 {
				if r.Filter.And.ObjectSizeLessThan < r.Filter.And.ObjectSizeGreaterThan {
					return false, errors.New("ObjectSize filter error!")
				}
			}

		}

	}

	return true, nil
}

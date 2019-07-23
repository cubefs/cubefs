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

package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"strconv"
	"time"
)

func (c *Cluster) startCheckLoadMetaPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkLoadMetaPartitions()
				}
			}
			time.Sleep(2 * time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
}

func (c *Cluster) checkLoadMetaPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDiskRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDiskRecoveryProgress occurred panic")
		}
	}()
	vols := c.allVols()
	for _, vol := range vols {
		mps := vol.cloneMetaPartitionMap()
		for _, mp := range mps {
			c.doLoadMetaPartition(mp)
		}
	}
}

func (mp *MetaPartition) checkSnapshot(clusterID string) {
	if len(mp.LoadResponse) == 0 {
		return
	}
	if !mp.doCompare() {
		return
	}
	if !mp.isSameApplyID() {
		return
	}
	mp.checkInodeSign(clusterID)
	mp.checkDentrySign(clusterID)
}

func (mp *MetaPartition) doCompare() bool {
	for _, lr := range mp.LoadResponse {
		if !lr.DoCompare {
			return false
		}
	}
	return true
}

func (mp *MetaPartition) isSameApplyID() bool {
	rst := true
	applyID := mp.LoadResponse[0].ApplyID
	for _, loadResponse := range mp.LoadResponse {
		if applyID != loadResponse.ApplyID {
			rst = false
		}
	}
	return rst
}

func (mp *MetaPartition) checkInodeSign(clusterID string) {
	isEqual := true
	inodeSign := mp.LoadResponse[0].InodeSign
	for _, loadResponse := range mp.LoadResponse {
		if inodeSign != loadResponse.InodeSign {
			isEqual = false
		}
	}

	if !isEqual {
		msg := fmt.Sprintf("inode sign is not equal,vol[%v],mpID[%v],", mp.volName, mp.PartitionID)
		for _, lr := range mp.LoadResponse {
			signStr := strconv.FormatUint(uint64(lr.InodeSign), 10)
			applyIDStr := strconv.FormatUint(uint64(lr.ApplyID), 10)
			msg = msg + lr.Addr + " applyId[" + applyIDStr + "] inodeSign[" + signStr + "],"
		}
		Warn(clusterID, msg)
	}
}

func (mp *MetaPartition) checkDentrySign(clusterID string) {
	isEqual := true
	dentrySign := mp.LoadResponse[0].DentrySign
	for _, loadResponse := range mp.LoadResponse {
		if dentrySign != loadResponse.DentrySign {
			isEqual = false
		}
	}

	if !isEqual {
		msg := fmt.Sprintf("dentry sign is not equal,vol[%v],mpID[%v],", mp.volName, mp.PartitionID)
		for _, lr := range mp.LoadResponse {
			signStr := strconv.FormatUint(uint64(lr.DentrySign), 10)
			applyIDStr := strconv.FormatUint(uint64(lr.ApplyID), 10)
			msg = msg + lr.Addr + " applyId[" + applyIDStr + "] dentrySign[" + signStr + "],"
		}
		Warn(clusterID, msg)
	}
}

// Copyright 2018 The CubeFS Authors.
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
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func (c *Cluster) scheduleToLoadMetaPartitions(ctx context.Context) {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkLoadMetaPartitions(ctx)
				}
			}
			time.Sleep(2 * time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
}

func (c *Cluster) checkLoadMetaPartitions(ctx context.Context) {
	span := proto.SpanFromContext(ctx)
	defer func() {
		if r := recover(); r != nil {
			span.Warnf("checkDiskRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(ctx, fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDiskRecoveryProgress occurred panic")
		}
	}()
	vols := c.allVols()
	for _, vol := range vols {
		mps := vol.cloneMetaPartitionMap()
		for _, mp := range mps {
			c.doLoadMetaPartition(ctx, mp)
		}
	}
}

func (mp *MetaPartition) checkSnapshot(ctx context.Context, c *Cluster) {
	if len(mp.LoadResponse) == 0 {
		return
	}
	if !mp.doCompare() {
		return
	}
	if !mp.isSameApplyID() {
		return
	}
	ckInode := mp.checkInodeCount(ctx, c)
	ckDentry := mp.checkDentryCount(ctx, c)
	if ckInode && ckDentry {
		mp.EqualCheckPass = true
	} else {
		mp.EqualCheckPass = false
	}
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

func (mp *MetaPartition) checkInodeCount(ctx context.Context, c *Cluster) (isEqual bool) {
	isEqual = true
	maxInode := mp.LoadResponse[0].MaxInode
	maxInodeCount := mp.LoadResponse[0].InodeCount
	inodeEqual := true
	maxInodeEqual := true
	if mp.IsRecover {
		return
	}
	for _, loadResponse := range mp.LoadResponse {
		diff := math.Abs(float64(loadResponse.MaxInode) - float64(maxInode))
		if diff > defaultRangeOfCountDifferencesAllowed {
			isEqual = false
			inodeEqual = false
			break
		}
		diff = math.Abs(float64(loadResponse.InodeCount) - float64(maxInodeCount))
		if diff > defaultRangeOfCountDifferencesAllowed {
			isEqual = false
			maxInodeEqual = false
			break
		}
	}
	if !isEqual {
		msg := fmt.Sprintf("inode count is not equal,vol[%v],mpID[%v],", mp.volName, mp.PartitionID)
		for _, lr := range mp.LoadResponse {
			lrMsg := fmt.Sprintf(msg+lr.Addr, "applyId[%d],committedId[%d],maxInode[%d],InodeCnt[%d]", lr.ApplyID, lr.CommittedID, lr.MaxInode, lr.InodeCount)
			Warn(ctx, c.Name, lrMsg)
		}
		if !maxInodeEqual {
			c.inodeCountNotEqualMP.Store(mp.PartitionID, mp)
		}
		if !inodeEqual {
			c.maxInodeNotEqualMP.Store(mp.PartitionID, mp)
		}

	} else {
		if _, ok := c.inodeCountNotEqualMP.Load(mp.PartitionID); ok {
			c.inodeCountNotEqualMP.Delete(mp.PartitionID)
		}
		if _, ok := c.maxInodeNotEqualMP.Load(mp.PartitionID); ok {
			c.maxInodeNotEqualMP.Delete(mp.PartitionID)
		}
	}
	return
}

func (mp *MetaPartition) checkDentryCount(ctx context.Context, c *Cluster) (isEqual bool) {
	isEqual = true
	if mp.IsRecover {
		return
	}
	dentryCount := mp.LoadResponse[0].DentryCount
	for _, loadResponse := range mp.LoadResponse {
		diff := math.Abs(float64(loadResponse.DentryCount) - float64(dentryCount))
		if diff > defaultRangeOfCountDifferencesAllowed {
			isEqual = false
		}
	}

	if !isEqual {
		msg := fmt.Sprintf("dentry count is not equal,vol[%v],mpID[%v],", mp.volName, mp.PartitionID)
		for _, lr := range mp.LoadResponse {
			lrMsg := fmt.Sprintf(msg+lr.Addr, "applyId[%d],committedId[%d],dentryCount[%d]", lr.ApplyID, lr.CommittedID, lr.DentryCount)
			Warn(ctx, c.Name, lrMsg)
		}
		c.dentryCountNotEqualMP.Store(mp.PartitionID, mp)
	} else {
		if _, ok := c.dentryCountNotEqualMP.Load(mp.PartitionID); ok {
			c.dentryCountNotEqualMP.Delete(mp.PartitionID)
		}
	}
	return
}

func (c *Cluster) scheduleToCheckMetaPartitionRecoveryProgress(ctx context.Context) {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkMetaPartitionRecoveryProgress(ctx)
				}
			}
			time.Sleep(time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
}

func (c *Cluster) checkMetaPartitionRecoveryProgress(ctx context.Context) {
	span := proto.SpanFromContext(ctx)
	defer func() {
		if r := recover(); r != nil {
			span.Warnf("checkMetaPartitionRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(ctx, fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMetaPartitionRecoveryProgress occurred panic")
		}
	}()

	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	c.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badMetaPartitionIds := value.([]uint64)
		newBadMpIds := make([]uint64, 0)
		for _, partitionID := range badMetaPartitionIds {
			partition, err := c.getMetaPartitionByID(partitionID)
			if err != nil {
				Warn(ctx, c.Name, fmt.Sprintf("checkMetaPartitionRecoveryProgress clusterID[%v], partitionID[%v] is not exist", c.Name, partitionID))
				continue
			}

			vol, err := c.getVol(partition.volName)
			if err != nil {
				Warn(ctx, c.Name, fmt.Sprintf("checkMetaPartitionRecoveryProgress clusterID[%v],vol[%v] partitionID[%v]is not exist",
					c.Name, partition.volName, partitionID))
				continue
			}

			if len(partition.Replicas) == 0 || len(partition.Replicas) < int(vol.mpReplicaNum) {
				newBadMpIds = append(newBadMpIds, partitionID)
				continue
			}

			if partition.getMinusOfMaxInodeID() < defaultMinusOfMaxInodeID {
				partition.IsRecover = false
				partition.RLock()
				c.syncUpdateMetaPartition(ctx, partition)
				partition.RUnlock()
				Warn(ctx, c.Name, fmt.Sprintf("checkMetaPartitionRecoveryProgress clusterID[%v],vol[%v] partitionID[%v] has recovered success",
					c.Name, partition.volName, partitionID))
			} else {
				newBadMpIds = append(newBadMpIds, partitionID)
			}
		}

		if len(newBadMpIds) == 0 {
			Warn(ctx, c.Name, fmt.Sprintf("checkMetaPartitionRecoveryProgress clusterID[%v],node[%v] has recovered success", c.Name, key))
			c.BadMetaPartitionIds.Delete(key)
		} else {
			c.BadMetaPartitionIds.Store(key, newBadMpIds)
			span.Infof("checkMetaPartitionRecoveryProgress BadMetaPartitionIds there is still (%d) mp in recover, addr (%s)", len(newBadMpIds), key)
		}

		return true
	})
}

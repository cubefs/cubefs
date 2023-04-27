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
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (c *Cluster) scheduleToLoadMetaPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkLoadMetaPartitions()
				}
			}
			time.Sleep(60 * time.Second * defaultIntervalToCheckDataPartition)
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
	mp.checkInodeCount(clusterID)
	mp.checkDentryCount(clusterID)
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

func (mp *MetaPartition) checkInodeCount(clusterID string) {
	isEqual := true
	inodeCount := mp.LoadResponse[0].InodeCount
	for _, loadResponse := range mp.LoadResponse {
		diff := math.Abs(float64(loadResponse.InodeCount) - float64(inodeCount))
		if diff > defaultRangeOfCountDifferencesAllowed {
			isEqual = false
		}
	}

	if !isEqual {
		msg := fmt.Sprintf("inode count is not equal,vol[%v],mpID[%v],", mp.volName, mp.PartitionID)
		for _, lr := range mp.LoadResponse {
			inodeCountStr := strconv.FormatUint(lr.InodeCount, 10)
			applyIDStr := strconv.FormatUint(uint64(lr.ApplyID), 10)
			msg = msg + lr.Addr + " applyId[" + applyIDStr + "] inodeCount[" + inodeCountStr + "],"
		}
		Warn(clusterID, msg)
	}
}

func (mp *MetaPartition) checkDentryCount(clusterID string) {
	isEqual := true
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
			dentryCountStr := strconv.FormatUint(lr.DentryCount, 10)
			applyIDStr := strconv.FormatUint(uint64(lr.ApplyID), 10)
			msg = msg + lr.Addr + " applyId[" + applyIDStr + "] dentryCount[" + dentryCountStr + "],"
		}
		Warn(clusterID, msg)
	}
}

func (c *Cluster) scheduleToCheckMetaPartitionRecoveryProgress() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkMetaPartitionRecoveryProgress()
					c.checkMigratedMetaPartitionRecoveryProgress()
				}
			}
			time.Sleep(3 * time.Minute)
		}
	}()
}

func (c *Cluster) checkMetaPartitionRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMetaPartitionRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMetaPartitionRecoveryProgress occurred panic")
		}
	}()

	var normalReplicaCount int
	c.checkFulfillMetaReplica()
	unrecoverMpIDs := make(map[uint64]int64, 0)
	c.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		partitionID := value.(uint64)
		partition, err := c.getMetaPartitionByID(partitionID)
		if err != nil {
			return true
		}
		vol, err := c.getVol(partition.volName)
		if err != nil {
			return true
		}
		if len(partition.Replicas) == 0 {
			return true
		}
		_, normalReplicaCount = partition.getMinusOfMaxInodeID()
		if int(vol.mpReplicaNum) <= normalReplicaCount && partition.allReplicaHasRecovered() {
			partition.RLock()
			partition.IsRecover = false
			c.syncUpdateMetaPartition(partition)
			partition.RUnlock()
			c.BadMetaPartitionIds.Delete(key)
		} else {
			if time.Now().Unix()-partition.modifyTime > defaultUnrecoverableDuration {
				unrecoverMpIDs[partitionID] = partition.modifyTime
			}
		}

		return true
	})
	if len(unrecoverMpIDs) != 0 {
		Warn(c.Name, fmt.Sprintf("action[checkMetaPartitionRecoveryProgress] clusterID[%v],[%v] has migrated more than 24 hours,still not recovered,ids[%v]", c.Name, len(unrecoverMpIDs), unrecoverMpIDs))
	}
}

// Add replica for the partition whose replica number is less than replicaNum
func (c *Cluster) checkFulfillMetaReplica() {
	c.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		partitionID := value.(uint64)
		badAddr := getAddrFromDecommissionMetaPartitionKey(key.(string))
		isPushBackToBadIDs := c.fulfillMetaReplica(partitionID, badAddr)
		if !isPushBackToBadIDs {
			c.BadMetaPartitionIds.Delete(key)
		}
		//Todo: write BadMetaPartitionIds to raft log
		return true
	})

}

func (c *Cluster) fulfillMetaReplica(partitionID uint64, badAddr string) (isPushBackToBadIDs bool) {
	var (
		newPeer   proto.Peer
		partition *MetaPartition
		vol       *Vol
		err       error
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[fulfillMetaReplica], clusterID[%v], partitionID[%v], err[%v] ", c.Name, partitionID, err)
		}
	}()
	isPushBackToBadIDs = true
	if partition, err = c.getMetaPartitionByID(partitionID); err != nil {
		return
	}
	partition.offlineMutex.Lock()
	defer partition.offlineMutex.Unlock()

	//len(partition.Hosts) >= int(partition.ReplicaNum) occurs when decommission failed, this need to be decommission again, do not fulfill replica
	if len(partition.Replicas) >= int(partition.ReplicaNum) || len(partition.Hosts) >= int(partition.ReplicaNum) {
		return
	}
	if _, err = partition.getMetaReplicaLeader(); err != nil {
		return
	}
	if newPeer, err = c.chooseTargetMetaPartitionHost(badAddr, partition); err != nil {
		return
	}
	if vol, err = c.getVol(partition.volName); err != nil {
		return
	}
	if err = c.addMetaReplica(partition, newPeer.Addr, vol.DefaultStoreMode); err != nil {
		return
	}
	newPanicHost := make([]string, 0)
	for _, h := range partition.PanicHosts {
		if h == badAddr {
			continue
		}
		newPanicHost = append(newPanicHost, h)
	}
	partition.Lock()
	partition.PanicHosts = newPanicHost
	c.syncUpdateMetaPartition(partition)
	partition.Unlock()
	//if len(replica) >= replicaNum, keep badDiskAddr to check recover later
	//if len(replica) <  replicaNum, discard badDiskAddr to avoid add replica by the same badDiskAddr twice.
	isPushBackToBadIDs = len(partition.Replicas) >= int(partition.ReplicaNum)
	return
}

func (c *Cluster) scheduleToCheckAutoMetaPartitionCreation() {
	go func() {
		// check volumes after switching leader four minutes
		time.Sleep(4 * time.Minute)
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				vols := c.copyVols()
				for _, vol := range vols {
					if !c.isLeader.Load() {
						break
					}
					vol.checkAutoMetaPartitionCreation(c)
				}
			}
			time.Sleep(time.Second * defaultIntervalToWaitMetaPartitionElectionLeader)
		}
	}()
}

func (vol *Vol) checkAutoMetaPartitionCreation(c *Cluster) {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkAutoMetaPartitionCreation occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkAutoMetaPartitionCreation occurred panic")
		}
	}()
	if vol.status() == proto.VolStMarkDelete {
		return
	}
	if vol.status() == proto.VolStNormal && !c.DisableAutoAllocate {
		vol.autoCreateMetaPartitions(c)
	}
}

func (vol *Vol) autoCreateMetaPartitions(c *Cluster) {
	writableMpCount := int(vol.getWritableMpCount())
	if writableMpCount < vol.MinWritableMPNum {
		maxPartitionID := vol.maxPartitionID()
		mp, err := vol.metaPartition(maxPartitionID)
		if err != nil {
			log.LogErrorf("action[autoCreateMetaPartitions],cluster[%v],vol[%v],err[%v]", c.Name, vol.Name, err)
			return
		}
		// wait for leader ready
		_, err = mp.getMetaReplicaLeader()
		if err != nil {
			log.LogWarnf("action[autoCreateMetaPartitions],cluster[%v],vol[%v],err[%v],create it later", c.Name, vol.Name, err)
			return
		}
		var nextStart uint64
		if mp.MaxInodeID <= 0 {
			nextStart = mp.Start + proto.DefaultMetaPartitionInodeIDStep
		} else {
			nextStart = mp.MaxInodeID + proto.DefaultMetaPartitionInodeIDStep
		}
		log.LogDebugf("action[autoCreateMetaPartitions],cluster[%v],vol[%v],writableMPCount[%v] less than %v, do split.",
			c.Name, vol.Name, writableMpCount, vol.MinWritableMPNum)
		if err = vol.splitMetaPartition(c, mp, nextStart); err != nil {
			msg := fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits failed,err[%v]",
				c.Name, vol.Name, mp.PartitionID, err)
			Warn(c.Name, msg)
		}
	}
}

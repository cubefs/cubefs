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
	"strings"
	"sync"
	"time"
)

func (c *Cluster) scheduleToCheckDiskRecoveryProgress() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkDiskRecoveryProgress()
					c.checkMigratedDataPartitionsRecoveryProgress()
				}
			}
			time.Sleep(time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
}

func (c *Cluster) checkDiskRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDiskRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDiskRecoveryProgress occurred panic")
		}
	}()
	c.checkFulfillDataReplica()
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		newBadDpIds := make([]uint64, 0)
		for _, partitionID := range badDataPartitionIds {
			partition, err := c.getDataPartitionByID(partitionID)
			if err != nil {
				continue
			}
			vol, err := c.getVol(partition.VolName)
			if err != nil {
				continue
			}
			if len(partition.Replicas) == 0 {
				continue
			}
			if partition.isDataCatchUp() && len(partition.Replicas) >= int(vol.dpReplicaNum) {
				partition.isRecover = false
				partition.RLock()
				c.syncUpdateDataPartition(partition)
				partition.RUnlock()
				Warn(c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress] clusterID[%v],partitionID[%v] has recovered success", c.Name, partitionID))
			} else {
				newBadDpIds = append(newBadDpIds, partitionID)
			}
		}

		if len(newBadDpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress] clusterID[%v],node:disk[%v] has recovered success", c.Name, key))
			c.BadDataPartitionIds.Delete(key)
		} else {
			c.BadDataPartitionIds.Store(key, newBadDpIds)
		}

		return true
	})

}

// Add replica for the partition whose replica number is less than replicaNum
func (c *Cluster) checkFulfillDataReplica() {
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		//badDiskAddr: '127.0.0.1:17210:/data1'
		badDiskAddr := strings.Split(key.(string), ":")
		if len(badDiskAddr) < 2 {
			return true
		}
		newBadParitionIds := make([]uint64, 0)
		for _, partitionID := range badDataPartitionIds {
			var err error
			var partition *DataPartition
			if partition, err = c.getDataPartitionByID(partitionID); err != nil {
				newBadParitionIds = append(newBadParitionIds, partitionID)
				continue
			}
			if len(partition.Replicas) >= int(partition.ReplicaNum) || len(partition.Hosts) >= int(partition.ReplicaNum) {
				newBadParitionIds = append(newBadParitionIds, partitionID)
				continue
			}
			//Not until the learners promote strategy is enhanced to guarantee peers consistency, can we add more learners at the same time.
			if len(partition.Learners) > 0 {
				newBadParitionIds = append(newBadParitionIds, partitionID)
				continue
			}
			if err = c.fulfillDataReplicaByLearner(partition, badDiskAddr[0]+":"+badDiskAddr[1], partitionID); err != nil {
				log.LogWarnf(fmt.Sprintf("action[checkFulfillDataReplica], clusterID[%v], partitionID[%v], err[%v] ", c.Name, partitionID, err))
				newBadParitionIds = append(newBadParitionIds, partitionID)
				continue
			}
			//only if the len(replica + learner) equals to replicaNum, will we keep the badPartitionID to check the recover progress of this partition later
			//if len(replica + learner) is less than replicaNum, we should discard this badDiskAddr to avoid add raft learner twice.
			if len(partition.Replicas) < int(partition.ReplicaNum) {
				continue
			}
			newBadParitionIds = append(newBadParitionIds, partitionID)
		}
		c.BadDataPartitionIds.Store(key, newBadParitionIds)
		return true
	})

}

//Raft instance will not start until the data has been synchronized by simple repair-read consensus algorithm, and it spends a long time.
//The raft group can not come to an agreement with a leader, and the data partition will be unavailable.
//Introducing raft learner can solve the problem.
func (c *Cluster) fulfillDataReplicaByLearner(partition *DataPartition, badAddr string, partitionID uint64) (err error) {
	var (
		newAddr         string
		excludeNodeSets []uint64
	)
	excludeNodeSets = make([]uint64, 0)
	if leaderAddr := partition.getLeaderAddr(); leaderAddr == "" {
		err = fmt.Errorf("Action[fulfillDataReplicaByLearner], partitionID[%v], no leader", partitionID)
		return
	}

	if _, newAddr, err = getTargetAddressForDataPartitionDecommission(c, badAddr, partition, excludeNodeSets, "", false); err != nil {
		return
	}
	if err = c.addDataReplicaLearner(partition, newAddr, true, 90); err != nil {
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
	c.syncUpdateDataPartition(partition)
	partition.Unlock()
	return
}

func (c *Cluster) decommissionDisk(dataNode *DataNode, badDiskPath string, badPartitions []*DataPartition) (err error) {
	msg := fmt.Sprintf("action[decommissionDisk], Node[%v] OffLine,disk[%v]", dataNode.Addr, badDiskPath)
	log.LogWarn(msg)
	var wg sync.WaitGroup
	errChannel := make(chan error, len(badPartitions))
	defer func() {
		close(errChannel)
	}()
	for _, dp := range badPartitions {
		wg.Add(1)
		go func(dp *DataPartition) {
			defer wg.Done()
			if err1 := c.decommissionDataPartition(dataNode.Addr, dp, getTargetAddressForDataPartitionDecommission, diskOfflineErr, "", false); err1 != nil {
				errChannel <- err1
			}
		}(dp)
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		return
	default:
	}
	msg = fmt.Sprintf("action[decommissionDisk],clusterID[%v] Node[%v] disk[%v] OffLine success",
		c.Name, dataNode.Addr, badDiskPath)
	Warn(c.Name, msg)
	return
}

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
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

func (c *Cluster) scheduleToCheckDiskRecoveryProgress() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkDiskRecoveryProgress()
					c.checkEcDiskRecoveryProgress()
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
			replicaNum := vol.dpReplicaNum
			if vol.DPConvertMode == proto.IncreaseReplicaNum && vol.dpReplicaNum == maxQuorumVolDataPartitionReplicaNum {
				replicaNum = partition.ReplicaNum
				if replicaNum < defaultReplicaNum {
					replicaNum = defaultReplicaNum
				}
			}
			if partition.isDataCatchUp() && len(partition.Replicas) >= int(replicaNum) {
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
			isPushBackToBadIDs := c.fulfillDataReplica(partitionID, badDiskAddr[0]+":"+badDiskAddr[1])
			if isPushBackToBadIDs {
				newBadParitionIds = append(newBadParitionIds, partitionID)
			}
		}
		c.BadDataPartitionIds.Store(key, newBadParitionIds)
		return true
	})

}

//Raft instance will not start until the data has been synchronized by simple repair-read consensus algorithm, and it spends a long time.
//The raft group can not come to an agreement with a leader, and the data partition will be unavailable.
//Introducing raft learner can solve the problem.
func (c *Cluster) fulfillDataReplica(partitionID uint64, badAddr string) (isPushBackToBadIDs bool) {
	var (
		newAddr         string
		excludeNodeSets []uint64
		partition       *DataPartition
		err             error
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[fulfillDataReplica], clusterID[%v], partitionID[%v], err[%v] ", c.Name, partitionID, err)
		}
	}()
	excludeNodeSets = make([]uint64, 0)
	isPushBackToBadIDs = true
	if partition, err = c.getDataPartitionByID(partitionID); err != nil {
		return
	}
	partition.offlineMutex.Lock()
	defer partition.offlineMutex.Unlock()

	if len(partition.Replicas) >= int(partition.ReplicaNum) || len(partition.Hosts) >= int(partition.ReplicaNum) {
		return
	}
	//Not until the learners promote strategy is enhanced to guarantee peers consistency, can we add more learners at the same time.
	if len(partition.Learners) > 0 {
		return
	}
	if leaderAddr := partition.getLeaderAddr(); leaderAddr == "" {
		return
	}
	if _, newAddr, err = getTargetAddressForDataPartitionDecommission(c, badAddr, partition, excludeNodeSets, "", false); err != nil {
		return
	}
	//if there is only one live replica, add learner can avoid no leader
	if len(partition.Replicas) == 1 {
		if err = c.addDataReplicaLearner(partition, newAddr, true, 90); err != nil {
			return
		}
	} else {
		if err = c.addDataReplica(partition, newAddr, false); err != nil {
			return
		}
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
	//if len(replica) >= replicaNum, keep badDiskAddr to check recover later
	//if len(replica) <  replicaNum, discard badDiskAddr to avoid add replica by the same badDiskAddr twice.
	isPushBackToBadIDs = len(partition.Replicas) >= int(partition.ReplicaNum)
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
			if err1 := c.decommissionDataPartition(dataNode.Addr, dp, getTargetAddressForDataPartitionDecommission, diskOfflineErr, "", "", false); err1 != nil {
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

func (c *Cluster) checkDecommissionBadDiskDataPartitions(dataNode *DataNode, badDiskPath string) {
	msg := fmt.Sprintf("action[checkDecommissionBadDiskDataPartitions], Node[%v] OffLine,disk[%v]", dataNode.Addr, badDiskPath)
	log.LogWarn(msg)
	inRecoveringDps := make([]uint64, 0)
	toBeOfflineDpChan := make(chan *BadDiskDataPartition, diskErrDataPartitionOfflineBatchCount)
	defer close(toBeOfflineDpChan)
	go c.decommissionBadDiskDataPartitions(toBeOfflineDpChan, dataNode.Addr, badDiskPath)
	for {
		inRecoveringDps = c.checkIfDPIdIsInRecovering(inRecoveringDps)
		maxOfflineDpCount := cap(toBeOfflineDpChan) - len(toBeOfflineDpChan) - len(inRecoveringDps)
		if maxOfflineDpCount <= 0 {
			continue
		}
		badPartitions := dataNode.badPartitions(badDiskPath, c)
		if len(badPartitions) == 0 {
			break
		}
		offlineDpCount := 0
		for _, dp := range badPartitions {
			if offlineDpCount >= maxOfflineDpCount {
				break
			}
			toBeOfflineDpChan <- &BadDiskDataPartition{dp: dp, diskErrAddr: dataNode.Addr}
			inRecoveringDps = append(inRecoveringDps, dp.PartitionID)
			offlineDpCount++
		}
		if maxOfflineDpCount >= len(badPartitions) {
			break
		}
		time.Sleep(5 * time.Second * defaultIntervalToCheckDataPartition)
	}
	msg = fmt.Sprintf("action[checkDecommissionBadDiskDataPartitions],clusterID[%v] Node[%v] disk[%v] OffLine success",
		c.Name, dataNode.Addr, badDiskPath)
	Warn(c.Name, msg)
	return
}

func (c *Cluster) decommissionBadDiskDataPartitions(toBeOfflineDpChan <-chan *BadDiskDataPartition, nodeAddr, diskPath string) {
	var (
		badDp *BadDiskDataPartition
		err   error
		ok    bool
	)
	successDpIds := make([]uint64, 0)
	failedDpIds := make([]uint64, 0)
	defer func() {
		if len(successDpIds) != 0 || len(failedDpIds) != 0 {
			rstMsg := fmt.Sprintf("decommissionBadDiskDataPartitions node[%v] disk[%v], successDpIds[%v] failedDpIds[%v]",
				nodeAddr, diskPath, successDpIds, failedDpIds)
			Warn(c.Name, rstMsg)
		}
	}()
	for {
		select {
		case badDp, ok = <-toBeOfflineDpChan:
			if !ok {
				return
			}
			err = c.decommissionDataPartition(badDp.diskErrAddr, badDp.dp, getTargetAddressForDataPartitionDecommission, diskAutoOfflineErr, "", "", false)
			if err != nil {
				failedDpIds = append(failedDpIds, badDp.dp.PartitionID)
			} else {
				successDpIds = append(successDpIds, badDp.dp.PartitionID)
			}
		}
	}
}

func (c *Cluster) checkIfDPIdIsInRecovering(dpIds []uint64) (inRecoveringDps []uint64) {
	inRecoveringDps = make([]uint64, 0)
	if len(dpIds) == 0 {
		return
	}
	allInRecoveringDPIds := make(map[uint64]bool, 0)
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		if badDataPartitionIds, ok := value.([]uint64); ok {
			for _, dataPartitionId := range badDataPartitionIds {
				allInRecoveringDPIds[dataPartitionId] = true
			}
		}
		return true
	})
	for _, recoveringDp := range dpIds {
		if allInRecoveringDPIds[recoveringDp] {
			inRecoveringDps = append(inRecoveringDps, recoveringDp)
		}
	}
	return
}

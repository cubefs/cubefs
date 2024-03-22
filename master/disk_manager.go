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
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func (c *Cluster) scheduleToCheckDiskRecoveryProgress(ctx context.Context) {
	go func() {
		rCtx := proto.RoundContext("check-disk-recover")
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkDiskRecoveryProgress(rCtx())
				}
			}
			time.Sleep(time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
}

func (c *Cluster) checkDiskRecoveryProgress(ctx context.Context) {
	span := proto.SpanFromContext(ctx)
	defer func() {
		if r := recover(); r != nil {
			span.Warnf("checkDiskRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(ctx, fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDiskRecoveryProgress occurred panic")
		}
	}()

	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		newBadDpIds := make([]uint64, 0)
		for _, partitionID := range badDataPartitionIds {
			partition, err := c.getDataPartitionByID(partitionID)
			if err != nil {
				Warn(ctx, c.Name, fmt.Sprintf("checkDiskRecoveryProgress clusterID[%v],partitionID[%v] is not exist", c.Name, partitionID))
				continue
			}
			// do not update status if paused
			if partition.IsDecommissionPaused() {
				continue
			}
			_, err = c.getVol(partition.VolName)
			if err != nil {
				Warn(ctx, c.Name, fmt.Sprintf("checkDiskRecoveryProgress clusterID[%v],partitionID[%v] vol(%s) is not exist",
					c.Name, partitionID, partition.VolName))
				continue
			}
			span.Infof("action[checkDiskRecoveryProgress] dp %v isSpec %v replicas %v conf replicas num %v",
				partition.PartitionID, partition.isSpecialReplicaCnt(), len(partition.Replicas), int(partition.ReplicaNum))
			if len(partition.Replicas) == 0 {
				partition.SetDecommissionStatus(DecommissionSuccess)
				span.Warnf("action[checkDiskRecoveryProgress] dp %v maybe deleted", partition.PartitionID)
				continue
			}
			//if len(partition.Replicas) == 0 ||
			//	(!partition.isSpecialReplicaCnt() && len(partition.Replicas) < int(partition.ReplicaNum)) ||
			//	(partition.isSpecialReplicaCnt() && len(partition.Replicas) > int(partition.ReplicaNum)) {
			//	newBadDpIds = append(newBadDpIds, partitionID)
			//	log.Infof("action[checkDiskRecoveryProgress] dp %v newBadDpIds [%v] replics %v conf replics num %v",
			//		partition.PartitionID, newBadDpIds, len(partition.Replicas), int(partition.ReplicaNum))
			//	continue
			//}

			newReplica, _ := partition.getReplica(ctx, partition.DecommissionDstAddr)
			if newReplica == nil {
				span.Warnf("action[checkDiskRecoveryProgress] dp %v cannot find replica %v", partition.PartitionID,
					partition.DecommissionDstAddr)
				partition.DecommissionNeedRollback = true
				partition.SetDecommissionStatus(DecommissionFail)
				continue
			}
			if newReplica.isRepairing() {
				if !partition.isSpecialReplicaCnt() &&
					time.Since(partition.RecoverStartTime) > c.GetDecommissionDataPartitionRecoverTimeOut() {
					partition.DecommissionNeedRollback = true
					partition.SetDecommissionStatus(DecommissionFail)
					Warn(ctx, c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress]clusterID[%v],partitionID[%v]  recovered timeout %s",
						c.Name, partitionID, time.Since(partition.RecoverStartTime).String()))
				} else {
					newBadDpIds = append(newBadDpIds, partitionID)
				}
			} else {
				if partition.isSpecialReplicaCnt() {
					continue // change dp decommission status in decommission function
				}
				// do not add to BadDataPartitionIds
				if newReplica.isUnavailable() {
					partition.DecommissionNeedRollback = true
					partition.SetDecommissionStatus(DecommissionFail)
					Warn(ctx, c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress]clusterID[%v],partitionID[%v] has recovered failed", c.Name, partitionID))
				} else {
					partition.SetDecommissionStatus(DecommissionSuccess) // can be readonly or readwrite
					Warn(ctx, c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress]clusterID[%v],partitionID[%v] has recovered success", c.Name, partitionID))
				}
				partition.RLock()
				c.syncUpdateDataPartition(ctx, partition)
				partition.RUnlock()
			}
		}

		if len(newBadDpIds) == 0 {
			Warn(ctx, c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress]clusterID[%v],node:disk[%v] has recovered success", c.Name, key))
			c.BadDataPartitionIds.Delete(key)
		} else {
			c.BadDataPartitionIds.Store(key, newBadDpIds)
			span.Infof("action[checkDiskRecoveryProgress]BadDataPartitionIds key(%s) still have (%d) dp in recover", key, len(newBadDpIds))
		}

		return true
	})
}

func (c *Cluster) addAndSyncDecommissionedDisk(ctx context.Context, dataNode *DataNode, diskPath string) (err error) {
	span := proto.SpanFromContext(ctx)
	if exist := dataNode.addDecommissionedDisk(ctx, diskPath); exist {
		return
	}
	if err = c.syncUpdateDataNode(ctx, dataNode); err != nil {
		dataNode.deleteDecommissionedDisk(ctx, diskPath)
		return
	}
	span.Infof("action[addAndSyncDecommissionedDisk] finish, remaining decommissioned disks[%v], dataNode[%v]", dataNode.getDecommissionedDisks(), dataNode.Addr)
	return
}

func (c *Cluster) deleteAndSyncDecommissionedDisk(ctx context.Context, dataNode *DataNode, diskPath string) (err error) {
	span := proto.SpanFromContext(ctx)
	if exist := dataNode.deleteDecommissionedDisk(ctx, diskPath); !exist {
		return
	}
	if err = c.syncUpdateDataNode(ctx, dataNode); err != nil {
		dataNode.addDecommissionedDisk(ctx, diskPath)
		return
	}
	span.Infof("action[deleteAndSyncDecommissionedDisk] finish, remaining decommissioned disks[%v], dataNode[%v]", dataNode.getDecommissionedDisks(), dataNode.Addr)
	return
}

func (c *Cluster) decommissionDisk(ctx context.Context, dataNode *DataNode, raftForce bool, badDiskPath string,
	badPartitions []*DataPartition, diskDisable bool,
) (err error) {
	span := proto.SpanFromContext(ctx)
	msg := fmt.Sprintf("action[decommissionDisk], Node[%v] OffLine,disk[%v]", dataNode.Addr, badDiskPath)
	span.Warn(msg)

	for _, dp := range badPartitions {
		go func(dp *DataPartition) {
			if err = c.decommissionDataPartition(ctx, dataNode.Addr, dp, raftForce, diskOfflineErr); err != nil {
				return
			}
		}(dp)
	}
	msg = fmt.Sprintf("action[decommissionDisk],clusterID[%v] node[%v] OffLine success",
		c.Name, dataNode.Addr)
	Warn(ctx, c.Name, msg)
	return
}

const (
	ManualDecommission uint32 = iota
	AutoDecommission
)

type DecommissionDisk struct {
	SrcAddr                  string
	DstAddr                  string
	DiskPath                 string
	DecommissionStatus       uint32
	DecommissionRaftForce    bool
	DecommissionRetry        uint8
	DecommissionDpTotal      int
	DecommissionTerm         uint64
	DecommissionDpCount      int
	DiskDisable              bool
	Type                     uint32
	DecommissionCompleteTime int64
}

func (dd *DecommissionDisk) GenerateKey() string {
	return fmt.Sprintf("%s_%s", dd.SrcAddr, dd.DiskPath)
}

func (dd *DecommissionDisk) updateDecommissionStatus(ctx context.Context, c *Cluster, debug bool) (uint32, float64) {
	var (
		progress            float64
		totalNum            = dd.DecommissionDpTotal
		partitionIds        []uint64
		failedPartitionIds  []uint64
		runningPartitionIds []uint64
		preparePartitionIds []uint64
		stopPartitionIds    []uint64
	)
	span := proto.SpanFromContext(ctx)
	if dd.GetDecommissionStatus() == DecommissionInitial {
		return DecommissionInitial, float64(0)
	}

	if dd.GetDecommissionStatus() == markDecommission {
		return markDecommission, float64(0)
	}

	if totalNum == InvalidDecommissionDpCnt && dd.GetDecommissionStatus() == DecommissionFail {
		return DecommissionFail, float64(0)
	}

	if dd.GetDecommissionStatus() == DecommissionSuccess {
		return DecommissionSuccess, float64(1)
	}

	if dd.GetDecommissionStatus() == DecommissionPause {
		return DecommissionPause, float64(0)
	}

	defer func() {
		c.syncUpdateDecommissionDisk(ctx, dd)
	}()
	if dd.DecommissionRetry >= defaultDecommissionRetryLimit {
		dd.markDecommissionFailed()
		return DecommissionFail, float64(0)
	}
	// Get all dp on this disk
	failedNum := 0
	runningNum := 0
	prepareNum := 0
	stopNum := 0
	// get the latest decommission result
	partitions := c.getAllDecommissionDataPartitionByDiskAndTerm(dd.SrcAddr, dd.DiskPath, dd.DecommissionTerm)

	if len(partitions) == 0 {
		span.Debugf("action[updateDecommissionDiskStatus]no partitions left:%v", dd.GenerateKey())
		dd.markDecommissionSuccess()
		return DecommissionSuccess, float64(1)
	}

	for _, dp := range partitions {
		if dp.IsDecommissionFailed() && !dp.needRollback(ctx, c) {
			failedNum++
			failedPartitionIds = append(failedPartitionIds, dp.PartitionID)
		}
		if dp.GetDecommissionStatus() == DecommissionRunning {
			runningNum++
			runningPartitionIds = append(runningPartitionIds, dp.PartitionID)
		}
		if dp.GetDecommissionStatus() == DecommissionPrepare {
			prepareNum++
			preparePartitionIds = append(preparePartitionIds, dp.PartitionID)
		}
		// disk may stop before and will be counted into partitions
		if dp.GetDecommissionStatus() == DecommissionPause {
			stopNum++
			stopPartitionIds = append(stopPartitionIds, dp.PartitionID)
		}
		partitionIds = append(partitionIds, dp.PartitionID)
	}
	progress = float64(totalNum-len(partitions)) / float64(totalNum)
	if debug {
		span.Infof("action[updateDecommissionDiskStatus] disk[%v] progress[%v] totalNum[%v] "+
			"partitionIds %v  FailedNum[%v] failedPartitionIds %v, runningNum[%v] runningDp %v, prepareNum[%v] prepareDp %v "+
			"stopNum[%v] stopPartitionIds %v ",
			dd.GenerateKey(), progress, totalNum, partitionIds, failedNum, failedPartitionIds, runningNum, runningPartitionIds,
			prepareNum, preparePartitionIds, stopNum, stopPartitionIds)
	}
	if failedNum >= (len(partitions)-stopNum) && failedNum != 0 {
		dd.markDecommissionFailed()
		return DecommissionFail, progress
	}
	dd.SetDecommissionStatus(DecommissionRunning)
	return DecommissionRunning, progress
}

func (dd *DecommissionDisk) GetDecommissionStatus() uint32 {
	return atomic.LoadUint32(&dd.DecommissionStatus)
}

func (dd *DecommissionDisk) SetDecommissionStatus(status uint32) {
	atomic.StoreUint32(&dd.DecommissionStatus, status)
}

func (dd *DecommissionDisk) markDecommissionSuccess() {
	dd.SetDecommissionStatus(DecommissionSuccess)
	dd.DecommissionCompleteTime = time.Now().Unix()
}

func (dd *DecommissionDisk) markDecommissionFailed() {
	dd.SetDecommissionStatus(DecommissionFail)
	dd.DecommissionCompleteTime = time.Now().Unix()
}

func (dd *DecommissionDisk) GetLatestDecommissionDP(c *Cluster) (partitions []*DataPartition) {
	partitions = c.getAllDecommissionDataPartitionByDiskAndTerm(dd.SrcAddr, dd.DiskPath, dd.DecommissionTerm)
	return
}

func (dd *DecommissionDisk) GetDecommissionFailedDP(ctx context.Context, c *Cluster) (error, []uint64) {
	var (
		failedDps     []uint64
		err           error
		badPartitions []*DataPartition
	)
	span := proto.SpanFromContext(ctx)
	if dd.GetDecommissionStatus() != DecommissionFail {
		err = fmt.Errorf("action[GetDecommissionDiskFailedDP]dataNode[%s] disk[%s] status must be failed,but[%d]",
			dd.SrcAddr, dd.DiskPath, dd.GetDecommissionStatus())
		return err, failedDps
	}

	badPartitions = c.getAllDecommissionDataPartitionByDisk(dd.SrcAddr, dd.DiskPath)
	for _, dp := range badPartitions {
		if dp.IsDecommissionFailed() {
			failedDps = append(failedDps, dp.PartitionID)
		}
	}
	span.Warnf("action[GetDecommissionDiskFailedDP] failed dp list [%v]", failedDps)
	return nil, failedDps
}

func (dd *DecommissionDisk) markDecommission(dstPath string, raftForce bool, limit int) {
	// if transfer from pause,do not change these attrs
	if dd.GetDecommissionStatus() != DecommissionPause {
		dd.DecommissionDpTotal = InvalidDecommissionDpCnt
		dd.DecommissionDpCount = limit
		dd.DecommissionRaftForce = raftForce
		dd.DstAddr = dstPath
		dd.DecommissionRetry = 0
	}
	dd.DecommissionTerm = dd.DecommissionTerm + 1
	dd.SetDecommissionStatus(markDecommission)
}

func (dd *DecommissionDisk) canAddToDecommissionList() bool {
	status := dd.GetDecommissionStatus()
	if status == DecommissionRunning ||
		status == markDecommission {
		return true
	}
	return false
}

func (dd *DecommissionDisk) AddToNodeSet() bool {
	status := dd.GetDecommissionStatus()
	if status == DecommissionRunning ||
		status == markDecommission {
		return true
	}
	return false
}

func (dd *DecommissionDisk) IsManualDecommissionDisk() bool {
	return dd.Type == ManualDecommission
}

func (dd *DecommissionDisk) CanBePaused() bool {
	status := dd.GetDecommissionStatus()
	if status == DecommissionRunning || status == markDecommission ||
		status == DecommissionPause {
		return true
	}
	return false
}

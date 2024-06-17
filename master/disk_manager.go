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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

func (c *Cluster) scheduleToCheckDiskRecoveryProgress() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkDiskRecoveryProgress()
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

	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	log.LogDebugf("[checkDiskRecoveryProgress] check disk recovery progress")
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		newBadDpIds := make([]uint64, 0)
		for _, partitionID := range badDataPartitionIds {
			partition, err := c.getDataPartitionByID(partitionID)
			if err != nil {
				Warn(c.Name, fmt.Sprintf("checkDiskRecoveryProgress clusterID[%v],partitionID[%v] is not exist", c.Name, partitionID))
				continue
			}
			// do not update status if paused
			if partition.IsDecommissionPaused() {
				log.LogInfof("[checkDiskRecoveryProgress] dp(%v) decommission pause", partitionID)
				continue
			}
			_, err = c.getVol(partition.VolName)
			if err != nil {
				Warn(c.Name, fmt.Sprintf("checkDiskRecoveryProgress clusterID[%v],partitionID[%v] vol(%s) is not exist",
					c.Name, partitionID, partition.VolName))
				continue
			}
			log.LogInfof("action[checkDiskRecoveryProgress] dp %v isSpec %v replicas %v conf replicas num %v  status(%v)",
				partition.decommissionInfo(), partition.isSpecialReplicaCnt(), len(partition.Replicas), int(partition.ReplicaNum), partition.GetDecommissionStatus())
			if len(partition.Replicas) == 0 {
				partition.SetDecommissionStatus(DecommissionSuccess)
				log.LogWarnf("action[checkDiskRecoveryProgress] dp %v maybe deleted", partition.PartitionID)
				continue
			}
			if partition.IsDiscard {
				partition.SetDecommissionStatus(DecommissionSuccess)
				log.LogWarnf("[checkDiskRecoveryProgress] dp(%v) is discard, decommission successfully", partition.PartitionID)
				continue
			}
			//if len(partition.Replicas) == 0 ||
			//	(!partition.isSpecialReplicaCnt() && len(partition.Replicas) < int(partition.ReplicaNum)) ||
			//	(partition.isSpecialReplicaCnt() && len(partition.Replicas) > int(partition.ReplicaNum)) {
			//	newBadDpIds = append(newBadDpIds, partitionID)
			//	log.LogInfof("action[checkDiskRecoveryProgress] dp %v newBadDpIds [%v] replics %v conf replics num %v",
			//		partition.PartitionID, newBadDpIds, len(partition.Replicas), int(partition.ReplicaNum))
			//	continue
			//}

			newReplica, _ := partition.getReplica(partition.DecommissionDstAddr)
			if newReplica == nil {
				log.LogWarnf("action[checkDiskRecoveryProgress] dp %v cannot find replica %v", partition.PartitionID,
					partition.DecommissionDstAddr)
				if partition.DecommissionType == ManualAddReplica {
					partition.resetForManualAddReplica()
				} else {
					partition.DecommissionNeedRollback = true
					partition.SetDecommissionStatus(DecommissionFail)
				}
				partition.DecommissionErrorMessage = fmt.Sprintf("Decommission target node %v not found", partition.DecommissionDstAddr)
				partition.RLock()
				err = c.syncUpdateDataPartition(partition)
				if err != nil {
					log.LogErrorf("[checkDiskRecoveryProgress] update dp(%v) fail, err(%v)", partitionID, err)
					err = nil
				}
				partition.RUnlock()
				continue
			}
			if newReplica.isRepairing() {
				log.LogInfof("[checkDiskRecoveryProgress] dp(%v) new replica(%v) report time(%v) is repairing", partition.PartitionID, newReplica.Addr, time.Unix(newReplica.ReportTime, 0).String())
				// special replica with force still need to check status of new replica here
				if !partition.isSpecialReplicaCnt() || (partition.isSpecialReplicaCnt() && partition.DecommissionRaftForce == true) {
					masterNode, _ := partition.getReplica(partition.Hosts[0])
					duration := time.Unix(masterNode.ReportTime, 0).Sub(time.Unix(newReplica.ReportTime, 0))
					if math.Abs(duration.Minutes()) > 10 {
						if partition.DecommissionType == ManualAddReplica {
							partition.resetForManualAddReplica()
						} else {
							partition.markRollbackFailed(false)
						}
						partition.DecommissionErrorMessage = fmt.Sprintf("Decommission target node %v cannot finish recover"+
							" for host[0] %v is down ", partition.DecommissionDstAddr, masterNode.Addr)
						Warn(c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress]clusterID[%v],partitionID[%v] %v",
							c.Name, partitionID, partition.DecommissionErrorMessage))
						partition.RLock()
						err = c.syncUpdateDataPartition(partition)
						if err != nil {
							log.LogErrorf("[checkDiskRecoveryProgress] update dp(%v) fail, err(%v)", partitionID, err)
							err = nil
						}
						partition.RUnlock()
						continue
					} else if time.Since(partition.RecoverStartTime) > c.GetDecommissionDataPartitionRecoverTimeOut() {
						if partition.DecommissionType == ManualAddReplica {
							partition.resetForManualAddReplica()
						} else {
							partition.DecommissionNeedRollback = true
							partition.SetDecommissionStatus(DecommissionFail)
						}
						partition.DecommissionErrorMessage = fmt.Sprintf("Decommission target node %v repair timeout", partition.DecommissionDstAddr)
						Warn(c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress]clusterID[%v],partitionID[%v] replica %v_%v recovered timeout %s",
							c.Name, partitionID, newReplica.Addr, newReplica.DiskPath, time.Since(partition.RecoverStartTime)))
						partition.RLock()
						err = c.syncUpdateDataPartition(partition)
						if err != nil {
							log.LogErrorf("[checkDiskRecoveryProgress] update dp(%v) fail, err(%v)", partitionID, err)
							err = nil
						}
						partition.RUnlock()
						continue
					}
				}
				newBadDpIds = append(newBadDpIds, partitionID)
			} else {
				if partition.DecommissionType == ManualAddReplica {
					if newReplica.isUnavailable() {
						partition.DecommissionErrorMessage = fmt.Sprintf("New replica %v is unavailable", partition.DecommissionDstAddr)
						Warn(c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress]clusterID[%v],partitionID[%v] replica %v has recovered failed",
							c.Name, partitionID, partition.DecommissionDstAddr))
					} else {
						partition.DecommissionErrorMessage = ""
						Warn(c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress]clusterID[%v],partitionID[%v] replica %v has recovered success",
							c.Name, partitionID, partition.DecommissionDstAddr))
					}
					partition.resetForManualAddReplica()
					log.LogInfof("[checkDiskRecoveryProgress] dp(%v) manual add new replica addr %v status(%v)",
						partitionID, newReplica.Addr, newReplica.Status)
					partition.RLock()
					err = c.syncUpdateDataPartition(partition)
					if err != nil {
						log.LogErrorf("[checkDiskRecoveryProgress] update dp(%v) fail, err(%v)", partitionID, err)
						err = nil
					}
					partition.RUnlock()
					continue
				}
				if partition.isSpecialReplicaCnt() && !partition.DecommissionRaftForce {
					log.LogInfof("[checkDiskRecoveryProgress] special dp(%v) new replica addr %v status(%v)",
						partitionID, newReplica.Addr, newReplica.Status)
					continue // change dp decommission status in decommission function
				}
				// do not add to BadDataPartitionIds
				if newReplica.isUnavailable() {
					partition.DecommissionNeedRollback = true
					partition.SetDecommissionStatus(DecommissionFail)
					partition.DecommissionErrorMessage = fmt.Sprintf("New replica %v is unavailable", partition.DecommissionDstAddr)
					Warn(c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress]clusterID[%v],partitionID[%v] replica %v has recovered failed",
						c.Name, partitionID, partition.DecommissionDstAddr))
				} else {
					partition.DecommissionErrorMessage = ""
					partition.SetDecommissionStatus(DecommissionSuccess) // can be readonly or readwrite
					Warn(c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress]clusterID[%v],partitionID[%v] "+
						"replica %v has recovered success,cost(%v)",
						c.Name, partitionID, partition.DecommissionDstAddr, time.Since(partition.RecoverStartTime).String()))
				}
				partition.RLock()
				err = c.syncUpdateDataPartition(partition)
				if err != nil {
					log.LogErrorf("[checkDiskRecoveryProgress] update dp(%v) fail, err(%v)", partitionID, err)
					err = nil
				}
				partition.RUnlock()
			}
		}

		if len(newBadDpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("action[checkDiskRecoveryProgress]clusterID[%v],node:disk[%v] has recovered success", c.Name, key))
			c.BadDataPartitionIds.Delete(key)
		} else {
			c.BadDataPartitionIds.Store(key, newBadDpIds)
			log.LogInfof("action[checkDiskRecoveryProgress]BadDataPartitionIds key(%s) still have (%d) dp in recover", key, len(newBadDpIds))
		}

		return true
	})
}

func (c *Cluster) addAndSyncDecommissionedDisk(dataNode *DataNode, diskPath string) (err error) {
	if exist := dataNode.addDecommissionedDisk(diskPath); exist {
		return
	}
	if err = c.syncUpdateDataNode(dataNode); err != nil {
		dataNode.deleteDecommissionedDisk(diskPath)
		log.LogWarnf("action[addAndSyncDecommissionedDisk]submit raft failed: %v, delete disks[%v], dataNode[%v]",
			err, diskPath, dataNode.Addr)
		return
	}
	log.LogInfof("action[addAndSyncDecommissionedDisk] finish, remaining decommissioned disks[%v], dataNode[%v]", dataNode.getDecommissionedDisks(), dataNode.Addr)
	return
}

func (c *Cluster) deleteAndSyncDecommissionedDisk(dataNode *DataNode, diskPath string) (err error) {
	if exist := dataNode.deleteDecommissionedDisk(diskPath); !exist {
		return
	}
	if err = c.syncUpdateDataNode(dataNode); err != nil {
		dataNode.addDecommissionedDisk(diskPath)
		log.LogWarnf("action[deleteAndSyncDecommissionedDisk]submit raft failed: %v, delete disks[%v], dataNode[%v]",
			err, diskPath, dataNode.Addr)
		return
	}
	log.LogInfof("action[deleteAndSyncDecommissionedDisk] finish, remaining decommissioned disks[%v], dataNode[%v]", dataNode.getDecommissionedDisks(), dataNode.Addr)
	return
}

func (c *Cluster) decommissionDisk(dataNode *DataNode, raftForce bool, badDiskPath string,
	badPartitions []*DataPartition, diskDisable bool,
) (err error) {
	msg := fmt.Sprintf("action[decommissionDisk], Node[%v] OffLine,disk[%v]", dataNode.Addr, badDiskPath)
	log.LogWarn(msg)

	for _, dp := range badPartitions {
		go func(dp *DataPartition) {
			if err = c.decommissionDataPartition(dataNode.Addr, dp, raftForce, diskOfflineErr); err != nil {
				return
			}
		}(dp)
	}
	msg = fmt.Sprintf("action[decommissionDisk],clusterID[%v] node[%v] OffLine success",
		c.Name, dataNode.Addr)
	Warn(c.Name, msg)
	return
}

const (
	InitialDecommission = proto.InitialDecommission
	ManualDecommission  = proto.ManualDecommission
	AutoDecommission    = proto.AutoDecommission
	QueryDecommission   = proto.QueryDecommission
	AutoAddReplica      = proto.AutoAddReplica
	ManualAddReplica    = proto.ManualAddReplica
)

type DecommissionDisk struct {
	SrcAddr                  string
	DstAddr                  string
	DiskPath                 string
	DecommissionStatus       uint32
	DecommissionRaftForce    bool
	DecommissionTimes        uint8
	DecommissionDpTotal      int
	DecommissionTerm         uint64
	DecommissionDpCount      int
	DiskDisable              bool
	IgnoreDecommissionDps    []proto.IgnoreDecommissionDP
	ResidualDecommissionDps  []proto.IgnoreDecommissionDP
	Type                     uint32
	DecommissionCompleteTime int64
	UpdateMutex              sync.RWMutex `json:"-"`
}

func (dd *DecommissionDisk) GenerateKey() string {
	return fmt.Sprintf("%s_%s", dd.SrcAddr, dd.DiskPath)
}

func (dd *DecommissionDisk) updateDecommissionStatus(c *Cluster, debug, persist bool) (uint32, float64) {
	var (
		progress             float64
		totalNum             = dd.DecommissionDpTotal
		partitionIds         []uint64
		failedPartitionIds   []uint64
		runningPartitionIds  []uint64
		preparePartitionIds  []uint64
		stopPartitionIds     []uint64
		ignorePartitionIds   []uint64
		residualPartitionIds []uint64
	)

	if dd.GetDecommissionStatus() == DecommissionInitial {
		return DecommissionInitial, float64(0)
	}

	if dd.GetDecommissionStatus() == markDecommission {
		return markDecommission, float64(0)
	}

	if totalNum == InvalidDecommissionDpCnt && dd.GetDecommissionStatus() == DecommissionFail {
		return DecommissionFail, float64(0)
	}

	if dd.GetDecommissionStatus() == DecommissionPause {
		return DecommissionPause, float64(0)
	}

	defer func() {
		if persist {
			c.syncUpdateDecommissionDisk(dd)
		}
	}()

	// Get all dp on this disk
	failedNum := 0
	runningNum := 0
	prepareNum := 0
	stopNum := 0
	// get the latest decommission result
	partitions := c.getAllDecommissionDataPartitionByDiskAndTerm(dd.SrcAddr, dd.DiskPath, dd.DecommissionTerm)

	for _, info := range dd.IgnoreDecommissionDps {
		ignorePartitionIds = append(ignorePartitionIds, info.PartitionID)
		failedNum++
	}

	for _, info := range dd.ResidualDecommissionDps {
		residualPartitionIds = append(residualPartitionIds, info.PartitionID)
		failedNum++
	}

	if len(partitions)+len(ignorePartitionIds)+len(residualPartitionIds) == 0 {
		log.LogDebugf("action[updateDecommissionDiskStatus]no partitions left:%v", dd.GenerateKey())
		if persist {
			dd.markDecommissionSuccess()
		}
		return DecommissionSuccess, float64(1)
	}

	for _, dp := range partitions {
		if dp.IsRollbackFailed() {
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

	progress = float64(totalNum-len(partitions)-len(ignorePartitionIds)-len(residualPartitionIds)) / float64(totalNum)
	// ignorePartitionIds may be failed when decommission for other replica is completed
	if progress < 0 {
		progress = 0
	}
	if debug {
		log.LogInfof("action[updateDecommissionStatus] disk[%v] progress[%v] totalNum[%v] "+
			"partitionIds %v left %v FailedNum[%v] failedPartitionIds %v, runningNum[%v] runningDp %v, prepareNum[%v] prepareDp %v "+
			"stopNum[%v] stopPartitionIds %v ignorePartitionIds %v term %v",
			dd.GenerateKey(), progress, totalNum, partitionIds, len(partitionIds), failedNum, failedPartitionIds, runningNum, runningPartitionIds,
			prepareNum, preparePartitionIds, stopNum, stopPartitionIds, ignorePartitionIds, dd.DecommissionTerm)
	}
	// if decommission is cancel, len(partitions) is 0
	if dd.GetDecommissionStatus() == DecommissionCancel {
		return DecommissionCancel, progress
	}
	if failedNum >= (len(partitions)+len(ignorePartitionIds)+len(residualPartitionIds)-stopNum) && failedNum != 0 {
		if persist {
			dd.markDecommissionFailed()
		}
		return DecommissionFail, progress
	}
	// dp is put into decommission list, status is DecommissionRunning
	// maybe set DecommissionCancel here
	return dd.GetDecommissionStatus(), progress
}

func (dd *DecommissionDisk) Abort(c *Cluster) (err error) {
	dd.UpdateMutex.Lock()
	defer dd.UpdateMutex.Unlock()

	err = c.syncDeleteDecommissionDisk(dd)
	if err != nil {
		return
	}
	c.DecommissionDisks.Delete(dd.GenerateKey())
	return
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

func (dd *DecommissionDisk) GetDecommissionFailedDPByTerm(c *Cluster) []proto.FailedDpInfo {
	partitions := c.getAllDecommissionDataPartitionByDiskAndTerm(dd.SrcAddr, dd.DiskPath, dd.DecommissionTerm)
	var failedDps []proto.FailedDpInfo
	log.LogDebugf("action[GetDecommissionFailedDPByTerm] partitions len %v", len(partitions))
	for _, dp := range partitions {
		if dp.IsRollbackFailed() {
			failedDps = append(failedDps, proto.FailedDpInfo{PartitionID: dp.PartitionID, ErrMsg: dp.DecommissionErrorMessage})
			log.LogWarnf("action[GetDecommissionFailedDPByTerm] dp[%v] failed", dp.PartitionID)
		}
	}
	log.LogWarnf("action[GetDecommissionFailedDPByTerm] failed dp list [%v]", failedDps)
	return failedDps
}

func (dd *DecommissionDisk) GetDecommissionFailedDP(c *Cluster) (error, []uint64) {
	var (
		failedDps     []uint64
		err           error
		badPartitions []*DataPartition
	)
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
	log.LogWarnf("action[GetDecommissionDiskFailedDP] failed dp list [%v]", failedDps)
	return nil, failedDps
}

func (dd *DecommissionDisk) markDecommission(dstAddr string, raftForce bool, limit int) {
	// if transfer from pause,do not change these attrs
	if dd.GetDecommissionStatus() != DecommissionPause {
		dd.DecommissionDpTotal = InvalidDecommissionDpCnt
		dd.DecommissionDpCount = limit
		dd.DecommissionRaftForce = raftForce
		dd.DstAddr = dstAddr
		dd.DecommissionTimes = 0
	}
	dd.DecommissionTerm = uint64(time.Now().Unix())
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

func (dd *DecommissionDisk) decommissionInfo() string {
	return fmt.Sprintf("disk(%v_%v)_dst(%v)_total(%v)_term(%v)_type(%v)_force(%v)_retry(%v)_status(%v)_disable(%v)",
		dd.SrcAddr, dd.DiskPath, dd.DstAddr, dd.DecommissionDpTotal, dd.DecommissionTerm,
		GetDecommissionTypeMessage(dd.Type), dd.DecommissionRaftForce, dd.DecommissionTimes,
		GetDecommissionStatusMessage(dd.DecommissionStatus), dd.DiskDisable)
}

func (dd *DecommissionDisk) cancelDecommission(cluster *Cluster, ns *nodeSet) (err error) {
	dps := cluster.getAllDecommissionDataPartitionByDiskAndTerm(dd.SrcAddr, dd.DiskPath, dd.DecommissionTerm)
	for _, dp := range dps {
		if dp.GetDecommissionStatus() == DecommissionSuccess || dp.IsRollbackFailed() || ns.HasDecommissionToken(dp.PartitionID) {
			continue
		}
		msg := fmt.Sprintf("dp(%v) cancel decommission", dp.decommissionInfo())
		dp.ResetDecommissionStatus()
		dp.setRestoreReplicaStop()
		cluster.syncUpdateDataPartition(dp)
		auditlog.LogMasterOp("CancelDataPartitionDecommission", msg, nil)
	}
	dd.SetDecommissionStatus(DecommissionCancel)
	msg := fmt.Sprintf("disk(%v) cancel decommission", dd.decommissionInfo())
	auditlog.LogMasterOp("CancelDiskDecommission", msg, nil)
	err = cluster.syncUpdateDecommissionDisk(dd)
	return err
}

func (dd *DecommissionDisk) residualDecommissionDpsHas(id uint64) bool {
	dd.UpdateMutex.RLock()
	defer dd.UpdateMutex.RUnlock()
	for _, dp := range dd.ResidualDecommissionDps {
		if dp.PartitionID == id {
			return true
		}
	}
	return false
}

func (dd *DecommissionDisk) residualDecommissionDpsSave(id uint64, msg string, c *Cluster) {
	dd.UpdateMutex.Lock()
	defer dd.UpdateMutex.Unlock()
	dd.ResidualDecommissionDps = append(dd.ResidualDecommissionDps, proto.IgnoreDecommissionDP{
		PartitionID: id,
		ErrMsg:      msg,
	})
	c.syncUpdateDecommissionDisk(dd)
}

func (dd *DecommissionDisk) residualDecommissionDpsGetAll() []proto.IgnoreDecommissionDP {
	dd.UpdateMutex.RLock()
	defer dd.UpdateMutex.RUnlock()
	res := make([]proto.IgnoreDecommissionDP, 0)
	for _, dp := range dd.ResidualDecommissionDps {
		res = append(res, dp)
	}
	return res
}

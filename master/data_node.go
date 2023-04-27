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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/log"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
)

// DataNode stores all the information about a data node
type DataNode struct {
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	AvailableSpace            uint64
	ID                        uint64
	ZoneName                  string `json:"Zone"`
	Addr                      string
	DomainAddr                string
	ReportTime                time.Time
	StartTime                 int64
	LastUpdateTime            time.Time
	isActive                  bool
	sync.RWMutex              `graphql:"-"`
	UsageRatio                float64           // used / total space
	SelectedTimes             uint64            // number times that this datanode has been selected as the location for a data partition.
	Carry                     float64           // carry is a factor used in cacluate the node's weight
	TaskManager               *AdminTaskManager `graphql:"-"`
	DataPartitionReports      []*proto.PartitionReport
	DataPartitionCount        uint32
	TotalPartitionSize        uint64
	NodeSetID                 uint64
	PersistenceDataPartitions []uint64
	BadDisks                  []string
	DecommissionedDisks       sync.Map
	ToBeOffline               bool
	RdOnly                    bool
	MigrateLock               sync.RWMutex
	QosIopsRLimit             uint64
	QosIopsWLimit             uint64
	QosFlowRLimit             uint64
	QosFlowWLimit             uint64
	DecommissionStatus        uint32
	DecommissionDstAddr       string
	DecommissionRaftForce     bool
	DecommissionRetry         uint8
	DecommissionDpTotal       int
	DecommissionLimit         int
	DecommissionTerm          uint64
	DecommissionCompleteTime  int64
	DpCntLimit                DpCountLimiter `json:"-"` // max count of data partition in a data node
}

func newDataNode(addr, zoneName, clusterID string) (dataNode *DataNode) {
	dataNode = new(DataNode)
	dataNode.Carry = rand.Float64()
	dataNode.Total = 1
	dataNode.Addr = addr
	dataNode.ZoneName = zoneName
	dataNode.LastUpdateTime = time.Now().Add(-time.Minute)
	dataNode.TaskManager = newAdminTaskManager(dataNode.Addr, clusterID)
	dataNode.DecommissionStatus = DecommissionInitial
	dataNode.DecommissionTerm = 0
	dataNode.DecommissionDpTotal = InvalidDecommissionDpCnt
	dataNode.DpCntLimit = newDpCountLimiter(nil)
	return
}

func (dataNode *DataNode) checkLiveness() {
	dataNode.Lock()
	defer dataNode.Unlock()
	log.LogInfof("action[checkLiveness] datanode[%v] report time[%v],since report time[%v], need gap [%v]",
		dataNode.Addr, dataNode.ReportTime, time.Since(dataNode.ReportTime), time.Second*time.Duration(defaultNodeTimeOutSec))
	if time.Since(dataNode.ReportTime) > time.Second*time.Duration(defaultNodeTimeOutSec) {
		dataNode.isActive = false
	}

	return
}

func (dataNode *DataNode) badPartitions(diskPath string, c *Cluster) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	vols := c.copyVols()
	if len(vols) == 0 {
		return partitions
	}
	for _, vol := range vols {
		dps := vol.dataPartitions.checkBadDiskDataPartitions(diskPath, dataNode.Addr)
		partitions = append(partitions, dps...)
	}
	return
}

func (dataNode *DataNode) updateNodeMetric(resp *proto.DataNodeHeartbeatResponse) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.DomainAddr = util.ParseIpAddrToDomainAddr(dataNode.Addr)
	dataNode.Total = resp.Total
	dataNode.Used = resp.Used
	if dataNode.AvailableSpace > resp.Available ||
		time.Since(dataNode.LastUpdateTime) > defaultNodeTimeOutSec*time.Second {
		dataNode.AvailableSpace = resp.Available
		dataNode.LastUpdateTime = time.Now()
	}
	dataNode.ZoneName = resp.ZoneName
	dataNode.DataPartitionCount = resp.CreatedPartitionCnt
	dataNode.DataPartitionReports = resp.PartitionReports
	dataNode.TotalPartitionSize = resp.TotalPartitionSize
	dataNode.BadDisks = resp.BadDisks
	dataNode.StartTime = resp.StartTime
	if dataNode.Total == 0 {
		dataNode.UsageRatio = 0.0
	} else {
		dataNode.UsageRatio = (float64)(dataNode.Used) / (float64)(dataNode.Total)
	}
	dataNode.ReportTime = time.Now()
	dataNode.isActive = true
}

func (dataNode *DataNode) canAlloc() bool {
	dataNode.RLock()
	defer dataNode.RUnlock()

	if !overSoldLimit() {
		return true
	}

	maxCapacity := overSoldCap(dataNode.Total)
	if maxCapacity < dataNode.TotalPartitionSize {
		return false
	}

	return true
}

func (dataNode *DataNode) isWriteAble() (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	if dataNode.isActive && dataNode.AvailableSpace > 10*util.GB && !dataNode.RdOnly {
		ok = true
	}

	return
}

func (dataNode *DataNode) canAllocDp() bool {
	if !dataNode.isWriteAble() {
		return false
	}

	if dataNode.ToBeOffline {
		log.LogWarnf("action[canAllocDp] dataNode [%v] is offline ", dataNode.Addr)
		return false
	}

	if !dataNode.dpCntInLimit() {
		return false
	}

	return true
}

func (dataNode *DataNode) GetDpCntLimit() uint32 {
	return uint32(dataNode.DpCntLimit.GetCntLimit())
}

func (dataNode *DataNode) dpCntInLimit() bool {
	return dataNode.DataPartitionCount <= dataNode.GetDpCntLimit()
}

func (dataNode *DataNode) isWriteAbleWithSize(size uint64) (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	if dataNode.isActive == true && dataNode.AvailableSpace > size {
		ok = true
	}

	return
}

func (dataNode *DataNode) isAvailCarryNode() (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	return dataNode.Carry >= 1
}

func (dataNode *DataNode) GetID() uint64 {
	dataNode.RLock()
	defer dataNode.RUnlock()
	return dataNode.ID
}

func (dataNode *DataNode) GetAddr() string {
	dataNode.RLock()
	defer dataNode.RUnlock()
	return dataNode.Addr
}

// SetCarry implements "SetCarry" in the Node interface
func (dataNode *DataNode) SetCarry(carry float64) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.Carry = carry
}

// SelectNodeForWrite implements "SelectNodeForWrite" in the Node interface
func (dataNode *DataNode) SelectNodeForWrite() {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.UsageRatio = float64(dataNode.Used) / float64(dataNode.Total)
	dataNode.SelectedTimes++
	dataNode.Carry = dataNode.Carry - 1.0
}

func (dataNode *DataNode) clean() {
	dataNode.TaskManager.exitCh <- struct{}{}
}

func (dataNode *DataNode) createHeartbeatTask(masterAddr string, enableDiskQos bool) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	request.EnableDiskQos = enableDiskQos
	request.QosIopsReadLimit = dataNode.QosIopsRLimit
	request.QosIopsWriteLimit = dataNode.QosIopsWLimit
	request.QosFlowReadLimit = dataNode.QosFlowRLimit
	request.QosFlowWriteLimit = dataNode.QosFlowWLimit
	task = proto.NewAdminTask(proto.OpDataNodeHeartbeat, dataNode.Addr, request)
	return
}

func (dataNode *DataNode) addDecommissionedDisk(diskPath string) (exist bool) {
	_, exist = dataNode.DecommissionedDisks.LoadOrStore(diskPath, struct{}{})
	log.LogInfof("action[addDecommissionedDisk] finish, exist[%v], decommissioned disk[%v], dataNode[%v]", exist, diskPath, dataNode.Addr)
	return
}

func (dataNode *DataNode) deleteDecommissionedDisk(diskPath string) (exist bool) {
	_, exist = dataNode.DecommissionedDisks.LoadAndDelete(diskPath)
	log.LogInfof("action[deleteDecommissionedDisk] finish, exist[%v], decommissioned disk[%v], dataNode[%v]", exist, diskPath, dataNode.Addr)
	return
}

func (dataNode *DataNode) getDecommissionedDisks() (decommissionedDisks []string) {
	dataNode.DecommissionedDisks.Range(func(key, value interface{}) bool {
		if diskPath, ok := key.(string); ok {
			decommissionedDisks = append(decommissionedDisks, diskPath)
		}
		return true
	})
	return
}

func (dataNode *DataNode) updateDecommissionStatus(c *Cluster, debug bool) (uint32, float64) {
	var (
		progress            float64
		totalNum            = dataNode.DecommissionDpTotal
		partitionIds        []uint64
		failedPartitionIds  []uint64
		runningPartitionIds []uint64
		preparePartitionIds []uint64
		stopPartitionIds    []uint64
	)
	if dataNode.GetDecommissionStatus() == DecommissionInitial {
		return DecommissionInitial, float64(0)
	}
	//not enter running status
	if dataNode.DecommissionRetry >= defaultDecommissionRetryLimit {
		dataNode.markDecommissionFail()
		return DecommissionFail, float64(0)
	}

	if dataNode.GetDecommissionStatus() == markDecommission {
		return markDecommission, float64(0)
	}

	if totalNum == InvalidDecommissionDpCnt && dataNode.GetDecommissionStatus() == DecommissionFail {
		return DecommissionFail, float64(0)
	}

	if dataNode.GetDecommissionStatus() == DecommissionSuccess {
		return DecommissionSuccess, float64(1)
	}

	if dataNode.GetDecommissionStatus() == DecommissionStop {
		return DecommissionStop, float64(0)
	}
	defer func() {
		c.syncUpdateDataNode(dataNode)
	}()

	//Get all dp on this dataNode
	failedNum := 0
	runningNum := 0
	prepareNum := 0
	stopNum := 0
	partitions := c.getAllDecommissionDataPartitionByDataNodeAndTerm(dataNode.Addr, dataNode.DecommissionTerm)
	//log.LogDebugf("action[updateDecommissionDataNodeStatus] partitions len %v", len(partitions))
	//only failed or stopped or markDeleted
	if len(partitions) == 0 {
		dataNode.markDecommissionSuccess()
		return DecommissionSuccess, float64(1)
	}

	for _, dp := range partitions {
		if dp.IsDecommissionFailed() {
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
		//datanode may stop before and will be counted into partitions
		if dp.GetDecommissionStatus() == DecommissionStop {
			stopNum++
			stopPartitionIds = append(stopPartitionIds, dp.PartitionID)
		}
		partitionIds = append(partitionIds, dp.PartitionID)
	}
	progress = float64(totalNum-len(partitions)+stopNum) / float64(totalNum)
	if debug {
		log.LogInfof("action[updateDecommissionDataNodeStatus]dataNode[%v] progress[%v] totalNum[%v] "+
			"partitionIds[%v] FailedNum[%v] failedPartitionIds[%v], runningNum[%v] runningDp[%v], prepareNum[%v] prepareDp[%v] "+
			"stopNum[%v] stopPartitionIds[%v]",
			dataNode.Addr, progress, totalNum, partitionIds, failedNum, failedPartitionIds, runningNum, runningPartitionIds,
			prepareNum, preparePartitionIds, stopNum, stopPartitionIds)
	}

	if failedNum >= (len(partitions) - stopNum) {
		dataNode.markDecommissionFail()
		return DecommissionFail, progress
	}
	return dataNode.GetDecommissionStatus(), progress
}

func (dataNode *DataNode) GetDecommissionStatus() uint32 {
	return atomic.LoadUint32(&dataNode.DecommissionStatus)
}

func (dataNode *DataNode) SetDecommissionStatus(status uint32) {
	atomic.StoreUint32(&dataNode.DecommissionStatus, status)
}

func (dataNode *DataNode) GetDecommissionFailedDPByTerm(c *Cluster) (error, []uint64) {
	var (
		failedDps []uint64
		err       error
	)
	if dataNode.GetDecommissionStatus() != DecommissionFail {
		err = fmt.Errorf("action[GetDecommissionDataNodeFailedDP]dataNode[%s] status must be failed,but[%d]",
			dataNode.Addr, dataNode.GetDecommissionStatus())
		return err, failedDps
	}
	partitions := c.getAllDecommissionDataPartitionByDataNodeAndTerm(dataNode.Addr, dataNode.DecommissionTerm)
	log.LogDebugf("action[GetDecommissionDataNodeFailedDP] partitions len %v", len(partitions))
	for _, dp := range partitions {
		if dp.IsDecommissionFailed() {
			failedDps = append(failedDps, dp.PartitionID)
			log.LogWarnf("action[GetDecommissionDataNodeFailedDP] dp[%v] failed", dp.PartitionID)
		}
	}
	log.LogWarnf("action[GetDecommissionDataNodeFailedDP] failed dp list [%v]", failedDps)
	return nil, failedDps
}

func (dataNode *DataNode) GetDecommissionFailedDP(c *Cluster) (error, []uint64) {
	var (
		failedDps []uint64
		err       error
	)
	if dataNode.GetDecommissionStatus() != DecommissionFail {
		err = fmt.Errorf("action[GetDecommissionDataNodeFailedDP]dataNode[%s] status must be failed,but[%d]",
			dataNode.Addr, dataNode.GetDecommissionStatus())
		return err, failedDps
	}
	partitions := c.getAllDecommissionDataPartitionByDataNode(dataNode.Addr)
	log.LogDebugf("action[GetDecommissionDataNodeFailedDP] partitions len %v", len(partitions))
	for _, dp := range partitions {
		if dp.IsDecommissionFailed() {
			failedDps = append(failedDps, dp.PartitionID)
			log.LogWarnf("action[GetDecommissionDataNodeFailedDP] dp[%v] failed", dp.PartitionID)
		}
	}
	log.LogWarnf("action[GetDecommissionDataNodeFailedDP] failed dp list [%v]", failedDps)
	return nil, failedDps
}

func (dataNode *DataNode) markDecommission(targetAddr string, raftForce bool, limit int) {
	dataNode.SetDecommissionStatus(markDecommission)
	dataNode.DecommissionRaftForce = raftForce
	dataNode.DecommissionDstAddr = targetAddr
	//reset decommission status for failed once
	dataNode.DecommissionRetry = 0
	dataNode.DecommissionLimit = limit
	dataNode.DecommissionDpTotal = InvalidDecommissionDpCnt
	dataNode.DecommissionTerm = dataNode.DecommissionTerm + 1
}

func (dataNode *DataNode) markDecommissionSuccess() {
	dataNode.SetDecommissionStatus(DecommissionSuccess)
	dataNode.ToBeOffline = false
	dataNode.DecommissionCompleteTime = time.Now().Unix()
}

func (dataNode *DataNode) markDecommissionFail() {
	dataNode.SetDecommissionStatus(DecommissionFail)
	//dataNode.ToBeOffline = false
	//dataNode.DecommissionCompleteTime = time.Now().Unix()
}

func (dataNode *DataNode) resetDecommissionStatus() {
	dataNode.SetDecommissionStatus(DecommissionInitial)
	dataNode.DecommissionRaftForce = false
	dataNode.DecommissionDstAddr = ""
	dataNode.DecommissionRetry = 0
	dataNode.DecommissionLimit = 0
	dataNode.DecommissionDpTotal = InvalidDecommissionDpCnt
	dataNode.DecommissionCompleteTime = 0
}

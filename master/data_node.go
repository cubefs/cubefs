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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

// DataNode stores all the information about a data node
type DataNode struct {
	Total                            uint64 `json:"TotalWeight"`
	Used                             uint64 `json:"UsedWeight"`
	AvailableSpace                   uint64
	ID                               uint64
	ZoneName                         string `json:"Zone"`
	Addr                             string
	DomainAddr                       string
	ReportTime                       time.Time
	StartTime                        int64
	LastUpdateTime                   time.Time
	isActive                         bool
	sync.RWMutex                     `graphql:"-"`
	UsageRatio                       float64           // used / total space
	SelectedTimes                    uint64            // number times that this datanode has been selected as the location for a data partition.
	TaskManager                      *AdminTaskManager `graphql:"-"`
	DataPartitionReports             []*proto.DataPartitionReport
	DataPartitionCount               uint32
	TotalPartitionSize               uint64
	NodeSetID                        uint64
	PersistenceDataPartitions        []uint64
	BadDisks                         []string            // Keep this old field for compatibility
	BadDiskStats                     []proto.BadDiskStat // key: disk path
	DecommissionedDisks              sync.Map            `json:"-"` // NOTE: the disks that already be decommissioned
	AllDisks                         []string            // TODO: remove me when merge to github master
	ToBeOffline                      bool
	RdOnly                           bool
	MigrateLock                      sync.RWMutex
	QosIopsRLimit                    uint64
	QosIopsWLimit                    uint64
	QosFlowRLimit                    uint64
	QosFlowWLimit                    uint64
	DecommissionStatus               uint32
	DecommissionDstAddr              string
	DecommissionRaftForce            bool
	DecommissionLimit                int
	DecommissionCompleteTime         int64
	DpCntLimit                       LimitCounter       `json:"-"` // max count of data partition in a data node
	CpuUtil                          atomicutil.Float64 `json:"-"`
	ioUtils                          atomic.Value       `json:"-"`
	DecommissionDiskList             []string           // NOTE: the disks that running decommission
	DecommissionDpTotal              int
	DecommissionSyncMutex            sync.Mutex
	BackupDataPartitions             []proto.BackupDataPartitionInfo
	MediaType                        uint32
	ReceivedForbidWriteOpOfProtoVer0 bool
	DiskOpLogs                       []proto.OpLog
	DpOpLogs                         []proto.OpLog
}

func newDataNode(addr, zoneName, clusterID string, mediaType uint32) (dataNode *DataNode) {
	if zoneName == "" {
		zoneName = DefaultZoneName
	}

	dataNode = new(DataNode)
	dataNode.Total = 1
	dataNode.Addr = addr
	dataNode.ZoneName = zoneName
	dataNode.LastUpdateTime = time.Now().Add(-time.Minute)
	dataNode.TaskManager = newAdminTaskManager(dataNode.Addr, clusterID)
	dataNode.DecommissionStatus = DecommissionInitial
	dataNode.DpCntLimit = newLimitCounter(nil, defaultMaxDpCntLimit)
	dataNode.CpuUtil.Store(0)
	dataNode.SetIoUtils(make(map[string]float64))
	dataNode.AllDisks = make([]string, 0)
	dataNode.ReportTime = time.Now()
	dataNode.MediaType = mediaType
	return
}

func (dataNode *DataNode) IsActiveNode() bool {
	return dataNode.isActive
}

func (dataNode *DataNode) GetIoUtils() map[string]float64 {
	return dataNode.ioUtils.Load().(map[string]float64)
}

func (dataNode *DataNode) SetIoUtils(used map[string]float64) {
	dataNode.ioUtils.Store(used)
}

func (dataNode *DataNode) checkLiveness() {
	dataNode.Lock()
	defer dataNode.Unlock()
	if time.Since(dataNode.ReportTime) > time.Second*time.Duration(defaultNodeTimeOutSec) {
		dataNode.isActive = false
		msg := fmt.Sprintf("datanode[%v] report time[%v],since report time[%v], need gap [%v]",
			dataNode.Addr, dataNode.ReportTime, time.Since(dataNode.ReportTime), time.Second*time.Duration(defaultNodeTimeOutSec))
		log.LogWarnf("action[checkLiveness]  %v", msg)
		auditlog.LogMasterOp("DataNodeLive", msg, nil)
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

func (dataNode *DataNode) getDisks(c *Cluster) (diskPaths []string) {
	diskPaths = make([]string, 0)
	vols := c.copyVols()
	if len(vols) == 0 {
		return diskPaths
	}
	for _, vol := range vols {
		disks := vol.dataPartitions.getReplicaDiskPaths(dataNode.Addr)
		for _, disk := range disks {
			if inStingList(disk, diskPaths) {
				continue
			}
			diskPaths = append(diskPaths, disk)
		}
	}

	return
}

func (dataNode *DataNode) updateBadDisks(latest []string) (ok bool, removed []string) {
	sort.Slice(latest, func(i, j int) bool {
		return latest[i] < latest[j]
	})

	curr := dataNode.BadDisks
	dataNode.BadDisks = latest
	if len(curr) != len(latest) {
		ok = true
	}

	if !ok {
		for i := 0; i < len(curr); i++ {
			if curr[i] != latest[i] {
				ok = true
			}
		}
	}

	if ok {
		removed = make([]string, 0)
		latestMap := make(map[string]bool)
		for _, disk := range latest {
			latestMap[disk] = true
		}
		for _, disk := range curr {
			if !latestMap[disk] {
				removed = append(removed, disk)
			}
		}
	}
	return
}

func (dataNode *DataNode) updateNodeMetric(c *Cluster, resp *proto.DataNodeHeartbeatResponse) {
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

	dataNode.AllDisks = resp.AllDisks
	updated, removedDisks := dataNode.updateBadDisks(resp.BadDisks)
	dataNode.BadDiskStats = resp.BadDiskStats
	dataNode.BackupDataPartitions = resp.BackupDataPartitions

	dataNode.DiskOpLogs = resp.DiskOpLogs
	dataNode.DpOpLogs = resp.DpOpLogs

	dataNode.StartTime = resp.StartTime
	if dataNode.Total == 0 {
		dataNode.UsageRatio = 0.0
	} else {
		dataNode.UsageRatio = (float64)(dataNode.Used) / (float64)(dataNode.Total)
	}
	dataNode.ReportTime = time.Now()
	dataNode.isActive = true

	if len(removedDisks) != 0 {
		log.LogInfof("[updateNodeMetric] dataNode %v removedDisks (%v)", dataNode.Addr, removedDisks)
		for _, disk := range removedDisks {
			key := fmt.Sprintf("%s_%s", dataNode.Addr, disk)
			if value, ok := c.DecommissionDisks.Load(key); ok {
				disk := value.(*DecommissionDisk)
				if disk.GetDecommissionStatus() == DecommissionCancel {
					if err := c.syncDeleteDecommissionDisk(disk); err != nil {
						log.LogWarnf("[updateNodeMetric] dataNode %v disk (%v) is recovered, but remove failed %v",
							dataNode.Addr, key, err)
					} else {
						c.DecommissionDisks.Delete(key)
						// can allocate dp again
						c.deleteAndSyncDecommissionedDisk(dataNode, disk.DiskPath)
						log.LogInfof("[updateNodeMetric] dataNode %v disk (%v) is recovered", dataNode.Addr, key)
					}
				}
			}
		}
	}

	if updated {
		log.LogInfof("[updateNodeMetric] update data node(%v)", dataNode.Addr)
		if err := c.syncUpdateDataNode(dataNode); err != nil {
			log.LogErrorf("[updateNodeMetric] failed to update datanode(%v), err(%v)", dataNode.Addr, err)
		}
	}

	log.LogDebugf("updateNodeMetric. datanode id %v addr %v total %v used %v avaliable %v", dataNode.ID, dataNode.Addr,
		dataNode.Total, dataNode.Used, dataNode.AvailableSpace)
}

func (dataNode *DataNode) getDataNodeOpLog() []proto.OpLog {
	dataNodeOpLogs := make([]proto.OpLog, 0)
	opCounts := make(map[string]int32)
	for _, opLog := range dataNode.DiskOpLogs {
		opCounts[opLog.Op] += opLog.Count
	}

	for op, count := range opCounts {
		dataNodeOpLogs = append(dataNodeOpLogs, proto.OpLog{
			Name:  dataNode.Addr,
			Op:    op,
			Count: count,
		})
	}
	return dataNodeOpLogs
}

func (dataNode *DataNode) getVolOpLog(c *Cluster, volName string) []proto.OpLog {
	volOpLogs := make([]proto.OpLog, 0)
	for _, opLog := range dataNode.DpOpLogs {
		parts := strings.Split(opLog.Name, "_")
		dpId, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			log.LogErrorf("Failed to parse DP ID from %s: %v", opLog.Name, err)
			continue
		}

		dp, err := c.getDataPartitionByID(dpId)
		if err != nil {
			log.LogErrorf("Partition with ID %d not found", dpId)
			continue
		}

		if dp.VolName != volName {
			continue
		}

		newName := fmt.Sprintf("%s_%d", dp.VolName, dpId)
		volOpLogs = append(volOpLogs, proto.OpLog{
			Name:  newName,
			Op:    opLog.Op,
			Count: opLog.Count,
		})
	}
	return volOpLogs
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

func (dataNode *DataNode) IsWriteAble() (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()
	return dataNode.isWriteAbleWithSizeNoLock(10 * util.GB)
}

func (dataNode *DataNode) availableDiskCount() (cnt int) {
	for _, disk := range dataNode.AllDisks {
		if ok := dataNode.checkDecommissionedDisks(disk); !ok {
			cnt++
		}
	}
	return
}

func (dataNode *DataNode) canAllocDp() bool {
	if !dataNode.IsWriteAble() {
		return false
	}

	if dataNode.ToBeOffline {
		log.LogWarnf("action[canAllocDp] dataNode [%v] is offline ", dataNode.Addr)
		return false
	}

	// compatible with 3.4.0 before
	if len(dataNode.AllDisks) != 0 {
		if cnt := dataNode.availableDiskCount(); cnt == 0 {
			log.LogWarnf("action[canAllocDp] dataNode [%v] availableDiskCount is 0 ", dataNode.Addr)
			return false
		}
	}

	if !dataNode.PartitionCntLimited() {
		return false
	}

	return true
}

func (dataNode *DataNode) GetPartitionLimitCnt() uint32 {
	return uint32(dataNode.DpCntLimit.GetCntLimit())
}

func (dataNode *DataNode) GetAvailableSpace() uint64 {
	return dataNode.AvailableSpace
}

func (dataNode *DataNode) PartitionCntLimited() bool {
	limited := dataNode.DataPartitionCount <= dataNode.GetPartitionLimitCnt()
	if !limited {
		log.LogInfof("dpCntInLimit: dp count is already over limit for node %s, cnt %d, limit %d",
			dataNode.Addr, dataNode.DataPartitionCount, dataNode.GetPartitionLimitCnt())
	}
	return limited
}

func (dataNode *DataNode) GetStorageInfo() string {
	return fmt.Sprintf("data node(%v) cannot alloc dp, total space(%v) avaliable space(%v) used space(%v), offline(%v), avaliable disk cnt(%v), dp count(%v), over sold(%v))",
		dataNode.GetAddr(), dataNode.GetTotal(), dataNode.GetTotal()-dataNode.GetUsed(), dataNode.GetUsed(),
		dataNode.ToBeOffline, dataNode.availableDiskCount(), dataNode.DataPartitionCount, !dataNode.canAlloc())
}

func (dataNode *DataNode) isWriteAbleWithSizeNoLock(size uint64) (ok bool) {
	if dataNode.isActive == true && dataNode.AvailableSpace > size && !dataNode.RdOnly &&
		dataNode.Total > dataNode.Used && (dataNode.Total-dataNode.Used) > size {
		ok = true
	}
	if !ok {
		log.LogInfof("node %v, isActive %v, RdOnly %v, Total %v AvailableSpace %v, "+
			"used %v, dp cnt %v required size %v",
			dataNode.Addr, dataNode.isActive, dataNode.RdOnly, dataNode.Total, dataNode.AvailableSpace, dataNode.Used,
			dataNode.DataPartitionCount, size)
	}

	return
}

func (dataNode *DataNode) isWriteAbleWithSize(size uint64) (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	return dataNode.isWriteAbleWithSizeNoLock(size)
}

func (dataNode *DataNode) GetUsed() uint64 {
	dataNode.RLock()
	defer dataNode.RUnlock()
	return dataNode.Used
}

func (dataNode *DataNode) GetTotal() uint64 {
	dataNode.RLock()
	defer dataNode.RUnlock()
	return dataNode.Total
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

func (dataNode *DataNode) GetZoneName() string {
	dataNode.RLock()
	defer dataNode.RUnlock()
	return dataNode.ZoneName
}

// SelectNodeForWrite implements "SelectNodeForWrite" in the Node interface
func (dataNode *DataNode) SelectNodeForWrite() {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.UsageRatio = float64(dataNode.Used) / float64(dataNode.Total)
	dataNode.SelectedTimes++
}

func (dataNode *DataNode) clean() {
	dataNode.TaskManager.exitCh <- struct{}{}
}

func (dataNode *DataNode) createHeartbeatTask(masterAddr string, enableDiskQos bool,
	dpBackupTimeout string, forbiddenWriteOpVerBitmask bool,
) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:             time.Now().Unix(),
		MasterAddr:           masterAddr,
		VolDpRepairBlockSize: make(map[string]uint64),
	}
	request.EnableDiskQos = enableDiskQos
	request.QosIopsReadLimit = dataNode.QosIopsRLimit
	request.QosIopsWriteLimit = dataNode.QosIopsWLimit
	request.QosFlowReadLimit = dataNode.QosFlowRLimit
	request.QosFlowWriteLimit = dataNode.QosFlowWLimit
	request.DecommissionDisks = dataNode.getDecommissionedDisks()
	request.DpBackupTimeout = dpBackupTimeout
	request.NotifyForbidWriteOpOfProtoVer0 = forbiddenWriteOpVerBitmask

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

func (dataNode *DataNode) checkDecommissionedDisks(d string) (ok bool) {
	_, ok = dataNode.DecommissionedDisks.Load(d)
	return
}

func (dataNode *DataNode) updateDecommissionStatus(c *Cluster, debug, persist bool) (uint32, float64) {
	var (
		totalDisk      = len(dataNode.DecommissionDiskList)
		markDiskNum    = 0
		successDiskNum = 0
		failedDiskNum  = 0
		cancelDiskNum  = 0
		markDisks      = make([]string, 0)
		successDisks   = make([]string, 0)
		failedDisks    = make([]string, 0)
		cancelDisks    = make([]string, 0)
		progress       float64
		status         = dataNode.GetDecommissionStatus()
	)
	// prevent data node from marking decommission
	if status == DecommissionInitial {
		return DecommissionInitial, float64(0)
	}
	if status == markDecommission {
		return markDecommission, float64(0)
	}
	// if dataNode.GetDecommissionStatus() == DecommissionSuccess {
	//	return DecommissionSuccess, float64(1)
	// }
	// if dataNode.GetDecommissionStatus() == DecommissionPause {
	//	return DecommissionPause, float64(0)
	// }
	// // trigger error when try to decommission dataNode
	// if dataNode.GetDecommissionStatus() == DecommissionFail && dataNode.DecommissionDpTotal == 0 {
	//	return DecommissionFail, float64(0)
	// }

	// if no disk to decommission when executing TryDecommissionDataNode(running or success or fail)
	if totalDisk == 0 {
		if status == DecommissionFail {
			return DecommissionFail, float64(0)
		} else {
			return DecommissionSuccess, float64(1)
		}
	}
	dataNode.DecommissionSyncMutex.Lock()
	defer dataNode.DecommissionSyncMutex.Unlock()

	defer func() {
		if persist {
			c.syncUpdateDataNode(dataNode)
		}
	}()
	log.LogDebugf("action[updateDecommissionStatus]dataNode %v diskList %v",
		dataNode.Addr, dataNode.DecommissionDiskList)

	// if totalDisk == 0 {
	//	dataNode.SetDecommissionStatus(DecommissionInitial)
	//	return DecommissionInitial, float64(0)
	// }
	for _, disk := range dataNode.DecommissionDiskList {
		key := fmt.Sprintf("%s_%s", dataNode.Addr, disk)
		// if not found, may already success, so only care running disk
		if value, ok := c.DecommissionDisks.Load(key); ok {
			dd := value.(*DecommissionDisk)
			status, diskProgress := dd.updateDecommissionStatus(c, debug, persist)
			if status == DecommissionSuccess {
				successDiskNum++
				successDisks = append(successDisks, dd.DiskPath)
			} else if status == markDecommission {
				markDiskNum++
				markDisks = append(markDisks, dd.DiskPath)
			} else if status == DecommissionFail {
				failedDiskNum++
				failedDisks = append(failedDisks, dd.DiskPath)
			} else if status == DecommissionCancel {
				cancelDiskNum++
				cancelDisks = append(cancelDisks, dd.DiskPath)
			}
			progress += diskProgress
		} else {
			successDiskNum++ // disk with DecommissionSuccess will be removed from cache
			progress += float64(1)
		}

	}
	// all disk is waiting for token
	if markDiskNum == totalDisk && markDiskNum != 0 {
		return DecommissionRunning, float64(0)
	}
	if debug {
		log.LogInfof("action[updateDecommissionStatus] dataNode[%v] progress[%v] DecommissionDiskNum[%v] "+
			"DecommissionDisks %v  markDiskNum[%v] %v  successDiskNum[%v] %v failedDiskNum[%v] %v  cancelDiskNum[%v] %v",
			dataNode.Addr, progress/float64(totalDisk), len(dataNode.DecommissionDiskList), dataNode.DecommissionDiskList, markDiskNum,
			markDisks, successDiskNum, successDisks, failedDiskNum, failedDisks, cancelDiskNum, cancelDisks)
	}
	if successDiskNum+failedDiskNum+cancelDiskNum == totalDisk {
		if successDiskNum == totalDisk {
			if persist {
				dataNode.SetDecommissionStatus(DecommissionSuccess)
			}
			return DecommissionSuccess, float64(1)
		}
		if cancelDiskNum != 0 {
			if persist {
				dataNode.SetDecommissionStatus(DecommissionCancel)
			} else {
				return DecommissionCancel, progress / float64(totalDisk)
			}
		} else {
			if persist {
				dataNode.SetDecommissionStatus(DecommissionFail)
			} else {
				return DecommissionFail, progress / float64(totalDisk)
			}
		}
	}
	return dataNode.GetDecommissionStatus(), progress / float64(totalDisk)
}

func (dataNode *DataNode) GetLatestDecommissionDataPartition(c *Cluster) (partitions []*DataPartition) {
	log.LogDebugf("action[GetLatestDecommissionDataPartition]dataNode %v diskList %v", dataNode.Addr, dataNode.DecommissionDiskList)
	for _, disk := range dataNode.DecommissionDiskList {
		key := fmt.Sprintf("%s_%s", dataNode.Addr, disk)
		// if not found, may already success, so only care running disk
		if value, ok := c.DecommissionDisks.Load(key); ok {
			dd := value.(*DecommissionDisk)
			dps := c.getAllDecommissionDataPartitionByDiskAndTerm(dd.SrcAddr, dd.DiskPath, dd.DecommissionTerm)
			partitions = append(partitions, dps...)
			dpIds := make([]uint64, 0)
			for _, dp := range dps {
				dpIds = append(dpIds, dp.PartitionID)
			}
			log.LogDebugf("action[GetLatestDecommissionDataPartition]dataNode %v disk %v dps[%v]",
				dataNode.Addr, dd.DiskPath, dpIds)
		}
	}
	return
}

func (dataNode *DataNode) GetDecommissionStatus() uint32 {
	return atomic.LoadUint32(&dataNode.DecommissionStatus)
}

func (dataNode *DataNode) SetDecommissionStatus(status uint32) {
	atomic.StoreUint32(&dataNode.DecommissionStatus, status)
}

func (dataNode *DataNode) GetDecommissionFailedDPByTerm(c *Cluster) []proto.FailedDpInfo {
	var failedDps []proto.FailedDpInfo
	partitions := dataNode.GetLatestDecommissionDataPartition(c)
	log.LogDebugf("action[GetDecommissionDataNodeFailedDP] partitions len %v", len(partitions))
	for _, dp := range partitions {
		if dp.IsRollbackFailed() {
			failedDps = append(failedDps, proto.FailedDpInfo{PartitionID: dp.PartitionID, ErrMsg: dp.DecommissionErrorMessage})
			log.LogWarnf("action[GetDecommissionDataNodeFailedDP] dp[%v] failed", dp.PartitionID)
		}
	}
	log.LogWarnf("action[GetDecommissionDataNodeFailedDP] failed dp list [%v]", failedDps)
	return failedDps
}

func (dataNode *DataNode) GetDecommissionFailedDP(c *Cluster) (error, []uint64) {
	var failedDps []uint64

	partitions := c.getAllDecommissionDataPartitionByDataNode(dataNode.Addr)
	log.LogDebugf("action[GetDecommissionDataNodeFailedDP] partitions len %v", len(partitions))
	for _, dp := range partitions {
		if dp.IsDecommissionFailed() {
			failedDps = append(failedDps, dp.PartitionID)
		}
	}
	log.LogInfof("action[GetDecommissionDataNodeFailedDP] failed dp list [%v]", failedDps)
	return nil, failedDps
}

func (dataNode *DataNode) markDecommission(targetAddr string, raftForce bool, limit int) {
	dataNode.DecommissionSyncMutex.Lock()
	defer dataNode.DecommissionSyncMutex.Unlock()
	dataNode.SetDecommissionStatus(markDecommission)
	dataNode.DecommissionRaftForce = raftForce
	dataNode.DecommissionDstAddr = targetAddr
	dataNode.DecommissionLimit = limit
	dataNode.DecommissionDiskList = make([]string, 0)
}

func (dataNode *DataNode) markDecommissionSuccess(c *Cluster) {
	dataNode.SetDecommissionStatus(DecommissionSuccess)
	partitions := c.getAllDataPartitionByDataNode(dataNode.Addr)
	// if only decommission part of data partitions, can alloc dp in future
	if len(partitions) != 0 {
		dataNode.ToBeOffline = false
	}
	dataNode.DecommissionCompleteTime = time.Now().Unix()
}

func (dataNode *DataNode) markDecommissionFail() {
	dataNode.SetDecommissionStatus(DecommissionFail)
	// dataNode.ToBeOffline = false
	// dataNode.DecommissionCompleteTime = time.Now().Unix()
}

func (dataNode *DataNode) resetDecommissionStatus() {
	dataNode.SetDecommissionStatus(DecommissionInitial)
	dataNode.DecommissionRaftForce = false
	dataNode.DecommissionDstAddr = ""
	dataNode.DecommissionLimit = 0
	dataNode.DecommissionCompleteTime = 0
	dataNode.DecommissionDiskList = make([]string, 0)
	dataNode.ToBeOffline = false
}

func (dataNode *DataNode) createVersionTask(volume string, version uint64, op uint8, addr string, verList []*proto.VolVersionInfo) (task *proto.AdminTask) {
	request := &proto.MultiVersionOpRequest{
		VolumeID:   volume,
		VerSeq:     version,
		Op:         uint8(op),
		Addr:       addr,
		VolVerList: verList,
	}
	log.LogInfof("action[createVersionTask] op %v  datanode addr %v addr %v volume %v seq %v", op, dataNode.Addr, addr, volume, version)
	task = proto.NewAdminTask(proto.OpVersionOperation, dataNode.Addr, request)
	return
}

func (dataNode *DataNode) CanBePaused() bool {
	status := dataNode.GetDecommissionStatus()
	if status == DecommissionRunning || status == markDecommission || status == DecommissionPause {
		return true
	}
	return false
}

func (dataNode *DataNode) delDecommissionDiskFromCache(c *Cluster) {
	for _, diskPath := range dataNode.AllDisks {
		key := fmt.Sprintf("%s_%s", dataNode.Addr, diskPath)
		if value, ok := c.DecommissionDisks.Load(key); ok {
			disk := value.(*DecommissionDisk)
			c.DecommissionDisks.Delete(key)
			err := c.syncDeleteDecommissionDisk(disk)
			if err != nil {
				log.LogWarnf("action[delDecommissionDiskFromCache] remove %v failed %v", key, err)
			} else {
				log.LogDebugf("action[delDecommissionDiskFromCache] remove %v", key)
			}

		}
	}
}

func (dataNode *DataNode) getBackupDataPartitionIDs() (ids []uint64) {
	dataNode.RLock()
	dataNode.RUnlock()
	ids = make([]uint64, 0)
	for _, info := range dataNode.BackupDataPartitions {
		ids = append(ids, info.PartitionID)
	}
	return ids
}

func (dataNode *DataNode) getBackupDataPartitionInfo(id uint64) (proto.BackupDataPartitionInfo, error) {
	dataNode.RLock()
	dataNode.RUnlock()
	for _, info := range dataNode.BackupDataPartitions {
		if info.PartitionID == id {
			return info, nil
		}
	}
	return proto.BackupDataPartitionInfo{}, errors.NewErrorf("cannot find backup info "+
		"for dp (%v) on datanode (%v)", id, dataNode.Addr)
}

func (dataNode *DataNode) createTaskToRecoverBadDisk(diskPath string) (err error) {
	task := proto.NewAdminTask(proto.OpRecoverBadDisk, dataNode.Addr, newRecoverBadDiskRequest(diskPath))
	_, err = dataNode.TaskManager.syncSendAdminTask(task)
	return err
}

func (dataNode *DataNode) IsOffline() bool {
	// check old version dataNode
	if len(dataNode.AllDisks) == 0 {
		return dataNode.ToBeOffline
	}
	if cnt := dataNode.availableDiskCount(); cnt == 0 {
		return true
	}
	return dataNode.ToBeOffline
}

func (dataNode *DataNode) createTaskToQueryBadDiskRecoverProgress(diskPath string) (resp *proto.Packet, err error) {
	task := proto.NewAdminTask(proto.OpQueryBadDiskRecoverProgress, dataNode.Addr, newRecoverBadDiskRequest(diskPath))
	resp, err = dataNode.TaskManager.syncSendAdminTask(task)
	return resp, err
}

func (dataNode *DataNode) isBadDisk(disk string) bool {
	dataNode.RLock()
	defer dataNode.RUnlock()
	for _, entry := range dataNode.BadDisks {
		if entry == disk {
			return true
		}
	}
	return false
}

func (dataNode *DataNode) getIgnoreDecommissionDpList(c *Cluster) (dps []proto.IgnoreDecommissionDP) {
	dps = make([]proto.IgnoreDecommissionDP, 0)
	for _, disk := range dataNode.DecommissionDiskList {
		key := fmt.Sprintf("%s_%s", dataNode.Addr, disk)
		// if not found, may already success, so only care running disk
		if value, ok := c.DecommissionDisks.Load(key); ok {
			dd := value.(*DecommissionDisk)
			dps = append(dps, dd.IgnoreDecommissionDps...)
		}
	}
	return dps
}

func (dataNode *DataNode) getResidualDecommissionDpList(c *Cluster) (dps []proto.IgnoreDecommissionDP) {
	dps = make([]proto.IgnoreDecommissionDP, 0)
	for _, disk := range dataNode.DecommissionDiskList {
		key := fmt.Sprintf("%s_%s", dataNode.Addr, disk)
		// if not found, may already success, so only care running disk
		if value, ok := c.DecommissionDisks.Load(key); ok {
			dd := value.(*DecommissionDisk)
			dps = append(dps, dd.residualDecommissionDpsGetAll()...)
		}
	}
	return dps
}

func (dataNode *DataNode) createTaskToDeleteBackupDirectories(diskPath string) (resp *proto.Packet, err error) {
	task := proto.NewAdminTask(proto.OpDeleteBackupDirectories, dataNode.Addr, newDeleteBackupDirectoriesRequest(diskPath))
	resp, err = dataNode.TaskManager.syncSendAdminTask(task)
	return resp, err
}

// Copyright 2025 The CubeFS Authors.
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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

const (
	MetaPartitionInodeSize  = 512
	MetaPartitionDentrySize = 64
	MetaPartitionMemMin     = 256 * 1024 * 1024
)

type GetMigrateAddrParam struct {
	Topo         map[string]*proto.ZonePressureView
	RocksdbTopo  map[string]*proto.ZonePressureView
	ZoneName     string
	NodeSetID    uint64
	Excludes     []string
	ExcludeRacks []string
	RequestNum   int
	LeastSize    uint64
	IsRocksdb    bool
	RackLevel    proto.RackAwareLevel
}

var (
	NotEnoughResource        = fmt.Errorf("not enough resource")
	ErrClusterPlanNotRunning = fmt.Errorf("plan task is stopped by user")
	ErrClusterStop           = fmt.Errorf("cluster is stopp")
)

func (c *Cluster) FreezeEmptyMetaPartitionJob(name string, freezeList []*MetaPartition) error {
	c.mu.Lock()
	task, ok := c.cleanTask[name]
	if ok {
		if task.Status == CleanTaskFreezing || task.Status == CleanTaskBackuping {
			c.mu.Unlock()
			return fmt.Errorf("The clean task for volume(%s) is %s", name, task.Status)
		}
		task.Status = CleanTaskFreezing
		task.TaskCnt += len(freezeList)
	} else {
		task = &CleanTask{
			Name:    name,
			Status:  CleanTaskFreezing,
			TaskCnt: len(freezeList),
		}
		c.cleanTask[name] = task
	}
	c.mu.Unlock()

	go func() {
		// waiting for client to update meta partition 10 minutes.
		time.Sleep(WaitForClientUpdateTimeMin * time.Minute)

		for _, mp := range freezeList {
			// freeze meta partition.
			err := c.FreezeEmptyMetaPartition(mp, true)
			if err != nil {
				log.LogErrorf("Failed to freeze volume(%s) meta partition(%d), error: %s", name, mp.PartitionID, err.Error())
				mp.Freeze = proto.FreezeMetaPartitionInit
				mp.Status = proto.ReadWrite
				task.ResetCnt += 1
			} else {
				mp.Freeze = proto.FreezedMetaPartition
				task.FreezeCnt += 1
			}

			// store the meta partition status.
			err = c.syncUpdateMetaPartition(mp)
			if err != nil {
				log.LogErrorf("volume(%s) meta partition(%d) update failed: %s", mp.volName, mp.PartitionID, err.Error())
				continue
			}
		}

		task.Status = CleanTaskFreezed
		task.Timeout = time.Now().Add(WaitForTaskDeleteByHour * time.Hour)
		c.mu.Lock()
		if !c.Cleaning {
			go c.DeleteCleanTasks()
			c.Cleaning = true
		}
		c.mu.Unlock()
	}()

	return nil
}

func (c *Cluster) FreezeEmptyMetaPartition(mp *MetaPartition, freeze bool) error {
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		log.LogErrorf("get meta replica leader error: %s", err.Error())
		return err
	}
	task := mr.createTaskToFreezeReplica(mp.PartitionID, freeze)
	metaNode, err := c.metaNode(task.OperatorAddr)
	if err != nil {
		log.LogErrorf("failed to get metanode(%s), error: %s", task.OperatorAddr, err.Error())
		return err
	}
	_, err = metaNode.Sender.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[FreezeEmptyMetaPartition] meta partition(%d), err: %s", mp.PartitionID, err.Error())
		return err
	}

	return nil
}

func (c *Cluster) StartCleanEmptyMetaPartition(name string) error {
	c.mu.Lock()
	task, ok := c.cleanTask[name]
	if ok {
		if task.Status == CleanTaskFreezing || task.Status == CleanTaskBackuping {
			c.mu.Unlock()
			return fmt.Errorf("The clean task for volume(%s) is %s", name, task.Status)
		}
		task.Status = CleanTaskBackuping
	} else {
		task = &CleanTask{
			Name:   name,
			Status: CleanTaskBackuping,
		}
		c.cleanTask[name] = task
	}
	c.mu.Unlock()

	go func() {
		err := c.DoCleanEmptyMetaPartition(name)
		if err != nil {
			log.LogErrorf("Failed to clean volume(%s) empty meta partition, error: %s", name, err.Error())
		}

		task.Status = CleanTaskBackuped
		task.Timeout = time.Now().Add(WaitForTaskDeleteByHour * time.Hour)
		c.mu.Lock()
		if !c.Cleaning {
			go c.DeleteCleanTasks()
			c.Cleaning = true
		}
		c.mu.Unlock()
	}()

	return nil
}

func (c *Cluster) DoCleanEmptyMetaPartition(name string) error {
	c.mu.Lock()
	task, ok := c.cleanTask[name]
	if !ok {
		log.LogErrorf("Can't find clean task for volume(%s)", name)
		c.mu.Unlock()
		return fmt.Errorf("Can't find clean task for volume(%s)", name)
	}
	c.mu.Unlock()

	vol, err := c.getVol(name)
	if err != nil {
		log.LogErrorf("DoCleanEmptyMetaPartition get volume(%s) error: %s", name, err.Error())
		return err
	}

	if vol.isUnavailable() {
		log.LogInfof("volume(%s) is deleted or init failed before cleaned empty meta partitions.", name)
		return nil
	}

	deleteMaps := make(map[uint64]*MetaPartition)
	mps := vol.cloneMetaPartitionMap()
	for key, mp := range mps {
		if mp.Freeze != proto.FreezedMetaPartition {
			continue
		}

		// restore back the mp status if it is written.
		if !mp.IsEmptyToBeClean() {
			// freeze meta partition.
			err = c.FreezeEmptyMetaPartition(mp, false)
			if err != nil {
				log.LogErrorf("Failed to unfreeze volume(%s) meta partition(%d), error: %s", name, mp.PartitionID, err.Error())
				continue
			}

			mp.Freeze = proto.FreezeMetaPartitionInit
			mp.Status = proto.ReadWrite
			// store the meta partition status.
			err = c.syncUpdateMetaPartition(mp)
			if err != nil {
				log.LogErrorf("volume(%s) meta partition(%d) update failed: %s", name, mp.PartitionID, err.Error())
				continue
			}
			task.ResetCnt += 1
		} else {
			err = c.CleanEmptyMetaPartition(mp)
			if err != nil {
				log.LogErrorf("action[DoCleanEmptyMetaPartition] clean meta partition(%d) error: %s", mp.PartitionID, err.Error())
			}

			deleteMaps[key] = mp
			task.CleanCnt += 1
		}
	}

	vol.mpsLock.Lock()
	for key, val := range deleteMaps {
		c.syncDeleteMetaPartition(val)
		delete(vol.MetaPartitions, key)
	}
	vol.mpsLock.UnLock()

	return nil
}

func (c *Cluster) CleanEmptyMetaPartition(mp *MetaPartition) error {
	for _, replica := range mp.Replicas {
		task := replica.createTaskToBackupReplica(mp.PartitionID)
		metaNode, err := c.metaNode(task.OperatorAddr)
		if err != nil {
			log.LogErrorf("failed to get metanode(%s), error: %s", task.OperatorAddr, err.Error())
			continue
		}
		_, err = metaNode.Sender.syncSendAdminTask(task)
		if err != nil {
			log.LogErrorf("action[CleanEmptyMetaPartition] meta partition(%d), err: %s", mp.PartitionID, err.Error())
			continue
		}
	}

	return nil
}

func (c *Cluster) DeleteCleanTasks() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			keysToDelete := make([]string, 0, len(c.cleanTask))
			for key, task := range c.cleanTask {
				if task.Status == CleanTaskFreezing || task.Status == CleanTaskBackuping {
					continue
				}
				if task.Timeout.Before(now) {
					keysToDelete = append(keysToDelete, key)
				}
			}
			for _, key := range keysToDelete {
				delete(c.cleanTask, key)
			}
			if len(c.cleanTask) == 0 {
				c.Cleaning = false
				c.mu.Unlock()
				return
			}
			c.mu.Unlock()
		case <-c.stopc:
			return
		}
	}
}

func (c *Cluster) MetaNodeRecord(metaNode *MetaNode) *proto.MetaNodeBalanceInfo {
	mnView := &proto.MetaNodeBalanceInfo{
		ID:             metaNode.ID,
		Addr:           metaNode.Addr,
		DomainAddr:     metaNode.DomainAddr,
		ZoneName:       metaNode.ZoneName,
		Rack:           metaNode.Rack,
		NodeSetID:      metaNode.NodeSetID,
		Total:          metaNode.Total,
		Used:           metaNode.Used,
		Free:           metaNode.Total - metaNode.Used,
		Ratio:          metaNode.Ratio,
		NodeMemTotal:   metaNode.NodeMemTotal,
		NodeMemUsed:    metaNode.NodeMemUsed,
		NodeMemFree:    metaNode.NodeMemTotal - metaNode.NodeMemUsed,
		MpCount:        metaNode.MetaPartitionCount,
		MetaPartitions: metaNode.PersistenceMetaPartitions,
		InodeCount:     0,
		PlanCnt:        0,
	}
	mnView.NodeMemRatio = CaculateNodeMemoryRatio(metaNode)
	for _, mpid := range mnView.MetaPartitions {
		mp, err := c.getMetaPartitionByID(mpid)
		if err != nil {
			log.LogErrorf("Error to get meta partition by ID(%d), err: %s", mpid, err.Error())
			continue
		}
		mnView.InodeCount += mp.InodeCount
	}

	return mnView
}

func (c *Cluster) GetMetaNodePressureView() (*proto.ClusterPlan, error) {
	cView := &proto.ClusterPlan{
		Low:            make(map[string]*proto.ZonePressureView),
		RocksdbLow:     make(map[string]*proto.ZonePressureView),
		Plan:           make([]*proto.MetaBalancePlan, 0),
		Status:         PlanTaskInit,
		Mode:           proto.StoreModeMem,
		RackLevel:      c.getRackAwareLevel(),
		FailedList:     make([]uint64, 0),
		DoneNum:        0,
		RunningNum:     0,
		DoneReplicaNum: 0,
		RunReplicaNum:  0,
	}

	err := c.GetLowMemPressureTopology(cView)
	if err != nil {
		log.LogErrorf("GetLowMemPressureTopology error: %s", err.Error())
		return cView, err
	}

	err = c.CreateMetaPartitionMigratePlan(cView)
	if err != nil {
		log.LogErrorf("CreateMetaPartitionMigratePlan error: %s", err.Error())
		return cView, err
	}

	err = c.FindMigrateDestination(cView)
	if err != nil {
		log.LogErrorf("FindMigrateDestination error: %s", err.Error())
		return cView, err
	}
	cView.Total = len(cView.Plan)
	cView.UndoNum = int32(cView.Total)
	for _, plan := range cView.Plan {
		cView.TotalReplicaNum += len(plan.Plan)
	}
	cView.UndoReplicaNum = int32(cView.TotalReplicaNum)

	return cView, nil
}

func (c *Cluster) GetLowMemPressureTopology(migratePlan *proto.ClusterPlan) error {
	if migratePlan == nil || migratePlan.Low == nil || migratePlan.RocksdbLow == nil {
		err := fmt.Errorf("The migratePlan parameter is nil")
		log.LogErrorf(err.Error())
		return err
	}

	migratePlan.RackLevel = c.getRackAwareLevel()

	var zones []*Zone
	if migratePlan.SelectType == SelectTypeZoneName {
		zone, err := c.t.getZone(migratePlan.ZoneName)
		if zone == nil || err != nil {
			err := fmt.Errorf("zone %s not found", migratePlan.ZoneName)
			log.LogErrorf(err.Error())
			return err
		}
		zones = make([]*Zone, 0, 1)
		zones = append(zones, zone)
	} else {
		zones = c.t.getAllZones()
	}

	var nodeMemRatio float64
	var nsc nodeSetCollection
	for _, zone := range zones {
		zoneView := &proto.ZonePressureView{
			ZoneName: zone.name,
			Status:   zone.getStatusToString(),
			NodeSet:  make(map[uint64]*proto.NodeSetPressureView),
		}
		rocksdbZoneView := &proto.ZonePressureView{
			ZoneName: zone.name,
			Status:   zone.getStatusToString(),
			NodeSet:  make(map[uint64]*proto.NodeSetPressureView),
		}
		if migratePlan.SelectType == SelectTypeNodeSetId {
			ns, err := zone.getNodeSet(migratePlan.NodeSetID)
			if ns == nil || err != nil {
				log.LogErrorf("getNodeSet(%d) error: %s", migratePlan.NodeSetID, err.Error())
				continue
			}
			nsc = make([]*nodeSet, 0, 1)
			nsc = append(nsc, ns)
			// set the zone name with node set id.
			migratePlan.ZoneName = zone.name
		} else {
			nsc = zone.getAllNodeSet()
		}
		for _, ns := range nsc {
			nsView := &proto.NodeSetPressureView{
				NodeSetID: ns.ID,
				MetaNodes: make(map[uint64]*proto.MetaNodeBalanceInfo),
			}
			zoneView.NodeSet[ns.ID] = nsView
			rocksdbNsView := &proto.NodeSetPressureView{
				NodeSetID: ns.ID,
				MetaNodes: make(map[uint64]*proto.MetaNodeBalanceInfo),
			}
			rocksdbZoneView.NodeSet[ns.ID] = rocksdbNsView
			ns.metaNodes.Range(func(key, value interface{}) bool {
				metaNode := value.(*MetaNode)

				if migratePlan.SelectType == SelectTypeNodeAddrs {
					if metaNode.SelectTag != migratePlan.SelectTag {
						return true
					}
				}

				nodeMemRatio = CaculateNodeMemoryRatio(metaNode)
				if nodeMemRatio > gConfig.metaNodeMemHighPer {
					return true
				}

				if canAllocPartition(metaNode, MetaNodeType, 1) {
					if metaNode.Ratio <= gConfig.metaNodeMemLowPer && nodeMemRatio <= gConfig.metaNodeMemLowPer {
						mnView := c.MetaNodeRecord(metaNode)
						nsView.MetaNodes[metaNode.ID] = mnView
					}
				}

				if canAllocPartition(metaNode, RocksdbType, 1) {
					if IsRocksdbDiskUsageLow(metaNode) {
						mnView := c.MetaNodeRecord(metaNode)
						rocksdbNsView.MetaNodes[metaNode.ID] = mnView
					}
				}

				return true
			})
			nsView.Number = len(nsView.MetaNodes)
			rocksdbNsView.Number = len(rocksdbNsView.MetaNodes)
		}
		migratePlan.Lock()
		migratePlan.Low[zone.name] = zoneView
		migratePlan.RocksdbLow[zone.name] = rocksdbZoneView
		migratePlan.Unlock()
	}

	return nil
}

func (c *Cluster) CreateMetaPartitionMigratePlan(migratePlan *proto.ClusterPlan) error {
	// Get the meta node list that memory usage percent larger than metaNodeMemHighThresPer
	overLoadNodes := make([]*proto.MetaNodeBalanceInfo, 0)
	var nodeMemRatio float64
	c.metaNodes.Range(func(key, value interface{}) bool {
		metanode, ok := value.(*MetaNode)
		if !ok {
			return true
		}
		nodeMemRatio = CaculateNodeMemoryRatio(metanode)
		if metanode.Ratio >= gConfig.metaNodeMemHighPer || nodeMemRatio >= gConfig.metaNodeMemHighPer {
			metaRecord := c.MetaNodeRecord(metanode)
			overLoadNodes = append(overLoadNodes, metaRecord)
		}

		return true
	})

	err := CalculateMetaNodeEstimate(overLoadNodes)
	if err != nil {
		log.LogErrorf("CalculateMetaNodeEstimate err: %s", err.Error())
		return err
	}

	for _, metaNode := range overLoadNodes {
		err = c.AddMetaPartitionIntoPlan(metaNode, migratePlan, overLoadNodes)
		if err != nil {
			log.LogErrorf("Error to add meta partition into plan: %s", err.Error())
			continue
		}
	}

	return nil
}

func CalculateMetaNodeEstimate(overLoadNodes []*proto.MetaNodeBalanceInfo) error {
	for _, metaNode := range overLoadNodes {
		if metaNode.Ratio <= 0 || metaNode.NodeMemRatio <= 0 {
			err := fmt.Errorf("The meta node ratio (%f) is <= 0", metaNode.Ratio)
			log.LogErrorf(err.Error())
			return err
		}
		if metaNode.Ratio > metaNode.NodeMemRatio {
			metaNode.Estimate = int((metaNode.Ratio - gConfig.metaNodeMemHighPer) / metaNode.Ratio * float64(metaNode.MpCount))
		} else {
			metaNode.Estimate = int((metaNode.NodeMemRatio - gConfig.metaNodeMemHighPer) / metaNode.NodeMemRatio * float64(metaNode.MpCount))
		}
		metaNode.Estimate += 1
		if metaNode.Estimate <= 0 {
			log.LogWarnf("the calculate estimate(%d) is forced to 1", metaNode.Estimate)
			metaNode.Estimate = 1
		}
	}

	return nil
}

func (c *Cluster) AddMetaPartitionIntoPlan(metaNode *proto.MetaNodeBalanceInfo, migratePlan *proto.ClusterPlan, overLoadNodes []*proto.MetaNodeBalanceInfo) error {
	if metaNode.PlanCnt >= metaNode.Estimate || migratePlan.Total >= MaxMpMigrateNum {
		return nil
	}

	safeVols := c.allVols()

	// get the copied meta partition list.
	mps := c.getAllMetaPartitionsByMetaNode(metaNode.Addr)
	// Sort the meta partitions by inode count.
	sort.Slice(mps, func(i, j int) bool { return mps[i].InodeCount >= mps[j].InodeCount })

	for _, mp := range mps {
		// The meta partition is in plan list already.
		if CheckMetaPartitionInPlan(mp, migratePlan) {
			continue
		}

		if (migratePlan.Type == ManualPlan || migratePlan.Type == AutoPlan) && checkMetaReplicasIsRocksdb(mp) {
			continue
		}

		mpPlan := &proto.MetaBalancePlan{
			ID:         mp.PartitionID,
			Original:   make([]*proto.MrBalanceInfo, 0, len(mp.Replicas)),
			OverLoad:   make([]*proto.MrBalanceInfo, 0, len(mp.Replicas)),
			Plan:       make([]*proto.MrBalanceInfo, 0, len(mp.Replicas)),
			InodeCount: mp.InodeCount,
			PlanNum:    0,
		}

		for _, mr := range mp.Replicas {
			mn, err := c.metaNode(mr.Addr)
			if err != nil {
				log.LogErrorf("Failed to get meta node(%s), err: %s", mr.Addr, err.Error())
				return err
			}
			storeMode, err := c.getMetaPartitionStoreMode(mp, mr.Addr)
			if err != nil {
				log.LogErrorf("Failed to get meta replica store mode, err: %s", err.Error())
				return err
			}
			mrRec := GetMetaReplicaRecord(mn)
			mrRec.StoreMode = storeMode
			mpPlan.Original = append(mpPlan.Original, mrRec)

			if !CheckMetaReplicaIsOverLoad(mr, overLoadNodes) {
				continue
			}

			mpPlan.OverLoad = append(mpPlan.OverLoad, mrRec)
			mpPlan.PlanNum += 1
		}
		if mpPlan.PlanNum <= 0 {
			continue
		}
		// Update the meta node plan count.
		err := UpdateMetaReplicaPlanCount(mpPlan, overLoadNodes)
		if err != nil {
			log.LogErrorf("Error to update meta node plan count: %s", err.Error())
			return err
		}

		// Get the CrossZone value.
		mpPlan.CrossZone = GetVolumeCrossZone(safeVols, mpPlan)

		migratePlan.Plan = append(migratePlan.Plan, mpPlan)
		migratePlan.Total++

		if metaNode.PlanCnt >= metaNode.Estimate || migratePlan.Total >= MaxMpMigrateNum {
			break
		}
	}

	return nil
}

func GetMetaReplicaRecord(metaNode *MetaNode) *proto.MrBalanceInfo {
	ret := &proto.MrBalanceInfo{
		Source:       metaNode.Addr,
		SrcNodeSetId: metaNode.NodeSetID,
		SrcZoneName:  metaNode.ZoneName,
		Status:       PlanTaskInit,
		SrcRack:      metaNode.Rack,
	}
	return ret
}

func CheckMetaPartitionInPlan(mp *MetaPartition, migratePlan *proto.ClusterPlan) bool {
	for _, planItem := range migratePlan.Plan {
		if mp.PartitionID == planItem.ID {
			return true
		}
	}

	return false
}

func CheckMetaReplicaIsOverLoad(mr *MetaReplica, overLoadNodes []*proto.MetaNodeBalanceInfo) bool {
	for _, node := range overLoadNodes {
		if mr.Addr == node.Addr {
			return true
		}
	}

	return false
}

func GetVolumeCrossZone(vols map[string]*Vol, mpPlan *proto.MetaBalancePlan) bool {
	for _, vol := range vols {
		for _, entry := range vol.MetaPartitions {
			if entry.PartitionID == mpPlan.ID {
				return vol.crossZone
			}
		}
	}

	return false
}

func UpdateMetaReplicaPlanCount(mpPlan *proto.MetaBalancePlan, overLoadNodes []*proto.MetaNodeBalanceInfo) error {
	for _, mrRec := range mpPlan.OverLoad {
		for _, node := range overLoadNodes {
			if mrRec.Source == node.Addr {
				// Add the meta node plan count by 1.
				node.PlanCnt += 1

				if node.InodeCount > 0 {
					// Update the meta replica source memory size at the same time.
					mrRec.SrcMemSize = uint64(float64(mpPlan.InodeCount) / float64(node.InodeCount) * float64(node.Used))
				}
				break
			}
		}
	}

	return nil
}

func (c *Cluster) FindMigrateDestination(migratePlan *proto.ClusterPlan) (err error) {
	for i, mp := range migratePlan.Plan {
		if mp.CrossZone {
			err = FindMigrateDestRetainZone(migratePlan, mp)
		} else {
			err = FindMigrateDestInOneNodeSet(migratePlan, mp)
		}
		if err == NotEnoughResource {
			log.LogWarnf("Analyze the meta nodes:")
			c.AnalyzeMetaNodes(migratePlan.Mode)

			if i <= 0 {
				migratePlan.Msg = fmt.Sprintf("require to migrate (%d) mp, but not create plan", len(migratePlan.Plan))
				log.LogErrorf(migratePlan.Msg)
				return
			}

			migratePlan.Msg = fmt.Sprintf("require to migrate (%d) mp, only create (%d) plan", len(migratePlan.Plan), i)
			migratePlan.Plan = migratePlan.Plan[:i]
			log.LogWarnf(migratePlan.Msg)
			return nil
		} else if err != nil {
			log.LogErrorf("Fail to find reasonable metanode to create plan: %s", err.Error())
			return err
		}
	}

	return nil
}

func FindMigrateDestRetainZone(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan) error {
	// clean all the planed value.
	mpPlan.Plan = []*proto.MrBalanceInfo{}

	for _, highPressure := range mpPlan.OverLoad {
		// If it in the plan array. Skip it.
		done := false
		for _, item := range mpPlan.Plan {
			if item.Source == highPressure.Source {
				done = true
				break
			}
		}
		if done {
			continue
		}

		srcNode := GetOverLoadNodeArray(mpPlan, highPressure)
		err := CreateMigratePlanInNodeSet(migratePlan, mpPlan, srcNode)
		if err == nil {
			// done.
			continue
		}

		srcNode = GetSameNodeSetArray(mpPlan, highPressure)
		err = CreateMigratePlanExcludeNodeSet(migratePlan, mpPlan, srcNode)
		if err != nil {
			log.LogErrorf("CreateMigratePlanExcludeNodeSet error: %s", err.Error())
			return err
		}
	}

	return nil
}

func GetOverLoadNodeArray(mpPlan *proto.MetaBalancePlan, mrRec *proto.MrBalanceInfo) []*proto.MrBalanceInfo {
	ret := make([]*proto.MrBalanceInfo, 0, len(mpPlan.OverLoad))
	for _, entry := range mpPlan.OverLoad {
		if entry.SrcNodeSetId == mrRec.SrcNodeSetId {
			ret = append(ret, entry)
		}
	}

	return ret
}

func CreateMigratePlanInNodeSet(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan, srcNode []*proto.MrBalanceInfo) error {
	if len(srcNode) <= 0 {
		return nil
	}

	var maxMemSize uint64
	for _, node := range srcNode {
		if maxMemSize < node.SrcMemSize {
			maxMemSize = node.SrcMemSize
		}
	}
	isRocksdb := CheckStoreModeIsRocksdb(migratePlan, mpPlan)

	getParam := &GetMigrateAddrParam{
		Topo:        migratePlan.Low,
		RocksdbTopo: migratePlan.RocksdbLow,
		ZoneName:    srcNode[0].SrcZoneName,
		NodeSetID:   srcNode[0].SrcNodeSetId,
		Excludes:    make([]string, 0),
		RequestNum:  len(srcNode),
		LeastSize:   maxMemSize,
		IsRocksdb:   isRocksdb,
		RackLevel:   migratePlan.RackLevel,
	}
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// try to find one meta node from the same node set.
	find, dests := GetMigrateDestAddr(getParam)
	if !find {
		return fmt.Errorf("Can't find request num (%d) free nodes from the nodeset(%d)", getParam.RequestNum, getParam.NodeSetID)
	}

	err := FillMigratePlanArray(migratePlan, mpPlan, srcNode, dests, isRocksdb)
	if err != nil {
		log.LogErrorf("FillMigratePlanArray error: %s", err.Error())
		return err
	}

	return nil
}

func GetSameNodeSetArray(mpPlan *proto.MetaBalancePlan, mrRec *proto.MrBalanceInfo) []*proto.MrBalanceInfo {
	ret := make([]*proto.MrBalanceInfo, 0, len(mpPlan.Original))
	for _, entry := range mpPlan.Original {
		if entry.SrcNodeSetId == mrRec.SrcNodeSetId {
			ret = append(ret, entry)
		}
	}

	return ret
}

func CreateMigratePlanExcludeNodeSet(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan, srcNode []*proto.MrBalanceInfo) error {
	if len(srcNode) <= 0 {
		return nil
	}

	var maxMemSize uint64
	for _, node := range srcNode {
		if maxMemSize < node.SrcMemSize {
			maxMemSize = node.SrcMemSize
		}
	}
	isRocksdb := CheckStoreModeIsRocksdb(migratePlan, mpPlan)

	getParam := &GetMigrateAddrParam{
		Topo:        migratePlan.Low,
		RocksdbTopo: migratePlan.RocksdbLow,
		ZoneName:    srcNode[0].SrcZoneName,
		NodeSetID:   srcNode[0].SrcNodeSetId,
		Excludes:    make([]string, 0),
		RequestNum:  len(srcNode),
		LeastSize:   maxMemSize,
		IsRocksdb:   isRocksdb,
		RackLevel:   migratePlan.RackLevel,
	}
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// try to find new meta node from the same zone.
	find, dests := GetMigrateAddrExcludeNodeSet(getParam)
	if !find {
		log.LogErrorf("Can't find %d free nodes from the zone(%s)", getParam.RequestNum, getParam.ZoneName)
		log.LogWarnf("Display the failed details:")
		DisplayPlanFailedDetails(getParam, mpPlan, migratePlan, isRocksdb)
		return NotEnoughResource
	}

	err := FillMigratePlanArray(migratePlan, mpPlan, srcNode, dests, isRocksdb)
	if err != nil {
		log.LogErrorf("FillMigratePlanArray error: %s", err.Error())
		return err
	}

	return nil
}

func FillMigratePlanArray(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan, srcNode []*proto.MrBalanceInfo, dests []*proto.MrBalanceInfo, isRocksdb bool) error {
	storeMode := proto.StoreModeMem
	if isRocksdb {
		storeMode = proto.StoreModeRocksDb
	}
	for i := 0; i < len(srcNode); i++ {
		var item *proto.MrBalanceInfo
		index, bExist := SrcIsPlaned(mpPlan, srcNode[i].Source)
		if bExist {
			// Overwrite the destination info.
			mpPlan.Plan[index].Destination = dests[i].Destination
			mpPlan.Plan[index].DstId = dests[i].DstId
			mpPlan.Plan[index].DstNodeSetId = dests[i].DstNodeSetId
			mpPlan.Plan[index].DstZoneName = dests[i].DstZoneName
			mpPlan.Plan[index].DstRack = dests[i].DstRack
			mpPlan.Plan[index].StoreMode = storeMode
			item = mpPlan.Plan[index]
		} else {
			item = &proto.MrBalanceInfo{
				Source:       srcNode[i].Source,
				SrcRack:      srcNode[i].SrcRack,
				SrcMemSize:   srcNode[i].SrcMemSize,
				SrcNodeSetId: srcNode[i].SrcNodeSetId,
				SrcZoneName:  srcNode[i].SrcZoneName,
				Destination:  dests[i].Destination,
				DstId:        dests[i].DstId,
				DstNodeSetId: dests[i].DstNodeSetId,
				DstZoneName:  dests[i].DstZoneName,
				DstRack:      dests[i].DstRack,
				Status:       PlanTaskInit,
				StoreMode:    storeMode,
			}
			mpPlan.Plan = append(mpPlan.Plan, item)
		}
		// rocksdb don't update the topology. It checks the topology realtime.
		if isRocksdb {
			continue
		}
		// Update the low pressure topology
		err := UpdateLowPressureNodeTopo(migratePlan, item)
		if err != nil {
			log.LogErrorf("UpdateLowPressureNodeTopo error: %s", err.Error())
			return err
		}
	}

	return nil
}

func SrcIsPlaned(mpPlan *proto.MetaBalancePlan, srcAddr string) (int, bool) {
	for index, item := range mpPlan.Plan {
		if item.Source == srcAddr {
			return index, true
		}
	}
	return -1, false
}

func UpdateLowPressureNodeTopo(migratePlan *proto.ClusterPlan, newPlan *proto.MrBalanceInfo) error {
	migratePlan.Lock()
	defer migratePlan.Unlock()

	zone, ok := migratePlan.Low[newPlan.DstZoneName]
	if !ok {
		return fmt.Errorf("Error to get destination zone: %s", newPlan.DstZoneName)
	}

	nodeSet, ok := zone.NodeSet[newPlan.DstNodeSetId]
	if !ok {
		return fmt.Errorf("Error to get node set %d", newPlan.DstNodeSetId)
	}

	metaNode, ok := nodeSet.MetaNodes[newPlan.DstId]
	if !ok {
		return fmt.Errorf("Error to get meta node %d", newPlan.DstId)
	}

	metaNode.Used += newPlan.SrcMemSize * metaNodeMemoryRatio
	metaNode.Free = metaNode.Total - metaNode.Used
	if metaNode.Total > 0 {
		metaNode.Ratio = float64(metaNode.Used) / float64(metaNode.Total)
	}
	metaNode.NodeMemUsed += newPlan.SrcMemSize * metaNodeMemoryRatio
	metaNode.NodeMemFree = metaNode.NodeMemTotal - metaNode.NodeMemUsed
	if metaNode.NodeMemTotal > 0 {
		metaNode.NodeMemRatio = float64(metaNode.NodeMemUsed) / float64(metaNode.NodeMemTotal)
	}

	if metaNode.Ratio >= gConfig.metaNodeMemMidPer || metaNode.NodeMemRatio >= gConfig.metaNodeMemMidPer {
		delete(nodeSet.MetaNodes, metaNode.ID)
		nodeSet.Number -= 1
	}

	return nil
}

func FillExcludeAddrIntoGetParam(mpPlan *proto.MetaBalancePlan, getParam *GetMigrateAddrParam) {
	getParam.Excludes = make([]string, 0, len(mpPlan.Original)+len(mpPlan.Plan))
	getParam.ExcludeRacks = make([]string, 0, len(mpPlan.Original)+len(mpPlan.Plan))
	for _, mrRec := range mpPlan.Original {
		getParam.Excludes = append(getParam.Excludes, mrRec.Source)

		inOverLoad := false
		for _, overLoadAddr := range mpPlan.OverLoad {
			if overLoadAddr.Source == mrRec.Source {
				inOverLoad = true
			}
		}

		if !inOverLoad {
			getParam.ExcludeRacks = append(getParam.ExcludeRacks, mrRec.SrcRack)
		}
	}

	for _, mrRec := range mpPlan.Plan {
		getParam.Excludes = append(getParam.Excludes, mrRec.Destination)
		getParam.ExcludeRacks = append(getParam.ExcludeRacks, mrRec.DstRack)
	}
}

func FindMigrateDestInOneNodeSet(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan) error {
	requestNum := len(mpPlan.OverLoad)
	if requestNum <= 0 {
		return fmt.Errorf("The high memory pressure meta node list is null")
	}
	var maxMemSize uint64
	for _, item := range mpPlan.OverLoad {
		if item.SrcMemSize > maxMemSize {
			maxMemSize = item.SrcMemSize
		}
	}
	isRocksdb := CheckStoreModeIsRocksdb(migratePlan, mpPlan)

	mpPlan.Plan = []*proto.MrBalanceInfo{}
	getParam := &GetMigrateAddrParam{
		Topo:        migratePlan.Low,
		RocksdbTopo: migratePlan.RocksdbLow,
		ZoneName:    mpPlan.OverLoad[0].SrcZoneName,
		NodeSetID:   mpPlan.OverLoad[0].SrcNodeSetId,
		Excludes:    make([]string, 0),
		RequestNum:  requestNum,
		LeastSize:   maxMemSize,
		IsRocksdb:   isRocksdb,
		RackLevel:   migratePlan.RackLevel,
	}
	FillExcludeAddrIntoGetParam(mpPlan, getParam)
	find, dests := GetMigrateDestAddr(getParam)
	if find {
		err := MigratePlanOverLoadToDest(migratePlan, mpPlan, dests, isRocksdb)
		if err != nil {
			log.LogErrorf("MigratePlanOverLoadToDest error: %s", err.Error())
			return err
		}

		return nil
	}

	FillExcludeAddrIntoGetParam(mpPlan, getParam)
	// try to find resource from the mp member which has the same store mode.
	err := TryNodeSetIdFromOtherNodes(migratePlan, mpPlan, getParam)
	if err == nil {
		return nil
	}
	log.LogWarnf("Display the failed details:")
	DisplayPlanFailedDetails(getParam, mpPlan, migratePlan, isRocksdb)

	// try the others node set under the same zone.
	FillExcludeAddrIntoGetParam(mpPlan, getParam)
	getParam.RequestNum = 3
	find, dests = GetMigrateAddrExcludeNodeSet(getParam)
	if !find {
		FillExcludeAddrIntoGetParam(mpPlan, getParam)
		find, dests = GetMigrateAddrExcludeZone(getParam)
	}
	if !find {
		log.LogWarnf("Display the failed details:")
		DisplayPlanFailedDetails(getParam, mpPlan, migratePlan, isRocksdb)
		return NotEnoughResource
	}

	err = MigratePlanOriginalToDest(migratePlan, mpPlan, dests, isRocksdb)
	if err != nil {
		log.LogErrorf("MigratePlanOriginalToDest error: %s", err.Error())
		return err
	}

	return nil
}

func MigratePlanOverLoadToDest(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan, dests []*proto.MrBalanceInfo, isRocksdb bool) error {
	srcNode := make([]*proto.MrBalanceInfo, 0, len(mpPlan.OverLoad))
	srcNode = append(srcNode, mpPlan.OverLoad...)

	err := FillMigratePlanArray(migratePlan, mpPlan, srcNode, dests, isRocksdb)
	if err != nil {
		log.LogErrorf("FillMigratePlanArray error: %s", err.Error())
		return err
	}

	return nil
}

func MigratePlanOriginalToDest(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan, dests []*proto.MrBalanceInfo, isRocksdb bool) error {
	srcNode := make([]*proto.MrBalanceInfo, 0, len(mpPlan.Original))
	srcNode = append(srcNode, mpPlan.Original...)

	err := FillMigratePlanArray(migratePlan, mpPlan, srcNode, dests, isRocksdb)
	if err != nil {
		log.LogErrorf("FillMigratePlanArray returns err: %s", err.Error())
		return err
	}

	return nil
}

func GetMigrateDestAddr(param *GetMigrateAddrParam) (find bool, address []*proto.MrBalanceInfo) {
	find = false

	// Select the corresponding topology according to the storage mode
	var zone *proto.ZonePressureView
	var ok bool

	if param.IsRocksdb {
		zone, ok = param.RocksdbTopo[param.ZoneName]
	} else {
		zone, ok = param.Topo[param.ZoneName]
	}

	if !ok {
		log.LogWarnf("Can't find zone: %s", param.ZoneName)
		return
	}

	nodeSet, ok := zone.NodeSet[param.NodeSetID]
	if !ok {
		log.LogWarnf("Can't find node set: %d", param.NodeSetID)
		return
	}

	if nodeSet.Number < param.RequestNum {
		log.LogWarnf("RequestNum: %d, but node set: %d only has %d free nodes", param.RequestNum, param.NodeSetID, nodeSet.Number)
		return
	}

	address = make([]*proto.MrBalanceInfo, 0, param.RequestNum)
	SortMetaNodes := make([]*proto.MetaNodeBalanceInfo, 0, len(nodeSet.MetaNodes))
	for _, metaNode := range nodeSet.MetaNodes {
		SortMetaNodes = append(SortMetaNodes, metaNode)
	}
	sort.Slice(SortMetaNodes, func(i, j int) bool {
		return SortMetaNodes[i].Selected < SortMetaNodes[j].Selected
	})
	rackLevel := param.RackLevel

	selectAddr := func() {
		for _, entry := range SortMetaNodes {

			if contains(param.Excludes, entry.Addr) {
				continue
			}

			if contains(param.ExcludeRacks, entry.Rack) && rackLevel == proto.RackAwareStrong {
				continue
			}

			if !param.IsRocksdb {
				// check the free memory.
				if entry.Free <= metaNodeReserveMemorySize || entry.NodeMemFree <= metaNodeReserveMemorySize {
					continue
				}
				// the free memory size is larger than 2 * source meta partition's used.
				if entry.Free <= metaNodeMemoryRatio*param.LeastSize || entry.NodeMemFree <= metaNodeMemoryRatio*param.LeastSize {
					continue
				}
			}

			param.Excludes = append(param.Excludes, entry.Addr)
			param.ExcludeRacks = append(param.ExcludeRacks, entry.Rack)

			dstVal := &proto.MrBalanceInfo{
				Destination:  entry.Addr,
				DstId:        entry.ID,
				DstNodeSetId: entry.NodeSetID,
				DstZoneName:  entry.ZoneName,
				DstRack:      entry.Rack,
				Status:       PlanTaskInit,
			}
			address = append(address, dstVal)
			if len(address) >= param.RequestNum {
				find = true
				break
			}
		}
	}

	// rack aware level is weak, so we need try to find the strong rack first.
	if rackLevel == proto.RackAwareWeak {
		rackLevel = proto.RackAwareStrong
	}
	selectAddr()
	if find {
		for _, item := range address {
			nodeSet.MetaNodes[item.DstId].Selected += 1
		}
		return
	}

	// try to find with no rack aware.
	if param.RackLevel == proto.RackAwareWeak {
		rackLevel = proto.RackAwareNone
		selectAddr()
	}
	if find {
		for _, item := range address {
			nodeSet.MetaNodes[item.DstId].Selected += 1
		}
	}

	return
}

func GetMigrateAddrExcludeNodeSet(param *GetMigrateAddrParam) (find bool, address []*proto.MrBalanceInfo) {
	zoneMap := param.Topo
	if param.IsRocksdb {
		zoneMap = param.RocksdbTopo
	}

	zone, ok := zoneMap[param.ZoneName]
	if !ok {
		log.LogWarnf("Can't find zone: %s", param.ZoneName)
		return
	}

	for _, nodeSet := range zone.NodeSet {
		if nodeSet.NodeSetID == param.NodeSetID {
			continue
		}
		excludes := append([]string(nil), param.Excludes...)
		excludeRacks := append([]string(nil), param.ExcludeRacks...)
		newParam := &GetMigrateAddrParam{
			Topo:         param.Topo,
			RocksdbTopo:  param.RocksdbTopo,
			ZoneName:     param.ZoneName,
			NodeSetID:    nodeSet.NodeSetID,
			Excludes:     excludes,
			ExcludeRacks: excludeRacks,
			RequestNum:   param.RequestNum,
			LeastSize:    param.LeastSize,
			IsRocksdb:    param.IsRocksdb,
			RackLevel:    param.RackLevel,
		}
		find, address = GetMigrateDestAddr(newParam)
		if find {
			return
		}
	}

	log.LogWarnf("requestNum(%d) zone(%s) not enough resource in the same zone.", param.RequestNum, param.ZoneName)
	find = false
	return
}

func GetMigrateAddrExcludeZone(param *GetMigrateAddrParam) (find bool, address []*proto.MrBalanceInfo) {
	zoneMap := param.Topo
	if param.IsRocksdb {
		zoneMap = param.RocksdbTopo
	}

	for _, zone := range zoneMap {
		if zone.ZoneName == param.ZoneName {
			continue
		}

		for _, nodeSet := range zone.NodeSet {
			excludes := append([]string(nil), param.Excludes...)
			excludeRacks := append([]string(nil), param.ExcludeRacks...)
			newParam := &GetMigrateAddrParam{
				Topo:         param.Topo,
				RocksdbTopo:  param.RocksdbTopo,
				ZoneName:     zone.ZoneName,
				NodeSetID:    nodeSet.NodeSetID,
				Excludes:     excludes,
				ExcludeRacks: excludeRacks,
				RequestNum:   param.RequestNum,
				LeastSize:    param.LeastSize,
				IsRocksdb:    param.IsRocksdb,
				RackLevel:    param.RackLevel,
			}
			find, address = GetMigrateDestAddr(newParam)
			if find {
				return
			}
		}
	}

	log.LogWarnf("RequestNum(%d) not enough resource in other zones.", param.RequestNum)
	find = false
	return
}

func (c *Cluster) UpdateMigrateDestination(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan) error {
	// Renew the low pressure memory topology.
	migratePlan.Lock()
	migratePlan.Low = make(map[string]*proto.ZonePressureView)
	migratePlan.RocksdbLow = make(map[string]*proto.ZonePressureView)
	migratePlan.Unlock()

	err := c.GetLowMemPressureTopology(migratePlan)
	if err != nil {
		log.LogErrorf("GetLowMemPressureTopology error: %s", err.Error())
		return err
	}

	if migratePlan.Type == AddLearner {
		param := &MetaPartitionPlanUserParams{
			Mode:               migratePlan.Mode,
			Count:              migratePlan.ModeCnt,
			StartID:            migratePlan.StartId,
			EndID:              migratePlan.EndId,
			SelectType:         migratePlan.SelectType,
			ZoneName:           migratePlan.ZoneName,
			NodeSetID:          migratePlan.NodeSetID,
			SelectTag:          migratePlan.SelectTag,
			AutoPromoteLearner: migratePlan.AutoPromote,
		}
		err = c.AddLearnerToDestination(migratePlan, mpPlan, param)
		if err != nil {
			log.LogErrorf("AddLearnerToDestination error: %s", err.Error())
			return err
		}

		return nil
	}

	// renew the planed destination meta node.
	if mpPlan.CrossZone {
		err = FindMigrateDestRetainZone(migratePlan, mpPlan)
		if err != nil {
			log.LogErrorf("FindMigrateDestRetainZone error: %s", err.Error())
			return err
		}
	} else {
		err = FindMigrateDestInOneNodeSet(migratePlan, mpPlan)
		if err != nil {
			log.LogErrorf("FindMigrateDestInOneNodeSet error: %s", err.Error())
			return err
		}
	}

	return nil
}

func (c *Cluster) RunMetaPartitionBalanceTask() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.IsClusterPlanNotIdle() {
		return nil
	}

	// Get the planed balance task.
	plan, err := c.loadBalanceTask()
	if err != nil {
		log.LogErrorf("loadBalanceTask err: %s", err.Error())
		return fmt.Errorf("can't find meta partition balance task in raft storage: %s", err.Error())
	}

	plan.Status = PlanTaskRun
	plan.StartTime = time.Now()
	plan.EndTime = time.Time{}
	err = c.syncUpdateBalanceTask(plan)
	if err != nil {
		log.LogErrorf("syncUpdateBalanceTask err: %s", err.Error())
		return err
	}

	c.SetClusterPlanRunning()
	go c.DoMetaPartitionBalanceTask(plan)

	return nil
}

func (c *Cluster) DoMetaPartitionBalanceTask(plan *proto.ClusterPlan) {
	defer func() {
		// clear the run flag.
		c.SetClusterPlanIdle()
	}()

	concurrency := gConfig.mpMigrateThreads
	if concurrency <= 0 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	var (
		wg     sync.WaitGroup
		failMu sync.Mutex
	)

	var stopProcess uint32
	atomic.StoreUint32(&stopProcess, 0)
	for _, mpPlan := range plan.Plan {
		if atomic.LoadUint32(&stopProcess) != 0 {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(mpPlan *proto.MetaBalancePlan) {
			defer wg.Done()
			err := c.handleMetaPartitionPlan(plan, mpPlan)
			if err != nil {
				log.LogErrorf("mp[%v] handleMetaPartitionPlan err: %s", mpPlan.ID, err.Error())
				atomic.StoreUint32(&stopProcess, 1)
				failMu.Lock()
				plan.FailedList = append(plan.FailedList, mpPlan.ID)
				failMu.Unlock()
			}
			<-sem
		}(mpPlan)
	}
	wg.Wait()

	if c.IsClusterPlanStopping() {
		plan.Status = PlanTaskStop
		plan.Msg = "migrate plan is stopped"
	} else {
		if atomic.LoadUint32(&stopProcess) != 0 {
			plan.Status = PlanTaskError
			plan.Msg = "Stop task because some meta partition failed. Please check the detail in each msg."
		} else {
			plan.Status = PlanTaskDone
			plan.Expire = time.Now().Add(defaultPlanExpireHours * time.Hour)
		}
	}
	plan.EndTime = time.Now()

	if plan.Type == OfflinePlan && plan.Status == PlanTaskDone {
		err := c.offlineMetaNode(plan)
		if err != nil {
			log.LogErrorf("offlineMetaNode err: %s", err.Error())
			plan.Msg = err.Error()
		}
	}

	err := c.syncUpdateBalanceTask(plan)
	if err != nil {
		log.LogErrorf("syncUpdateBalanceTask err: %s", err.Error())
		plan.Msg = err.Error()
	}
}

func (c *Cluster) handleMetaPartitionPlan(plan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan) (err error) {
	if c.IsClusterPlanNotRun() {
		log.LogWarnf("plan task is stopped by user. mp[%v]", mpPlan.ID)
		return nil
	}

	if VerifyMetaReplicaPlanNotAllInit(mpPlan) {
		return nil
	}
	err = c.VerifyAllDestinationsIsLowLoad(plan, mpPlan)
	if err != nil {
		log.LogErrorf("VerifyAllDestinationsIsLowLoad err: %s", err.Error())
		mpPlan.Msg = err.Error()
		return err
	}

	mp, err := c.getMetaPartitionByID(mpPlan.ID)
	if err != nil {
		log.LogErrorf("skip rebalance meta partition(%d) error: %s", mpPlan.ID, err.Error())
		mpPlan.Msg = err.Error()
		return err
	}

	if checkPlanSourceChanged(mpPlan, mp) {
		err = fmt.Errorf("skip rebalance meta partition(%d) because source changed", mpPlan.ID)
		log.LogWarnf(err.Error())
		mpPlan.Msg = err.Error()
		return err
	}

	err = c.waitForMetaPartitionReady(mp)
	if err != nil {
		log.LogErrorf("waitForMetaPartitionReady err: %s", err.Error())
		mpPlan.Msg = err.Error()
		return err
	}

	mpPlan.StartTime = time.Now()
	atomic.AddInt32(&plan.UndoNum, -1)
	atomic.AddInt32(&plan.RunningNum, 1)
	for _, mrPlan := range mpPlan.Plan {
		atomic.AddInt32(&plan.UndoReplicaNum, -1)
		atomic.AddInt32(&plan.RunReplicaNum, 1)
		err = c.handleMetaReplicaPlan(plan, mpPlan, mp, mrPlan)
		if err != nil {
			log.LogErrorf("handleMetaReplicaPlan mp[%v] err: %s", mp.PartitionID, err.Error())
			mpPlan.Msg = err.Error()
			mrPlan.Msg = err.Error()
			mrPlan.Status = PlanTaskError
			atomic.AddInt32(&plan.RunReplicaNum, -1)
			atomic.AddInt32(&plan.ErrorReplicaNum, 1)
			atomic.AddInt32(&plan.RunningNum, -1)
			atomic.AddInt32(&plan.ErrorNum, 1)
			return err
		}
		atomic.AddInt32(&plan.RunReplicaNum, -1)
		doneReplicaNum := atomic.AddInt32(&plan.DoneReplicaNum, 1)
		plan.ProcessPercent = float64(doneReplicaNum) / float64(plan.TotalReplicaNum) * 100
	}
	atomic.AddInt32(&plan.RunningNum, -1)
	atomic.AddInt32(&plan.DoneNum, 1)
	return nil
}

func (c *Cluster) handleMetaReplicaPlan(plan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan, mp *MetaPartition, mrPlan *proto.MrBalanceInfo) (err error) {
	if c.IsClusterPlanNotRun() {
		err = fmt.Errorf("plan task is stopped by user. mp[%v]", mpPlan.ID)
		log.LogWarnf(err.Error())
		return err
	}

	// check the memory/rocksdb disk usage of destination metanode.
	overLoad, err := c.VerifyMetaNodeExceedMemMid(mrPlan.Destination, plan.Mode)
	if err != nil {
		log.LogErrorf("VerifyMetaNodeExceedMemMid err: %s", err.Error())
		return err
	}
	if overLoad {
		metaNode, err := c.metaNode(mrPlan.Destination)
		if err != nil {
			log.LogErrorf("Failed to get meta node(%s): err: %s", mrPlan.Destination, err.Error())
			return err
		}
		msg, _ := GetMetaNodeResourceReason(metaNode, plan.Mode)
		return fmt.Errorf("stop to migrate mp(%v) with reason: %s", mpPlan.ID, msg)
	}

	err = c.waitForMetaPartitionReady(mp)
	if err != nil {
		log.LogErrorf("waitForMetaPartitionReady err: %s", err.Error())
		return err
	}

	// Update raft storage.
	mrPlan.Status = PlanTaskRun
	err = c.syncUpdateBalanceTask(plan)
	if err != nil {
		log.LogErrorf("syncUpdateBalanceTask error: %s", err.Error())
		return err
	}

	err = c.doMetaPartitionMigrate(plan, mpPlan, mrPlan, mp)
	if err != nil {
		log.LogErrorf("doMetaPartitionMigrate(%d) from %s to %s error: %s", mpPlan.ID, mrPlan.Source, mrPlan.Destination, err.Error())
		return err
	}

	rstMsg := fmt.Sprintf("migrate meta partition(%d) from %s to %s", mpPlan.ID, mrPlan.Source, mrPlan.Destination)
	auditlog.LogMasterOp("handleMetaReplicaPlan", rstMsg, nil)

	// Wait for migrating done.
	err = c.WaitForMetaPartitionMigrateDone(mp, mrPlan.Destination)
	if err != nil {
		log.LogErrorf("WaitForMetaPartitionMigrateDone mpid(%d) meta replica(%s) error: %s", mpPlan.ID, mrPlan.Destination, err.Error())
		return err
	}

	// Update raft storage.
	mrPlan.Status = PlanTaskDone
	err = c.syncUpdateBalanceTask(plan)
	if err != nil {
		log.LogErrorf("syncUpdateBalanceTask error: %s", err.Error())
		return err
	}
	log.LogDebugf("Migrate meta partition(%d) from %s to %s done", mpPlan.ID, mrPlan.Source, mrPlan.Destination)
	return nil
}

func (c *Cluster) WaitForMetaPartitionMigrateDone(mp *MetaPartition, addr string) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var err error
	var ready bool
	maxRetry := CalcuMetaPartitionReadyMaxRetry(mp)
	for i := 0; i < maxRetry; i++ {
		select {
		case <-ticker.C:
			if c.IsClusterPlanNotRun() {
				return fmt.Errorf("plan task is stopped by user. mp[%v]", mp.PartitionID)
			}
			if mp.IsRecover.Load() {
				continue
			}
			if !mp.isLeaderExist() {
				continue
			}
			if mp.RecoverState != proto.RecoverStateInit {
				continue
			}
			ready, err = CheckRaftStatus(mp, addr)
			if err != nil {
				continue
			}
			if ready {
				return nil
			}
		case <-c.stopc:
			c.SetClusterPlanStopping()
			return ErrClusterStop
		}
	}
	if err != nil {
		log.LogWarnf("CheckRaftStatus err: %s", err.Error())
		return err
	}

	return fmt.Errorf("Waiting for meta partition(%d) destination(%s) retry(%d) timeout", mp.PartitionID, addr, maxRetry)
}

func (c *Cluster) VerifyMetaNodeExceedMemMid(addr string, storeMode proto.StoreMode) (bool, error) {
	metaNode, err := c.metaNode(addr)
	if err != nil {
		log.LogErrorf("Failed to get meta node(%s): err: %s", addr, err.Error())
		return false, err
	}

	nodeType := MetaNodeType
	if storeMode == proto.StoreModeRocksDb {
		nodeType = RocksdbType
	}
	if !canAllocPartition(metaNode, nodeType, 1) {
		return true, nil
	}

	if storeMode == proto.StoreModeRocksDb {
		if !IsRocksdbDiskUsageLow(metaNode) {
			return true, nil
		}
	} else {
		nodeMemRatio := CaculateNodeMemoryRatio(metaNode)
		if metaNode.Ratio >= gConfig.metaNodeMemMidPer || nodeMemRatio >= gConfig.metaNodeMemMidPer {
			return true, nil
		}
	}

	return false, nil
}

func VerifyMetaReplicaPlanNotAllInit(mpPlan *proto.MetaBalancePlan) bool {
	for _, mrPlan := range mpPlan.Plan {
		if mrPlan.Status != PlanTaskInit {
			return true
		}
	}

	return false
}

func (c *Cluster) StopMetaPartitionBalanceTask(force bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if force {
		c.SetClusterPlanIdle()
		return nil
	}

	if c.IsClusterPlanNotRun() {
		return fmt.Errorf("error: %s", c.GetClusterPlanStatusMsg())
	}

	c.SetClusterPlanStopping()
	return nil
}

func (c *Cluster) VerifyAllDestinationsIsLowLoad(plan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan) (err error) {
	overLoad := false
	// Verify the destination node memory pressure is low.
	for _, mrPlan := range mpPlan.Plan {
		overLoad, err = c.VerifyMetaNodeExceedMemMid(mrPlan.Destination, plan.Mode)
		if err != nil {
			log.LogErrorf("VerifyMetaNodeExceedMemMid err: %s", err.Error())
			return
		}
		if overLoad {
			break
		}
	}
	// If the memory is over load, update the meta replica plans.
	if overLoad {
		err = c.UpdateMigrateDestination(plan, mpPlan)
		if err != nil {
			log.LogErrorf("UpdateMigrateDestination err: %s", err.Error())
			if err == NotEnoughResource {
				log.LogWarnf("Analyze the meta nodes:")
				c.AnalyzeMetaNodes(plan.Mode)
			}
			err = fmt.Errorf("mpid(%v) error: %s", mpPlan.ID, err.Error())
			return err
		}
	}

	return nil
}

func (c *Cluster) scheduleStartBalanceTask() {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if c.partition == nil || !c.partition.IsRaftLeader() || c.IsClusterPlanNotIdle() {
					continue
				}

				err := c.RestartMetaPartitionBalanceTask()
				if err != nil && err != proto.ErrNoMpMigratePlan {
					log.LogErrorf("RestartMetaPartitionBalanceTask err: %s", err.Error())
				}
				err = c.RestartMetaPartitionCheckSumTask()
				if err != nil && err != proto.ErrNoCheckSumPlan {
					log.LogErrorf("RestartMetaPartitionCheckSumTask err: %s", err.Error())
				}
				err = c.RestartPromoteLearnerPlan()
				if err != nil && err != proto.ErrNoPromoteLearnerPlan {
					log.LogErrorf("RestartPromoteLearnerPlan err: %s", err.Error())
				}
			case <-c.stopc:
				return
			}
		}
	}()
}

func (c *Cluster) AutoCreateRunningMigratePlan() (*proto.ClusterPlan, error) {
	// check all the meta node which memory usage ratio >= high percent.
	overLoadNodes := make([]*proto.MetaNodeBalanceInfo, 0)
	var nodeMemRatio float64
	c.metaNodes.Range(func(key, value interface{}) bool {
		metanode, ok := value.(*MetaNode)
		if !ok {
			return true
		}
		nodeMemRatio = CaculateNodeMemoryRatio(metanode)

		if metanode.Ratio >= gConfig.metaNodeMemHighPer || nodeMemRatio >= gConfig.metaNodeMemHighPer {
			metaRecord := c.MetaNodeRecord(metanode)
			overLoadNodes = append(overLoadNodes, metaRecord)
		}

		return true
	})
	if len(overLoadNodes) == 0 {
		return nil, nil
	}

	// create a new meta partition migrate plan.
	plan, err := c.GetMetaNodePressureView()
	if err != nil {
		log.LogErrorf("GetMetaNodePressureView err: %s", err.Error())
		return nil, err
	}

	if plan.Total <= 0 {
		return nil, nil
	}

	plan.Type = AutoPlan
	plan.Status = PlanTaskRun
	plan.StartTime = time.Now()

	// Save into raft storage.
	err = c.syncAddBalanceTask(plan)
	if err != nil {
		log.LogErrorf("syncAddBalanceTask err: %s", err.Error())
		return nil, err
	}

	body, err := json.Marshal(plan)
	if err != nil {
		log.LogErrorf("Error to encode migrate plan: %s", err.Error())
		return nil, err
	}
	log.LogDebugf("Create meta partition auto migrating plan: %s", string(body))

	return plan, nil
}

func (c *Cluster) RestartMetaPartitionBalanceTask() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.IsClusterPlanNotIdle() {
		return nil
	}

	// Get the planed balance task.
	plan, err := c.loadBalanceTask()
	if err != nil {
		// If the raft stoarge is null, just return nil.
		if gConfig.AutoMpMigrate && err == proto.ErrNoMpMigratePlan {
			plan, err = c.AutoCreateRunningMigratePlan()
			if err != nil {
				log.LogErrorf("AutoCreateRunningMigratePlan err: %s", err.Error())
				return err
			}
			if plan == nil {
				return nil
			}
		} else {
			if err != proto.ErrNoMpMigratePlan {
				log.LogErrorf("loadBalanceTask err: %s", err.Error())
			}
			return err
		}
	} else {
		if plan.Status == PlanTaskDone {
			now := time.Now()
			if plan.Expire.Before(now) {
				err = c.syncDeleteBalanceTask()
				if err != nil {
					log.LogErrorf("syncDeleteBalanceTask err: %s", err.Error())
					return err
				}
			}

			return nil
		}
	}

	if plan.Status != PlanTaskRun {
		// No start the plan task if the status is not running.
		return nil
	}

	c.SetClusterPlanRunning()
	go c.DoMetaPartitionBalanceTask(plan)

	return nil
}

func (c *Cluster) DeleteMetaPartitionBalanceTask() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.IsClusterPlanNotIdle() {
		return fmt.Errorf("Please stop the running task before deleting it.")
	}

	// search the raft storage. Only store one plan
	err := c.syncDeleteBalanceTask()
	if err != nil {
		log.LogErrorf("syncDeleteBalanceTask err: %s", err.Error())
	}

	return err
}

func CheckRaftStatus(mp *MetaPartition, mrAddr string) (bool, error) {
	mr, err := mp.getMetaReplica(mrAddr)
	if err != nil {
		log.LogErrorf("mp[%v] getMetaReplica: %s", mp.PartitionID, err.Error())
		return false, err
	}

	task := mr.createTaskToGetRaftStatus(mp.PartitionID, len(mp.Replicas))
	response, err := mr.metaNode.Sender.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("syncSendAdminTask: %s", err.Error())
		return false, err
	}
	loadResponse := &proto.IsRaftStatusOKRequest{}
	if err = json.Unmarshal(response.Data, loadResponse); err != nil {
		log.LogErrorf("json decode error: %s", err.Error())
		return false, err
	}

	return loadResponse.Ready, nil
}

func CaculateNodeMemoryRatio(metanode *MetaNode) float64 {
	var nodeMemRatio float64
	if metanode == nil {
		return 0.0
	}

	if metanode.NodeMemTotal > 0 {
		nodeMemRatio = float64(metanode.NodeMemUsed) / float64(metanode.NodeMemTotal)
	} else {
		nodeMemRatio = 0.0
	}
	return nodeMemRatio
}

func (c *Cluster) CreateOfflineMetaNodePlan(offLineAddr string) (*proto.ClusterPlan, error) {
	cView := &proto.ClusterPlan{
		Low:            make(map[string]*proto.ZonePressureView),
		RocksdbLow:     make(map[string]*proto.ZonePressureView),
		Plan:           make([]*proto.MetaBalancePlan, 0),
		Status:         PlanTaskInit,
		RackLevel:      c.getRackAwareLevel(),
		FailedList:     make([]uint64, 0),
		DoneNum:        0,
		RunningNum:     0,
		DoneReplicaNum: 0,
		RunReplicaNum:  0,
	}

	err := c.GetLowMemPressureTopology(cView)
	if err != nil {
		log.LogErrorf("GetLowMemPressureTopology error: %s", err.Error())
		return cView, err
	}

	err = c.FillOffLineAddrToPlan(offLineAddr, cView)
	if err != nil {
		log.LogErrorf("FillOffLineAddrToPlan error: %s", err.Error())
		return cView, err
	}

	err = c.FindMigrateDestination(cView)
	if err != nil {
		log.LogErrorf("FindMigrateDestination error: %s", err.Error())
		return cView, err
	}
	cView.Total = len(cView.Plan)
	cView.UndoNum = int32(cView.Total)
	for _, plan := range cView.Plan {
		cView.TotalReplicaNum += len(plan.Plan)
	}
	cView.UndoReplicaNum = int32(cView.TotalReplicaNum)

	return cView, nil
}

func (c *Cluster) FillOffLineAddrToPlan(offLineAddr string, migratePlan *proto.ClusterPlan) (err error) {
	// Get the meta node list that memory usage percent larger than metaNodeMemHighThresPer
	overLoadNodes := make([]*proto.MetaNodeBalanceInfo, 0, 1)
	value, ok := c.metaNodes.Load(offLineAddr)
	if !ok {
		err = fmt.Errorf("Failed to load %s from c.metaNodes", offLineAddr)
		log.LogError(err.Error())
		return err
	}
	metanode, ok := value.(*MetaNode)
	if !ok {
		err = fmt.Errorf("Failed to convert to metanode for %s", offLineAddr)
		log.LogError(err.Error())
		return err
	}
	metaRecord := c.MetaNodeRecord(metanode)
	// remove all the meta partition from the off line meta node.
	metaRecord.Estimate = metanode.MetaPartitionCount
	overLoadNodes = append(overLoadNodes, metaRecord)

	for _, metaNode := range overLoadNodes {
		err = c.AddMetaPartitionIntoPlan(metaNode, migratePlan, overLoadNodes)
		if err != nil {
			log.LogErrorf("Error to add meta partition into plan: %s", err.Error())
			continue
		}
	}

	return nil
}

func (c *Cluster) offlineMetaNode(plan *proto.ClusterPlan) (err error) {
	if plan.Type != OfflinePlan {
		err = fmt.Errorf("Invalid plan type: %s", plan.Type)
		log.LogErrorf("offlineMetaNode err: %s", err.Error())
		return err
	}

	if len(plan.Plan) <= 0 {
		err = fmt.Errorf("empty plan")
		log.LogErrorf("offlineMetaNode err: %s", err.Error())
		return err
	}

	if len(plan.Plan[0].Plan) <= 0 {
		err = fmt.Errorf("empty value in plan[0]")
		log.LogErrorf("offlineMetaNode err: %s", err.Error())
		return err
	}

	if int(plan.DoneNum) != len(plan.Plan) {
		err = fmt.Errorf("plan count(%d) not equal to done num(%d)", len(plan.Plan), plan.DoneNum)
		log.LogErrorf("offlineMetaNode err: %s", err.Error())
		return err
	}

	offLineAddr := plan.Plan[0].Plan[0].Source
	err = c.DoMetaNodeOffline(offLineAddr)
	if err != nil {
		log.LogErrorf("DoMetaNodeOffline err: %s", err.Error())
		return err
	}

	return nil
}

func (c *Cluster) DoMetaNodeOffline(offLineAddr string) (err error) {
	metaNode, err := c.metaNode(offLineAddr)
	if err != nil {
		return err
	}

	metaNode.MigrateLock.Lock()
	defer metaNode.MigrateLock.Unlock()

	metaNode.ToBeOffline = true
	defer func() {
		metaNode.ToBeOffline = false
	}()

	count := c.GetMpCountByMetaNode(metaNode.Addr)
	if count != 0 {
		err = fmt.Errorf("metaNode[%s] has persistence meta partitions (%d)", metaNode.Addr, count)
		log.LogErrorf(err.Error())
		return
	}

	if err = c.syncDeleteMetaNode(metaNode); err != nil {
		msg := fmt.Sprintf("action[offlineMetaNode], clusterID[%v] node[%v] synDelMetaNode failed,err[%s]",
			c.Name, offLineAddr, err.Error())
		Warn(c.Name, msg)
		return err
	}

	c.deleteMetaNodeFromCache(metaNode)
	msg := fmt.Sprintf("action[offlineMetaNode],clusterID[%v] kickout node[%v] success", c.Name, offLineAddr)
	Warn(c.Name, msg)

	return nil
}

func (c *Cluster) changeAndCheckMetaPartitionLeader(mrPlan *proto.MrBalanceInfo, mpPlan *proto.MetaBalancePlan, mp *MetaPartition) error {
	var newLeader string
	learners := make([]string, 0, len(mp.Peers))
	for _, peer := range mp.Peers {
		if peer.Type == raftProto.PeerLearner {
			learners = append(learners, peer.Addr)
		}
	}
	for i := 0; i < CheckMetaLeaderRetry; i++ {
		leader, err := mp.getMetaReplicaLeader()
		if err != nil {
			log.LogWarnf("metapartition[%d] has no leader", mp.PartitionID)
			time.Sleep(CheckMetaLeaderInterval * time.Second)
			continue
		}
		if leader.Addr != mrPlan.Source {
			// the leader is not the source node, the meta partition can be migrated.
			log.LogInfof("metapartition[%d] leader(%s) is not mrPlan(%s)", mp.PartitionID, leader.Addr, mrPlan.Source)
			return nil
		}

		// try to change leader.
		excludeAddrs := make([]string, 0, len(learners)+1)
		excludeAddrs = append(excludeAddrs, learners...)
		excludeAddrs = append(excludeAddrs, newLeader)
		newLeader = selectOneLeaderAddr(mrPlan, mpPlan, mp, excludeAddrs)
		if newLeader == "" {
			err = fmt.Errorf("selectOneLeaderAddr mp[%d] source: %s failed", mp.PartitionID, mrPlan.Source)
			log.LogErrorf(err.Error())
			return err
		}
		err = mp.tryToChangeLeaderByHost(newLeader)
		if err != nil {
			log.LogErrorf("metapartition[%d] try to change leader to host[%s] failed, err[%s]",
				mp.PartitionID, newLeader, err.Error())
		}
		time.Sleep(CheckMetaLeaderInterval * time.Second)
	}
	leader, err := mp.getMetaReplicaLeader()
	if err != nil {
		log.LogErrorf("metapartition[%d] has no leader", mp.PartitionID)
		return err
	}
	if leader.Addr != mrPlan.Source {
		log.LogInfof("metapartition[%d] leader(%s) is not mrPlan(%s)", mp.PartitionID, leader.Addr, mrPlan.Source)
		return nil
	}
	return fmt.Errorf("Try to change leader to %s failed. leader: %s, migrate source: %s", newLeader, leader.Addr, mrPlan.Source)
}

func selectOneLeaderAddr(mrPlan *proto.MrBalanceInfo, mpPlan *proto.MetaBalancePlan, mp *MetaPartition, excludeAddrs []string) string {
	// Select one address which is not in the meta partition plan.
	for _, replica := range mp.Replicas {
		if contains(excludeAddrs, replica.Addr) {
			continue
		}
		isSelected := true
		for _, item := range mpPlan.Plan {
			if item.Source == replica.Addr {
				isSelected = false
				break
			}
		}
		if isSelected {
			return replica.Addr
		}
	}

	// Select one address which is not the current source address.
	for _, replica := range mp.Replicas {
		if contains(excludeAddrs, replica.Addr) {
			continue
		}
		if mrPlan.Source != replica.Addr {
			return replica.Addr
		}
	}

	return ""
}

func (c *Cluster) waitForMetaPartitionReady(mp *MetaPartition) error {
	if !mp.IsRecover.Load() && mp.isLeaderExist() {
		return nil
	}

	ticker := time.NewTicker(CheckMetaLeaderInterval * time.Second)
	defer ticker.Stop()

	for i := 0; i < CheckMetaLeaderRetry; i++ {
		select {
		case <-ticker.C:
			if c.IsClusterPlanNotRun() {
				return fmt.Errorf("plan task is stopped by user. mp[%v]", mp.PartitionID)
			}
			if !mp.IsRecover.Load() && mp.isLeaderExist() {
				return nil
			}
		case <-c.stopc:
			c.SetClusterPlanStopping()
			return ErrClusterStop
		}
	}

	if mp.IsRecover.Load() {
		err := fmt.Errorf("skip rebalance meta partition(%d), because it is recovering.", mp.PartitionID)
		log.LogWarn(err.Error())
		return err
	}

	if !mp.isLeaderExist() {
		err := fmt.Errorf("skip rebalance meta partition(%d), because no leader exist.", mp.PartitionID)
		log.LogWarn(err.Error())
		return err
	}

	return nil
}

func (c *Cluster) GetMpCountByMetaNode(addr string) int {
	ret := 0
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, mp := range vol.MetaPartitions {
			vol.mpsLock.RLock()
			for _, host := range mp.Hosts {
				if host == addr {
					ret += 1
					break
				}
			}
			vol.mpsLock.RUnlock()
		}
	}
	return ret
}

func checkPlanSourceChanged(mpPlan *proto.MetaBalancePlan, mp *MetaPartition) bool {
	for _, item := range mpPlan.Original {
		found := false
		for _, mr := range mp.Replicas {
			if mr.Addr == item.Source {
				found = true
				break
			}
		}
		if !found {
			return true
		}
	}

	return false
}

func verifyDestinationInMetaReplicas(mp *MetaPartition, dst string) bool {
	for _, item := range mp.Replicas {
		if item.Addr == dst {
			return true
		}
	}

	return false
}

func (c *Cluster) CalculateMetaPartitionFreezeCount(name string) (*CleanTask, error) {
	vol, err := c.getVol(name)
	if err != nil {
		log.LogErrorf("CalculateMetaPartitionFreezeCount get volume(%s) error: %s", name, err.Error())
		return nil, err
	}

	if vol.isUnavailable() {
		err = fmt.Errorf("volume(%s) is deleted or init failed before cleaned empty meta partitions.", name)
		log.LogInfof(err.Error())
		return nil, err
	}

	ret := &CleanTask{
		Name:     name,
		UnFreeze: 0,
		Freezing: 0,
		Freezed:  0,
	}
	mps := vol.cloneMetaPartitionMap()
	for _, mp := range mps {
		switch mp.Freeze {
		case proto.FreezeMetaPartitionInit:
			ret.UnFreeze += 1
		case proto.FreezingMetaPartition:
			ret.Freezing += 1
		case proto.FreezedMetaPartition:
			ret.Freezed += 1
		}
	}

	return ret, nil
}

func checkMetaReplicasIsRocksdb(mp *MetaPartition) bool {
	for _, mr := range mp.Replicas {
		if mr.StoreMode == proto.StoreModeRocksDb {
			return true
		}
	}

	return false
}

func IsRocksdbDiskUsageLow(metaNode *MetaNode) bool {
	for _, rocksdbDisk := range metaNode.RocksdbDisks {
		if rocksdbDisk.UsageRatio < gConfig.metaNodeMemLowPer {
			return true
		}
	}

	return false
}

func CheckStoreModeIsRocksdb(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan) bool {
	var isRocksdb bool
	if migratePlan.Type == ModifyStore {
		if migratePlan.Mode == proto.StoreModeRocksDb {
			isRocksdb = true
		}
	} else {
		for _, item := range mpPlan.OverLoad {
			if item.StoreMode == proto.StoreModeRocksDb {
				isRocksdb = true
				break
			}
		}
	}
	return isRocksdb
}

func GetMetaPartitionMemorySize(mp *MetaPartition) uint64 {
	estimateSize := mp.InodeCount*MetaPartitionInodeSize + mp.DentryCount*MetaPartitionDentrySize
	if estimateSize < MetaPartitionMemMin {
		return MetaPartitionMemMin
	}
	return estimateSize
}

func CalcuMetaPartitionReadyMaxRetry(mp *MetaPartition) int {
	sum := mp.InodeCount + mp.DentryCount
	if sum <= MaxInodePerMp {
		return RetryCheckStatusNum
	}
	return int(sum / MaxInodePerMp * RetryCheckStatusNum)
}

func (c *Cluster) getAllMetaPartitions() (mps map[uint64]*MetaPartition) {
	mps = make(map[uint64]*MetaPartition)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		vol.rangeMetaPartition(func(mp *MetaPartition) bool {
			mps[mp.PartitionID] = mp
			return true
		})
	}

	return mps
}

func (c *Cluster) doMetaPartitionMigrate(plan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan, mrPlan *proto.MrBalanceInfo, mp *MetaPartition) (err error) {
	log.LogWarnf("Start to migrate meta partition(%d) from %s to %s", mpPlan.ID, mrPlan.Source, mrPlan.Destination)
	for i := 0; i < RetryDoMigrateNum; i++ {
		if c.IsClusterPlanNotRun() {
			err = fmt.Errorf("status(%d) plan is stopped by user. mp[%v]", atomic.LoadUint32(&c.planStatus), mpPlan.ID)
			log.LogWarnf(err.Error())
			return err
		}
		if c.partition == nil || !c.partition.IsRaftLeader() {
			err = fmt.Errorf("master leader is changed")
			return err
		}

		// switch raft leader if the source is leader. And waiting for the leader to be elected.
		err = c.changeAndCheckMetaPartitionLeader(mrPlan, mpPlan, mp)
		if err != nil {
			log.LogErrorf("changeAndCheckMetaPartitionLeader error: %s", err.Error())
			return err
		}

		if verifyDestinationInMetaReplicas(mp, mrPlan.Destination) {
			err = fmt.Errorf("destination %s is in mpid(%d) meta replicas[%v]", mrPlan.Destination, mp.PartitionID, mp.Hosts)
			log.LogErrorf(err.Error())
			return err
		}

		if !mp.CheckLastDelReplicaTime() {
			log.LogWarnf("doMetaPartitionMigrate: mp try wait, last %d, mp %d", mp.LastDelReplicaTime, mp.PartitionID)
			err = c.SleepInMetaReplicaDelInterval()
			if err != nil {
				log.LogErrorf("SleepInMetaReplicaDelInterval error: %s", err.Error())
				return err
			}
		}

		if plan.Type == AddLearner {
			err = c.addMetaReplicaLearner(mp, mrPlan.Destination, plan.Mode, mrPlan.Source, !plan.AutoPromote, proto.MpBalance)
		} else {
			err = c.migrateMetaPartition(mrPlan.Source, mrPlan.Destination, mp, mrPlan.StoreMode, proto.MpBalance)
		}

		if err == nil {
			return nil
		}
		if IsRetryMigrateMpError(err) {
			time.Sleep(time.Second * RetryMigrateInterVal)
			continue
		}
		log.LogErrorf("doMetaPartitionMigrate(%d) from %s to %s error: %s", mpPlan.ID, mrPlan.Source, mrPlan.Destination, err.Error())
		return err
	}
	log.LogErrorf("doMetaPartitionMigrate(%d) from %s to %s error: %s", mpPlan.ID, mrPlan.Source, mrPlan.Destination, err.Error())

	return err
}

func IsRetryMigrateMpError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())

	retryMsgList := []string{
		"no leader",
		"not leader",
		"raft is not the leader",
		"try again",
		"no enough replicas",
		"i/o timeout",
		"deadline exceeded",
		"connection refused",
		"connection reset by peer",
		"use of closed network connection",
		"no route to host",
		"network is unreachable",
		"host is down",
		"eof",
		"raft already exists",
	}
	for _, retryMsg := range retryMsgList {
		if strings.Contains(msg, retryMsg) {
			return true
		}
	}

	return strings.Contains(msg, "downreplicas") && strings.Contains(msg, "so donnot offline")
}

func (c *Cluster) IsClusterPlanNotIdle() bool {
	return atomic.LoadUint32(&c.planStatus) != PlanStatusIdle
}

func (c *Cluster) IsClusterPlanNotRun() bool {
	return atomic.LoadUint32(&c.planStatus) != PlanStatusRun
}

func (c *Cluster) IsClusterPlanStopping() bool {
	return atomic.LoadUint32(&c.planStatus) == PlanStatusStopping
}

func (c *Cluster) SetClusterPlanRunning() {
	atomic.StoreUint32(&c.planStatus, PlanStatusRun)
}

func (c *Cluster) SetClusterPlanIdle() {
	atomic.StoreUint32(&c.planStatus, PlanStatusIdle)
}

func (c *Cluster) SetClusterPlanStopping() {
	atomic.StoreUint32(&c.planStatus, PlanStatusStopping)
}

func (c *Cluster) GetClusterPlanStatusMsg() string {
	switch atomic.LoadUint32(&c.planStatus) {
	case PlanStatusIdle:
		return "plan task status is idle"
	case PlanStatusRun:
		return "plan task status is running"
	case PlanStatusStopping:
		return "plan task status is stopping"
	}
	return "plan task status is unknown"
}

func (c *Cluster) AnalyzeMetaNodes(storeMode proto.StoreMode) {
	var unusableBuf strings.Builder
	unusableBuf.WriteString("Unusable metanodes status:\n")
	var usableBuf strings.Builder
	if storeMode == proto.StoreModeRocksDb {
		usableBuf.WriteString("Rocksdb resource list:\n")
	} else {
		usableBuf.WriteString("Memory resource list:\n")
	}

	c.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)

		msg, allocatable := GetMetaNodeResourceReason(metaNode, storeMode)
		if allocatable {
			usableBuf.WriteString(msg + "\n")
		} else {
			unusableBuf.WriteString(msg + "\n")
		}

		return true
	})
	log.LogWarnf(unusableBuf.String())
	log.LogWarnf(usableBuf.String())
}

func TryNodeSetIdFromOtherNodes(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan, getParm *GetMigrateAddrParam) error {
	nodeSetList := make([]uint64, 0, len(mpPlan.Original))
	zoneList := make([]string, 0, len(mpPlan.Original))
	bFind := false
	for _, mr := range mpPlan.Original {
		bFind = false
		for _, item := range mpPlan.OverLoad {
			if item.Source == mr.Source {
				bFind = true
				break
			}
		}
		if bFind {
			continue
		}
		for _, id := range nodeSetList {
			if id == mr.SrcNodeSetId {
				bFind = true
				break
			}
		}
		if bFind {
			continue
		}
		nodeSetList = append(nodeSetList, mr.SrcNodeSetId)
		zoneList = append(zoneList, mr.SrcZoneName)
	}
	if len(nodeSetList) == 0 {
		return fmt.Errorf("no node set id found")
	}

	for i, nodeSetId := range nodeSetList {
		// clone param to avoid side effects from GetMigrateDestAddr (it will append Excludes/ExcludeRacks)
		excludes := append([]string(nil), getParm.Excludes...)
		excludeRacks := append([]string(nil), getParm.ExcludeRacks...)
		curParam := &GetMigrateAddrParam{
			Topo:         getParm.Topo,
			RocksdbTopo:  getParm.RocksdbTopo,
			ZoneName:     zoneList[i],
			NodeSetID:    nodeSetId,
			Excludes:     excludes,
			ExcludeRacks: excludeRacks,
			RequestNum:   getParm.RequestNum,
			LeastSize:    getParm.LeastSize,
			IsRocksdb:    getParm.IsRocksdb,
			RackLevel:    getParm.RackLevel,
		}

		find, dests := GetMigrateDestAddr(curParam)
		if find {
			if migratePlan.Type == AddLearner {
				mpPlan.Plan = dests
			} else {
				err := MigratePlanOverLoadToDest(migratePlan, mpPlan, dests, getParm.IsRocksdb)
				if err != nil {
					log.LogErrorf("MigratePlanOverLoadToDest error: %s", err.Error())
					return err
				}
			}
			return nil
		}
	}
	return fmt.Errorf("no resource in the members nodeset")
}

func DisplayPlanFailedDetails(getParam *GetMigrateAddrParam, mpPlan *proto.MetaBalancePlan, migratePlan *proto.ClusterPlan, isRocksdb bool) {
	var output strings.Builder
	fmt.Fprintf(&output, "getParam: zone(%v) nodeset(%v) mp(%v) exclude(%v) excludeRack(%v) requestNum(%v) size(%v) isRocksdb(%v) rackLevel(%v)\n",
		getParam.ZoneName, getParam.NodeSetID, mpPlan.ID, getParam.Excludes, getParam.ExcludeRacks, getParam.RequestNum, getParam.LeastSize, isRocksdb, getParam.RackLevel)

	fmt.Fprintf(&output, "mp(%v) crossZone(%v) planNum(%v) inodeCount(%v)", mpPlan.ID, mpPlan.CrossZone, mpPlan.PlanNum, mpPlan.InodeCount)
	fmt.Fprintf(&output, " original:")
	for _, mr := range mpPlan.Original {
		fmt.Fprintf(&output, " %s", mr.Source)
	}
	fmt.Fprintf(&output, " overLoad:")
	for _, mr := range mpPlan.OverLoad {
		fmt.Fprintf(&output, " %s", mr.Source)
	}
	fmt.Fprintf(&output, " plan:")
	for _, mr := range mpPlan.Plan {
		fmt.Fprintf(&output, " src(%s)->dst(%s)", mr.Source, mr.Destination)
	}
	fmt.Fprintf(&output, "\n")

	var zoneView map[string]*proto.ZonePressureView
	if isRocksdb {
		zoneView = migratePlan.RocksdbLow
	} else {
		zoneView = migratePlan.Low
	}

	fmt.Fprintf(&output, "resource:\n")
	for _, zone := range zoneView {
		for _, nodeSet := range zone.NodeSet {
			for _, mnInfo := range nodeSet.MetaNodes {
				body, err := json.Marshal(mnInfo)
				if err != nil {
					log.LogErrorf("DisplayPlanFailedDetails Marshal error: %s", err.Error())
					continue
				}
				fmt.Fprintf(&output, "%s\n", body)
			}
		}
	}

	log.LogWarnf(output.String())
}

// CreatePromoteLearnerPlan promotes all RocksDB-mode replicas that are learners to voters within [startID, endID].
// If volName is empty, it scans all volumes; otherwise, only the specified volume.
func (c *Cluster) CreatePromoteLearnerPlan(param *MetaPartitionPlanUserParams) (*proto.PromoteLearnerPlan, error) {
	var mps map[uint64]*MetaPartition

	promotePlan := &proto.PromoteLearnerPlan{
		Name:       param.Name,
		StartID:    param.StartID,
		EndID:      param.EndID,
		Mode:       param.Mode,
		SelectType: param.SelectType,
		ZoneName:   param.ZoneName,
		NodeSetID:  param.NodeSetID,
		SelectTag:  param.SelectTag,
		UndoNum:    0,
		RunningNum: 0,
		DoneNum:    0,
		FailedNum:  0,
		Progress:   0,
		Status:     PlanTaskRun,
	}

	if param.Name != "" {
		vol, err := c.getVol(param.Name)
		if err != nil {
			return nil, fmt.Errorf("get volume(%s) failed: %v", param.Name, err)
		}
		if vol.isUnavailable() {
			return nil, fmt.Errorf("volume(%s) is marked delete or init failed", param.Name)
		}
		mps = vol.cloneMetaPartitionMap()
	} else {
		mps = c.getAllMetaPartitions()
	}

	count := 0
	for _, mp := range mps {
		if param.StartID != 0 && mp.PartitionID < param.StartID {
			continue
		}
		if param.EndID != 0 && mp.PartitionID > param.EndID {
			continue
		}
		for _, peer := range mp.Peers {
			if peer.Type == raftProto.PeerLearner {
				count++
				break
			}
		}
		if count >= MaxMpMigrateNum {
			break
		}
	}
	promotePlan.Learners = make([]*proto.MetaPartitionLearnerInfo, 0, count)

	for _, mp := range mps {
		// filter by id range
		if param.StartID != 0 && mp.PartitionID < param.StartID {
			continue
		}
		if param.EndID != 0 && mp.PartitionID > param.EndID {
			continue
		}

		// scan replicas and promote rocksdb replicas
		learnerInfo := &proto.MetaPartitionLearnerInfo{
			ID:         mp.PartitionID,
			Learners:   make([]string, 0, len(mp.Peers)),
			DeleteAddr: make([]string, 0, len(mp.Peers)),
		}
		for _, peer := range mp.Peers {
			if peer.Type != raftProto.PeerLearner {
				continue
			}
			learnerInfo.Learners = append(learnerInfo.Learners, peer.Addr)
		}
		if len(learnerInfo.Learners) > 0 {
			promotePlan.Learners = append(promotePlan.Learners, learnerInfo)
		}
	}

	promotePlan.StartTime = time.Now()
	promotePlan.UndoNum = int32(count)
	promotePlan.TotalNum = int32(count)

	err := c.syncAddPromoteLearnerPlan(promotePlan)
	if err != nil {
		log.LogErrorf("syncAddPromoteLearnerPlan error: %s", err.Error())
		return nil, err
	}

	c.SetClusterPlanRunning()
	go c.DoPromoteLearnerPlan(promotePlan)

	return promotePlan, nil
}

func (c *Cluster) CreateMetaPartitionAddLearnerPlan(param *MetaPartitionPlanUserParams) (*proto.ClusterPlan, error) {
	// Create a new meta partition migrate plan
	plan := &proto.ClusterPlan{
		Low:            make(map[string]*proto.ZonePressureView),
		RocksdbLow:     make(map[string]*proto.ZonePressureView),
		Plan:           make([]*proto.MetaBalancePlan, 0),
		Name:           param.Name,
		Status:         PlanTaskRun,
		Type:           AddLearner,
		Mode:           param.Mode,
		ModeCnt:        param.Count,
		StartId:        param.StartID,
		EndId:          param.EndID,
		RackLevel:      c.getRackAwareLevel(),
		FailedList:     make([]uint64, 0),
		DoneNum:        0,
		RunningNum:     0,
		DoneReplicaNum: 0,
		RunReplicaNum:  0,
		AutoPromote:    param.AutoPromoteLearner,
		SelectType:     param.SelectType,
		ZoneName:       param.ZoneName,
		NodeSetID:      param.NodeSetID,
		SelectTag:      param.SelectTag,
	}

	err := c.GetLowMemPressureTopology(plan)
	if err != nil {
		log.LogErrorf("GetLowMemPressureTopology error name(%s) start(%d) end(%d) mode(%d) cnt(%d): %s",
			param.Name, param.StartID, param.EndID, param.Mode, param.Count, err.Error())
		return plan, err
	}

	err = c.FillAddLearnerPlan(plan, param.Name)
	if err != nil {
		log.LogErrorf("FillAddLearnerPlan error name(%s) start(%d) end(%d) mode(%d) cnt(%d): %s",
			param.Name, param.StartID, param.EndID, param.Mode, param.Count, err.Error())
		return plan, err
	}

	if len(plan.Plan) == 0 {
		log.LogWarnf("Empty plan. request parameters: %+v", param)
		return nil, c.AnalyzeEmptyPlanReason(plan)
	}

	err = c.FindAddLearnerDestination(plan, param)
	if err != nil {
		log.LogWarnf("FindAddLearnerDestination error name(%s) start(%d) end(%d) mode(%d) cnt(%d): %s",
			param.Name, param.StartID, param.EndID, param.Mode, param.Count, err.Error())
		return plan, err
	}

	plan.Total = len(plan.Plan)
	plan.UndoNum = int32(plan.Total)
	for _, item := range plan.Plan {
		plan.TotalReplicaNum += len(item.Plan)
	}
	plan.UndoReplicaNum = int32(plan.TotalReplicaNum)
	plan.StartTime = time.Now()

	// Save plan into raft storage
	err = c.syncAddBalanceTask(plan)
	if err != nil {
		log.LogErrorf("syncAddBalanceTask err: %s", err.Error())
		return nil, err
	}

	c.SetClusterPlanRunning()
	go c.DoMetaPartitionBalanceTask(plan)

	return plan, nil
}

func (c *Cluster) FillAddLearnerPlan(plan *proto.ClusterPlan, volName string) error {
	var mps map[uint64]*MetaPartition
	if volName != "" {
		vol, err := c.getVol(volName)
		if err != nil {
			return fmt.Errorf("get volume(%s) failed: %v", volName, err)
		}

		if vol.isUnavailable() {
			return fmt.Errorf("volume(%s) is marked delete or init failed", volName)
		}

		if vol.DefaultStoreMode != plan.Mode {
			plan.Msg = fmt.Sprintf("Please use 'cfs-cli volume update %s --store-mode %s' to update the store mode", volName, plan.Mode.Str())
		}

		mps = vol.cloneMetaPartitionMap()
	} else {
		mps = c.getAllMetaPartitions()
	}

	if plan.Plan == nil {
		plan.Plan = make([]*proto.MetaBalancePlan, 0, len(mps))
	}

	for _, mp := range mps {
		// Check partition ID range
		if plan.StartId != 0 && mp.PartitionID < plan.StartId {
			continue
		}
		if plan.EndId != 0 && mp.PartitionID > plan.EndId {
			continue
		}
		count := GetMetaPartitionReadyReplicaCount(plan, mp)
		if count >= plan.ModeCnt {
			continue
		}

		mpPlan := &proto.MetaBalancePlan{
			ID:         mp.PartitionID,
			Original:   make([]*proto.MrBalanceInfo, 0, len(mp.Replicas)),
			OverLoad:   make([]*proto.MrBalanceInfo, 0),
			Plan:       make([]*proto.MrBalanceInfo, 0),
			InodeCount: mp.InodeCount,
			PlanNum:    0,
		}

		memorySize := GetMetaPartitionMemorySize(mp)
		// Find replicas that need to be migrated
		for _, mr := range mp.Replicas {
			mn, err := c.metaNode(mr.Addr)
			if err != nil {
				log.LogErrorf("Failed to get meta node(%s) for mp(%d), err: %s", mr.Addr, mp.PartitionID, err.Error())
				continue
			}

			storeMode, err := c.getMetaPartitionStoreMode(mp, mr.Addr)
			if err != nil {
				log.LogErrorf("Failed to get meta replica store mode mp(%d) addr(%s), err: %s", mp.PartitionID, mr.Addr, err.Error())
				continue
			}

			// Record original replica info
			mrRec := &proto.MrBalanceInfo{
				Source:       mr.Addr,
				SrcMemSize:   memorySize,
				SrcNodeSetId: mn.NodeSetID,
				SrcZoneName:  mn.ZoneName,
				Status:       PlanTaskInit,
				StoreMode:    storeMode,
			}
			mpPlan.Original = append(mpPlan.Original, mrRec)
		}

		// skip empty plan if all replicas were filtered out due to errors
		if len(mpPlan.Original) == 0 {
			continue
		}

		plan.Plan = append(plan.Plan, mpPlan)
		plan.Total++
		if plan.Total >= MaxMpMigrateNum {
			break
		}
	}

	return nil
}

func (c *Cluster) FindAddLearnerDestination(migratePlan *proto.ClusterPlan, param *MetaPartitionPlanUserParams) (err error) {
	for i, mpPlan := range migratePlan.Plan {
		err = c.AddLearnerToDestination(migratePlan, mpPlan, param)
		if err == NotEnoughResource {
			log.LogWarnf("Analyze the meta nodes:")
			c.AnalyzeMetaNodes(migratePlan.Mode)

			if i <= 0 {
				if migratePlan.Msg == "" {
					migratePlan.Msg = fmt.Sprintf("require to migrate (%d) mp, but not create plan", len(migratePlan.Plan))
				} else {
					migratePlan.Msg = fmt.Sprintf("%s, require to migrate (%d) mp, but not create plan", migratePlan.Msg, len(migratePlan.Plan))
				}
				log.LogWarnf(migratePlan.Msg)
				return
			}

			if migratePlan.Msg == "" {
				migratePlan.Msg = fmt.Sprintf("require to migrate (%d) mp, only create (%d) plan", len(migratePlan.Plan), i)
			} else {
				migratePlan.Msg = fmt.Sprintf("%s, require to migrate (%d) mp, only create (%d) plan", migratePlan.Msg, len(migratePlan.Plan), i)
			}
			migratePlan.Plan = migratePlan.Plan[:i]
			log.LogWarnf(migratePlan.Msg)
			return nil
		} else if err != nil {
			log.LogErrorf("Fail to find reasonable metanode to create plan: %s", err.Error())
			return err
		}
	}

	return nil
}

func (c *Cluster) AddLearnerToDestination(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan, param *MetaPartitionPlanUserParams) error {
	if len(mpPlan.Original) == 0 {
		return fmt.Errorf("no original replicas")
	}

	mp, err := c.getMetaPartitionByID(mpPlan.ID)
	if err != nil {
		log.LogErrorf("getMetaPartitionByID error: %s", err.Error())
		return err
	}

	count := GetMetaPartitionReadyReplicaCount(migratePlan, mp)
	if count >= migratePlan.ModeCnt {
		log.LogWarnf("mp(%d) already has (%d) ready replicas, no need to fill learner plan", mpPlan.ID, count)
		return fmt.Errorf("mp(%d) already has (%d) ready replicas, no need to fill learner plan", mpPlan.ID, count)
	}
	count = migratePlan.ModeCnt - count

	learnerCount := GetMetaPartitionLearnerCount(mp)
	if (count + learnerCount) > proto.MaxMetaPartitionLearnerNum {
		count = proto.MaxMetaPartitionLearnerNum - learnerCount
		mpPlan.Msg = fmt.Sprintf("mp(%d) already has (%d) learners, only add (%d) learners", mpPlan.ID, learnerCount, count)
		log.LogWarnf(mpPlan.Msg)
	}

	if count <= 0 {
		log.LogInfof("mp(%d) no need to add learners, count: %d", mpPlan.ID, count)
		return nil
	}

	getParam := &GetMigrateAddrParam{
		Topo:        migratePlan.Low,
		RocksdbTopo: migratePlan.RocksdbLow,
		ZoneName:    migratePlan.ZoneName,
		NodeSetID:   migratePlan.NodeSetID,
		RequestNum:  count,
		LeastSize:   mpPlan.Original[0].SrcMemSize,
		IsRocksdb:   migratePlan.Mode == proto.StoreModeRocksDb,
		RackLevel:   migratePlan.RackLevel,
	}
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// try to find resource from the same node set.
	find, dests := GetMigrateDestAddr(getParam)
	if find {
		err := c.FillLearnerPlanDestination(migratePlan, mpPlan, dests, param, mp)
		if err != nil {
			log.LogErrorf("FillLearnerPlanDestination error: %s", err.Error())
			return err
		}
		return nil
	}

	// try the others node set under the same zone.
	FillExcludeAddrIntoGetParam(mpPlan, getParam)
	find, dests = GetMigrateAddrExcludeNodeSet(getParam)
	if !find {
		// try to find resource from other zone.
		FillExcludeAddrIntoGetParam(mpPlan, getParam)
		find, dests = GetMigrateAddrExcludeZone(getParam)
	}
	if !find {
		log.LogWarnf("Display the failed details: mp(%d) need(%d)", mpPlan.ID, migratePlan.ModeCnt)
		DisplayPlanFailedDetails(getParam, mpPlan, migratePlan, migratePlan.Mode == proto.StoreModeRocksDb)
		return NotEnoughResource
	}

	err = c.FillLearnerPlanDestination(migratePlan, mpPlan, dests, param, mp)
	if err != nil {
		log.LogErrorf("FillLearnerPlanDestination error: %s", err.Error())
		return err
	}

	return nil
}

func SelectOneReplicaStrickly(mp *MetaPartition, excludeAddrs []string, param *MetaPartitionPlanUserParams) string {
	for _, mr := range mp.Replicas {
		if contains(excludeAddrs, mr.Addr) {
			continue
		}
		switch param.SelectType {
		case SelectTypeZoneName:
			if mr.metaNode.ZoneName != param.ZoneName {
				return mr.Addr
			}
		case SelectTypeNodeSetId:
			if mr.metaNode.NodeSetID != param.NodeSetID {
				return mr.Addr
			}
		case SelectTypeNodeAddrs:
			if mr.metaNode.SelectTag != param.SelectTag {
				return mr.Addr
			}
		default:
			if mr.StoreMode != param.Mode {
				return mr.Addr
			}
		}
	}
	return ""
}

func SelectOneReplicaToDelete(mp *MetaPartition, excludeAddrs []string, param *MetaPartitionPlanUserParams) string {
	addr := SelectOneReplicaStrickly(mp, excludeAddrs, param)
	if addr != "" {
		return addr
	}

	for _, mr := range mp.Replicas {
		if contains(excludeAddrs, mr.Addr) {
			continue
		}
		return mr.Addr
	}
	return ""
}

func (c *Cluster) CreateAndRunCheckSumPlan(param *MetaPartitionPlanUserParams) (*proto.MetaPartitionsChecksumPlan, error) {
	plan, err := c.CreateCheckSumPlan(param)
	if err != nil {
		log.LogErrorf("CreateCheckSumPlan failed: %s", err.Error())
		return nil, err
	}

	if c.IsClusterPlanNotIdle() {
		return plan, nil
	}

	c.SetClusterPlanRunning()
	go c.DoMetaPartitionCheckSumTask(plan)

	return plan, nil
}

func (c *Cluster) CreateCheckSumPlan(param *MetaPartitionPlanUserParams) (*proto.MetaPartitionsChecksumPlan, error) {
	var mps map[uint64]*MetaPartition
	if param.Name != "" {
		vol, err := c.getVol(param.Name)
		if err != nil {
			log.LogErrorf("CreateCheckSumPlan get volume(%s) failed: %v", param.Name, err)
			return nil, fmt.Errorf("get volume(%s) failed: %v", param.Name, err)
		}
		if vol.isUnavailable() {
			log.LogErrorf("CreateCheckSumPlan volume(%s) is marked delete or init failed", param.Name)
			return nil, fmt.Errorf("volume(%s) is marked delete or init failed", param.Name)
		}
		mps = vol.cloneMetaPartitionMap()
	} else {
		mps = c.getAllMetaPartitions()
	}

	count := 0
	for _, mp := range mps {
		if param.StartID != 0 && mp.PartitionID < param.StartID {
			continue
		}
		if param.EndID != 0 && mp.PartitionID > param.EndID {
			continue
		}
		count += 1
	}
	if count == 0 {
		return nil, fmt.Errorf("no meta partitions in range start(%d) end(%d) volume(%s)", param.StartID, param.EndID, param.Name)
	}

	plan := &proto.MetaPartitionsChecksumPlan{
		Status:       PlanTaskRun,
		StartTime:    time.Now(),
		FailedList:   make([]uint64, 0),
		CheckSumList: make([]*proto.MetaPartitionChecksumInfo, 0, count),
		Total:        int32(count),
		Undo:         int32(count),
	}

	for _, mp := range mps {
		if param.StartID != 0 && mp.PartitionID < param.StartID {
			continue
		}
		if param.EndID != 0 && mp.PartitionID > param.EndID {
			continue
		}
		checksumInfo := &proto.MetaPartitionChecksumInfo{
			PartitionID: mp.PartitionID,
			Status:      PlanTaskInit,
			Replicas:    make([]*proto.MetaReplicaChecksumInfo, 0, len(mp.Replicas)),
		}
		for _, mr := range mp.Replicas {
			checksumInfo.Replicas = append(checksumInfo.Replicas, &proto.MetaReplicaChecksumInfo{
				Addr: mr.Addr,
			})
		}
		plan.CheckSumList = append(plan.CheckSumList, checksumInfo)
	}

	err := c.syncAddCheckSumPlan(plan)
	if err != nil {
		log.LogErrorf("syncAddCheckSumPlan failed: %s", err.Error())
		return nil, err
	}

	return plan, nil
}

func (c *Cluster) RestartMetaPartitionCheckSumTask() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.IsClusterPlanNotIdle() {
		return nil
	}

	// Get the planed check sum task.
	plan, err := c.loadCheckSumPlan()
	if err != nil && err != proto.ErrNoCheckSumPlan {
		log.LogErrorf("loadCheckSumPlan failed: %s", err.Error())
		return err
	} else if plan != nil && plan.Status == PlanTaskDone {
		now := time.Now()
		if plan.Expire.Before(now) {
			err = c.syncDeleteCheckSumPlan()
			if err != nil {
				log.LogErrorf("syncDeleteCheckSumPlan err: %s", err.Error())
			}
		}
		return nil
	}

	if plan == nil {
		return nil
	}

	if plan.Status != PlanTaskRun {
		// No start the plan task if the status is not running.
		return nil
	}

	c.SetClusterPlanRunning()
	go c.DoMetaPartitionCheckSumTask(plan)

	return nil
}

func (c *Cluster) DoMetaPartitionCheckSumTask(plan *proto.MetaPartitionsChecksumPlan) {
	defer func() {
		// clear the run flag.
		c.SetClusterPlanIdle()
	}()

	if plan.Total <= 0 {
		plan.Status = PlanTaskDone
		plan.Msg = "No meta partitions to check"
		plan.EndTime = time.Now()
		plan.Expire = time.Now().Add(defaultPlanExpireHours * time.Hour)
		err := c.syncUpdateCheckSumPlan(plan)
		if err != nil {
			log.LogErrorf("syncUpdateCheckSumPlan err: %s", err.Error())
			plan.Msg = err.Error()
		}
		return
	}

	concurrency := gConfig.mpMigrateThreads
	if concurrency <= 0 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var planMutex sync.Mutex

	plan.StartTime = time.Now()

	for _, checksumInfo := range plan.CheckSumList {
		sem <- struct{}{}
		wg.Add(1)
		go func(info *proto.MetaPartitionChecksumInfo) {
			defer wg.Done()
			atomic.AddInt32(&plan.Undo, -1)
			atomic.AddInt32(&plan.Running, 1)
			err := c.StartTodoMetaPartitionCheckSum(plan, info)
			done := atomic.AddInt32(&plan.Done, 1)
			atomic.AddInt32(&plan.Running, -1)
			planMutex.Lock()
			if err != nil {
				log.LogErrorf("StartTodoMetaPartitionCheckSum err: %s", err.Error())
				plan.FailedList = append(plan.FailedList, info.PartitionID)
			}
			plan.Progress = float64(done) / float64(plan.Total) * 100
			err = c.syncUpdateCheckSumPlan(plan)
			if err != nil {
				log.LogErrorf("syncUpdateCheckSumPlan err: %s", err.Error())
				plan.Msg = err.Error()
			}
			planMutex.Unlock()
			<-sem
		}(checksumInfo)
	}
	wg.Wait()

	if c.IsClusterPlanStopping() {
		plan.Status = PlanTaskStop
		plan.Msg = "check sum plan is stopped"
	} else {
		if len(plan.FailedList) > 0 {
			plan.Status = PlanTaskError
			plan.Msg = "Please check the detail in each msg."
		} else {
			plan.Status = PlanTaskDone
		}
	}
	plan.EndTime = time.Now()
	plan.Expire = time.Now().Add(defaultPlanExpireHours * time.Hour)

	err := c.syncUpdateCheckSumPlan(plan)
	if err != nil {
		log.LogErrorf("syncUpdateCheckSumPlan err: %s", err.Error())
		plan.Msg = err.Error()
	}
}

func (c *Cluster) StartTodoMetaPartitionCheckSum(plan *proto.MetaPartitionsChecksumPlan, checksumInfo *proto.MetaPartitionChecksumInfo) error {
	checksumInfo.StartTime = time.Now()
	checksumInfo.Status = PlanTaskRun

	err := c.syncUpdateCheckSumPlan(plan)
	if err != nil {
		log.LogErrorf("syncUpdateCheckSumPlan err: %s", err.Error())
		return err
	}

	err = c.DoMetaPartitionCheckSum(checksumInfo)
	if err != nil {
		log.LogErrorf("DoMetaPartitionCheckSum err: %s", err.Error())
		checksumInfo.Status = PlanTaskError
		checksumInfo.Msg = err.Error()
		return err
	}

	checksumInfo.Status = PlanTaskDone

	return nil
}

func (c *Cluster) DoMetaPartitionCheckSum(checksumInfo *proto.MetaPartitionChecksumInfo) error {
	if c.IsClusterPlanNotRun() {
		return ErrClusterPlanNotRunning
	}

	mp, err := c.getMetaPartitionByID(checksumInfo.PartitionID)
	if err != nil {
		log.LogErrorf("getMetaPartitionByID err: %s", err.Error())
		return err
	}
	if len(mp.LoadResponse) > 0 {
		checksumInfo.LastApplyID = mp.LoadResponse[0].Md5ApplyId
	}
	err = c.startMetaPartitionCheckSum(mp)
	if err != nil {
		log.LogErrorf("startMetaPartitionCheckSum err: %s", err.Error())
		return err
	}

	err = c.WaitForMetaPartitionCheckSumResult(mp, checksumInfo)
	if err != nil {
		log.LogErrorf("waitForMetaPartitionCheckSumResult err: %s", err.Error())
		return err
	}

	return nil
}

func (c *Cluster) startMetaPartitionCheckSum(partition *MetaPartition) (err error) {
	var (
		candidateAddrs []string
		leaderAddr     string
	)
	log.LogWarnf("action[startMetaPartitionCheckSum] start,vol[%v],meta partition[%v],hosts[%v]",
		partition.volName, partition.PartitionID, partition.Hosts)

	candidateAddrs = make([]string, 0, len(partition.Hosts))
	leaderMr, err := partition.getMetaReplicaLeader()
	if err == nil {
		leaderAddr = leaderMr.Addr
		if contains(partition.Hosts, leaderAddr) {
			candidateAddrs = append(candidateAddrs, leaderAddr)
			log.LogWarnf("action[startMetaPartitionCheckSum] found leader,vol[%v],meta partition[%v],leader[%v]",
				partition.volName, partition.PartitionID, leaderAddr)
		} else {
			leaderAddr = ""
			log.LogWarnf("action[startMetaPartitionCheckSum] leader not in hosts,vol[%v],meta partition[%v],leader[%v],hosts[%v]",
				partition.volName, partition.PartitionID, leaderMr.Addr, partition.Hosts)
		}
	} else {
		log.LogWarnf("action[startMetaPartitionCheckSum] getLeader failed,vol[%v],meta partition[%v],err[%v]",
			partition.volName, partition.PartitionID, err)
	}
	for _, host := range partition.Hosts {
		if host == leaderAddr {
			continue
		}
		candidateAddrs = append(candidateAddrs, host)
	}
	log.LogWarnf("action[startMetaPartitionCheckSum] candidateAddrs[%v],vol[%v],meta partition[%v]",
		candidateAddrs, partition.volName, partition.PartitionID)
	// send task to leader addr first,if need to retry,then send to other addr
	for _, host := range candidateAddrs {
		_, err = c.buildStartMetaPartitionCheckSumTaskAndSyncSend(partition, host)
		if err == nil {
			log.LogWarnf("action[startMetaPartitionCheckSum] success,vol[%v],meta partition[%v],leader[%v],host[%v]",
				partition.volName, partition.PartitionID, leaderAddr, host)
			return
		}

		log.LogWarnf("action[startMetaPartitionCheckSum] retry error,vol[%v],meta partition[%v],leader[%v],host[%v],err[%v]",
			partition.volName, partition.PartitionID, leaderAddr, host, err)
	}

	log.LogWarnf("action[startMetaPartitionCheckSum] failed after all retries,vol[%v],meta partition[%v],err[%v]",
		partition.volName, partition.PartitionID, err)
	return
}

func (c *Cluster) buildStartMetaPartitionCheckSumTaskAndSyncSend(mp *MetaPartition, leaderAddr string) (resp *proto.Packet, err error) {
	defer func() {
		var resultCode uint8
		if resp != nil {
			resultCode = resp.ResultCode
		}

		if err != nil {
			log.LogErrorf("action[buildStartMetaPartitionCheckSumTaskAndSyncSend],vol[%v],meta partition[%v],leader[%v],resultCode[%v],err[%v]",
				mp.volName, mp.PartitionID, leaderAddr, resultCode, err)
		} else {
			log.LogWarnf("action[buildStartMetaPartitionCheckSumTaskAndSyncSend],vol[%v],meta partition[%v],leader[%v],resultCode[%v] success",
				mp.volName, mp.PartitionID, leaderAddr, resultCode)
		}
	}()

	t, err := mp.createTaskToCalculateCheckSum(leaderAddr)
	if err != nil {
		log.LogWarnf("action[buildStartMetaPartitionCheckSumTaskAndSyncSend] createTask failed,vol[%v],meta partition[%v],err[%v]",
			mp.volName, mp.PartitionID, err)
		return
	}
	leaderMetaNode, err := c.metaNode(leaderAddr)
	if err != nil {
		log.LogWarnf("action[buildStartMetaPartitionCheckSumTaskAndSyncSend] getMetaNode failed,vol[%v],meta partition[%v],leader[%v],err[%v]",
			mp.volName, mp.PartitionID, leaderAddr, err)
		return
	}

	if resp, err = leaderMetaNode.Sender.syncSendAdminTask(t); err != nil {
		log.LogWarnf("action[buildStartMetaPartitionCheckSumTaskAndSyncSend] sendTask failed,vol[%v],meta partition[%v],leader[%v],err[%v]",
			mp.volName, mp.PartitionID, leaderAddr, err)
		return
	}
	return
}

func (mp *MetaPartition) createTaskToCalculateCheckSum(leaderAddr string) (t *proto.AdminTask, err error) {
	req := &proto.CalcMetaPartitionMd5SumRequest{
		PartitionID: mp.PartitionID,
	}
	t = proto.NewAdminTask(proto.OpCalcMetaPartitionMd5Sum, leaderAddr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	log.LogWarnf("action[createTaskToCalculateCheckSum] task created,vol[%v],meta partition[%v],leader[%v],taskID[%v]",
		mp.volName, mp.PartitionID, leaderAddr, t.ID)
	return
}

func (c *Cluster) WaitForMetaPartitionCheckSumResult(mp *MetaPartition, checksumInfo *proto.MetaPartitionChecksumInfo) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var err error
	maxRetry := CalcuMetaPartitionReadyMaxRetry(mp)
	for i := 0; i < maxRetry; i++ {
		select {
		case <-ticker.C:
			if c.IsClusterPlanNotRun() {
				return ErrClusterPlanNotRunning
			}
			err = c.CopyMd5SumToChecksumInfo(mp, checksumInfo)
			if err == nil {
				return nil
			}
		case <-c.stopc:
			c.SetClusterPlanStopping()
			return ErrClusterStop
		}
	}
	if err != nil {
		log.LogWarnf("Check Mp[%v] Md5Sum err: %s", mp.PartitionID, err.Error())
		return err
	}

	return fmt.Errorf("Waiting for meta partition(%d) Md5Sum retry(%d) timeout", mp.PartitionID, maxRetry)
}

func (c *Cluster) CopyMd5SumToChecksumInfo(mp *MetaPartition, checksumInfo *proto.MetaPartitionChecksumInfo) error {
	if len(checksumInfo.Replicas) == 0 {
		return fmt.Errorf("No replicas for mp[%v]", mp.PartitionID)
	}

	replicaByAddr := make(map[string]*proto.MetaReplicaChecksumInfo, len(checksumInfo.Replicas))
	for _, replica := range checksumInfo.Replicas {
		replicaByAddr[replica.Addr] = replica
	}

	count := 0
	for _, response := range mp.LoadResponse {
		if response.Md5Sum == "" || response.Md5ApplyId <= checksumInfo.LastApplyID {
			continue
		}
		if replica := replicaByAddr[response.Addr]; replica != nil {
			replica.Md5Sum = response.Md5Sum
			replica.ApplyID = response.Md5ApplyId
			count++
		}
	}

	if count != len(checksumInfo.Replicas) {
		return fmt.Errorf("Not receive all the Md5Sum for mp[%v]", mp.PartitionID)
	}

	firstApplyID := checksumInfo.Replicas[0].ApplyID
	firstMd5Sum := checksumInfo.Replicas[0].Md5Sum
	for _, replica := range checksumInfo.Replicas {
		if replica.ApplyID != firstApplyID {
			return fmt.Errorf("mp[%v] applyID not same, %v[%v], %v[%v]",
				mp.PartitionID, checksumInfo.Replicas[0].Addr, firstApplyID, replica.Addr, replica.ApplyID)
		}
		if replica.Md5Sum != firstMd5Sum {
			return fmt.Errorf("mp[%v] Md5Sum not same, %v[%v], %v[%v]",
				mp.PartitionID, checksumInfo.Replicas[0].Addr, firstMd5Sum, replica.Addr, replica.Md5Sum)
		}
	}

	rstMsg := fmt.Sprintf("mp[%v] Md5Sum check success, applyID[%v], md5Sum[%v]", mp.PartitionID, firstApplyID, firstMd5Sum)
	auditlog.LogMasterOp("checkMetaPartitionMd5Sum", rstMsg, nil)

	return nil
}

func (c *Cluster) FillLearnerPlanDestination(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan, dest []*proto.MrBalanceInfo, param *MetaPartitionPlanUserParams, mp *MetaPartition) error {
	if len(dest) == 0 {
		log.LogWarnf("FillLearnerPlanDestination: no destination mp[%v]", mpPlan.ID)
		return fmt.Errorf("no destination")
	}

	if !migratePlan.AutoPromote {
		mpPlan.Plan = dest
		return nil
	}

	excludeAddrs := make([]string, 0, len(dest))
	for _, item := range dest {
		srcAddr := SelectOneReplicaToDelete(mp, excludeAddrs, param)
		if srcAddr == "" {
			return fmt.Errorf("no replica found for mp[%v]", mpPlan.ID)
		}
		excludeAddrs = append(excludeAddrs, srcAddr)
		item.Source = srcAddr
		for _, original := range mpPlan.Original {
			if original.Source == srcAddr {
				item.SrcMemSize = original.SrcMemSize
				item.SrcNodeSetId = original.SrcNodeSetId
				item.SrcZoneName = original.SrcZoneName
				item.SrcRack = original.SrcRack
				break
			}
		}
	}

	mpPlan.Plan = dest

	return nil
}

func GetMetaPartitionLearnerList(mp *MetaPartition) []string {
	learnerList := make([]string, 0, len(mp.Peers))
	for _, peer := range mp.Peers {
		if peer.Type == raftProto.PeerLearner {
			learnerList = append(learnerList, peer.Addr)
		}
	}
	return learnerList
}

func GetMetaPartitionLearnerCount(mp *MetaPartition) int {
	count := 0
	for _, peer := range mp.Peers {
		if peer.Type == raftProto.PeerLearner {
			count += 1
		}
	}
	return count
}

func GetMetaPartitionReadyReplicaCount(plan *proto.ClusterPlan, mp *MetaPartition) int {
	count := 0
	for _, mr := range mp.Replicas {
		switch plan.SelectType {
		case SelectTypeZoneName:
			if mr.metaNode.ZoneName == plan.ZoneName {
				count += 1
			}
		case SelectTypeNodeSetId:
			if mr.metaNode.NodeSetID == plan.NodeSetID {
				count += 1
			}
		case SelectTypeNodeAddrs:
			if mr.metaNode.SelectTag == plan.SelectTag {
				count += 1
			}
		default:
			if mr.StoreMode == plan.Mode {
				count += 1
			}
		}
	}
	return count
}

func (c *Cluster) tryToChangeMetaPartitionLeader(mp *MetaPartition, oldLeader string) error {
	var newLeader string
	for _, replica := range mp.Replicas {
		// Learner cannot be leader, skip it
		if replica.Addr != oldLeader && !replica.IsLearner {
			newLeader = replica.Addr
			break
		}
	}

	if newLeader == "" {
		return fmt.Errorf("no new leader found for mp[%v]", mp.PartitionID)
	}

	for i := 0; i < CheckMetaLeaderRetry; i++ {
		err := mp.tryToChangeLeaderByHost(newLeader)
		if err != nil {
			log.LogErrorf("metapartition[%d] try to change leader to host[%s] failed, err[%s]",
				mp.PartitionID, newLeader, err.Error())
		}

		leader, err := mp.getMetaReplicaLeader()
		if err != nil {
			log.LogWarnf("metapartition[%d] has no leader", mp.PartitionID)
			time.Sleep(CheckMetaLeaderInterval * time.Second)
			continue
		}
		if leader.Addr != oldLeader {
			// the leader is not the source node, the meta partition can be migrated.
			return nil
		}

		time.Sleep(CheckMetaLeaderInterval * time.Second)
	}

	return fmt.Errorf("Try to change mp[%v] leader from %s to %s failed", mp.PartitionID, oldLeader, newLeader)
}

func (c *Cluster) DoPromoteLearnerPlan(plan *proto.PromoteLearnerPlan) error {
	defer c.SetClusterPlanIdle()

	concurrency := gConfig.mpMigrateThreads
	if concurrency <= 0 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	var (
		wg     sync.WaitGroup
		planMu sync.Mutex
	)

	var stopProcess int32
	atomic.StoreInt32(&stopProcess, 0)
	for _, learnerInfo := range plan.Learners {
		if atomic.LoadInt32(&stopProcess) != 0 {
			break
		}
		sem <- struct{}{}
		wg.Add(1)

		planMu.Lock()
		plan.RunningNum++
		plan.UndoNum--
		err := c.syncUpdatePromoteLearnerPlan(plan)
		if err != nil {
			log.LogErrorf("syncUpdatePromoteLearnerPlan error: %s", err.Error())
		}
		planMu.Unlock()

		go func(info *proto.MetaPartitionLearnerInfo) {
			defer wg.Done()

			err := c.PromoteMetaReplicaAndWait(plan, info)
			<-sem

			if err != nil {
				log.LogErrorf("DoPromoteLearnerPlan PromoteMetaReplicaAndWait error: %s", err.Error())
				atomic.StoreInt32(&stopProcess, 1)
				planMu.Lock()
				plan.FailedNum++
				plan.RunningNum--
				plan.FailedList = append(plan.FailedList, info.ID)
				plan.Msg = err.Error()
				err := c.syncUpdatePromoteLearnerPlan(plan)
				if err != nil {
					log.LogErrorf("syncUpdatePromoteLearnerPlan error: %s", err.Error())
				}
				planMu.Unlock()
				return
			}
			planMu.Lock()
			plan.RunningNum--
			plan.DoneNum++
			if plan.TotalNum > 0 {
				plan.Progress = float64(plan.DoneNum) / float64(plan.TotalNum) * 100
			}
			err = c.syncUpdatePromoteLearnerPlan(plan)
			if err != nil {
				log.LogErrorf("syncUpdatePromoteLearnerPlan error: %s", err.Error())
			}
			planMu.Unlock()
		}(learnerInfo)
	}
	wg.Wait()

	if c.IsClusterPlanStopping() {
		plan.Status = PlanTaskStop
		plan.Msg = "promote learner plan is stopped"
	} else {
		if atomic.LoadInt32(&stopProcess) != 0 {
			plan.Status = PlanTaskError
		} else {
			plan.Status = PlanTaskDone
		}
	}
	plan.EndTime = time.Now()
	plan.Expire = time.Now().Add(defaultPlanExpireHours * time.Hour)
	err := c.syncUpdatePromoteLearnerPlan(plan)
	if err != nil {
		log.LogErrorf("syncUpdatePromoteLearnerPlan error: %s", err.Error())
		return err
	}

	return nil
}

func (c *Cluster) PromoteMetaReplicaAndWait(plan *proto.PromoteLearnerPlan, learnerInfo *proto.MetaPartitionLearnerInfo) error {
	var (
		mp      *MetaPartition
		srcAddr string
		err     error
	)

	defer func() {
		if err != nil {
			learnerInfo.Msg = err.Error()
		}
	}()

	mp, err = c.getMetaPartitionByID(learnerInfo.ID)
	if err != nil {
		log.LogErrorf("PromoteMetaReplicaAndWait getMetaPartition error: %s", err.Error())
		return err
	}

	for _, learner := range learnerInfo.Learners {
		// wait for mp ready.
		err = c.waitForMetaPartitionReady(mp)
		if err != nil {
			log.LogErrorf("waitForMetaPartitionReady error: %s", err.Error())
			return err
		}

		if !IsMetaReplicaLearner(mp, learner) {
			continue
		}
		srcAddr, err = c.TryToSelectOneReplica(mp, learnerInfo.Learners, plan)
		if err != nil {
			log.LogErrorf("[PromoteMetaReplicaAndWait] mp[%v] select one memory store mode replica failed, err: %s", mp.PartitionID, err.Error())
			return err
		}
		learnerInfo.DeleteAddr = append(learnerInfo.DeleteAddr, srcAddr)

		mp.IsRecover.Store(true)
		mp.SrcAddr = srcAddr
		mp.LearnerDstAddr = learner
		mp.RecoverStartTime = time.Now().Unix()
		mp.RecoverFailCount = 0
		mp.RecoverState = proto.RecoverStateRecovering
		c.putBadMetaPartitions(srcAddr, mp.PartitionID)

		// wait for mp ready.
		err = c.waitForMetaPartitionReady(mp)
		if err != nil {
			log.LogErrorf("waitForMetaPartitionReady error: %s", err.Error())
			return err
		}
	}

	return nil
}

func IsMetaReplicaLearner(mp *MetaPartition, learnerAddr string) bool {
	for _, peer := range mp.Peers {
		if peer.Type == raftProto.PeerLearner && peer.Addr == learnerAddr {
			return true
		}
	}
	return false
}

func (c *Cluster) TryToSelectOneReplica(mp *MetaPartition, excludeAddrs []string, plan *proto.PromoteLearnerPlan) (string, error) {
	excludeAddrsBackup := make([]string, 0, len(excludeAddrs)+1)
	excludeAddrsBackup = append(excludeAddrsBackup, excludeAddrs...)
	hasLeader := false
	leaderAddr := ""
	for _, mr := range mp.Replicas {
		if mr.IsLeader {
			excludeAddrsBackup = append(excludeAddrsBackup, mr.Addr)
			hasLeader = true
			leaderAddr = mr.Addr
			break
		}
	}
	param := &MetaPartitionPlanUserParams{
		SelectType: plan.SelectType,
		ZoneName:   plan.ZoneName,
		NodeSetID:  plan.NodeSetID,
		SelectTag:  plan.SelectTag,
		Mode:       plan.Mode,
	}

	// select one replica that is not leader and not in excludeAddr
	srcAddr := SelectOneReplicaStrickly(mp, excludeAddrsBackup, param)
	if srcAddr != "" {
		return srcAddr, nil
	}

	if !hasLeader {
		err := fmt.Errorf("can not select one replica to be removed")
		log.LogErrorf("[TryToSelectOneReplica] %s", err.Error())
		return "", err
	}

	srcAddr = SelectOneReplicaToDelete(mp, excludeAddrs, param)
	if srcAddr == "" {
		err := fmt.Errorf("no replica found after changing leader")
		log.LogErrorf("[TryToSelectOneReplica] %s", err.Error())
		return "", err
	}

	if srcAddr == leaderAddr {
		err := c.tryToChangeMetaPartitionLeader(mp, srcAddr)
		if err != nil {
			log.LogErrorf("tryToChangeMetaPartitionLeader error: %s", err.Error())
			return "", err
		}
	}

	return srcAddr, nil
}

func (c *Cluster) RestartPromoteLearnerPlan() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.IsClusterPlanNotIdle() {
		return nil
	}

	// Get the planed promote learner plan.
	plan, err := c.loadPromoteLearnerPlan()
	if err != nil {
		if err == proto.ErrNoPromoteLearnerPlan {
			return nil
		}
		log.LogErrorf("loadPromoteLearnerPlan err: %s", err.Error())
		return err
	}

	if plan.Status == PlanTaskDone {
		now := time.Now()
		if plan.Expire.Before(now) {
			err = c.syncDeletePromoteLearnerPlan()
			if err != nil {
				log.LogErrorf("syncDeletePromoteLearnerPlan err: %s", err.Error())
			}
		}
		return nil
	}

	if plan.Status != PlanTaskRun {
		// No start the plan task if the status is not running.
		return nil
	}

	c.SetClusterPlanRunning()
	go c.DoPromoteLearnerPlan(plan)

	return nil
}

func (c *Cluster) CreateDecommissionRocksdbDirPlan(param *MetaPartitionPlanUserParams) (*proto.ClusterPlan, error) {
	plan := &proto.ClusterPlan{
		Low:        make(map[string]*proto.ZonePressureView),
		RocksdbLow: make(map[string]*proto.ZonePressureView),
		Type:       DecommissionDir,
		Status:     PlanTaskRun,
		Mode:       proto.StoreModeRocksDb,
		RackLevel:  c.getRackAwareLevel(),
		FailedList: make([]uint64, 0),
		SelectType: param.SelectType,
		ZoneName:   param.ZoneName,
		NodeSetID:  param.NodeSetID,
		SelectTag:  param.SelectTag,
	}

	err := c.GetLowMemPressureTopology(plan)
	if err != nil {
		log.LogErrorf("GetLowMemPressureTopology error: %s", err.Error())
		return plan, err
	}

	err = c.FillDecommissionRocksdbDirToPlan(param.MetaNodeAddr, param.RocksdbDir, plan)
	if err != nil {
		log.LogErrorf("FillDecommissionRocksdbDirToPlan error: %s", err.Error())
		return plan, err
	}

	err = c.FindMigrateDestination(plan)
	if err != nil {
		log.LogErrorf("FindMigrateDestination error: %s", err.Error())
		return plan, err
	}
	plan.Total = len(plan.Plan)
	if plan.Total <= 0 {
		plan.Msg = "failed to create decommission rocksdb dir plan"
		return plan, fmt.Errorf("failed to create decommission rocksdb dir plan")
	}
	plan.UndoNum = int32(plan.Total)
	for _, mpPlan := range plan.Plan {
		plan.TotalReplicaNum += len(mpPlan.Plan)
	}
	plan.UndoReplicaNum = int32(plan.TotalReplicaNum)
	plan.StartTime = time.Now()

	err = c.syncAddBalanceTask(plan)
	if err != nil {
		log.LogErrorf("syncAddBalanceTask error: %s", err.Error())
		return plan, err
	}

	c.SetClusterPlanRunning()
	go c.DoMetaPartitionBalanceTask(plan)

	return plan, nil
}

func (c *Cluster) FillDecommissionRocksdbDirToPlan(addr string, rocksdbDir string, plan *proto.ClusterPlan) (err error) {
	// get the copied meta partition list.
	mps := c.getAllMetaPartitionsByMetaNode(addr)
	plan.Plan = make([]*proto.MetaBalancePlan, 0, len(mps))

	for _, mp := range mps {
		if !IsRocksdbDirInMetaPartition(mp, addr, rocksdbDir) {
			continue
		}
		isRocksdbReplica := false
		for _, mr := range mp.Replicas {
			if mr.Addr == addr {
				isRocksdbReplica = mr.StoreMode == proto.StoreModeRocksDb
				break
			}
		}
		if !isRocksdbReplica {
			continue
		}
		mpPlan := &proto.MetaBalancePlan{
			ID:         mp.PartitionID,
			Original:   make([]*proto.MrBalanceInfo, 0, len(mp.Replicas)),
			OverLoad:   make([]*proto.MrBalanceInfo, 0, 1),
			Plan:       make([]*proto.MrBalanceInfo, 0, 1),
			InodeCount: mp.InodeCount,
			PlanNum:    0,
		}
		estimateSize := GetMetaPartitionMemorySize(mp)
		for _, mr := range mp.Replicas {
			mn, err := c.metaNode(mr.Addr)
			if err != nil {
				log.LogErrorf("Failed to get meta node(%s), err: %s", mr.Addr, err.Error())
				return err
			}
			mrRec := GetMetaReplicaRecord(mn)
			mrRec.SrcMemSize = estimateSize
			mrRec.StoreMode = mr.StoreMode
			mpPlan.Original = append(mpPlan.Original, mrRec)
			if mr.Addr == addr {
				mpPlan.OverLoad = append(mpPlan.OverLoad, mrRec)
				mpPlan.PlanNum += 1
			}
		}
		if mpPlan.PlanNum <= 0 {
			continue
		}
		plan.Plan = append(plan.Plan, mpPlan)

		plan.Total++
		if plan.Total >= MaxMpMigrateNum {
			break
		}
	}

	return nil
}

func IsRocksdbDirInMetaPartition(mp *MetaPartition, addr, rocksdbDir string) bool {
	for _, resp := range mp.LoadResponse {
		if resp.Addr == addr && resp.RocksdbDir == rocksdbDir {
			return true
		}
	}
	return false
}

func GetMetaNodeResourceReason(metaNode *MetaNode, storeMode proto.StoreMode) (msg string, allocatAble bool) {
	nodeMemRatio := CaculateNodeMemoryRatio(metaNode)
	if nodeMemRatio > gConfig.metaNodeMemHighPer {
		msg = fmt.Sprintf("metanode memory %s total: %d, used: %d, ratio: %f > %f", metaNode.Addr, metaNode.NodeMemTotal, metaNode.NodeMemUsed, nodeMemRatio, gConfig.metaNodeMemHighPer)
		return
	}

	if storeMode == proto.StoreModeMem {
		if canAllocPartition(metaNode, MetaNodeType, 1) {
			if metaNode.Ratio <= gConfig.metaNodeMemLowPer && nodeMemRatio <= gConfig.metaNodeMemLowPer {
				msg = fmt.Sprintf("%s can allocate memory replica", metaNode.Addr)
				allocatAble = true
				return
			}
		}
	} else {
		if canAllocPartition(metaNode, RocksdbType, 1) {
			if IsRocksdbDiskUsageLow(metaNode) {
				msg = fmt.Sprintf("%s can allocate rocksdb replica", metaNode.Addr)
				allocatAble = true
				return
			}
		}
	}

	if !metaNode.PartitionCntLimitedEx(1) {
		msg = fmt.Sprintf("%s mpCount(%v) > limit(%v)", metaNode.Addr, metaNode.MetaPartitionCount, metaNode.GetPartitionLimitCnt())
		return
	}
	if !metaNode.IsActive {
		msg = fmt.Sprintf("%s is not active", metaNode.Addr)
		return
	}
	if metaNode.MetaPartitionCount >= defaultMaxMetaPartitionCountOnEachNode {
		msg = fmt.Sprintf("%s metaPartitionCount(%v) >= defaultMaxMetaPartitionCountOnEachNode(%v)", metaNode.Addr, metaNode.MetaPartitionCount, defaultMaxMetaPartitionCountOnEachNode)
		return
	}
	if metaNode.systemMemoryReachesThreshold() {
		msg = fmt.Sprintf("system memory %s total(%v) used(%v) threshold(%v)", metaNode.Addr, metaNode.NodeMemUsed, metaNode.NodeMemTotal, metaNode.Threshold)
		return
	}

	if storeMode == proto.StoreModeRocksDb {
		systemMemoryFreeSize := metaNode.NodeMemTotal - metaNode.NodeMemUsed
		if systemMemoryFreeSize <= gConfig.metaNodeReservedMem {
			msg = fmt.Sprintf("%s systemMemoryFreeSize(%v) <= reservedMem(%v)", metaNode.Addr, systemMemoryFreeSize, gConfig.metaNodeReservedMem)
			return
		}
		if metaNode.reachesRocksdbDisksThreshold() {
			msg = fmt.Sprintf("rocksdb disk %s total(%v) used(%v) threshold(%v)", metaNode.Addr, metaNode.GetRocksdbTotal(), metaNode.GetRocksdbUsed(), metaNode.RocksdbDiskThreshold)
			return
		}
		if !metaNode.rocksdbDiskKeyNumUnderMax() {
			msg = fmt.Sprintf("%s max(%v)", metaNode.Addr, metaNode.RocksdbKeyNumMax)
			return
		}
		if metaNode.RocksdbRdOnly {
			msg = fmt.Sprintf("%s RocksdbRdOnly is true", metaNode.Addr)
			return
		}
		if !IsRocksdbDiskUsageLow(metaNode) {
			for _, rocksdbDisk := range metaNode.RocksdbDisks {
				if rocksdbDisk.UsageRatio >= gConfig.metaNodeMemLowPer {
					msg = fmt.Sprintf("%s RocksdbDiskUsageRatio(%v) >= lowPer(%v)", metaNode.Addr, rocksdbDisk.UsageRatio, gConfig.metaNodeMemLowPer)
					return
				}
			}
		}
	} else {
		if metaNode.MaxMemAvailWeight <= gConfig.metaNodeReservedMem {
			msg = fmt.Sprintf("%s maxMemAvailWeight(%v) <= reservedMem(%v)", metaNode.Addr, metaNode.MaxMemAvailWeight, gConfig.metaNodeReservedMem)
			return
		}
		if metaNode.reachesThreshold() {
			msg = fmt.Sprintf("metanode memory %s total(%v) used(%v) threshold(%v)", metaNode.Addr, metaNode.Total, metaNode.Used, metaNode.Threshold)
			return
		}
		if metaNode.RdOnly {
			msg = fmt.Sprintf("%s is rdOnly", metaNode.Addr)
			return
		}
		if metaNode.Ratio > gConfig.metaNodeMemLowPer {
			msg = fmt.Sprintf("%s metanode memory ratio(%v) > lowPer(%v)", metaNode.Addr, metaNode.Ratio, gConfig.metaNodeMemLowPer)
			return
		}
		if nodeMemRatio > gConfig.metaNodeMemLowPer {
			msg = fmt.Sprintf("%s system memory ratio(%v) > lowPer(%v)", metaNode.Addr, nodeMemRatio, gConfig.metaNodeMemLowPer)
			return
		}
	}

	msg = fmt.Sprintf("metaNode(%s) is not writable with unknown reason", metaNode.Addr)
	return
}

func (c *Cluster) AnalyzeEmptyPlanReason(plan *proto.ClusterPlan) (err error) {
	var mps map[uint64]*MetaPartition
	if plan.Name != "" {
		vol, err := c.getVol(plan.Name)
		if err != nil {
			return err
		}

		mps = vol.cloneMetaPartitionMap()
	} else {
		mps = c.getAllMetaPartitions()
	}

	matchedCount := 0
	excessReplicaMps := make([]uint64, 0, len(mps))
	for _, mp := range mps {
		if plan.StartId != 0 && mp.PartitionID < plan.StartId {
			continue
		}
		if plan.EndId != 0 && mp.PartitionID > plan.EndId {
			continue
		}
		matchedCount++

		if len(mp.Replicas) > int(mp.ReplicaNum) {
			excessReplicaMps = append(excessReplicaMps, mp.PartitionID)
		}
	}

	if matchedCount == 0 {
		err = fmt.Errorf("no meta partitions found in vol(%s) start(%d) end(%d)", plan.Name, plan.StartId, plan.EndId)
		log.LogWarnf(err.Error())
		return err
	}

	if len(excessReplicaMps) == 0 {
		err = fmt.Errorf("volume(%s) total %d meta partitions checked, no replicas need to be migrated",
			plan.Name, matchedCount)
		log.LogWarnf(err.Error())
		return err
	}

	err = fmt.Errorf("no learner migration needed. But %d partitions have excess replicas: %v",
		len(excessReplicaMps), excessReplicaMps)
	log.LogWarnf(err.Error())
	return err
}

func (c *Cluster) SleepInMetaReplicaDelInterval() error {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for i := 0; i <= (mpReplicaDelInterval / 10); i++ {
		select {
		case <-ticker.C:
			if c.IsClusterPlanNotRun() {
				return ErrClusterPlanNotRunning
			}
		case <-c.stopc:
			c.SetClusterPlanStopping()
			return ErrClusterStop
		}
	}

	return nil
}

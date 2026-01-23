// Copyright 2026 The CubeFS Authors.
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
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	DefaultSelectTag            = ""
	MaxSelectTagDecommissionNum = 100
	CheckSelectTagInterval      = 1 * time.Minute
	StatusSleeping              = "sleeping"
	StatusChecking              = "checking"
	StatusDecommissioning       = "decommissioning"
	StatusCreatingPlan          = "creating plan"
	StatusIdle                  = "idle"
	StatusRunning               = "running"
	StatusStopping              = "stopping"
	EmptySelectTag              = "null"
)

var (
	DpSelectTagThreadStatus = StatusSleeping
	MpSelectTagThreadStatus = StatusSleeping
	MpFailedKeys            = make([]string, 0)
)

func (c *Cluster) scheduleToCheckDpSelectTag() {
	c.runTask(&cTask{
		tickTime: CheckSelectTagInterval,
		name:     "scheduleToCheckDpSelectTag",
		function: func() (fin bool) {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkDpSelectTag()
			}
			return
		},
	})
}

func (c *Cluster) checkDpSelectTag() {
	if !c.cfg.AutoFixSelectTag {
		return
	}
	DpSelectTagThreadStatus = StatusChecking
	defer func() {
		DpSelectTagThreadStatus = StatusSleeping
		if r := recover(); r != nil {
			log.LogWarnf("checkDpSelectTag occurred panic,err[%v]", r)
		}
	}()

	vols := c.allVols()
	count := 0
	for _, vol := range vols {
		if vol.isInitializingOrInitFailed() {
			continue
		}
		selectTagList := vol.GetDpSelectTagList(c)
		if len(selectTagList) == 0 {
			continue
		}

		vol.FixDataPartitionSelectTag(c)

		count += vol.countSelectTagDecommissionTask(c)
	}
	if count >= MaxSelectTagDecommissionNum {
		return
	}

	DpSelectTagThreadStatus = StatusDecommissioning

	total := MaxSelectTagDecommissionNum - count
	for _, vol := range vols {
		if vol.isInitializingOrInitFailed() {
			continue
		}

		num, err := vol.createSelectTagDecommissionTask(c, total)
		if err != nil {
			log.LogErrorf("checkDpSelectTag,vol[%v] create select tag decommission task failed,err[%v]", vol.Name, err)
			continue
		}
		total -= num
		if total <= 0 {
			break
		}
	}
}

func (vol *Vol) FixDataPartitionSelectTag(c *Cluster) {
	dpSelectTagList := vol.GetDpSelectTagList(c)
	if len(dpSelectTagList) == 0 {
		dpSelectTagList = []string{"", "", ""}
	}

	partitions := vol.dataPartitions.clonePartitions()
	for _, partition := range partitions {
		if partition == nil {
			continue
		}
		partition.Lock()
		replicas := partition.Replicas
		if len(replicas) == 0 {
			partition.Unlock()
			continue
		}

		desiredTags := make([]string, 0, len(replicas))
		if len(dpSelectTagList) >= len(replicas) {
			desiredTags = append(desiredTags, dpSelectTagList[:len(replicas)]...)
		} else {
			desiredTags = append(desiredTags, dpSelectTagList...)
			for i := len(dpSelectTagList); i < len(replicas); i++ {
				desiredTags = append(desiredTags, DefaultSelectTag)
			}
		}

		required := make(map[string]int)
		for _, tag := range desiredTags {
			required[tag]++
		}

		candidates := make([]*DataReplica, 0, len(replicas))
		changed := false
		for _, replica := range replicas {
			if replica == nil {
				continue
			}
			selectTag := GetDataPartitionPeerSelectTag(partition, replica.Addr)
			if required[selectTag] > 0 {
				required[selectTag]--
				continue
			}
			candidates = append(candidates, replica)
		}

		for _, tag := range desiredTags {
			if required[tag] == 0 || len(candidates) == 0 {
				continue
			}
			replica := candidates[0]
			candidates = candidates[1:]
			selectTag := GetDataPartitionPeerSelectTag(partition, replica.Addr)
			if selectTag != tag {
				SetDataPartitionPeerSelectTag(partition, replica.Addr, tag)
				changed = true
			}
			required[tag]--
		}

		for _, replica := range candidates {
			selectTag := GetDataPartitionPeerSelectTag(partition, replica.Addr)
			if selectTag != DefaultSelectTag {
				SetDataPartitionPeerSelectTag(partition, replica.Addr, DefaultSelectTag)
				changed = true
			}
		}

		partition.Unlock()
		if changed {
			err := c.syncUpdateDataPartition(partition)
			if err != nil {
				log.LogErrorf("FixDataPartitionSelectTag,vol[%v] partition[%v] fix dp select tag failed,err[%v]", vol.Name, partition.PartitionID, err)
			}
		}
	}
}

func (vol *Vol) countSelectTagDecommissionTask(c *Cluster) (count int) {
	partitions := vol.dataPartitions.clonePartitions()
	for _, partition := range partitions {
		if partition.IsDiscard {
			continue
		}
		if partition.DecommissionType == SelectTagDecommission && partition.isPerformingDecommission(c) {
			count++
		}
	}
	return count
}

func (vol *Vol) createSelectTagDecommissionTask(c *Cluster, limit int) (num int, err error) {
	partitions := vol.dataPartitions.clonePartitions()
	for _, partition := range partitions {
		if partition.IsDiscard {
			continue
		}
		if partition.isPerformingDecommission(c) {
			continue
		}

		for _, replica := range partition.Replicas {
			if replica == nil {
				continue
			}
			dataNode := replica.getReplicaNode()
			if dataNode == nil {
				continue
			}
			selectTag := GetDataPartitionPeerSelectTag(partition, replica.Addr)
			if dataNode.SelectTag == DefaultSelectTag || dataNode.SelectTag == selectTag {
				continue
			}
			err = c.markDecommissionDataPartition(partition, dataNode, &DecommissionMarkParam{
				DstNodeSetID:     0,
				RaftForce:        false,
				MigrateType:      SelectTagDecommission,
				SelectTag:        selectTag,
				Weight:           0,
				SrcAddrs:         nil,
				DstAddrs:         nil,
				TriggerCondition: "",
			})
			if err != nil {
				return num, err
			}
			num++
			if num >= limit {
				return num, nil
			}
			break
		}
	}
	return num, nil
}

func (vol *Vol) GetDpSelectTagList(c *Cluster) []string {
	var (
		dpSelectTagList []string
		result          []string
	)

	result = make([]string, 0, vol.dpReplicaNum)

	dpSelectTag := vol.DpSelectTag
	if dpSelectTag != "" {
		dpSelectTagList = strings.Split(dpSelectTag, ",")
		for _, tag := range dpSelectTagList {
			tag = strings.TrimSpace(tag)
			if tag == "" || tag == EmptySelectTag {
				continue
			}
			result = append(result, tag)
		}
	}

	if len(result) > 0 {
		return result
	}

	dpSelectTag = c.cfg.DefaultDpSelectTag
	if dpSelectTag != "" {
		dpSelectTagList = strings.Split(dpSelectTag, ",")
		for _, tag := range dpSelectTagList {
			tag = strings.TrimSpace(tag)
			if tag == "" || tag == EmptySelectTag {
				continue
			}
			result = append(result, tag)
		}
	}
	return result
}

func (c *Cluster) scheduleToCheckMpSelectTag() {
	c.runTask(&cTask{
		tickTime: CheckSelectTagInterval,
		name:     "scheduleToCheckMpSelectTag",
		function: func() (fin bool) {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkMpSelectTag()
			}
			return
		},
	})
}

func (c *Cluster) checkMpSelectTag() {
	if !c.cfg.AutoFixSelectTag {
		return
	}
	MpSelectTagThreadStatus = StatusChecking
	defer func() {
		MpSelectTagThreadStatus = StatusSleeping
		if r := recover(); r != nil {
			log.LogWarnf("checkMpSelectTag occurred panic,err[%v]", r)
		}
	}()

	vols := c.allVols()
	for _, vol := range vols {
		if vol.isInitializingOrInitFailed() {
			continue
		}
		selectTagList := vol.GetMpSelectTagList(c)
		if len(selectTagList) == 0 {
			continue
		}

		vol.FixMetaPartitionSelectTag(c)
		log.LogDebugf("checkMpSelectTag,vol[%v] fix mp select tag", vol.Name)
	}

	if c.IsClusterPlanNotIdle() {
		return
	}

	MpSelectTagThreadStatus = StatusCreatingPlan

	mismatches := c.collectMpSelectTagMismatches(vols)
	selectedGroup := c.selectMpSelectTagMismatchGroup(mismatches)
	if len(selectedGroup) == 0 {
		return
	}

	if err := c.createAndRunMpSelectTagPlan(selectedGroup); err != nil {
		log.LogWarnf("checkMpSelectTag, create and run mp select tag plan failed, err[%v]", err)
		return
	}
}

func (c *Cluster) createAndRunMpSelectTagPlan(selectedGroup []*mpSelectTagMismatch) error {
	clearPlanStatus := true
	c.SetClusterPlanRunning()
	defer func() {
		if clearPlanStatus {
			c.SetClusterPlanIdle()
		}
	}()

	plan := c.createMpSelectTagPlan(selectedGroup)
	if plan == nil {
		return nil
	}
	if err := c.syncAddBalanceTask(plan); err != nil {
		log.LogWarnf("checkMpSelectTag, syncAddBalanceTask failed, tag[%v] mode[%v] err[%v]",
			plan.SelectTag, plan.Mode, err)
		return err
	}

	log.LogWarnf("checkMpSelectTag, create plan success, tag[%v] mode[%v] mpCount[%v] replicaCount[%v]",
		plan.SelectTag, plan.Mode, plan.Total, plan.TotalReplicaNum)
	clearPlanStatus = false
	go c.DoMetaPartitionBalanceTask(plan)
	return nil
}

type mpSelectTagMismatch struct {
	vol       *Vol
	partition *MetaPartition
	replica   *MetaReplica
	metaNode  *MetaNode
	storeMode proto.StoreMode
	selectTag string
}

func (c *Cluster) collectMpSelectTagMismatches(vols map[string]*Vol) []*mpSelectTagMismatch {
	mismatches := make([]*mpSelectTagMismatch, 0)
	for _, vol := range vols {
		if vol.isInitializingOrInitFailed() {
			continue
		}
		partitions := vol.cloneMetaPartitionMap()
		for _, partition := range partitions {
			if partition == nil {
				continue
			}
			partition.RLock()
			replicas := append([]*MetaReplica(nil), partition.Replicas...)
			partition.RUnlock()
			if len(replicas) == 0 {
				continue
			}
			for _, replica := range replicas {
				if replica == nil {
					continue
				}
				metaNode := replica.metaNode
				if metaNode == nil {
					var err error
					metaNode, err = c.metaNode(replica.Addr)
					if err != nil {
						log.LogWarnf("checkMpSelectTag, get metanode failed, vol[%v] mp[%v] addr[%v] err[%v]",
							vol.Name, partition.PartitionID, replica.Addr, err)
						continue
					}
				}
				selectTag := GetMetaPartitionPeerSelectTag(partition, replica.Addr)
				if metaNode.SelectTag == selectTag {
					continue
				}
				storeMode, err := c.getMetaPartitionStoreMode(partition, replica.Addr)
				if err != nil {
					log.LogWarnf("checkMpSelectTag, get store mode failed, vol[%v] mp[%v] addr[%v] err[%v]",
						vol.Name, partition.PartitionID, replica.Addr, err)
					continue
				}
				mismatches = append(mismatches, &mpSelectTagMismatch{
					vol:       vol,
					partition: partition,
					replica:   replica,
					metaNode:  metaNode,
					storeMode: storeMode,
					selectTag: selectTag,
				})
			}
		}
	}
	return mismatches
}

func (c *Cluster) selectMpSelectTagMismatchGroup(mismatches []*mpSelectTagMismatch) []*mpSelectTagMismatch {
	if len(mismatches) == 0 {
		return nil
	}
	grouped := make(map[string][]*mpSelectTagMismatch)
	for _, item := range mismatches {
		if item.selectTag == DefaultSelectTag {
			continue
		}
		key := item.selectTag + "|" + item.storeMode.Str()
		if contains(MpFailedKeys, key) {
			continue
		}
		grouped[key] = append(grouped[key], item)
	}
	var selectedGroup []*mpSelectTagMismatch
	for _, group := range grouped {
		if len(group) > len(selectedGroup) {
			selectedGroup = group
		}
	}
	return selectedGroup
}

func (c *Cluster) createMpSelectTagPlan(group []*mpSelectTagMismatch) *proto.ClusterPlan {
	if len(group) == 0 {
		return nil
	}
	selectTag := group[0].selectTag
	storeMode := group[0].storeMode
	plan := &proto.ClusterPlan{
		Low:            make(map[string]*proto.ZonePressureView),
		RocksdbLow:     make(map[string]*proto.ZonePressureView),
		Plan:           make([]*proto.MetaBalancePlan, 0),
		Name:           "",
		Status:         PlanTaskRun,
		Type:           AddLearner,
		Mode:           storeMode,
		ModeCnt:        1,
		StartId:        0,
		EndId:          0,
		RackLevel:      c.getRackAwareLevel(),
		FailedList:     make([]uint64, 0),
		AutoPromote:    true,
		SelectType:     SelectTypeNodeAddrs,
		SelectTag:      selectTag,
		DoneNum:        0,
		RunningNum:     0,
		DoneReplicaNum: 0,
		RunReplicaNum:  0,
	}
	if err := c.GetLowMemPressureTopology(plan); err != nil {
		log.LogWarnf("checkMpSelectTag, GetLowMemPressureTopology failed, tag[%v] mode[%v] err[%v]",
			selectTag, storeMode, err)
		return nil
	}
	totalReplica, err := c.fillMpSelectTagPlan(plan, group)
	if err != nil {
		log.LogWarnf("checkMpSelectTag, fillMpSelectTagPlan failed, tag[%v] mode[%v] err[%v]",
			selectTag, storeMode, err)
		if err == NotEnoughResource {
			key := group[0].selectTag + "|" + group[0].storeMode.Str()
			MpFailedKeys = append(MpFailedKeys, key)
		}
		return nil
	}
	if len(plan.Plan) == 0 || totalReplica == 0 {
		return nil
	}
	plan.Total = len(plan.Plan)
	plan.TotalReplicaNum = totalReplica
	plan.UndoNum = int32(plan.Total)
	plan.UndoReplicaNum = int32(plan.TotalReplicaNum)
	plan.StartTime = time.Now()
	return plan
}

func (c *Cluster) fillMpSelectTagPlan(plan *proto.ClusterPlan, group []*mpSelectTagMismatch) (int, error) {
	mpPlanMap := make(map[uint64]*proto.MetaBalancePlan)
	totalReplica := 0
	for _, item := range group {
		if totalReplica >= MaxMpMigrateNum {
			break
		}
		mpPlan := mpPlanMap[item.partition.PartitionID]
		if mpPlan == nil {
			mpPlan = c.buildMpSelectTagMpPlan(item)
			if mpPlan == nil {
				continue
			}
			mpPlanMap[item.partition.PartitionID] = mpPlan
			plan.Plan = append(plan.Plan, mpPlan)
		}
		dest, err := c.pickMpSelectTagDestination(plan, mpPlan, item)
		if err != nil {
			return totalReplica, err
		}
		if dest == nil {
			continue
		}
		mpPlan.Plan = append(mpPlan.Plan, dest)
		mpPlan.PlanNum = len(mpPlan.Plan)
		totalReplica++
	}
	return totalReplica, nil
}

func (c *Cluster) buildMpSelectTagMpPlan(item *mpSelectTagMismatch) *proto.MetaBalancePlan {
	item.partition.RLock()
	replicas := append([]*MetaReplica(nil), item.partition.Replicas...)
	item.partition.RUnlock()
	mpPlan := &proto.MetaBalancePlan{
		ID:         item.partition.PartitionID,
		Original:   make([]*proto.MrBalanceInfo, 0, len(replicas)),
		OverLoad:   make([]*proto.MrBalanceInfo, 0),
		Plan:       make([]*proto.MrBalanceInfo, 0),
		InodeCount: item.partition.InodeCount,
		PlanNum:    0,
	}
	memorySize := GetMetaPartitionMemorySize(item.partition)
	for _, mr := range replicas {
		if mr == nil {
			continue
		}
		mn := mr.metaNode
		if mn == nil {
			var err error
			mn, err = c.metaNode(mr.Addr)
			if err != nil {
				log.LogWarnf("checkMpSelectTag, get metanode failed, vol[%v] mp[%v] addr[%v] err[%v]",
					item.vol.Name, item.partition.PartitionID, mr.Addr, err)
				continue
			}
		}
		replicaStoreMode, err := c.getMetaPartitionStoreMode(item.partition, mr.Addr)
		if err != nil {
			log.LogWarnf("checkMpSelectTag, get store mode failed, vol[%v] mp[%v] addr[%v] err[%v]",
				item.vol.Name, item.partition.PartitionID, mr.Addr, err)
			continue
		}
		mrRec := &proto.MrBalanceInfo{
			Source:       mr.Addr,
			SrcMemSize:   memorySize,
			SrcNodeSetId: mn.NodeSetID,
			SrcZoneName:  mn.ZoneName,
			SrcRack:      mn.Rack,
			Status:       PlanTaskInit,
			StoreMode:    replicaStoreMode,
		}
		mpPlan.Original = append(mpPlan.Original, mrRec)
	}
	if len(mpPlan.Original) == 0 {
		return nil
	}
	return mpPlan
}

func (c *Cluster) pickMpSelectTagDestination(plan *proto.ClusterPlan, mpPlan *proto.MetaBalancePlan, item *mpSelectTagMismatch) (*proto.MrBalanceInfo, error) {
	memorySize := GetMetaPartitionMemorySize(item.partition)
	buildGetParam := func() *GetMigrateAddrParam {
		getParam := &GetMigrateAddrParam{
			Topo:        plan.Low,
			RocksdbTopo: plan.RocksdbLow,
			ZoneName:    item.metaNode.ZoneName,
			NodeSetID:   item.metaNode.NodeSetID,
			RequestNum:  1,
			LeastSize:   memorySize,
			IsRocksdb:   plan.Mode == proto.StoreModeRocksDb,
			RackLevel:   plan.RackLevel,
		}
		FillExcludeAddrIntoGetParam(mpPlan, getParam)
		return getParam
	}
	find, dests := GetMigrateDestAddr(buildGetParam())
	if !find {
		find, dests = GetMigrateAddrExcludeNodeSet(buildGetParam())
	}
	if !find {
		find, dests = GetMigrateAddrExcludeZone(buildGetParam())
	}
	if !find || len(dests) == 0 {
		log.LogWarnf("checkMpSelectTag, no destination found, vol[%v] mp[%v] src[%v] tag[%v]",
			item.vol.Name, item.partition.PartitionID, item.replica.Addr, item.selectTag)
		return nil, NotEnoughResource
	}
	dest := dests[0]
	dest.Source = item.replica.Addr
	for _, original := range mpPlan.Original {
		if original.Source == item.replica.Addr {
			dest.SrcMemSize = original.SrcMemSize
			dest.SrcNodeSetId = original.SrcNodeSetId
			dest.SrcZoneName = original.SrcZoneName
			dest.SrcRack = original.SrcRack
			dest.StoreMode = original.StoreMode
			break
		}
	}
	return dest, nil
}

func (vol *Vol) FixMetaPartitionSelectTag(c *Cluster) {
	mpSelectTagList := vol.GetMpSelectTagList(c)
	if len(mpSelectTagList) == 0 {
		mpSelectTagList = []string{"", "", ""}
	}

	partitions := vol.cloneMetaPartitionMap()
	for _, partition := range partitions {
		if partition == nil {
			continue
		}
		partition.Lock()
		replicas := partition.Replicas
		if len(replicas) == 0 {
			partition.Unlock()
			continue
		}
		desiredTags := make([]string, 0, len(replicas))
		if len(mpSelectTagList) >= len(replicas) {
			desiredTags = append(desiredTags, mpSelectTagList[:len(replicas)]...)
		} else {
			desiredTags = append(desiredTags, mpSelectTagList...)
			for i := len(mpSelectTagList); i < len(replicas); i++ {
				desiredTags = append(desiredTags, DefaultSelectTag)
			}
		}

		required := make(map[string]int)
		for _, tag := range desiredTags {
			required[tag]++
		}

		candidates := make([]*MetaReplica, 0, len(replicas))
		changed := false
		for _, replica := range replicas {
			if replica == nil {
				continue
			}
			selectTag := GetMetaPartitionPeerSelectTag(partition, replica.Addr)
			if required[selectTag] > 0 {
				required[selectTag]--
				continue
			}
			candidates = append(candidates, replica)
		}

		for _, tag := range desiredTags {
			if required[tag] == 0 || len(candidates) == 0 {
				continue
			}
			replica := candidates[0]
			candidates = candidates[1:]
			selectTag := GetMetaPartitionPeerSelectTag(partition, replica.Addr)
			if selectTag != tag {
				SetMetaPartitionPeerSelectTag(partition, replica.Addr, tag)
				changed = true
			}
			required[tag]--
		}

		for _, replica := range candidates {
			selectTag := GetMetaPartitionPeerSelectTag(partition, replica.Addr)
			if selectTag != DefaultSelectTag {
				SetMetaPartitionPeerSelectTag(partition, replica.Addr, DefaultSelectTag)
				changed = true
			}
		}
		partition.Unlock()
		if changed {
			err := c.syncUpdateMetaPartition(partition)
			if err != nil {
				log.LogErrorf("FixMetaPartitionSelectTag,vol[%v] partition[%v] fix mp select tag failed,err[%v]", vol.Name, partition.PartitionID, err)
			}
		}
	}
}

func (vol *Vol) GetMpSelectTagList(c *Cluster) []string {
	var (
		mpSelectTagList []string
		result          []string
	)

	result = make([]string, 0, vol.mpReplicaNum)

	mpSelectTag := vol.MpSelectTag
	if mpSelectTag != "" {
		mpSelectTagList = strings.Split(mpSelectTag, ",")
		for _, tag := range mpSelectTagList {
			tag = strings.TrimSpace(tag)
			if tag == "" || tag == EmptySelectTag {
				continue
			}
			result = append(result, tag)
		}
	}
	if len(result) > 0 {
		return result
	}

	mpSelectTag = c.cfg.DefaultMpSelectTag
	if mpSelectTag != "" {
		mpSelectTagList = strings.Split(mpSelectTag, ",")
		for _, tag := range mpSelectTagList {
			tag = strings.TrimSpace(tag)
			if tag == "" || tag == EmptySelectTag {
				continue
			}
			result = append(result, tag)
		}
	}
	return result
}

func (c *Cluster) getSelectTagSummary() (summary *proto.SelectTagSummary, err error) {
	summary = &proto.SelectTagSummary{
		AutoFixSelectTag:    c.cfg.AutoFixSelectTag,
		ClusterDpSelectTag:  c.cfg.DefaultDpSelectTag,
		ClusterMpSelectTag:  c.cfg.DefaultMpSelectTag,
		MigratingDps:        make([]uint64, 0, MaxSelectTagDecommissionNum),
		MigratingMps:        make([]uint64, 0, MaxMpMigrateNum),
		DpCheckThreadStatus: DpSelectTagThreadStatus,
		MpCheckThreadStatus: MpSelectTagThreadStatus,
		MpFailedKeys:        MpFailedKeys,
	}

	vols := c.allVols()
	summary.VolWithSelectTag = make([]string, 0, len(vols))
	for _, vol := range vols {
		if vol.isInitializingOrInitFailed() {
			continue
		}
		summary.VolumeNum++
		if vol.MpSelectTag == "" && vol.DpSelectTag == "" {
			continue
		}
		summary.VolWithTagNum++
		summary.VolWithSelectTag = append(summary.VolWithSelectTag, vol.Name)
		summary.MismatchDpNum += vol.countDpSelectTagMismatch()
		summary.MismatchMpNum += vol.countMpSelectTagMismatch()
		vol.getDecommissionSelectTagDps(c, summary)
		summary.DecommissionDpNum += vol.countSelectTagDecommissionTask(c)
	}
	switch atomic.LoadUint32(&c.planStatus) {
	case PlanStatusRun:
		summary.MpPlanStatus = StatusRunning
	case PlanStatusIdle:
		summary.MpPlanStatus = StatusIdle
	case PlanStatusStopping:
		summary.MpPlanStatus = StatusStopping
	default:
		summary.MpPlanStatus = "unknown"
	}

	c.getMpSelectTagPlan(summary)

	return summary, nil
}

func (c *Cluster) getMpSelectTagPlan(summary *proto.SelectTagSummary) {
	plans, err := c.loadBalanceTask()
	if err != nil {
		return
	}
	if plans == nil {
		return
	}
	for _, plan := range plans.Plan {
		isMigrating := false
		for _, mrPlan := range plan.Plan {
			if mrPlan.Status != PlanTaskDone {
				isMigrating = true
				break
			}
		}
		if isMigrating {
			summary.MigratingMps = append(summary.MigratingMps, plan.ID)
		}
	}
}

func (vol *Vol) getDecommissionSelectTagDps(c *Cluster, summary *proto.SelectTagSummary) {
	if len(summary.MigratingDps) >= MaxSelectTagDecommissionNum {
		return
	}
	partitions := vol.dataPartitions.clonePartitions()
	for _, partition := range partitions {
		if partition.IsDiscard {
			continue
		}
		if partition.DecommissionType == SelectTagDecommission && partition.isPerformingDecommission(c) {
			summary.MigratingDps = append(summary.MigratingDps, partition.PartitionID)
			if len(summary.MigratingDps) >= MaxSelectTagDecommissionNum {
				return
			}
		}
	}
}

func (vol *Vol) countDpSelectTagMismatch() (count int) {
	partitions := vol.dataPartitions.clonePartitions()
	for _, partition := range partitions {
		if partition.IsDiscard {
			continue
		}
		for _, replica := range partition.Replicas {
			if replica == nil {
				continue
			}
			if replica.dataNode == nil {
				continue
			}
			selectTag := GetDataPartitionPeerSelectTag(partition, replica.Addr)
			if selectTag != replica.dataNode.SelectTag {
				count++
				break
			}
		}
	}
	return count
}

func (vol *Vol) countMpSelectTagMismatch() (count int) {
	partitions := vol.cloneMetaPartitionMap()
	for _, partition := range partitions {
		if partition == nil {
			continue
		}
		for _, replica := range partition.Replicas {
			if replica == nil {
				continue
			}
			if replica.metaNode == nil {
				continue
			}
			selectTag := GetMetaPartitionPeerSelectTag(partition, replica.Addr)
			if selectTag != replica.metaNode.SelectTag {
				count++
				break
			}
		}
	}
	return count
}

func formatMetaReplicaSelectTag(selectTag string, metanode *MetaNode) string {
	if selectTag == metanode.SelectTag || selectTag == DefaultSelectTag || strings.Contains(selectTag, "->") {
		return selectTag
	}
	return fmt.Sprintf("%s->%s", metanode.SelectTag, selectTag)
}

func formatDataReplicaSelectTag(selectTag string, datanode *DataNode) string {
	if selectTag == datanode.SelectTag || selectTag == DefaultSelectTag || strings.Contains(selectTag, "->") {
		return selectTag
	}
	return fmt.Sprintf("%s->%s", datanode.SelectTag, selectTag)
}

func GetMetaPartitionPeerSelectTag(mp *MetaPartition, addr string) string {
	for _, peer := range mp.Peers {
		if peer.Addr == addr {
			return peer.SelectTag
		}
	}
	return DefaultSelectTag
}

func SetMetaPartitionPeerSelectTag(mp *MetaPartition, addr, selectTag string) {
	for i, peer := range mp.Peers {
		if peer.Addr == addr {
			mp.Peers[i].SelectTag = selectTag
			return
		}
	}
}

func GetDataPartitionPeerSelectTag(dp *DataPartition, addr string) string {
	for _, peer := range dp.Peers {
		if peer.Addr == addr {
			return peer.SelectTag
		}
	}
	return DefaultSelectTag
}

func SetDataPartitionPeerSelectTag(dp *DataPartition, addr, selectTag string) {
	for i, peer := range dp.Peers {
		if peer.Addr == addr {
			dp.Peers[i].SelectTag = selectTag
			return
		}
	}
}

func (c *Cluster) GetMetaNodeSelectTag(addr string) string {
	metaNode, err := c.metaNode(addr)
	if err != nil {
		return DefaultSelectTag
	}
	return metaNode.SelectTag
}

func (c *Cluster) GetDataNodeSelectTag(addr string) string {
	dataNode, err := c.dataNode(addr)
	if err != nil {
		return DefaultSelectTag
	}
	return dataNode.SelectTag
}

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
	DefaultTag            = ""
	MaxTagDecommissionNum = 100
	CheckTagInterval      = 1 * time.Minute
	StatusSleeping        = "sleeping"
	StatusChecking        = "checking"
	StatusDecommissioning = "decommissioning"
	StatusCreatingPlan    = "creating plan"
	StatusIdle            = "idle"
	StatusRunning         = "running"
	StatusStopping        = "stopping"
	EmptyTag              = "null"
	MaxMpDecommissionNum  = 5
)

var (
	DpTagThreadStatus = StatusSleeping
	MpTagThreadStatus = StatusSleeping
	MpFailedKeys      = make([]string, 0)
)

func (c *Cluster) scheduleToCheckDpTag() {
	c.runTask(&cTask{
		tickTime: CheckTagInterval,
		name:     "scheduleToCheckDpTag",
		function: func() (fin bool) {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkDpTag()
			}
			return
		},
	})
}

func (c *Cluster) checkDpTag() {
	if !c.cfg.AutoFixTag {
		return
	}
	DpTagThreadStatus = StatusChecking
	defer func() {
		DpTagThreadStatus = StatusSleeping
		if r := recover(); r != nil {
			log.LogWarnf("checkDpTag occurred panic,err[%v]", r)
		}
	}()

	vols := c.allVols()
	count := 0
	for _, vol := range vols {
		if vol.isInitializingOrInitFailed() {
			continue
		}
		tagList := vol.GetDpTagList(c)
		if len(tagList) == 0 {
			continue
		}

		vol.FixDataPartitionTag(c)

		count += vol.countTagDecommissionTask(c)
	}
	if count >= MaxTagDecommissionNum {
		return
	}

	DpTagThreadStatus = StatusDecommissioning

	total := MaxTagDecommissionNum - count
	for _, vol := range vols {
		if vol.isInitializingOrInitFailed() {
			continue
		}

		num, err := vol.createTagDecommissionTask(c, total)
		if err != nil {
			log.LogErrorf("checkDpTag,vol[%v] create tag decommission task failed,err[%v]", vol.Name, err)
			continue
		}
		total -= num
		if total <= 0 {
			break
		}
	}
}

func (vol *Vol) FixDataPartitionTag(c *Cluster) {
	dpTagList := vol.GetDpTagList(c)
	if len(dpTagList) == 0 {
		dpTagList = []string{"", "", ""}
	}

	partitions := vol.dataPartitions.clonePartitions()
	for _, partition := range partitions {
		if partition == nil {
			continue
		}
		partition.Lock()
		replicas := partition.Replicas
		if len(replicas) < 3 {
			partition.Unlock()
			continue
		}

		desiredTags := make([]string, 0, len(replicas))
		if len(dpTagList) >= len(replicas) {
			desiredTags = append(desiredTags, dpTagList...)
		} else {
			desiredTags = append(desiredTags, dpTagList...)
			for i := len(dpTagList); i < len(replicas); i++ {
				desiredTags = append(desiredTags, DefaultTag)
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
			tag := GetDataPartitionPeerTag(partition, replica.Addr)
			if required[tag] > 0 {
				required[tag]--
				continue
			}
			candidates = append(candidates, replica)
		}
		if len(candidates) == 0 {
			partition.Unlock()
			continue
		}

		for _, tag := range desiredTags {
			if required[tag] == 0 || len(candidates) == 0 {
				continue
			}
			replica := candidates[0]
			candidates = candidates[1:]
			currentTag := GetDataPartitionPeerTag(partition, replica.Addr)
			if currentTag != tag {
				SetDataPartitionPeerTag(partition, replica.Addr, tag)
				changed = true
			}
			required[tag]--
		}

		for _, replica := range candidates {
			tag := GetDataPartitionPeerTag(partition, replica.Addr)
			if tag != DefaultTag {
				SetDataPartitionPeerTag(partition, replica.Addr, DefaultTag)
				changed = true
			}
		}

		partition.Unlock()
		if changed {
			err := c.syncUpdateDataPartition(partition)
			if err != nil {
				log.LogErrorf("FixDataPartitionTag,vol[%v] partition[%v] fix dp tag failed,err[%v]", vol.Name, partition.PartitionID, err)
			}
		}
	}
}

func (vol *Vol) countTagDecommissionTask(c *Cluster) (count int) {
	partitions := vol.dataPartitions.clonePartitions()
	for _, partition := range partitions {
		if partition.IsDiscard {
			continue
		}
		if partition.DecommissionType == proto.TagDecommission && partition.isPerformingDecommission(c) {
			count++
		}
	}
	return count
}

func (vol *Vol) createTagDecommissionTask(c *Cluster, limit int) (num int, err error) {
	partitions := vol.dataPartitions.clonePartitions()
	for _, partition := range partitions {
		if partition.IsDiscard {
			continue
		}
		if partition.isPerformingDecommission(c) {
			continue
		}
		if len(partition.Replicas) < 3 {
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
			tag := GetDataPartitionPeerTag(partition, replica.Addr)
			if tag == DefaultTag || dataNode.Tag == tag {
				continue
			}

			log.LogWarnf("checkDpTag create dp decommission: vol[%v] dpId[%v] addr[%v] tag[%v]", vol.Name, partition.PartitionID, replica.Addr, tag)
			err = c.markDecommissionDataPartition(partition, dataNode, &DecommissionMarkParam{
				DstNodeSetID:     0,
				RaftForce:        false,
				MigrateType:      proto.TagDecommission,
				Tag:              tag,
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

func (vol *Vol) GetDpTagList(c *Cluster) []string {
	var (
		dpTagList []string
		result    []string
	)

	result = make([]string, 0, vol.dpReplicaNum)

	dpTag := vol.DpTag
	if dpTag != "" {
		dpTagList = strings.Split(dpTag, ",")
		for _, tag := range dpTagList {
			tag = strings.TrimSpace(tag)
			if tag == "" || tag == EmptyTag {
				continue
			}
			result = append(result, tag)
		}
	}

	if len(result) > 0 {
		return result
	}

	dpTag = c.cfg.DefaultDpTag
	if dpTag != "" {
		dpTagList = strings.Split(dpTag, ",")
		for _, tag := range dpTagList {
			tag = strings.TrimSpace(tag)
			if tag == "" || tag == EmptyTag {
				continue
			}
			result = append(result, tag)
		}
	}
	return result
}

func (c *Cluster) scheduleToCheckMpTag() {
	c.runTask(&cTask{
		tickTime: CheckTagInterval,
		name:     "scheduleToCheckMpTag",
		function: func() (fin bool) {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkMpTag()
			}
			return
		},
	})
}

func (c *Cluster) checkMpTag() {
	if !c.cfg.AutoFixTag {
		return
	}
	if c.IsClusterPlanNotIdle() {
		return
	}
	MpTagThreadStatus = StatusChecking
	if !c.TrySetClusterPlanRunning() {
		MpTagThreadStatus = StatusSleeping
		return
	}
	defer func() {
		MpTagThreadStatus = StatusSleeping
		c.SetClusterPlanIdle()
		if r := recover(); r != nil {
			log.LogWarnf("checkMpTag occurred panic,err[%v]", r)
		}
	}()

	vols := c.allVols()
	for _, vol := range vols {
		if vol.isInitializingOrInitFailed() {
			continue
		}
		tagList := vol.GetMpTagList(c)
		if len(tagList) == 0 {
			continue
		}

		vol.FixMetaPartitionTag(c)
		log.LogDebugf("checkMpTag,vol[%v] fix mp tag", vol.Name)
	}

	MpTagThreadStatus = StatusCreatingPlan

	mismatches := c.collectMpTagMismatches(vols)
	selectedGroup := c.selectMpTagMismatchGroup(mismatches)
	if len(selectedGroup) == 0 {
		return
	}

	num := c.GetMetaPartitionDecommissionCount(proto.TagDecommission)
	if num >= MaxMpDecommissionNum {
		return
	}

	for _, item := range selectedGroup {
		if item.tag == DefaultTag {
			continue
		}
		if contains(MpFailedKeys, item.tag+"|"+item.storeMode.Str()) {
			continue
		}
		if item.partition.IsRecover.Load() {
			continue
		}

		dstAddr, err := c.selectOneTargetMetaReplica(item.partition, item.replica.Addr, item.tag, item.storeMode)
		if err != nil {
			log.LogWarnf("checkMpTag, select one target meta replica failed, vol[%v] mp[%v] addr[%v] err[%v]",
				item.vol.Name, item.partition.PartitionID, item.replica.Addr, err)
			key := item.tag + "|" + item.storeMode.Str()
			MpFailedKeys = append(MpFailedKeys, key)
			continue
		}

		if !item.partition.CheckLastDelReplicaTime() {
			continue
		}

		log.LogWarnf("checkMpTag add learner: mp[%d] addr[%s]->dst[%s] tag[%s] storeMode[%s]", item.partition.PartitionID, item.replica.Addr, dstAddr, item.tag, item.storeMode.Str())
		err = c.migrateMetaPartitionByLearner(item.replica.Addr, dstAddr, item.partition, item.storeMode, proto.TagDecommission)
		if err != nil {
			log.LogWarnf("checkMpTag, add meta replica learner failed, vol[%v] mp[%v] addr[%v] err[%v]",
				item.vol.Name, item.partition.PartitionID, item.replica.Addr, err)
			continue
		}

		num++
		if num >= MaxMpDecommissionNum {
			break
		}
	}
}

func (c *Cluster) selectOneTargetMetaReplica(mp *MetaPartition, srcAddr string, selectTag string, storeMode proto.StoreMode) (string, error) {
	mp.RLock()
	oldHosts := append([]string(nil), mp.Hosts...)
	mp.RUnlock()

	nodeType := TypeMetaPartition
	if storeMode == proto.StoreModeRocksDb {
		nodeType = TypeRocksdbPartition
	}

	metaNode, err := c.metaNode(srcAddr)
	if err != nil {
		log.LogWarnf("selectOneTargetMetaReplica, get meta node failed, mp[%d] addr[%s] err[%v]", mp.PartitionID, srcAddr, err)
		return "", err
	}

	zone, err := c.t.getZone(metaNode.ZoneName)
	if err != nil {
		log.LogWarnf("selectOneTargetMetaReplica, get zone failed, mp[%d] addr[%s] err[%v]", mp.PartitionID, srcAddr, err)
		return "", err
	}

	ns, err := zone.getNodeSet(metaNode.NodeSetID)
	if err != nil {
		log.LogWarnf("selectOneTargetMetaReplica, get node set failed, mp[%d] addr[%s] err[%v]", mp.PartitionID, srcAddr, err)
		return "", err
	}

	param := &selectParam{
		replicaNum:   1,
		excludeHosts: oldHosts,
		rackLevel:    c.getRackAwareLevel(),
		excludeRacks: c.GetExRacksByHosts(nodeType, oldHosts, srcAddr),
		selectType:   proto.SelectTypeTag,
		tag:          selectTag,
	}

	_, peers, err := ns.getAvailMetaNodeHosts(param, storeMode)
	if err == nil {
		return peers[0].Addr, nil
	}

	param.excludeNodeSets = append(param.excludeNodeSets, ns.ID)
	_, peers, err = zone.getAvailNodeHosts(nodeType, param)
	if err == nil {
		return peers[0].Addr, nil
	}

	zones := mp.getLiveZones(srcAddr)
	var excludeZone []string
	if len(zones) == 0 {
		excludeZone = append(excludeZone, zone.name)
	} else {
		excludeZone = append(excludeZone, zones[0])
	}

	_, peers, err = c.getHostFromNormalZone(nodeType, excludeZone, 1, "", proto.MediaType_Unspecified, param)
	if err == nil {
		return peers[0].Addr, nil
	}

	return "", fmt.Errorf("selectOneTargetMetaReplica, no available meta node hosts, mp[%d] addr[%s] err[%v]", mp.PartitionID, srcAddr, err)
}

type mpTagMismatch struct {
	vol       *Vol
	partition *MetaPartition
	replica   *MetaReplica
	metaNode  *MetaNode
	storeMode proto.StoreMode
	tag       string
}

func (c *Cluster) collectMpTagMismatches(vols map[string]*Vol) []*mpTagMismatch {
	mismatches := make([]*mpTagMismatch, 0)
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
			if partition.IsRecover.Load() {
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
						log.LogWarnf("checkMpTag, get metanode failed, vol[%v] mp[%v] addr[%v] err[%v]",
							vol.Name, partition.PartitionID, replica.Addr, err)
						continue
					}
				}
				tag := GetMetaPartitionPeerTag(partition, replica.Addr)
				if metaNode.Tag == tag {
					continue
				}
				storeMode, err := c.getMetaPartitionStoreMode(partition, replica.Addr)
				if err != nil {
					log.LogWarnf("checkMpTag, get store mode failed, vol[%v] mp[%v] addr[%v] err[%v]",
						vol.Name, partition.PartitionID, replica.Addr, err)
					continue
				}
				mismatches = append(mismatches, &mpTagMismatch{
					vol:       vol,
					partition: partition,
					replica:   replica,
					metaNode:  metaNode,
					storeMode: storeMode,
					tag:       tag,
				})
				break
			}
		}
	}
	return mismatches
}

func (c *Cluster) selectMpTagMismatchGroup(mismatches []*mpTagMismatch) []*mpTagMismatch {
	if len(mismatches) == 0 {
		return nil
	}
	grouped := make(map[string][]*mpTagMismatch)
	for _, item := range mismatches {
		if item.tag == DefaultTag {
			continue
		}
		key := item.tag + "|" + item.storeMode.Str()
		if contains(MpFailedKeys, key) {
			continue
		}
		grouped[key] = append(grouped[key], item)
	}
	var selectedGroup []*mpTagMismatch
	for _, group := range grouped {
		if len(group) > len(selectedGroup) {
			selectedGroup = group
		}
	}
	return selectedGroup
}

func (vol *Vol) FixMetaPartitionTag(c *Cluster) {
	mpTagList := vol.GetMpTagList(c)
	if len(mpTagList) == 0 {
		mpTagList = []string{"", "", ""}
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
		nonLearnerCount := 0
		for _, replica := range replicas {
			if replica != nil && !replica.IsLearner {
				nonLearnerCount++
			}
		}

		desiredTags := make([]string, 0, nonLearnerCount)
		if len(mpTagList) >= nonLearnerCount {
			desiredTags = append(desiredTags, mpTagList...)
		} else {
			desiredTags = append(desiredTags, mpTagList...)
			for i := len(mpTagList); i < nonLearnerCount; i++ {
				desiredTags = append(desiredTags, DefaultTag)
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
			if replica.IsLearner {
				tag := GetMetaPartitionPeerTag(partition, replica.Addr)
				if tag != DefaultTag {
					SetMetaPartitionPeerTag(partition, replica.Addr, DefaultTag)
					changed = true
				}
				continue
			}

			tag := GetMetaPartitionPeerTag(partition, replica.Addr)
			if required[tag] > 0 {
				required[tag]--
				continue
			}
			candidates = append(candidates, replica)
		}

		if len(candidates) == 0 {
			partition.Unlock()
			continue
		}

		for _, tag := range desiredTags {
			if required[tag] == 0 || len(candidates) == 0 {
				continue
			}
			replica := candidates[0]
			candidates = candidates[1:]
			currentTag := GetMetaPartitionPeerTag(partition, replica.Addr)
			if currentTag != tag {
				SetMetaPartitionPeerTag(partition, replica.Addr, tag)
				changed = true
			}
			required[tag]--
		}

		for _, replica := range candidates {
			tag := GetMetaPartitionPeerTag(partition, replica.Addr)
			if tag != DefaultTag {
				SetMetaPartitionPeerTag(partition, replica.Addr, DefaultTag)
				changed = true
			}
		}
		partition.Unlock()
		if changed {
			err := c.syncUpdateMetaPartition(partition)
			if err != nil {
				log.LogErrorf("FixMetaPartitionTag,vol[%v] partition[%v] fix mp tag failed,err[%v]", vol.Name, partition.PartitionID, err)
			}
		}
	}
}

func (vol *Vol) GetMpTagList(c *Cluster) []string {
	var (
		mpTagList []string
		result    []string
	)

	result = make([]string, 0, vol.mpReplicaNum)

	mpTag := vol.MpTag
	if mpTag != "" {
		mpTagList = strings.Split(mpTag, ",")
		for _, tag := range mpTagList {
			tag = strings.TrimSpace(tag)
			if tag == "" || tag == EmptyTag {
				continue
			}
			result = append(result, tag)
		}
	}
	if len(result) > 0 {
		return result
	}

	mpTag = c.cfg.DefaultMpTag
	if mpTag != "" {
		mpTagList = strings.Split(mpTag, ",")
		for _, tag := range mpTagList {
			tag = strings.TrimSpace(tag)
			if tag == "" || tag == EmptyTag {
				continue
			}
			result = append(result, tag)
		}
	}
	return result
}

func (c *Cluster) getTagSummary() (summary *proto.TagSummary, err error) {
	summary = &proto.TagSummary{
		AutoFixTag:          c.cfg.AutoFixTag,
		ClusterDpTag:        c.cfg.DefaultDpTag,
		ClusterMpTag:        c.cfg.DefaultMpTag,
		MigratingDps:        make([]uint64, 0, MaxTagDecommissionNum),
		DpCheckThreadStatus: DpTagThreadStatus,
		MpCheckThreadStatus: MpTagThreadStatus,
		MpFailedKeys:        MpFailedKeys,
	}

	vols := c.allVols()
	summary.VolWithTag = make([]string, 0, len(vols))
	for _, vol := range vols {
		if vol.isInitializingOrInitFailed() {
			continue
		}
		summary.VolumeNum++
		if vol.MpTag == "" && vol.DpTag == "" {
			continue
		}
		summary.VolWithTagNum++
		summary.VolWithTag = append(summary.VolWithTag, vol.Name)
		summary.MismatchDpNum += vol.countDpTagMismatch()
		summary.MismatchMpNum += vol.countMpTagMismatch()
		vol.getDecommissionTagDps(c, summary)
		summary.DecommissionDpNum += vol.countTagDecommissionTask(c)
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

	summary.MpDecommissionNum = c.GetMetaPartitionDecommissionCount(proto.TagDecommission)

	return summary, nil
}

func (vol *Vol) getDecommissionTagDps(c *Cluster, summary *proto.TagSummary) {
	if len(summary.MigratingDps) >= MaxTagDecommissionNum {
		return
	}
	partitions := vol.dataPartitions.clonePartitions()
	for _, partition := range partitions {
		if partition.IsDiscard {
			continue
		}
		if partition.DecommissionType == proto.TagDecommission && partition.isPerformingDecommission(c) {
			summary.MigratingDps = append(summary.MigratingDps, partition.PartitionID)
			if len(summary.MigratingDps) >= MaxTagDecommissionNum {
				return
			}
		}
	}
}

func (vol *Vol) countDpTagMismatch() (count int) {
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
			tag := GetDataPartitionPeerTag(partition, replica.Addr)
			if tag != replica.dataNode.Tag {
				count++
				break
			}
		}
	}
	return count
}

func (vol *Vol) countMpTagMismatch() (count int) {
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
			tag := GetMetaPartitionPeerTag(partition, replica.Addr)
			if tag != replica.metaNode.Tag {
				count++
				break
			}
		}
	}
	return count
}

func formatMetaReplicaTag(tag string, metanode *MetaNode) string {
	if tag == metanode.Tag || tag == DefaultTag || strings.Contains(tag, "->") {
		return tag
	}
	return fmt.Sprintf("%s->%s", metanode.Tag, tag)
}

func formatDataReplicaTag(tag string, datanode *DataNode) string {
	if tag == datanode.Tag || tag == DefaultTag || strings.Contains(tag, "->") {
		return tag
	}
	return fmt.Sprintf("%s->%s", datanode.Tag, tag)
}

func GetMetaPartitionPeerTag(mp *MetaPartition, addr string) string {
	for _, peer := range mp.Peers {
		if peer.Addr == addr {
			return peer.Tag
		}
	}
	return DefaultTag
}

func SetMetaPartitionPeerTag(mp *MetaPartition, addr, tag string) {
	for i, peer := range mp.Peers {
		if peer.Addr == addr {
			mp.Peers[i].Tag = tag
			return
		}
	}
}

func GetDataPartitionPeerTag(dp *DataPartition, addr string) string {
	for _, peer := range dp.Peers {
		if peer.Addr == addr {
			return peer.Tag
		}
	}
	return DefaultTag
}

func SetDataPartitionPeerTag(dp *DataPartition, addr, tag string) {
	for i, peer := range dp.Peers {
		if peer.Addr == addr {
			dp.Peers[i].Tag = tag
			return
		}
	}
}

func (c *Cluster) GetMetaNodeTag(addr string) string {
	metaNode, err := c.metaNode(addr)
	if err != nil {
		return DefaultTag
	}
	return metaNode.Tag
}

func (c *Cluster) GetDataNodeTag(addr string) string {
	dataNode, err := c.dataNode(addr)
	if err != nil {
		return DefaultTag
	}
	return dataNode.Tag
}

func (c *Cluster) IsMetaPartitionTagSet(volName string) bool {
	if c.cfg.DefaultMpTag != "" {
		return true
	}

	vol, err := c.getVol(volName)
	if err != nil {
		return false
	}

	return vol.MpTag != ""
}

func (c *Cluster) IsDataPartitionTagSet(volName string) bool {
	if c.cfg.DefaultDpTag != "" {
		return true
	}

	vol, err := c.getVol(volName)
	if err != nil {
		return false
	}

	return vol.DpTag != ""
}

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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	DefaultTag            = ""
	MaxTagDecommissionNum = 100
	TagReplicaRuleNum     = 3
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
	MaxMpFailedKeys       = 1024
	MaxTagSampleNum       = 10

	ReasonPlanBusy                = "plan status is busy"
	ReasonDisableAutoFixTag       = "cluster auto fix tag is disabled"
	ReasonFailedToSetPlanRun      = "failed to set plan run"
	ReasonCloseOK                 = "close ok"
	ReasonSelectTagEmpty          = "select tag is empty"
	ReasonReachMaxDecommissionNum = "reach max decommission num"
)

var (
	DpTagThreadStatus = StatusSleeping
	MpTagThreadStatus = StatusSleeping
	MpFailedKeys      = make([]string, 0)
	LastMpQuitReason  string
	LastDpQuitReason  string
	LastDpThreadTime  time.Time
	LastMpThreadTime  time.Time
	tagStateMu        sync.Mutex
)

func addMpFailedKey(key string) {
	tagStateMu.Lock()
	defer tagStateMu.Unlock()
	if contains(MpFailedKeys, key) {
		return
	}
	MpFailedKeys = append(MpFailedKeys, key)
	if len(MpFailedKeys) > MaxMpFailedKeys {
		MpFailedKeys = MpFailedKeys[len(MpFailedKeys)-MaxMpFailedKeys:]
	}
}

func snapshotTagState() (dpStatus, mpStatus, lastDpReason, lastMpReason string, lastDpTime, lastMpTime time.Time, failedKeys []string) {
	tagStateMu.Lock()
	defer tagStateMu.Unlock()
	failedKeys = append([]string(nil), MpFailedKeys...)
	return DpTagThreadStatus, MpTagThreadStatus, LastDpQuitReason, LastMpQuitReason, LastDpThreadTime, LastMpThreadTime, failedKeys
}

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
	if !c.cfg.AutoFixTag.Load() {
		tagStateMu.Lock()
		DpTagThreadStatus = StatusSleeping
		LastDpQuitReason = ReasonDisableAutoFixTag
		LastDpThreadTime = time.Now()
		tagStateMu.Unlock()
		return
	}
	tagStateMu.Lock()
	DpTagThreadStatus = StatusChecking
	tagStateMu.Unlock()
	defer func() {
		tagStateMu.Lock()
		DpTagThreadStatus = StatusSleeping
		LastDpThreadTime = time.Now()
		tagStateMu.Unlock()
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

		if !vol.IsDataPartitionHasTag(c) {
			continue
		}

		vol.FixDataPartitionTag(c)

		count += vol.countTagDecommissionTask(c)
	}
	if count >= MaxTagDecommissionNum {
		tagStateMu.Lock()
		LastDpQuitReason = ReasonReachMaxDecommissionNum
		tagStateMu.Unlock()
		return
	}

	tagStateMu.Lock()
	DpTagThreadStatus = StatusDecommissioning
	tagStateMu.Unlock()

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
	tagStateMu.Lock()
	LastDpQuitReason = ReasonCloseOK
	tagStateMu.Unlock()
}

type tagReplicaInfo struct {
	addr       string
	nodeTag    string
	hasNodeTag bool
	isLearner  bool
}

func applyTagRulesToPeers(tagRules *TagRulesInfo, peers []proto.Peer, replicas []tagReplicaInfo) (changed bool) {
	if tagRules == nil {
		return false
	}

	tagRules.ClearMatch()
	peerIndexMap := make(map[string]int, len(peers))
	for i, peer := range peers {
		peerIndexMap[peer.Addr] = i
	}
	getPeerTag := func(addr string) string {
		if idx, ok := peerIndexMap[addr]; ok {
			return peers[idx].Tag
		}
		return DefaultTag
	}
	setPeerTag := func(addr, tag string) {
		if idx, ok := peerIndexMap[addr]; ok {
			peers[idx].Tag = tag
		}
	}

	// First pass: preserve tags that already satisfy destination slots.
	for _, replica := range replicas {
		currentTag := getPeerTag(replica.addr)
		if replica.isLearner {
			if currentTag != DefaultTag {
				setPeerTag(replica.addr, DefaultTag)
				changed = true
			}
			continue
		}
		if !replica.hasNodeTag {
			continue
		}
		if tagRules.MarkDestinationTag(replica.nodeTag) {
			if currentTag != replica.nodeTag {
				setPeerTag(replica.addr, replica.nodeTag)
				changed = true
			}
			continue
		}
		if tagRules.IsRuleAllTagMarked() {
			break
		}
	}

	// Second pass: fill remaining rule slots by source tag mapping.
	for _, replica := range replicas {
		if replica.isLearner || !replica.hasNodeTag {
			continue
		}
		if tagRules.IsRuleAllTagMarked() {
			break
		}
		currentTag := getPeerTag(replica.addr)
		dst, ok := tagRules.FindDst(replica.nodeTag)
		if ok && currentTag != dst {
			setPeerTag(replica.addr, dst)
			changed = true
			continue
		}
		// No rule matches this source tag: clear stale peer tag to default
		// so historical mappings can converge instead of lingering forever.
		if !ok && currentTag != DefaultTag {
			setPeerTag(replica.addr, DefaultTag)
			changed = true
		}
	}
	return changed
}

func (vol *Vol) FixDataPartitionTag(c *Cluster) {
	dpTagRules := vol.GetDpTagList(c)
	if dpTagRules.IsEmpty() {
		return
	}

	partitions := vol.dataPartitions.clonePartitions()
	for _, partition := range partitions {
		if partition == nil {
			continue
		}
		partition.Lock()
		replicaInfos := make([]tagReplicaInfo, 0, len(partition.Replicas))
		for _, replica := range partition.Replicas {
			if replica == nil {
				continue
			}
			dataNode := replica.getReplicaNode()
			if dataNode == nil {
				var err error
				dataNode, err = c.dataNode(replica.Addr)
				if err != nil {
					log.LogWarnf("FixDataPartitionTag,vol[%v] partition[%v] get datanode failed, addr[%v] err[%v]",
						vol.Name, partition.PartitionID, replica.Addr, err)
					replicaInfos = append(replicaInfos, tagReplicaInfo{addr: replica.Addr, hasNodeTag: false, isLearner: false})
					continue
				}
			}
			replicaInfos = append(replicaInfos, tagReplicaInfo{addr: replica.Addr, nodeTag: dataNode.Tag, hasNodeTag: true, isLearner: false})
		}
		changed := applyTagRulesToPeers(dpTagRules, partition.Peers, replicaInfos)
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

type TagMapInfo struct {
	Src   string
	Dst   string
	Match bool
}

type TagGroupInfo struct {
	Groups []*TagMapInfo
}

type TagRulesInfo struct {
	Rules     []*TagGroupInfo
	unmatched int
}

func (t *TagRulesInfo) IsEmpty() bool {
	return t == nil || len(t.Rules) == 0
}

func (t *TagRulesInfo) MarkDestinationTag(dst string) bool {
	if t == nil {
		return false
	}
	for _, rule := range t.Rules {
		for _, m := range rule.Groups {
			if m.Dst == dst && !m.Match {
				m.Match = true
				if t.unmatched > 0 {
					t.unmatched--
				}
				return true
			}
		}
	}
	return false
}

func (t *TagRulesInfo) IsRuleAllTagMarked() bool {
	if t == nil {
		return true
	}
	return t.unmatched == 0
}

func (t *TagRulesInfo) FindDst(src string) (string, bool) {
	if t == nil {
		return "", false
	}
	// Prefer exact source match first; fallback to DefaultTag only when no exact rule remains.
	for _, rule := range t.Rules {
		for _, m := range rule.Groups {
			if m.Src == src && !m.Match {
				m.Match = true
				if t.unmatched > 0 {
					t.unmatched--
				}
				return m.Dst, true
			}
		}
	}
	for _, rule := range t.Rules {
		for _, m := range rule.Groups {
			if m.Src == DefaultTag && !m.Match {
				m.Match = true
				if t.unmatched > 0 {
					t.unmatched--
				}
				return m.Dst, true
			}
		}
	}
	return "", false
}

func (t *TagRulesInfo) ClearMatch() {
	if t == nil {
		return
	}
	t.unmatched = 0
	for _, rule := range t.Rules {
		for _, m := range rule.Groups {
			m.Match = false
			t.unmatched++
		}
	}
}

func (t *TagRulesInfo) DstTags() []string {
	if t == nil {
		return nil
	}
	var tags []string
	for _, rule := range t.Rules {
		for _, m := range rule.Groups {
			tags = append(tags, m.Dst)
		}
	}
	return tags
}

func getEmptyTagRulesInfo() *TagRulesInfo {
	info := &TagRulesInfo{}
	info.Rules = make([]*TagGroupInfo, 1)
	info.Rules[0] = &TagGroupInfo{}
	info.Rules[0].Groups = make([]*TagMapInfo, TagReplicaRuleNum)
	for i := 0; i < TagReplicaRuleNum; i++ {
		info.Rules[0].Groups[i] = &TagMapInfo{}
		info.Rules[0].Groups[i].Src = DefaultTag
		info.Rules[0].Groups[i].Dst = DefaultTag
		info.Rules[0].Groups[i].Match = false
	}
	info.ClearMatch()
	return info
}

func parseTagRules(tag string) *TagRulesInfo {
	if tag == "" {
		return nil
	}
	info := &TagRulesInfo{}
	totalMappings := 0
	rules := strings.Split(tag, ";")
	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" || rule == EmptyTag {
			continue
		}
		group := &TagGroupInfo{
			Groups: make([]*TagMapInfo, 0, TagReplicaRuleNum),
		}
		parts := strings.Split(rule, "->")
		if len(parts) == 2 {
			srcTags := splitTagItems(parts[0])
			dstTags := splitTagItems(parts[1])
			for i := 0; i < len(srcTags) && i < len(dstTags); i++ {
				group.Groups = append(group.Groups, &TagMapInfo{Src: srcTags[i], Dst: dstTags[i]})
			}
		} else {
			log.LogErrorf("parseTagRules, rule[%v] format error", rule)
			continue
		}
		if len(group.Groups) > 0 {
			info.Rules = append(info.Rules, group)
			totalMappings += len(group.Groups)
		}
	}
	if len(info.Rules) == 0 {
		return nil
	}
	if totalMappings < TagReplicaRuleNum {
		padCount := TagReplicaRuleNum - totalMappings
		for i := 0; i < padCount; i++ {
			info.Rules[0].Groups = append(info.Rules[0].Groups, &TagMapInfo{Src: DefaultTag, Dst: DefaultTag})
		}
	}
	info.ClearMatch()
	return info
}

func splitTagItems(s string) []string {
	items := strings.Split(s, ",")
	result := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" || item == EmptyTag {
			continue
		}
		result = append(result, item)
	}
	return result
}

func (vol *Vol) GetDpTagList(c *Cluster) *TagRulesInfo {
	result := parseTagRules(vol.DpTag)
	if result != nil {
		return result
	}
	result = parseTagRules(c.cfg.DefaultDpTag)
	if result != nil {
		return result
	}
	return getEmptyTagRulesInfo()
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
	if !c.cfg.AutoFixTag.Load() {
		tagStateMu.Lock()
		MpTagThreadStatus = StatusSleeping
		LastMpQuitReason = ReasonDisableAutoFixTag
		LastMpThreadTime = time.Now()
		tagStateMu.Unlock()
		return
	}
	if c.IsClusterPlanNotIdle() {
		tagStateMu.Lock()
		MpTagThreadStatus = StatusSleeping
		LastMpQuitReason = ReasonPlanBusy
		LastMpThreadTime = time.Now()
		tagStateMu.Unlock()
		return
	}
	tagStateMu.Lock()
	MpTagThreadStatus = StatusChecking
	tagStateMu.Unlock()
	if !c.TrySetClusterPlanRunning() {
		tagStateMu.Lock()
		MpTagThreadStatus = StatusSleeping
		LastMpQuitReason = ReasonFailedToSetPlanRun
		LastMpThreadTime = time.Now()
		tagStateMu.Unlock()
		return
	}
	defer func() {
		tagStateMu.Lock()
		MpTagThreadStatus = StatusSleeping
		LastMpThreadTime = time.Now()
		tagStateMu.Unlock()
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
		if !vol.IsMetaPartitionHasTag(c) {
			continue
		}

		vol.FixMetaPartitionTag(c)
		log.LogDebugf("checkMpTag,vol[%v] fix mp tag", vol.Name)
	}

	tagStateMu.Lock()
	MpTagThreadStatus = StatusCreatingPlan
	tagStateMu.Unlock()

	_, _, _, _, _, _, failedKeys := snapshotTagState()
	selectedGroup := c.collectAndSelectMpTagMismatchGroup(vols, failedKeys)
	if len(selectedGroup) == 0 {
		tagStateMu.Lock()
		LastMpQuitReason = ReasonSelectTagEmpty
		MpFailedKeys = make([]string, 0)
		tagStateMu.Unlock()
		return
	}

	num := c.GetMetaPartitionDecommissionCount(proto.TagDecommission)
	if num >= MaxMpDecommissionNum {
		tagStateMu.Lock()
		LastMpQuitReason = ReasonReachMaxDecommissionNum
		tagStateMu.Unlock()
		return
	}

	for _, item := range selectedGroup {
		if item.tag == DefaultTag {
			continue
		}
		if contains(failedKeys, item.tag+"|"+item.storeMode.Str()) {
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
			addMpFailedKey(key)
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
	tagStateMu.Lock()
	LastMpQuitReason = ReasonCloseOK
	tagStateMu.Unlock()
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
	if err == nil && len(peers) > 0 {
		return peers[0].Addr, nil
	}

	param.excludeNodeSets = append(param.excludeNodeSets, ns.ID)
	_, peers, err = zone.getAvailNodeHosts(nodeType, param)
	if err == nil && len(peers) > 0 {
		return peers[0].Addr, nil
	}

	zones := mp.getLiveZones(srcAddr)
	var excludeZone []string
	if len(zones) == 0 {
		excludeZone = append(excludeZone, zone.name)
	} else {
		excludeZone = append(excludeZone, zones[0])
	}

	_, peers, err = c.getHostFromNormalZone(nodeType, excludeZone, 1, "", param)
	if err == nil && len(peers) > 0 {
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

func (c *Cluster) collectAndSelectMpTagMismatchGroup(vols map[string]*Vol, failedKeys []string) []*mpTagMismatch {
	failedKeySet := make(map[string]struct{}, len(failedKeys))
	for _, key := range failedKeys {
		failedKeySet[key] = struct{}{}
	}

	grouped := make(map[string][]*mpTagMismatch)
	var selectedGroupKey string
	maxGroupSize := 0

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
				if metaNode.Tag == tag || tag == DefaultTag {
					continue
				}
				storeMode, err := c.getMetaPartitionStoreMode(partition, replica.Addr)
				if err != nil {
					log.LogWarnf("checkMpTag, get store mode failed, vol[%v] mp[%v] addr[%v] err[%v]",
						vol.Name, partition.PartitionID, replica.Addr, err)
					continue
				}
				key := tag + "|" + storeMode.Str()
				if _, skip := failedKeySet[key]; skip {
					continue
				}
				item := &mpTagMismatch{
					vol:       vol,
					partition: partition,
					replica:   replica,
					metaNode:  metaNode,
					storeMode: storeMode,
					tag:       tag,
				}
				grouped[key] = append(grouped[key], item)
				if len(grouped[key]) > maxGroupSize {
					maxGroupSize = len(grouped[key])
					selectedGroupKey = key
				}
				break
			}
		}
	}
	return grouped[selectedGroupKey]
}

func (vol *Vol) FixMetaPartitionTag(c *Cluster) {
	mpTagRules := vol.GetMpTagList(c)
	if mpTagRules.IsEmpty() {
		return
	}

	partitions := vol.cloneMetaPartitionMap()
	for _, partition := range partitions {
		if partition == nil {
			continue
		}
		partition.Lock()
		replicaInfos := make([]tagReplicaInfo, 0, len(partition.Replicas))
		for _, replica := range partition.Replicas {
			if replica == nil {
				continue
			}
			if replica.IsLearner {
				replicaInfos = append(replicaInfos, tagReplicaInfo{addr: replica.Addr, isLearner: true})
				continue
			}
			metaNode := replica.metaNode
			if metaNode == nil {
				var err error
				metaNode, err = c.metaNode(replica.Addr)
				if err != nil {
					log.LogWarnf("FixMetaPartitionTag,vol[%v] partition[%v] get metanode failed, addr[%v] err[%v]",
						vol.Name, partition.PartitionID, replica.Addr, err)
					replicaInfos = append(replicaInfos, tagReplicaInfo{addr: replica.Addr, hasNodeTag: false, isLearner: false})
					continue
				}
			}
			replicaInfos = append(replicaInfos, tagReplicaInfo{addr: replica.Addr, nodeTag: metaNode.Tag, hasNodeTag: true, isLearner: false})
		}
		changed := applyTagRulesToPeers(mpTagRules, partition.Peers, replicaInfos)
		partition.Unlock()
		if changed {
			err := c.syncUpdateMetaPartition(partition)
			if err != nil {
				log.LogErrorf("FixMetaPartitionTag,vol[%v] partition[%v] fix mp tag failed,err[%v]", vol.Name, partition.PartitionID, err)
			}
		}
	}
}

func (vol *Vol) GetMpTagList(c *Cluster) *TagRulesInfo {
	result := parseTagRules(vol.MpTag)
	if result != nil {
		return result
	}
	result = parseTagRules(c.cfg.DefaultMpTag)
	if result != nil {
		return result
	}
	return getEmptyTagRulesInfo()
}

func (c *Cluster) getTagSummary(detail bool) (summary *proto.TagSummary, err error) {
	dpStatus, mpStatus, lastDpReason, lastMpReason, lastDpTime, lastMpTime, failedKeys := snapshotTagState()
	summary = &proto.TagSummary{
		AutoFixTag:          c.cfg.AutoFixTag.Load(),
		ClusterDpTag:        c.cfg.DefaultDpTag,
		ClusterMpTag:        c.cfg.DefaultMpTag,
		DpCheckThreadStatus: dpStatus,
		MpCheckThreadStatus: mpStatus,
		UnmatchDpSamples:    make([]proto.TagMismatchSample, 0, MaxTagSampleNum),
		UnmatchMpSamples:    make([]proto.TagMismatchSample, 0, MaxTagSampleNum),
		DataNodeTagCount:    make(map[string]int),
		MetaNodeTagCount:    make(map[string]int),
		DataNodeSpace:       make(map[string]*proto.DataNodeSpace),
		MetaNodeSpace:       make(map[string]*proto.MetaNodeSpace),
		FailedMpKeys:        make([]string, 0, len(failedKeys)),
	}

	vols := c.allVols()
	for _, vol := range vols {
		if vol.isInitializingOrInitFailed() {
			continue
		}
		summary.VolumeNum++
		if vol.MpTag == "" && vol.DpTag == "" && c.cfg.DefaultMpTag == "" && c.cfg.DefaultDpTag == "" {
			summary.VolWithoutTagNum++
		} else {
			summary.VolWithTagNum++
		}
		dps := vol.dataPartitions.clonePartitions()
		for _, dp := range dps {
			if dp.IsDiscard {
				continue
			}
			summary.TotalDpNum++
		}
		mps := vol.cloneMetaPartitionMap()
		for _, mp := range mps {
			if mp == nil {
				continue
			}
			summary.TotalMpNum++
		}
		if detail {
			summary.UnmatchDpNum += vol.countDpTagUnmatch(summary)
			summary.UnmatchMpNum += vol.countMpTagUnmatch(summary)
		}

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

	if !detail {
		return summary, nil
	}

	c.collectNodeSpaceInfo(summary)

	summary.FailedMpKeys = append(summary.FailedMpKeys, failedKeys...)
	summary.LastDpQuitReason = lastDpReason
	summary.LastMpQuitReason = lastMpReason
	if !lastDpTime.IsZero() {
		last := lastDpTime
		summary.LastDpThreadTime = &last
	}
	if !lastMpTime.IsZero() {
		last := lastMpTime
		summary.LastMpThreadTime = &last
	}

	return summary, nil
}

func appendMismatchDpSample(summary *proto.TagSummary, volName string, partitionID uint64, addr, peerTag, nodeTag string) {
	if len(summary.UnmatchDpSamples) >= MaxTagSampleNum {
		return
	}
	summary.UnmatchDpSamples = append(summary.UnmatchDpSamples, proto.TagMismatchSample{
		Vol:         volName,
		PartitionID: partitionID,
		NodeAddr:    addr,
		PeerTag:     peerTag,
		NodeTag:     nodeTag,
	})
}

func appendMismatchMpSample(summary *proto.TagSummary, volName string, partitionID uint64, addr, peerTag, nodeTag string) {
	if len(summary.UnmatchMpSamples) >= MaxTagSampleNum {
		return
	}
	summary.UnmatchMpSamples = append(summary.UnmatchMpSamples, proto.TagMismatchSample{
		Vol:         volName,
		PartitionID: partitionID,
		NodeAddr:    addr,
		PeerTag:     peerTag,
		NodeTag:     nodeTag,
	})
}

func (vol *Vol) countDpTagUnmatch(summary *proto.TagSummary) (count int) {
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
			if tag != DefaultTag && tag != replica.dataNode.Tag {
				count++
				appendMismatchDpSample(summary, vol.Name, partition.PartitionID, replica.Addr, tag, replica.dataNode.Tag)
				break
			}
		}
	}
	return count
}

func (vol *Vol) countMpTagUnmatch(summary *proto.TagSummary) (count int) {
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
			if tag != DefaultTag && tag != replica.metaNode.Tag {
				count++
				appendMismatchMpSample(summary, vol.Name, partition.PartitionID, replica.Addr, tag, replica.metaNode.Tag)
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

func (vol *Vol) IsDataPartitionHasTag(c *Cluster) bool {
	if vol.DpTag != DefaultTag || c.cfg.DefaultDpTag != DefaultTag {
		return true
	}

	partitions := vol.dataPartitions.clonePartitions()
	for _, partition := range partitions {
		if partition.IsDiscard {
			continue
		}
		for _, peer := range partition.Peers {
			if peer.Tag != DefaultTag {
				return true
			}
		}
	}

	return false
}

func (vol *Vol) IsMetaPartitionHasTag(c *Cluster) bool {
	if vol.MpTag != DefaultTag || c.cfg.DefaultMpTag != DefaultTag {
		return true
	}

	partitions := vol.cloneMetaPartitionMap()
	for _, partition := range partitions {
		if partition == nil {
			continue
		}
		for _, peer := range partition.Peers {
			if peer.Tag != DefaultTag {
				return true
			}
		}
	}

	return false
}

func (c *Cluster) collectNodeSpaceInfo(summary *proto.TagSummary) {
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		summary.DataNodeTagCount[dataNode.Tag]++

		if v, ok := summary.DataNodeSpace[dataNode.Tag]; ok {
			v.Used += dataNode.Used
			v.Free += dataNode.AvailableSpace
			v.Total += dataNode.Total
		} else {
			summary.DataNodeSpace[dataNode.Tag] = &proto.DataNodeSpace{
				Used:        dataNode.Used,
				Free:        dataNode.AvailableSpace,
				Total:       dataNode.Total,
				Tag:         dataNode.Tag,
				WritableNum: 0,
			}
		}
		if dataNode.IsWriteAble() {
			summary.DataNodeSpace[dataNode.Tag].WritableNum++
		}
		return true
	})

	for _, val := range summary.DataNodeSpace {
		if val.Total > 0 {
			val.Ratio = float64(val.Used) / float64(val.Total)
		}
	}

	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		summary.MetaNodeTagCount[metaNode.Tag]++

		if v, ok := summary.MetaNodeSpace[metaNode.Tag]; ok {
			v.MemUsed += metaNode.Used
			v.MemTotal += metaNode.Total
			v.RocksdbUsed += metaNode.GetRocksdbUsed()
			v.RocksdbTotal += metaNode.GetRocksdbTotal()
			v.SystemMemoryUsed += metaNode.NodeMemUsed
			v.SystemMemoryTotal += metaNode.NodeMemTotal
		} else {
			summary.MetaNodeSpace[metaNode.Tag] = &proto.MetaNodeSpace{
				MemUsed:            metaNode.Used,
				MemTotal:           metaNode.Total,
				Tag:                metaNode.Tag,
				MemWritableNum:     0,
				RocksdbUsed:        metaNode.GetRocksdbUsed(),
				RocksdbTotal:       metaNode.GetRocksdbTotal(),
				RocksdbWritableNum: 0,
				SystemMemoryUsed:   metaNode.NodeMemUsed,
				SystemMemoryTotal:  metaNode.NodeMemTotal,
			}
		}
		if metaNode.IsWriteAble() {
			summary.MetaNodeSpace[metaNode.Tag].MemWritableNum++
		}
		if metaNode.IsRocksdbWriteAble() {
			summary.MetaNodeSpace[metaNode.Tag].RocksdbWritableNum++
		}

		return true
	})

	for _, val := range summary.MetaNodeSpace {
		val.MemFree = val.MemTotal - val.MemUsed
		val.RocksdbFree = val.RocksdbTotal - val.RocksdbUsed
		val.SystemMemoryFree = val.SystemMemoryTotal - val.SystemMemoryUsed
		if val.MemTotal > 0 {
			val.MemRatio = float64(val.MemUsed) / float64(val.MemTotal)
		}
		if val.RocksdbTotal > 0 {
			val.RocksdbRatio = float64(val.RocksdbUsed) / float64(val.RocksdbTotal)
		}
		if val.SystemMemoryTotal > 0 {
			val.SystemMemoryRatio = float64(val.SystemMemoryUsed) / float64(val.SystemMemoryTotal)
		}
	}
}

func (c *Cluster) getVolTagSummary(name string) (summary *proto.VolTagSummary, err error) {
	vol, err := c.getVol(name)
	if err != nil {
		log.LogErrorf("getVolTagSummary: get vol[%s] failed: %v", name, err)
		return nil, err
	}

	mps := vol.cloneMetaPartitionMap()
	dps := vol.dataPartitions.clonePartitions()
	UnmatchDps := make([]uint64, 0, len(dps))
	UnmatchMps := make([]uint64, 0, len(mps))

	summary = &proto.VolTagSummary{
		Vol:              name,
		MpTag:            vol.MpTag,
		DpTag:            vol.DpTag,
		EffectiveMpTags:  vol.GetMpTagList(c).DstTags(),
		EffectiveDpTags:  vol.GetDpTagList(c).DstTags(),
		VolStatus:        vol.Status,
		UnmatchDpNum:     0,
		UnmatchMpNum:     0,
		UnmatchDpSamples: make([]proto.TagMismatchSample, 0, MaxTagSampleNum),
		UnmatchMpSamples: make([]proto.TagMismatchSample, 0, MaxTagSampleNum),
		FailedMpKeys:     make([]string, 0),
	}

	for _, dp := range dps {
		if dp.IsDiscard {
			continue
		}
		summary.TotalDpNum++

		for _, replica := range dp.Replicas {
			if replica == nil {
				continue
			}
			if replica.dataNode == nil {
				continue
			}
			tag := GetDataPartitionPeerTag(dp, replica.Addr)
			if tag != DefaultTag && tag != replica.dataNode.Tag {
				if len(summary.UnmatchDpSamples) < MaxTagSampleNum {
					summary.UnmatchDpSamples = append(summary.UnmatchDpSamples, proto.TagMismatchSample{
						Vol:         vol.Name,
						PartitionID: dp.PartitionID,
						NodeAddr:    replica.Addr,
						PeerTag:     tag,
						NodeTag:     replica.dataNode.Tag,
					})
				}
				UnmatchDps = append(UnmatchDps, dp.PartitionID)
				break
			}
		}
	}

	for _, mp := range mps {
		if mp == nil {
			continue
		}
		summary.TotalMpNum++
		for _, replica := range mp.Replicas {
			if replica == nil {
				continue
			}
			if replica.metaNode == nil {
				continue
			}
			tag := GetMetaPartitionPeerTag(mp, replica.Addr)
			if tag != DefaultTag && tag != replica.metaNode.Tag {
				if len(summary.UnmatchMpSamples) < MaxTagSampleNum {
					summary.UnmatchMpSamples = append(summary.UnmatchMpSamples, proto.TagMismatchSample{
						Vol:         vol.Name,
						PartitionID: mp.PartitionID,
						NodeAddr:    replica.Addr,
						PeerTag:     tag,
						NodeTag:     replica.metaNode.Tag,
					})
				}
				UnmatchMps = append(UnmatchMps, mp.PartitionID)
				break
			}
		}
	}

	summary.UnmatchDpNum = len(UnmatchDps)
	summary.UnmatchMpNum = len(UnmatchMps)
	summary.UnmatchDps = joinUint64(UnmatchDps)
	summary.UnmatchMps = joinUint64(UnmatchMps)
	_, _, _, _, _, _, failedKeys := snapshotTagState()
	summary.FailedMpKeys = append(summary.FailedMpKeys, failedKeys...)

	return summary, nil
}

func FormatTag(tag string) string {
	if tag == EmptyTag {
		return DefaultTag
	}

	rules := strings.Split(tag, ";")
	formattedRules := make([]string, 0, len(rules))
	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" || rule == EmptyTag {
			continue
		}
		parts := strings.Split(rule, "->")
		if len(parts) == 2 {
			src := formatTagGroup(parts[0])
			dst := formatTagGroup(parts[1])
			if src != "" && dst != "" {
				formattedRules = append(formattedRules, src+"->"+dst)
			}
		} else {
			log.LogErrorf("FormatTag, rule[%s] format error", rule)
		}
	}
	if len(formattedRules) == 0 {
		return DefaultTag
	}
	return strings.Join(formattedRules, ";")
}

func formatTagGroup(group string) string {
	items := splitTagItems(group)
	if len(items) == 0 {
		return ""
	}
	return strings.Join(items, ",")
}

func joinUint64(values []uint64) string {
	if len(values) == 0 {
		return ""
	}
	items := make([]string, 0, len(values))
	for _, v := range values {
		items = append(items, strconv.FormatUint(v, 10))
	}
	return strings.Join(items, ",")
}

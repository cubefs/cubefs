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
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	raftProto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

type DiagnoseMetaPartitionInfo struct {
	NoLeaderMps                   []*MetaPartition
	LackReplicaMps                []*MetaPartition
	BadReplicaMps                 []*MetaPartition
	ExcessReplicaMPs              []*MetaPartition
	InodeCountNotEqualReplicaMps  []*MetaPartition
	MaxInodeNotEqualMPs           []*MetaPartition
	DentryCountNotEqualReplicaMps []*MetaPartition
	AutoLearner                   []*MetaPartition
	ManualLearner                 []*MetaPartition
}

func (c *Cluster) addDataNodeTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		c.addDataNodeTask(t)
	}
}

func (c *Cluster) addDataNodeTask(task *proto.AdminTask) {
	if task == nil {
		return
	}
	if node, err := c.dataNode(task.OperatorAddr); err != nil {
		log.LogWarnf("action[putTasks],nodeAddr:%s,taskID:%s,err:%v", task.OperatorAddr, task.ID, err)
	} else {
		node.TaskManager.AddTask(task)
	}
}

func (c *Cluster) addMetaNodeTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.metaNode(t.OperatorAddr); err != nil {
			log.LogWarnf("action[putTasks],nodeAddr:%s,taskID:%s,err:%v", t.OperatorAddr, t.ID, err)
		} else {
			node.Sender.AddTask(t)
		}
	}
}

func (c *Cluster) addLcNodeTasks(tasks []*proto.AdminTask) {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.lcNode(t.OperatorAddr); err != nil {
			log.LogWarnf("action[putTasks],nodeAddr:%s,taskID:%s,err:%v", t.OperatorAddr, t.ID, err)
		} else {
			node.TaskManager.AddTask(t)
		}
	}
}

func (c *Cluster) waitForResponseToLoadDataPartition(partitions []*DataPartition) {
	var wg sync.WaitGroup
	for _, dp := range partitions {
		wg.Add(1)
		go func(dp *DataPartition) {
			defer func() {
				wg.Done()
				if err := recover(); err != nil {
					const size = runtimeStackBufSize
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					log.LogErrorf("doLoadDataPartition panic %v: %s", err, buf)
				}
			}()
			c.doLoadDataPartition(dp)
		}(dp)
	}
	wg.Wait()
}

func (c *Cluster) loadDataPartition(dp *DataPartition) {
	go func() {
		c.doLoadDataPartition(dp)
	}()
}

func (c *Cluster) migrateMetaPartition(srcAddr, targetAddr string, mp *MetaPartition, dstStoreMode proto.StoreMode, decommissionType uint32) (err error) {
	if c.EnableMpDecommissionByLearner {
		return c.migrateMetaPartitionByLearner(srcAddr, targetAddr, mp, dstStoreMode, decommissionType)
	}

	var (
		newPeers          []proto.Peer
		finalDstStoreMode proto.StoreMode
		isLearner         bool
		manualPromote     bool
	)

	if err = c.CheckMetaPartitionDecommissionLimit(decommissionType); err != nil {
		return
	}

	log.LogWarnf("action[migrateMetaPartition],volName[%v], migrate from src[%s] to target[%s],partitionID[%v] begin",
		mp.volName, srcAddr, targetAddr, mp.PartitionID)

	// Note: Non-learner migration does not set RestoreStatus to avoid permanent blocking
	// when target node fails. This legacy migration mode will be deprecated in favor of
	// learner mode which has proper timeout and failure handling.
	// Prepare migration parameters
	newPeers, finalDstStoreMode, err = c.prepareMetaPartitionMigration(srcAddr, targetAddr, mp, dstStoreMode)
	if err != nil {
		log.LogErrorf("action[migrateMetaPartition],volName[%v], prepare failed,partitionID[%v],err[%v]",
			mp.volName, mp.PartitionID, err)
		goto errHandler
	}

	isLearner, manualPromote, err = getMetaReplicaLearnerInfo(mp, srcAddr)
	if err != nil {
		log.LogErrorf("action[migrateMetaPartition] getMetaReplicaLearnerInfo partitionID[%v] addr[%s], err[%v]",
			mp.PartitionID, srcAddr, err)
		goto errHandler
	}

	// Delete old replica and add new replica
	if err = c.deleteMetaReplica(mp, srcAddr, false, false); err != nil {
		goto errHandler
	}
	if isLearner {
		if manualPromote {
			err = c.addMetaReplicaLearner(mp, newPeers[0].Addr, finalDstStoreMode, "", true, decommissionType)
			if err != nil {
				log.LogErrorf("action[migrateMetaPartition] addMetaReplicaLearner partitionID[%v] addr[%s], err[%v]",
					mp.PartitionID, newPeers[0].Addr, err)
				goto errHandler
			}
		} else {
			log.LogWarnf("action[migrateMetaPartition] partitionID[%v] learner addr[%s] delete",
				mp.PartitionID, srcAddr)
		}
	} else {
		if err = c.addMetaReplica(mp, newPeers[0].Addr, finalDstStoreMode); err != nil {
			goto errHandler
		}
	}

	// Mark as recovering and put into recovery queue
	mp.IsRecover.Store(true)
	mp.RecoverStartTime = time.Now().Unix()
	mp.DecommissionType = decommissionType
	c.putBadMetaPartitions(srcAddr, mp.PartitionID)
	mp.RLock()
	c.syncUpdateMetaPartition(mp)
	mp.RUnlock()

	Warn(c.Name, fmt.Sprintf("action[migrateMetaPartition] clusterID[%v] vol[%v] meta partition[%v] "+
		"migrate addr[%v] success,new addr[%v]", c.Name, mp.volName, mp.PartitionID, srcAddr, newPeers[0].Addr))
	return

errHandler:
	msg := fmt.Sprintf("action[migrateMetaPartition],volName: %v,partitionID: %v,err: %v", mp.volName, mp.PartitionID, errors.Stack(err))
	log.LogError(msg)
	Warn(c.Name, msg)
	if err != nil {
		err = fmt.Errorf("action[migrateMetaPartition] vol[%v],partition[%v],err[%v]", mp.volName, mp.PartitionID, err)
	}
	return
}

// selectTargetMetaPeer encapsulates meta node selection with zone/nodeset fallback.
// srcAddr may be empty (self-heal) or provided (migration); targetAddr forces destination.
// It returns a slice to keep the call sites consistent with migration flows.
func (c *Cluster) selectTargetMetaPeer(mp *MetaPartition, srcAddr, targetAddr string, dstStoreMode proto.StoreMode) (
	peers []proto.Peer, finalDstStoreMode proto.StoreMode, err error,
) {
	mp.RLock()
	oldHosts := append([]string(nil), mp.Hosts...)
	mp.RUnlock()

	if len(oldHosts) == 0 {
		err = fmt.Errorf("no hosts in meta partition %v", mp.PartitionID)
		return
	}

	finalDstStoreMode = dstStoreMode
	if finalDstStoreMode == proto.StoreModeDef {
		finalDstStoreMode, err = c.getMetaPartitionStoreMode(mp, srcAddr)
		if err != nil {
			return
		}
	}

	nodeType := TypeMetaPartition
	if finalDstStoreMode == proto.StoreModeRocksDb {
		nodeType = TypeRocksdbPartition
	}

	baseAddr := srcAddr
	if baseAddr == "" {
		for _, host := range oldHosts {
			if _, e := c.metaNode(host); e == nil {
				baseAddr = host
				break
			}
		}
	}
	if baseAddr == "" {
		err = fmt.Errorf("no alive hosts in meta partition %v, hosts: %v", mp.PartitionID, oldHosts)
		return
	}

	metaNode, err := c.metaNode(baseAddr)
	if err != nil {
		return
	}
	zone, err := c.t.getZone(metaNode.ZoneName)
	if err != nil {
		return
	}
	ns, err := zone.getNodeSet(metaNode.NodeSetID)
	if err != nil {
		return
	}

	param := &selectParam{
		replicaNum:   1,
		excludeHosts: oldHosts,
		rackLevel:    c.getRackAwareLevel(),
		excludeRacks: c.GetExRacksByHosts(TypeMetaPartition, oldHosts, srcAddr),
	}
	if c.IsMetaPartitionTagSet(mp.volName) && srcAddr != "" {
		param.selectType = proto.SelectTypeTag
		param.tag = c.GetMetaNodeTag(srcAddr)
	}

	if targetAddr != "" {
		var targetMetaNode *MetaNode
		targetMetaNode, err = c.metaNode(targetAddr)
		if err != nil {
			err = fmt.Errorf("target node [%s] not found: %v", targetAddr, err)
			return
		}
		peers = []proto.Peer{{
			ID:            targetMetaNode.ID,
			Addr:          targetMetaNode.Addr,
			HeartbeatPort: targetMetaNode.HeartbeatPort,
			ReplicaPort:   targetMetaNode.ReplicaPort,
		}}
		return
	}

	if _, peers, err = ns.getAvailMetaNodeHosts(param, finalDstStoreMode); err == nil {
		return
	}

	if _, ok := c.vols[mp.volName]; !ok {
		log.LogWarnf("[selectTargetMetaPeer] clusterID[%v] partitionID:%v on node:[%v], err[%v]",
			c.Name, mp.PartitionID, mp.Hosts, err)
		return
	}
	if c.isFaultDomain(c.vols[mp.volName]) {
		log.LogWarnf("[selectTargetMetaPeer] clusterID[%v] partitionID:%v on node:[%v], err[%v]",
			c.Name, mp.PartitionID, mp.Hosts, err)
		return
	}

	param.excludeNodeSets = append(param.excludeNodeSets, ns.ID)
	if _, peers, err = zone.getAvailNodeHosts(nodeType, param); err == nil {
		return
	}

	zones := mp.getLiveZones(srcAddr)
	var excludeZone []string
	if len(zones) == 0 {
		excludeZone = append(excludeZone, zone.name)
	} else {
		excludeZone = append(excludeZone, zones[0])
	}

	if _, peers, err = c.getHostFromNormalZone(nodeType, excludeZone, 1, "", param); err == nil {
		return
	}

	return
}

// checkMultipleReplicasOnSameMachineForMigration checks if multiple replicas would be on the same machine after migration
func (c *Cluster) checkMultipleReplicasOnSameMachineForMigration(oldHosts []string, newPeerAddr string) error {
	finalHosts := make([]string, 0, len(oldHosts)+1)
	finalHosts = append(finalHosts, oldHosts...)
	finalHosts = append(finalHosts, newPeerAddr)
	return c.checkMultipleReplicasOnSameMachine(finalHosts)
}

// migrateMetaPartitionByLearner migrates meta partition using learner mode
// Synchronous process:
// 1. Check if learner mode is enabled
// 2. Select target node
// 3. Add learner replica
// 4. Add raft learner
// 5. Update master hosts and mark as recovering in learner mode
// 6. Put mp into recovery queue
func (c *Cluster) migrateMetaPartitionByLearner(srcAddr, targetAddr string, mp *MetaPartition, dstStoreMode proto.StoreMode, decommissionType uint32) (err error) {
	var (
		newPeers          []proto.Peer
		finalDstStoreMode proto.StoreMode
		isLearner         bool
		manualPromote     bool
	)

	log.LogWarnf("action[migrateMetaPartitionByLearner],volName[%v], migrate from src[%s] to target[%s],partitionID[%v] begin",
		mp.volName, srcAddr, targetAddr, mp.PartitionID)
	auditMsg := fmt.Sprintf("migrateMetaPartitionByLearner: vol[%v] mp[%v] start migrating from src[%v] to target[%v]",
		mp.volName, mp.PartitionID, srcAddr, targetAddr)
	auditlog.LogMasterOp("migrateMetaPartitionByLearner", auditMsg, nil)

	// Check if learner mode is enabled
	if !c.EnableMpDecommissionByLearner {
		log.LogWarnf("action[migrateMetaPartitionByLearner],volName[%v], learner mode is disabled, fallback to normal migration",
			mp.volName)
		return c.migrateMetaPartition(srcAddr, targetAddr, mp, dstStoreMode, decommissionType)
	}

	// Prepare migration parameters
	newPeers, finalDstStoreMode, err = c.prepareMetaPartitionMigration(srcAddr, targetAddr, mp, dstStoreMode)
	if err != nil {
		log.LogErrorf("action[migrateMetaPartitionByLearner],volName[%v], prepare failed,partitionID[%v],err[%v]",
			mp.volName, mp.PartitionID, err)
		goto errHandler
	}

	isLearner, manualPromote, err = getMetaReplicaLearnerInfo(mp, srcAddr)
	if err != nil {
		log.LogErrorf("action[migrateMetaPartitionByLearner] getMetaReplicaLearnerInfo partitionID[%v] addr[%s], err[%v]",
			mp.PartitionID, srcAddr, err)
		goto errHandler
	}

	if isLearner && manualPromote {
		if err = c.addMetaReplicaLearner(mp, newPeers[0].Addr, finalDstStoreMode, "", true, decommissionType); err != nil {
			log.LogErrorf("action[migrateMetaPartitionByLearner] addMetaReplicaLearner partitionID[%v] addr[%s], err[%v]",
				mp.PartitionID, newPeers[0].Addr, err)
			goto errHandler
		}
		if err = c.deleteMetaReplica(mp, srcAddr, false, false); err != nil {
			log.LogErrorf("action[migrateMetaPartitionByLearner] deleteMetaReplica partitionID[%v] addr[%s], err[%v]",
				mp.PartitionID, srcAddr, err)
			goto errHandler
		}
	} else {
		if err = c.addMetaReplicaLearner(mp, newPeers[0].Addr, finalDstStoreMode, srcAddr, false, decommissionType); err != nil {
			log.LogErrorf("action[migrateMetaPartitionByLearner] addMetaReplicaLearner partitionID[%v] addr[%s], err[%v]",
				mp.PartitionID, newPeers[0].Addr, err)
			goto errHandler
		}
	}

	auditMsg = fmt.Sprintf("migrateMetaPartitionByLearner: vol[%v] mp[%v] migrate from src[%v] to learner[%v] success",
		mp.volName, mp.PartitionID, srcAddr, newPeers[0].Addr)
	auditlog.LogMasterOp("migrateMetaPartitionByLearner", auditMsg, nil)
	Warn(c.Name, fmt.Sprintf("action[migrateMetaPartitionByLearner] clusterID[%v] vol[%v] meta partition[%v] "+
		"migrate addr[%v] to learner[%v] success", c.Name, mp.volName, mp.PartitionID, srcAddr, newPeers[0].Addr))
	return

errHandler:
	msg := fmt.Sprintf("action[migrateMetaPartitionByLearner],volName: %v,partitionID: %v,err: %v", mp.volName, mp.PartitionID, errors.Stack(err))
	log.LogError(msg)
	Warn(c.Name, msg)
	if err != nil {
		err = fmt.Errorf("action[migrateMetaPartitionByLearner] vol[%v],partition[%v],err[%v]", mp.volName, mp.PartitionID, err)
	}
	return
}

// taking the given mata partition offline.
// 1. checking if the meta partition can be offline.
// There are two cases where the partition is not allowed to be offline:
// (1) the replica is not in the latest host list
// (2) there are too few replicas
// 2. choosing a new available meta node
// 3. synchronized decommission meta partition
// 4. synchronized create a new meta partition
// 5. persistent the new host list
func (c *Cluster) decommissionMetaPartition(nodeAddr string, mp *MetaPartition, dstStoreMode proto.StoreMode) (err error) {
	if c.ForbidMpDecommission {
		err = fmt.Errorf("cluster mataPartition decommission switch is disabled")
		return
	}
	return c.migrateMetaPartition(nodeAddr, "", mp, dstStoreMode, proto.ManualDecommission)
}

// prepareMetaPartitionMigration prepares common parameters and validates for meta partition migration
// Returns: newPeers, finalDstStoreMode, oldHosts, or error
func (c *Cluster) prepareMetaPartitionMigration(srcAddr, targetAddr string, mp *MetaPartition, dstStoreMode proto.StoreMode) (
	newPeers []proto.Peer, finalDstStoreMode proto.StoreMode, err error,
) {
	// Validate source address
	mp.RLock()
	if !contains(mp.Hosts, srcAddr) {
		mp.RUnlock()
		err = fmt.Errorf("src [%s] is not exist in mp(%d)", srcAddr, mp.PartitionID)
		return
	}
	oldHosts := mp.Hosts
	if targetAddr != "" && contains(mp.Hosts, targetAddr) {
		mp.RUnlock()
		err = fmt.Errorf("target [%s] is already exist in mp(%d) hosts:[%v]", targetAddr, mp.PartitionID, mp.Hosts)
		return
	}
	mp.RUnlock()

	// Validate decommission
	if err = c.validateDecommissionMetaPartition(mp, srcAddr); err != nil {
		return
	}

	// Select target node
	var selected []proto.Peer
	selected, finalDstStoreMode, err = c.selectTargetMetaPeer(mp, srcAddr, targetAddr, dstStoreMode)
	if err != nil {
		return
	}
	newPeers = selected

	// Log audit
	auditMsg := fmt.Sprintf("volName[%v] partitionID[%v] hosts[%v] srcAddr[%v] choose targetAddr[%v]",
		mp.volName, mp.PartitionID, mp.Hosts, srcAddr, newPeers)
	auditlog.LogMasterOp("migrateMetaPartition", auditMsg, err)

	// Check multiple replicas on same machine
	if err = c.checkMultipleReplicasOnSameMachineForMigration(oldHosts, newPeers[0].Addr); err != nil {
		return
	}

	return
}

func (c *Cluster) validateDecommissionMetaPartition(mp *MetaPartition, nodeAddr string) (err error) {
	mp.RLock()
	defer mp.RUnlock()

	var vol *Vol
	if vol, err = c.getVol(mp.volName); err != nil {
		return
	}

	if err = mp.canBeOffline(nodeAddr, int(vol.mpReplicaNum)); err != nil {
		return
	}

	if err = mp.hasMissingOneReplica(nodeAddr, int(vol.mpReplicaNum)); err != nil {
		return
	}

	if mp.IsRecover.Load() && !mp.activeMaxInodeSimilar() {
		err = fmt.Errorf("vol[%v],meta partition[%v] is recovering,[%v] can't be decommissioned", vol.Name, mp.PartitionID, nodeAddr)
		return
	}
	return
}

func (c *Cluster) checkInactiveMetaNodes() (inactiveMetaNodes []string, err error) {
	inactiveMetaNodes = make([]string, 0)

	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		if !metaNode.IsActive {
			inactiveMetaNodes = append(inactiveMetaNodes, metaNode.Addr)
		}
		return true
	})

	log.LogInfof("clusterID[%v] inactiveMetaNodes:%v", c.Name, inactiveMetaNodes)
	return
}

type VolNameSet map[string]struct{}

func (c *Cluster) checkReplicaMetaPartitions() (
	diagnoseInfo *DiagnoseMetaPartitionInfo, err error,
) {
	diagnoseInfo = &DiagnoseMetaPartitionInfo{
		NoLeaderMps:                   make([]*MetaPartition, 0),
		LackReplicaMps:                make([]*MetaPartition, 0),
		BadReplicaMps:                 make([]*MetaPartition, 0),
		ExcessReplicaMPs:              make([]*MetaPartition, 0),
		InodeCountNotEqualReplicaMps:  make([]*MetaPartition, 0),
		MaxInodeNotEqualMPs:           make([]*MetaPartition, 0),
		DentryCountNotEqualReplicaMps: make([]*MetaPartition, 0),
		AutoLearner:                   make([]*MetaPartition, 0),
		ManualLearner:                 make([]*MetaPartition, 0),
	}
	markDeleteVolNames := make(VolNameSet)
	vols := c.copyVols()
	for _, vol := range vols {
		if vol.IsDeleted() {
			markDeleteVolNames[vol.Name] = struct{}{}
			continue
		}
		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			if uint8(len(mp.Hosts)) < mp.ReplicaNum || uint8(len(mp.getActiveAddrs(defaultMetaPartitionTimeOutSec))) < mp.ReplicaNum {
				diagnoseInfo.LackReplicaMps = append(diagnoseInfo.LackReplicaMps, mp)
			}
			if !mp.isLeaderExist() && (time.Now().Unix()-mp.LeaderReportTime > c.cfg.MpNoLeaderReportIntervalSec) {
				diagnoseInfo.NoLeaderMps = append(diagnoseInfo.NoLeaderMps, mp)
			}
			if uint8(len(mp.Hosts)) > mp.ReplicaNum || uint8(len(mp.Replicas)) > mp.ReplicaNum {
				diagnoseInfo.ExcessReplicaMPs = append(diagnoseInfo.ExcessReplicaMPs, mp)
			}
			for _, replica := range mp.Replicas {
				if replica.Status == proto.Unavailable {
					diagnoseInfo.BadReplicaMps = append(diagnoseInfo.BadReplicaMps, mp)
					break
				}
			}
			for _, peer := range mp.Peers {
				if peer.Type == raftProto.PeerLearner {
					if peer.ManualPromote {
						diagnoseInfo.ManualLearner = append(diagnoseInfo.ManualLearner, mp)
					} else {
						diagnoseInfo.AutoLearner = append(diagnoseInfo.AutoLearner, mp)
					}
					break
				}
			}
		}
		vol.mpsLock.RUnlock()
	}
	c.inodeCountNotEqualMP.Range(func(key, value interface{}) bool {
		mp := value.(*MetaPartition)
		if _, ok := markDeleteVolNames[mp.volName]; !ok {
			diagnoseInfo.InodeCountNotEqualReplicaMps = append(diagnoseInfo.InodeCountNotEqualReplicaMps, mp)
		}
		return true
	})
	c.maxInodeNotEqualMP.Range(func(key, value interface{}) bool {
		mp := value.(*MetaPartition)
		if _, ok := markDeleteVolNames[mp.volName]; !ok {
			diagnoseInfo.MaxInodeNotEqualMPs = append(diagnoseInfo.MaxInodeNotEqualMPs, mp)
		}
		return true
	})
	c.dentryCountNotEqualMP.Range(func(key, value interface{}) bool {
		mp := value.(*MetaPartition)
		if _, ok := markDeleteVolNames[mp.volName]; !ok {
			diagnoseInfo.DentryCountNotEqualReplicaMps = append(diagnoseInfo.DentryCountNotEqualReplicaMps, mp)
		}
		return true
	})
	log.LogInfof("clusterID[%v], lackReplicaMetaPartitions count:[%v], noLeaderMetaPartitions count[%v]"+
		"unavailableReplicaMPs count:[%v], excessReplicaMp count:[%v]",
		c.Name, len(diagnoseInfo.LackReplicaMps), len(diagnoseInfo.NoLeaderMps),
		len(diagnoseInfo.BadReplicaMps), len(diagnoseInfo.ExcessReplicaMPs))
	return
}

func (c *Cluster) checkReplicaMetaPartitionsV1() (diagnosis *proto.MetaPartitionDiagnosisV1, err error) {
	diagnosis = &proto.MetaPartitionDiagnosisV1{}
	markDeleteVolNames := make(VolNameSet)
	vols := c.copyVols()
	for _, vol := range vols {
		if vol.IsDeleted() {
			markDeleteVolNames[vol.Name] = struct{}{}
			continue
		}

		vol.mpsLock.RLock()
		for _, mp := range vol.MetaPartitions {
			if isLackReplicaMetaPartition(mp) {
				diagnosis.LackReplicaMetaPartitionIDs = append(diagnosis.LackReplicaMetaPartitionIDs, mp.PartitionID)
			}

			if !mp.isLeaderExist() && (time.Now().Unix()-mp.LeaderReportTime > c.cfg.MpNoLeaderReportIntervalSec) {
				diagnosis.NoLeaderMetaPartitionIDs = append(diagnosis.NoLeaderMetaPartitionIDs, mp.PartitionID)
			}

			if IsExcessiveReplicaMetaPartition(mp) {
				diagnosis.ExcessiveReplicaMetaPartitionIDs = append(diagnosis.ExcessiveReplicaMetaPartitionIDs, mp.PartitionID)
			}

			if hasLearnerFlagMismatch(mp) {
				diagnosis.LearnerFlagMismatchIDs = append(diagnosis.LearnerFlagMismatchIDs, mp.PartitionID)
			}

			for _, replica := range mp.Replicas {
				if replica.Status == proto.Unavailable {
					diagnosis.UnavailableMetaPartitionIDs = append(diagnosis.UnavailableMetaPartitionIDs, mp.PartitionID)
					break
				}
			}

			if mp.RecoverState == proto.RecoverStateFailed {
				diagnosis.FailedRecoveryMetaPartitionIDs = append(diagnosis.FailedRecoveryMetaPartitionIDs, mp.PartitionID)
			}

			for _, peer := range mp.Peers {
				if peer.Type == raftProto.PeerLearner {
					if peer.ManualPromote {
						diagnosis.ManualLearnerMetaPartitionIDs = append(diagnosis.ManualLearnerMetaPartitionIDs, mp.PartitionID)
					} else {
						diagnosis.AutoLearnerMetaPartitionIDs = append(diagnosis.AutoLearnerMetaPartitionIDs, mp.PartitionID)
					}
					break
				}
			}
		}
		vol.mpsLock.RUnlock()
	}

	setAbnormalIDs := func(mpMap *sync.Map) []uint64 {
		var resultIDs []uint64
		mpMap.Range(func(key, value interface{}) bool {
			mp := value.(*MetaPartition)
			if _, ok := markDeleteVolNames[mp.volName]; !ok {
				resultIDs = append(resultIDs, mp.PartitionID)
			}
			return true
		})
		return resultIDs
	}

	diagnosis.InodeCountNotEqualIDs = setAbnormalIDs(c.inodeCountNotEqualMP)
	diagnosis.MaxInodeNotEqualIDs = setAbnormalIDs(c.maxInodeNotEqualMP)
	diagnosis.DentryCountNotEqualIDs = setAbnormalIDs(c.dentryCountNotEqualMP)
	diagnosis.AbnormalRaftIDs = setAbnormalIDs(c.AbnormalRaftMP)

	diagnosis.BadMetaPartitionInfos = c.getBadMetaPartitionsRepairView()

	log.LogInfof("clusterID[%v], lackReplicaMetaPartitions count:[%v], noLeaderMetaPartitions count[%v]"+
		"unavailableReplicaMPs count:[%v], excessReplicaMp count:[%v], AbnormalRaftIDs count:[%v]",
		c.Name, len(diagnosis.LackReplicaMetaPartitionIDs), len(diagnosis.NoLeaderMetaPartitionIDs),
		len(diagnosis.UnavailableMetaPartitionIDs), len(diagnosis.ExcessiveReplicaMetaPartitionIDs), len(diagnosis.AbnormalRaftIDs))

	if diagnosis.InactiveMetaNodes, err = c.checkInactiveMetaNodes(); err != nil {
		return
	}

	return
}

func (c *Cluster) deleteMetaReplica(partition *MetaPartition, addr string, validate bool, forceDel bool) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[deleteMetaReplica],vol[%v],data partition[%v],forceDel[%v],err[%v]", partition.volName, partition.PartitionID, forceDel, err)
			auditMsg := fmt.Sprintf("deleteMetaReplica: vol[%v] mp[%v] delete replica[%v] failed, forceDel[%v], err[%v]",
				partition.volName, partition.PartitionID, addr, forceDel, err)
			auditlog.LogMasterOp("deleteMetaReplica", auditMsg, err)
		} else {
			auditMsg := fmt.Sprintf("deleteMetaReplica: vol[%v] mp[%v] delete replica[%v] success, forceDel[%v]",
				partition.volName, partition.PartitionID, addr, forceDel)
			auditlog.LogMasterOp("deleteMetaReplica", auditMsg, nil)
		}
	}()

	// if !contains(partition.Hosts, addr) {
	// 	err = fmt.Errorf("addr[%s] is not exist in mp(%d)", addr, partition.PartitionID)
	// 	return
	// }

	isLearner := false
	nonLearnerNum := 0
	aliveHosts := make([]string, 0)
	removePeer := proto.Peer{}
	for _, peer := range partition.Peers {
		if peer.Addr == addr {
			removePeer = peer
			if peer.Type == raftProto.PeerLearner {
				isLearner = true
			}
		}
		metaNode, err1 := c.metaNode(peer.Addr)
		if err1 != nil {
			log.LogWarnf("action[deleteMetaReplica] metaNode[%v] not found, err[%v]", peer.Addr, err1)
			continue
		}
		if !metaNode.IsActive {
			continue
		}
		if peer.Type != raftProto.PeerLearner {
			nonLearnerNum++
			aliveHosts = append(aliveHosts, peer.Addr)
		}
	}

	if nonLearnerNum == 0 {
		err = fmt.Errorf("deleteMetaReplica: no non-learner replica alive, forbid deleting addr[%v]", addr)
		return
	}

	// Do not allow deleting the last non-learner replica.
	if !isLearner && nonLearnerNum <= 1 && contains(aliveHosts, addr) {
		err = fmt.Errorf("deleteMetaReplica: non-learner replicas count[%d] <= 1, forbid deleting addr[%v]", nonLearnerNum, addr)
		return
	}

	// partition.SrcAddr == addr means learner mode, and already checked
	if !partition.CheckLastDelReplicaTime() && !isLearner {
		err = fmt.Errorf("deleteMetaReplica: the interval between deleting or decommission mp replica should over 5 minute. last %d,  addr[%v]",
			partition.LastDelReplicaTime, addr)
		return
	}

	if validate && !isLearner && !forceDel {
		if err = c.validateDecommissionMetaPartition(partition, addr); err != nil {
			return
		}
	}

	metaNode, err := c.metaNode(addr)
	if metaNode != nil {
		removePeer = proto.Peer{ID: metaNode.ID, Addr: addr, HeartbeatPort: metaNode.HeartbeatPort, ReplicaPort: metaNode.ReplicaPort}
	} else {
		log.LogWarnf("action[deleteMetaReplica] metaNode[%v] not found, err[%v]", addr, err)
	}

	partition.LastDelReplicaTime = time.Now().Unix()
	if removePeer.ID != 0 {
		if err = c.removeMetaPartitionRaftMember(partition, removePeer, forceDel, false); err != nil {
			log.LogErrorf("action[removeMetaPartitionRaftMember] vol[%v],data partition[%v],forceDel[%v],err[%v]", partition.volName, partition.PartitionID, forceDel, err)
			return
		}

		if err = c.removeMetaHostMember(partition, removePeer); err != nil {
			log.LogErrorf("action[removeMetaHostMember] vol[%v],data partition[%v],forceDel[%v],err[%v]", partition.volName, partition.PartitionID, forceDel, err)
			return
		}
	}

	if metaNode != nil {
		if err = c.deleteMetaPartition(partition, metaNode, forceDel); err != nil {
			log.LogErrorf("action[deleteMetaPartition] vol[%v],data partition[%v],err[%v]", partition.volName, partition.PartitionID, err)
			return
		}
	}

	// if delete learner replica, clear learner recovery state
	if partition.LearnerDstAddr == addr {
		err = c.clearLearnerRecoveryState(partition)
		if err != nil {
			log.LogErrorf("action[deleteMetaReplica] vol[%v],data partition[%v],err[%v]", partition.volName, partition.PartitionID, err)
			return
		}

		auditMsg := fmt.Sprintf("deleteMetaReplica: vol[%v] mp[%v] delete learner replica[%v] success",
			partition.volName, partition.PartitionID, addr)
		auditlog.LogMasterOp("deleteMetaReplica", auditMsg, nil)
		return
	}

	if mr, err := partition.getMetaReplicaLeader(); err == nil && mr.Addr == addr {
		if len(partition.Hosts) > 0 {
			if metaNode, err := c.metaNode(partition.Hosts[0]); err == nil {
				partition.tryToChangeLeader(c, metaNode)
			}
		}
	}

	return
}

func (c *Cluster) deleteMetaPartition(partition *MetaPartition, removeMetaNode *MetaNode, forceDel bool) (err error) {
	partition.Lock()
	mr, err := partition.getMetaReplica(removeMetaNode.Addr)
	if err != nil {
		partition.Unlock()
		log.LogErrorf("action[deleteMetaPartition] vol[%v],meta partition[%v], err[%v]", partition.volName, partition.PartitionID, err)
		return nil
	}
	task := mr.createTaskToDeleteReplica(partition.PartitionID, forceDel)
	partition.removeReplicaByAddr(removeMetaNode.Addr)
	partition.removeMissingReplica(removeMetaNode.Addr)
	partition.Unlock()
	_, err = removeMetaNode.Sender.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteMetaPartition] vol[%v],meta partition[%v],err[%v]", partition.volName, partition.PartitionID, err)
	}
	return nil
}

func (c *Cluster) removeMetaHostMember(partition *MetaPartition, removePeer proto.Peer) (err error) {
	partition.Lock()
	defer partition.Unlock()
	newHosts := make([]string, 0, len(partition.Hosts)-1)
	newPeers := make([]proto.Peer, 0, len(partition.Hosts)-1)
	for _, host := range partition.Hosts {
		if host == removePeer.Addr {
			continue
		}
		newHosts = append(newHosts, host)
	}
	for _, peer := range partition.Peers {
		if peer.Addr == removePeer.Addr && peer.ID == removePeer.ID {
			continue
		}
		newPeers = append(newPeers, peer)
	}
	if err = partition.persistToRocksDB("removeMetaPartitionRaftMember", partition.volName, newHosts, newPeers, c); err != nil {
		return
	}
	return
}

func (c *Cluster) removeMetaPartitionRaftMember(partition *MetaPartition, removePeer proto.Peer, force bool, autoRemove bool) (err error) {
	partition.offlineMutex.Lock()
	defer partition.offlineMutex.Unlock()
	defer func() {
		if err1 := c.updateMetaPartitionOfflinePeerIDWithLock(partition, 0); err1 != nil {
			err = errors.Trace(err, "updateMetaPartitionOfflinePeerIDWithLock failed, err[%v]", err1)
		}
	}()

	if err = c.updateMetaPartitionOfflinePeerIDWithLock(partition, removePeer.ID); err != nil {
		return
	}

	leaderReplica, leaderErr := partition.getMetaReplicaLeader()
	if leaderErr == nil {
		if force {
			autoRemove = true
		}
		task, err := partition.createTaskToRemoveRaftMember(removePeer, false, autoRemove)
		if err != nil {
			return err
		}

		task.OperatorAddr = leaderReplica.Addr
		mn := leaderReplica.metaNode
		if mn == nil {
			mn, err = c.metaNode(leaderReplica.Addr)
			if err != nil {
				return err
			}
		}

		if _, err = mn.Sender.syncSendAdminTask(task); err == nil {
			return nil
		}
	}

	if !force {
		if leaderErr != nil {
			return leaderErr
		}
		return err
	}

	task, err := partition.createTaskToRemoveRaftMember(removePeer, force, autoRemove)
	if err != nil {
		return err
	}

	for _, replica := range partition.Replicas {
		if replica.Addr == removePeer.Addr {
			continue
		}

		task.OperatorAddr = replica.Addr

		var mn *MetaNode
		if replica.metaNode != nil {
			mn = replica.metaNode
		} else {
			mn, _ = c.metaNode(replica.Addr)
		}

		if mn != nil && mn.IsActive {
			if _, err := mn.Sender.syncSendAdminTask(task); err != nil {
				return err
			}
		}
	}

	return
}

func (c *Cluster) updateMetaPartitionOfflinePeerIDWithLock(mp *MetaPartition, peerID uint64) (err error) {
	mp.Lock()
	defer mp.Unlock()
	mp.OfflinePeerID = peerID
	if err = mp.persistToRocksDB("updateMetaPartitionOfflinePeerIDWithLock", mp.volName, mp.Hosts, mp.Peers, c); err != nil {
		return
	}
	return
}

func (c *Cluster) addMetaReplica(partition *MetaPartition, addr string, storeMode proto.StoreMode) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addMetaReplica],vol[%v],data partition[%v],err[%v]", partition.volName, partition.PartitionID, err)
		}
	}()
	partition.Lock()
	defer partition.Unlock()
	if contains(partition.Hosts, addr) {
		err = fmt.Errorf("vol[%v],mp[%v] hosts[%v] has contains host[%v]", partition.volName, partition.PartitionID, partition.Hosts, addr)
		return
	}
	metaNode, err := c.metaNode(addr)
	if err != nil {
		return
	}
	addPeer := proto.Peer{ID: metaNode.ID, Addr: addr, HeartbeatPort: metaNode.HeartbeatPort, ReplicaPort: metaNode.ReplicaPort}
	if err = c.addMetaPartitionRaftMember(partition, addPeer); err != nil {
		return
	}
	newHosts := append(partition.Hosts, addPeer.Addr)
	newPeers := append(partition.Peers, addPeer)
	if err = partition.persistToRocksDB("addMetaReplica", partition.volName, newHosts, newPeers, c); err != nil {
		return
	}
	if err = c.createMetaReplica(partition, addPeer, storeMode); err != nil {
		return
	}
	if err = partition.afterCreation(addPeer.Addr, c, storeMode); err != nil {
		return
	}
	return
}

func (c *Cluster) addMetaReplicaLearner(partition *MetaPartition, targetAddr string, storeMode proto.StoreMode, srcAddr string, manualPromote bool, decommissionType uint32) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addMetaReplicaLearner],vol[%v],meta partition[%v],addr[%v],storeMode[%v],err[%v]",
				partition.volName, partition.PartitionID, targetAddr, storeMode, err)
		} else {
			log.LogWarnf("action[addMetaReplicaLearner] success,vol[%v],meta partition[%v],addr[%v],storeMode[%v]",
				partition.volName, partition.PartitionID, targetAddr, storeMode)
		}

		if partition.IsRecover.Load() {
			addr := partition.SrcAddr
			if addr == "" {
				addr = targetAddr
			}
			c.putBadMetaPartitions(addr, partition.PartitionID)
		} else {
			partition.setRestoreReplicaStatus(RestoreReplicaMetaStop)
		}

		partition.RLock()
		c.syncUpdateMetaPartition(partition)
		partition.RUnlock()
	}()
	log.LogWarnf("action[addMetaReplicaLearner] start,vol[%v],meta partition[%v],addr[%v],storeMode[%v],currentHosts[%v]",
		partition.volName, partition.PartitionID, targetAddr, storeMode, partition.Hosts)
	// partition.SrcAddr == addr means learner mode, and already checked
	if !partition.CheckLastDelReplicaTime() && srcAddr != "" {
		err = fmt.Errorf("addMetaReplicaLearner: the interval between migrate mp replica should over 5 minute. last %d,  addr[%v]",
			partition.LastDelReplicaTime, srcAddr)
		return
	}

	// partition.SrcAddr == addr means learner mode, and already checked
	if !manualPromote {
		if err = c.CheckMetaPartitionDecommissionLimit(decommissionType); err != nil {
			log.LogWarnf("action[addMetaReplicaLearner] checkMetaPartitionDecommissionLimit failed,vol[%v],meta partition[%v],err[%v]",
				partition.volName, partition.PartitionID, err)
			return
		}
		if !partition.setRestoreReplicaForbidden() {
			currentStatus := atomic.LoadUint32(&partition.RestoreReplicaMeta)
			message := ""
			if currentStatus == RestoreReplicaMetaForbidden {
				message = "mp is decommissioning, please wait for the decommission to complete"
			} else {
				message = "mp is autoHealing, please wait for the autoHealing to complete"
			}
			err = errors.NewErrorf("set RestoreReplicaMetaForbidden failed, %s", message)
			log.LogWarnf("action[addMetaReplicaLearner] setRestoreReplicaForbidden failed,vol[%v],meta partition[%v],err[%v]",
				partition.volName, partition.PartitionID, err)
			return
		}
	}

	partition.Lock()
	defer partition.Unlock()
	if contains(partition.Hosts, targetAddr) {
		err = fmt.Errorf("vol[%v],mp[%v] hosts[%v] has contains host[%v]", partition.volName, partition.PartitionID, partition.Hosts, targetAddr)
		log.LogWarnf("action[addMetaReplicaLearner] host already exists,vol[%v],meta partition[%v],addr[%v]",
			partition.volName, partition.PartitionID, targetAddr)
		return
	}

	if !manualPromote && partition.IsRecover.Load() {
		err = fmt.Errorf("vol[%v],mp[%v] is recovering, can't add learner", partition.volName, partition.PartitionID)
		log.LogWarnf("action[addMetaReplicaLearner] %v", err)
		return
	}

	// Check maximum learner number limit
	learnerCount := 0
	for _, peer := range partition.Peers {
		if peer.Type == raftProto.PeerLearner {
			learnerCount++
		}
	}

	if learnerCount >= proto.MaxMetaPartitionLearnerNum {
		err = fmt.Errorf("vol[%v],mp[%v] exceeds maximum learner number limit, current learners[%v], max allowed[%v]",
			partition.volName, partition.PartitionID, learnerCount, proto.MaxMetaPartitionLearnerNum)
		log.LogWarnf("action[addMetaReplicaLearner] %v", err)
		return
	}

	metaNode, err := c.metaNode(targetAddr)
	if err != nil {
		log.LogWarnf("action[addMetaReplicaLearner] getMetaNode failed,vol[%v],meta partition[%v],addr[%v],err[%v]",
			partition.volName, partition.PartitionID, targetAddr, err)
		return
	}

	addPeer := proto.Peer{
		ID:            metaNode.ID,
		Addr:          targetAddr,
		HeartbeatPort: metaNode.HeartbeatPort,
		ReplicaPort:   metaNode.ReplicaPort,
		Type:          raftProto.PeerLearner,
		ManualPromote: manualPromote,
	}
	log.LogWarnf("action[addMetaReplicaLearner] peer info,vol[%v],meta partition[%v], peer[%v]", partition.volName, partition.PartitionID, addPeer.String())

	partition.Peers = append(partition.Peers, addPeer)
	defer func() {
		if err != nil {
			partition.Peers = partition.Peers[:len(partition.Peers)-1]
		}
	}()

	if err = c.createMetaReplica(partition, addPeer, storeMode); err != nil {
		log.LogWarnf("action[addMetaReplicaLearner] createMetaReplica failed,vol[%v],meta partition[%v],peer[%v:%v],err[%v]",
			partition.volName, partition.PartitionID, addPeer.ID, addPeer.Addr, err)
		return
	}
	log.LogWarnf("action[addMetaReplicaLearner] calling afterCreation,vol[%v],meta partition[%v],addr[%v]",
		partition.volName, partition.PartitionID, addPeer.Addr)

	// Add learner to raft cluster
	if err = c.addMetaRaftLearner(partition, addPeer); err != nil {
		log.LogWarnf("action[addMetaReplicaLearner] addMetaPartitionRaftLearner failed,vol[%v],meta partition[%v],peer[%v:%v],err[%v]",
			partition.volName, partition.PartitionID, addPeer.ID, addPeer.Addr, err)
		return
	}

	if err = partition.afterCreation(addPeer.Addr, c, storeMode); err != nil {
		log.LogWarnf("action[addMetaReplicaLearner] afterCreation failed,vol[%v],meta partition[%v],addr[%v],err[%v]",
			partition.volName, partition.PartitionID, addPeer.Addr, err)
		return
	}

	// Add learner to metadata replica list
	newHosts := append(partition.Hosts, addPeer.Addr)
	newPeers := partition.Peers
	if !manualPromote {
		partition.IsRecover.Store(true)
		partition.DecommissionType = decommissionType
		partition.SrcAddr = srcAddr
		partition.LearnerDstAddr = addPeer.Addr
		partition.RecoverStartTime = time.Now().Unix()
		partition.RecoverFailCount = 0
		partition.RecoverState = proto.RecoverStateRecovering
		auditMsg := fmt.Sprintf("addMetaReplicaLearner: vol[%v] mp[%v] added learner[%v] for decommission, srcAddr[%v] recoverStartTime[%v]",
			partition.volName, partition.PartitionID, addPeer.Addr, srcAddr, time.Unix(partition.RecoverStartTime, 0).Format("2006-01-02 15:04:05"))
		auditlog.LogMasterOp("addMetaReplicaLearner", auditMsg, nil)
	} else {
		auditMsg := fmt.Sprintf("addMetaReplicaLearner: vol[%v] mp[%v] added learner[%v] (manualPromote=true)",
			partition.volName, partition.PartitionID, addPeer.Addr)
		auditlog.LogMasterOp("addMetaReplicaLearner", auditMsg, nil)
	}

	log.LogWarnf("action[addMetaReplicaLearner] persisting to rocksdb,vol[%v],meta partition[%v],newHosts[%v],newPeers[%v]",
		partition.volName, partition.PartitionID, newHosts, newPeers)
	if err = partition.persistToRocksDB("addMetaPartitionLearner", partition.volName, newHosts, newPeers, c); err != nil {
		log.LogWarnf("action[addMetaReplicaLearner] persistToRocksDB failed,vol[%v],meta partition[%v],err[%v]",
			partition.volName, partition.PartitionID, err)

		// Reset state on failure
		partition.IsRecover.Store(false)
		partition.SrcAddr = ""
		partition.LearnerDstAddr = ""
		partition.DecommissionType = proto.InitialDecommission
		return
	}

	log.LogWarnf("action[addMetaReplicaLearner] afterCreation completed,vol[%v],meta partition[%v],addr[%v]",
		partition.volName, partition.PartitionID, addPeer.Addr)
	return
}

func (c *Cluster) promoteMetaReplicaToVoter(partition *MetaPartition, addr string, check bool) (err error) {
	defer func() {
		auditMsg := fmt.Sprintf("promoteMetaReplicaToVoter: vol[%v] mp[%v] promote learner[%v] to voter finished, err[%v]",
			partition.volName, partition.PartitionID, addr, err)
		log.LogWarn(auditMsg)
		auditlog.LogMasterOp("promoteMetaReplicaToVoter", auditMsg, nil)
	}()

	log.LogWarnf("action[promoteMetaReplicaToVoter] start,vol[%v],meta partition[%v],addr[%v],currentHosts[%v]",
		partition.volName, partition.PartitionID, addr, partition.Hosts)

	// Validate learner recovery status
	if check {
		if err = c.validateLearnerRecoveryStatus(partition, addr); err != nil {
			return
		}
	}

	// Validate learner recovery status
	partition.Lock()
	defer partition.Unlock()
	if !contains(partition.Hosts, addr) {
		err = fmt.Errorf("vol[%v],mp[%v] hosts[%v] does not contain host[%v]", partition.volName, partition.PartitionID, partition.Hosts, addr)
		return
	}
	var promotePeer proto.Peer
	for _, peer := range partition.Peers {
		if peer.Addr == addr {
			promotePeer = peer
			break
		}
	}
	if promotePeer.ID == 0 {
		err = fmt.Errorf("vol[%v],mp[%v] peer with addr[%v] not found", partition.volName, partition.PartitionID, addr)
		return
	}
	// Promote learner to voter in raft cluster
	if err = c.promoteMetaReplica(partition, promotePeer); err != nil {
		log.LogWarnf("action[promoteMetaReplicaToVoter] promoteMetaReplica failed,vol[%v],meta partition[%v],peer[%v:%v],err[%v]",
			partition.volName, partition.PartitionID, promotePeer.ID, promotePeer.Addr, err)
		return
	}

	for idx, peer := range partition.Peers {
		if peer.ID == promotePeer.ID {
			partition.Peers[idx].Type = raftProto.PeerNormal
			log.LogWarnf("action[promoteMetaReplicaToVoter] promote peer to voter,vol[%v],meta partition[%v],peer[%v:%v]",
				partition.volName, partition.PartitionID, promotePeer.ID, promotePeer.Addr)
			break
		}
	}

	// Update persisted metadata (peer type is updated in raft layer)
	if err = partition.persistToRocksDB("promoteMetaReplica", partition.volName, partition.Hosts, partition.Peers, c); err != nil {
		log.LogWarnf("action[promoteMetaReplicaToVoter] persistToRocksDB failed,vol[%v],meta partition[%v],err[%v]",
			partition.volName, partition.PartitionID, err)
		return
	}
	log.LogWarnf("action[promoteMetaReplicaToVoter] persisted to rocksdb,vol[%v],meta partition[%v]",
		partition.volName, partition.PartitionID)
	return
}

func (c *Cluster) createMetaReplica(partition *MetaPartition, addPeer proto.Peer, storeMode proto.StoreMode) (err error) {
	task, err := partition.createTaskToCreateReplica(addPeer.Addr, storeMode)
	if err != nil {
		return
	}
	metaNode, err := c.metaNode(addPeer.Addr)
	if err != nil {
		return
	}
	if _, err = metaNode.Sender.syncSendAdminTask(task); err != nil {
		return
	}
	return
}

func (c *Cluster) buildAddMetaPartitionRaftMemberTaskAndSyncSend(mp *MetaPartition, addPeer proto.Peer, leaderAddr string) (resp *proto.Packet, err error) {
	defer func() {
		var resultCode uint8
		if resp != nil {
			resultCode = resp.ResultCode
		}

		if err != nil {
			log.LogErrorf("action[addMetaRaftMemberAndSend],vol[%v],meta partition[%v],resultCode[%v],err[%v]",
				mp.volName, mp.PartitionID, resultCode, err)
		} else {
			log.LogWarnf("action[addMetaRaftMemberAndSend],vol[%v],meta partition[%v],resultCode[%v]",
				mp.volName, mp.PartitionID, resultCode)
		}
	}()

	t, err := mp.createTaskToAddRaftMember(addPeer, leaderAddr)
	if err != nil {
		return
	}
	leaderMetaNode, err := c.metaNode(leaderAddr)
	if err != nil {
		return
	}
	if resp, err = leaderMetaNode.Sender.syncSendAdminTask(t); err != nil {
		return
	}
	return
}

func (c *Cluster) addMetaPartitionRaftMember(partition *MetaPartition, addPeer proto.Peer) (err error) {
	var (
		candidateAddrs []string
		leaderAddr     string
	)
	candidateAddrs = make([]string, 0, len(partition.Hosts))
	leaderMr, err := partition.getMetaReplicaLeader()
	if err == nil {
		leaderAddr = leaderMr.Addr
		if contains(partition.Hosts, leaderAddr) {
			candidateAddrs = append(candidateAddrs, leaderAddr)
		} else {
			leaderAddr = ""
		}
	}
	for _, host := range partition.Hosts {
		if host == leaderAddr {
			continue
		}
		candidateAddrs = append(candidateAddrs, host)
	}
	// send task to leader addr first,if need to retry,then send to other addr
	for index, host := range candidateAddrs {
		// wait for a new leader
		if leaderAddr == "" && len(candidateAddrs) < int(partition.ReplicaNum) {
			time.Sleep(retrySendSyncTaskInternal)
		}
		for i := 0; i < RetryDoMigrateNum; i++ {
			_, err = c.buildAddMetaPartitionRaftMemberTaskAndSyncSend(partition, addPeer, host)
			if err == nil {
				return
			}
			if !IsRetryMigrateMpError(err) {
				break
			}
			time.Sleep(retrySendSyncTaskInternal)
		}
		if index < len(candidateAddrs)-1 {
			time.Sleep(retrySendSyncTaskInternal)
		}
	}
	return
}

func (c *Cluster) buildAddMetaPartitionRaftLearnerTaskAndSyncSend(mp *MetaPartition, addPeer proto.Peer, leaderAddr string) (resp *proto.Packet, err error) {
	defer func() {
		var resultCode uint8
		if resp != nil {
			resultCode = resp.ResultCode
		}

		if err != nil {
			log.LogErrorf("action[addMetaRaftLearnerAndSend],vol[%v],meta partition[%v],peer[%v:%v],leader[%v],resultCode[%v],err[%v]",
				mp.volName, mp.PartitionID, addPeer.ID, addPeer.Addr, leaderAddr, resultCode, err)
		} else {
			log.LogWarnf("action[addMetaRaftLearnerAndSend],vol[%v],meta partition[%v],peer[%v:%v],leader[%v],resultCode[%v] success",
				mp.volName, mp.PartitionID, addPeer.ID, addPeer.Addr, leaderAddr, resultCode)
		}
	}()

	log.LogWarnf("action[buildAddMetaPartitionRaftLearnerTaskAndSyncSend] start,vol[%v],meta partition[%v],peer[%v:%v],leader[%v]",
		mp.volName, mp.PartitionID, addPeer.ID, addPeer.Addr, leaderAddr)

	t, err := mp.createTaskToAddRaftLearner(addPeer, leaderAddr)
	if err != nil {
		log.LogWarnf("action[buildAddMetaPartitionRaftLearnerTaskAndSyncSend] createTask failed,vol[%v],meta partition[%v],peer[%v:%v],err[%v]",
			mp.volName, mp.PartitionID, addPeer.ID, addPeer.Addr, err)
		return
	}
	leaderMetaNode, err := c.metaNode(leaderAddr)
	if err != nil {
		log.LogWarnf("action[buildAddMetaPartitionRaftLearnerTaskAndSyncSend] getMetaNode failed,vol[%v],meta partition[%v],leader[%v],err[%v]",
			mp.volName, mp.PartitionID, leaderAddr, err)
		return
	}
	log.LogWarnf("action[buildAddMetaPartitionRaftLearnerTaskAndSyncSend] sending task,vol[%v],meta partition[%v],peer[%v:%v],leader[%v]",
		mp.volName, mp.PartitionID, addPeer.ID, addPeer.Addr, leaderAddr)
	if resp, err = leaderMetaNode.Sender.syncSendAdminTask(t); err != nil {
		log.LogWarnf("action[buildAddMetaPartitionRaftLearnerTaskAndSyncSend] sendTask failed,vol[%v],meta partition[%v],peer[%v:%v],leader[%v],err[%v]",
			mp.volName, mp.PartitionID, addPeer.ID, addPeer.Addr, leaderAddr, err)
		return
	}
	return
}

func (c *Cluster) addMetaRaftLearner(partition *MetaPartition, addPeer proto.Peer) (err error) {
	var (
		candidateAddrs []string
		leaderAddr     string
	)
	log.LogWarnf("action[addMetaRaftLearner] start,vol[%v],meta partition[%v],peer[%v:%v],hosts[%v]",
		partition.volName, partition.PartitionID, addPeer.ID, addPeer.Addr, partition.Hosts)

	candidateAddrs = make([]string, 0, len(partition.Hosts))
	leaderMr, err := partition.getMetaReplicaLeader()
	if err == nil {
		leaderAddr = leaderMr.Addr
		if contains(partition.Hosts, leaderAddr) {
			candidateAddrs = append(candidateAddrs, leaderAddr)
			log.LogWarnf("action[addMetaRaftLearner] found leader,vol[%v],meta partition[%v],leader[%v]",
				partition.volName, partition.PartitionID, leaderAddr)
		} else {
			leaderAddr = ""
			log.LogWarnf("action[addMetaRaftLearner] leader not in hosts,vol[%v],meta partition[%v],leader[%v],hosts[%v]",
				partition.volName, partition.PartitionID, leaderMr.Addr, partition.Hosts)
		}
	} else {
		log.LogWarnf("action[addMetaRaftLearner] getLeader failed,vol[%v],meta partition[%v],err[%v]",
			partition.volName, partition.PartitionID, err)
	}

	for _, host := range partition.Hosts {
		if host == leaderAddr {
			continue
		}
		candidateAddrs = append(candidateAddrs, host)
	}
	log.LogWarnf("action[addMetaRaftLearner] candidateAddrs[%v],vol[%v],meta partition[%v]",
		candidateAddrs, partition.volName, partition.PartitionID)
	// send task to leader addr first,if need to retry,then send to other addr
	for _, host := range candidateAddrs {
		_, err = c.buildAddMetaPartitionRaftLearnerTaskAndSyncSend(partition, addPeer, host)
		if err == nil {
			log.LogWarnf("action[addMetaRaftLearner] success,vol[%v],meta partition[%v],peer[%v:%v],host[%v]",
				partition.volName, partition.PartitionID, addPeer.ID, addPeer.Addr, host)
			return
		}

		log.LogWarnf("action[addMetaRaftLearner] retry error,vol[%v],meta partition[%v],peer[%v:%v],host[%v],err[%v]",
			partition.volName, partition.PartitionID, addPeer.ID, addPeer.Addr, host, err)
	}

	log.LogWarnf("action[addMetaRaftLearner] failed after all retries,vol[%v],meta partition[%v],peer[%v:%v],err[%v]",
		partition.volName, partition.PartitionID, addPeer.ID, addPeer.Addr, err)
	return
}

func (c *Cluster) buildPromoteMetaReplicaTaskAndSyncSend(mp *MetaPartition, promotePeer proto.Peer, leaderAddr string) (resp *proto.Packet, err error) {
	defer func() {
		var resultCode uint8
		if resp != nil {
			resultCode = resp.ResultCode
		}

		if err != nil {
			log.LogErrorf("action[promoteMetaReplicaAndSend],vol[%v],meta partition[%v],peer[%v:%v],leader[%v],resultCode[%v],err[%v]",
				mp.volName, mp.PartitionID, promotePeer.ID, promotePeer.Addr, leaderAddr, resultCode, err)
		} else {
			log.LogWarnf("action[promoteMetaReplicaAndSend],vol[%v],meta partition[%v],peer[%v:%v],leader[%v],resultCode[%v] success",
				mp.volName, mp.PartitionID, promotePeer.ID, promotePeer.Addr, leaderAddr, resultCode)
		}
	}()

	log.LogWarnf("action[buildPromoteMetaReplicaTaskAndSyncSend] start,vol[%v],meta partition[%v],peer[%v:%v],leader[%v]",
		mp.volName, mp.PartitionID, promotePeer.ID, promotePeer.Addr, leaderAddr)

	t, err := mp.createTaskToPromoteLearner(promotePeer, leaderAddr)
	if err != nil {
		log.LogWarnf("action[buildPromoteMetaReplicaTaskAndSyncSend] createTask failed,vol[%v],meta partition[%v],peer[%v:%v],err[%v]",
			mp.volName, mp.PartitionID, promotePeer.ID, promotePeer.Addr, err)
		return
	}
	leaderMetaNode, err := c.metaNode(leaderAddr)
	if err != nil {
		log.LogWarnf("action[buildPromoteMetaReplicaTaskAndSyncSend] getMetaNode failed,vol[%v],meta partition[%v],leader[%v],err[%v]",
			mp.volName, mp.PartitionID, leaderAddr, err)
		return
	}
	log.LogWarnf("action[buildPromoteMetaReplicaTaskAndSyncSend] sending task,vol[%v],meta partition[%v],peer[%v:%v],leader[%v]",
		mp.volName, mp.PartitionID, promotePeer.ID, promotePeer.Addr, leaderAddr)
	if resp, err = leaderMetaNode.Sender.syncSendAdminTask(t); err != nil {
		log.LogWarnf("action[buildPromoteMetaReplicaTaskAndSyncSend] sendTask failed,vol[%v],meta partition[%v],peer[%v:%v],leader[%v],err[%v]",
			mp.volName, mp.PartitionID, promotePeer.ID, promotePeer.Addr, leaderAddr, err)
		return
	}
	return
}

func (c *Cluster) promoteMetaReplica(partition *MetaPartition, promotePeer proto.Peer) (err error) {
	var (
		candidateAddrs []string
		leaderAddr     string
	)
	log.LogWarnf("action[promoteMetaReplica] start,vol[%v],meta partition[%v],peer[%v:%v],hosts[%v]",
		partition.volName, partition.PartitionID, promotePeer.ID, promotePeer.Addr, partition.Hosts)

	candidateAddrs = make([]string, 0, len(partition.Hosts))
	leaderMr, err := partition.getMetaReplicaLeader()
	if err == nil {
		leaderAddr = leaderMr.Addr
		if contains(partition.Hosts, leaderAddr) {
			candidateAddrs = append(candidateAddrs, leaderAddr)
			log.LogWarnf("action[promoteMetaReplica] found leader,vol[%v],meta partition[%v],leader[%v]",
				partition.volName, partition.PartitionID, leaderAddr)
		} else {
			leaderAddr = ""
			log.LogWarnf("action[promoteMetaReplica] leader not in hosts,vol[%v],meta partition[%v],leader[%v],hosts[%v]",
				partition.volName, partition.PartitionID, leaderMr.Addr, partition.Hosts)
		}
	} else {
		log.LogWarnf("action[promoteMetaReplica] getLeader failed,vol[%v],meta partition[%v],err[%v]",
			partition.volName, partition.PartitionID, err)
	}
	for _, host := range partition.Hosts {
		if host == leaderAddr {
			continue
		}
		candidateAddrs = append(candidateAddrs, host)
	}
	log.LogWarnf("action[promoteMetaReplica] candidateAddrs[%v],vol[%v],meta partition[%v]",
		candidateAddrs, partition.volName, partition.PartitionID)
	// send task to leader addr first,if need to retry,then send to other addr
	for _, host := range candidateAddrs {
		_, err = c.buildPromoteMetaReplicaTaskAndSyncSend(partition, promotePeer, host)
		if err == nil {
			log.LogWarnf("action[promoteMetaReplica] success,vol[%v],meta partition[%v],peer[%v:%v],host[%v]",
				partition.volName, partition.PartitionID, promotePeer.ID, promotePeer.Addr, host)
			return
		}

		log.LogWarnf("action[promoteMetaReplica] retry error,vol[%v],meta partition[%v],peer[%v:%v],host[%v],err[%v]",
			partition.volName, partition.PartitionID, promotePeer.ID, promotePeer.Addr, host, err)
	}

	log.LogWarnf("action[promoteMetaReplica] failed after all retries,vol[%v],meta partition[%v],peer[%v:%v],err[%v]",
		partition.volName, partition.PartitionID, promotePeer.ID, promotePeer.Addr, err)
	return
}

func (c *Cluster) loadMetaPartitionAndCheckResponse(mp *MetaPartition) {
	go func() {
		c.doLoadMetaPartition(mp)
	}()
}

func (c *Cluster) doLoadMetaPartition(mp *MetaPartition) {
	var wg sync.WaitGroup
	mp.Lock()
	hosts := make([]string, len(mp.Hosts))
	copy(hosts, mp.Hosts)
	mp.LoadResponse = make([]*proto.MetaPartitionLoadResponse, 0)
	mp.Unlock()
	errChannel := make(chan error, len(hosts))
	for _, host := range hosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			mr, err := mp.getMetaReplica(host)
			if err != nil {
				errChannel <- err
				return
			}
			task := mr.createTaskToLoadMetaPartition(mp.PartitionID)
			response, err := mr.metaNode.Sender.syncSendAdminTask(task)
			if err != nil {
				errChannel <- err
				return
			}
			loadResponse := &proto.MetaPartitionLoadResponse{}
			if err = json.Unmarshal(response.Data, loadResponse); err != nil {
				errChannel <- err
				return
			}
			loadResponse.Addr = host
			mp.addOrReplaceLoadResponse(loadResponse)
		}(host)
	}
	wg.Wait()
	mp.checkPeerDiffWithRaft(c)
	select {
	case err := <-errChannel:
		msg := fmt.Sprintf("action[doLoadMetaPartition] vol[%v],mpID[%v],err[%v]", mp.volName, mp.PartitionID, err.Error())
		Warn(c.Name, msg)
		return
	default:
	}
	mp.checkSnapshot(c)
}

func (c *Cluster) doLoadDataPartition(dp *DataPartition) {
	log.LogInfof("action[doLoadDataPartition],partitionID:%d", dp.PartitionID)
	if !dp.needsToCompareCRC() {
		log.LogInfof("action[doLoadDataPartition],partitionID:%d isRecover[%t] don't need compare", dp.PartitionID, dp.isRecover)
		return
	}
	dp.resetFilesWithMissingReplica()
	loadTasks := dp.createLoadTasks()
	c.addDataNodeTasks(loadTasks)
	success := false
	for i := 0; i < timeToWaitForResponse; i++ {
		if dp.checkLoadResponse(c.getDataPartitionTimeoutSec()) {
			success = true
			break
		}
		time.Sleep(time.Second)
	}

	if !success {
		return
	}

	dp.getFileCount()
	if proto.IsNormalDp(dp.PartitionType) {
		dp.validateCRC(c.Name)
		dp.checkReplicaSize(c.Name, c.cfg.diffReplicaSpaceUsage)
	}

	dp.setToNormal()
}

func (c *Cluster) handleMetaNodeTaskResponse(nodeAddr string, task *proto.AdminTask) (err error) {
	if task == nil {
		return
	}
	log.LogDebugf("action[handleMetaNodeTaskResponse] receive Task response:%s from %s now:%d", task.IdString(), nodeAddr, time.Now().Unix())
	var metaNode *MetaNode

	if metaNode, err = c.metaNode(nodeAddr); err != nil {
		goto errHandler
	}
	metaNode.Sender.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpMetaNodeHeartbeat:
		response := task.Response.(*proto.MetaNodeHeartbeatResponse)
		err = c.dealMetaNodeHeartbeatResp(task.OperatorAddr, response)
	case proto.OpDeleteMetaPartition:
		response := task.Response.(*proto.DeleteMetaPartitionResponse)
		err = c.dealDeleteMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpUpdateMetaPartition:
		response := task.Response.(*proto.UpdateMetaPartitionResponse)
		err = c.dealUpdateMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpVersionOperation:
		response := task.Response.(*proto.MultiVersionOpResponse)
		err = c.dealOpMetaNodeMultiVerResp(task.OperatorAddr, response)
	default:
		err := fmt.Errorf("unknown operate code %v", task.OpCode)
		log.LogError(err)
	}

	if err != nil {
		log.LogErrorf("process task[%s] failed", task.ToString())
	} else {
		log.LogInfof("[handleMetaNodeTaskResponse] process task:%v status:%v success", task.IdString(), task.Status)
	}
	return
errHandler:
	log.LogErrorf("action[handleMetaNodeTaskResponse],nodeAddr %s,taskId %s,err %v",
		nodeAddr, task.IdString(), err)
	return
}

func (c *Cluster) dealUpdateMetaPartitionResp(nodeAddr string, resp *proto.UpdateMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealUpdateMetaPartitionResp],clusterID[%v] nodeAddr %v update meta partition failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
	}
	return
}

func (c *Cluster) dealOpMetaNodeMultiVerResp(nodeAddr string, resp *proto.MultiVersionOpResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealOpMetaNodeMultiVerResp],clusterID[%v] volume [%v] nodeAddr %v operate meta partition snapshot version,err %v",
			c.Name, resp.VolumeID, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
	}
	var vol *Vol
	if vol, err = c.getVol(resp.VolumeID); err != nil {
		return
	}
	vol.VersionMgr.handleTaskRsp(resp, TypeMetaPartition)
	return
}

func (c *Cluster) dealOpDataNodeMultiVerResp(nodeAddr string, resp *proto.MultiVersionOpResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealOpMetaNodeMultiVerResp],clusterID[%v] volume [%v] nodeAddr %v operate meta partition snapshot version,err %v",
			c.Name, resp.VolumeID, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
	}
	var vol *Vol
	if vol, err = c.getVol(resp.VolumeID); err != nil {
		return
	}
	vol.VersionMgr.handleTaskRsp(resp, TypeDataPartition)
	return
}

func (c *Cluster) dealDeleteMetaPartitionResp(nodeAddr string, resp *proto.DeleteMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealDeleteMetaPartitionResp],clusterID[%v] nodeAddr %v "+
			"delete meta partition failed,err %v", c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}
	var mr *MetaReplica
	mp, err := c.getMetaPartitionByID(resp.PartitionID)
	if err != nil {
		goto errHandler
	}
	mp.Lock()
	defer mp.Unlock()
	if mr, err = mp.getMetaReplica(nodeAddr); err != nil {
		goto errHandler
	}
	mp.removeReplica(mr)
	return

errHandler:
	log.LogErrorf("dealDeleteMetaPartitionResp %v", err)
	return
}

func (c *Cluster) dealMetaNodeHeartbeatResp(nodeAddr string, resp *proto.MetaNodeHeartbeatResponse) (err error) {
	var (
		metaNode *MetaNode
		logMsg   string
	)

	log.LogInfof("action[dealMetaNodeHeartbeatResp],clusterID[%v] receive nodeAddr[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealMetaNodeHeartbeatResp],clusterID[%v] nodeAddr %v heartbeat failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}

	if metaNode, err = c.metaNode(nodeAddr); err != nil {
		goto errHandler
	}

	if metaNode.ToBeOffline {
		log.LogInfof("action[dealMetaNodeHeartbeatResp] dataNode is toBeOffline, addr[%s]", nodeAddr)
		return
	}

	if resp.ZoneName == "" {
		resp.ZoneName = DefaultZoneName
	}

	if metaNode.ZoneName != resp.ZoneName {
		c.t.deleteMetaNode(metaNode)
		oldZoneName := metaNode.ZoneName
		metaNode.ZoneName = resp.ZoneName
		c.adjustMetaNode(metaNode)
		log.LogWarnf("metaNode zone changed from [%v] to [%v]", oldZoneName, resp.ZoneName)
	}

	metaNode.ReceivedForbidWriteOpOfProtoVer0 = resp.ReceivedForbidWriteOpOfProtoVer0
	if metaNode.ReceivedForbidWriteOpOfProtoVer0 != c.cfg.forbidWriteOpOfProtoVer0 {
		log.LogWarnf("[dealMetaNodeHeartbeatResp] metaNode[%v] ReceivedForbidWriteOpOfProtoVer0(%v) is different from master forbidWriteOpOfProtoVer0(%v)",
			metaNode.Addr, metaNode.ReceivedForbidWriteOpOfProtoVer0, c.cfg.forbidWriteOpOfProtoVer0)
	}

	// change cpu util and io used
	metaNode.CpuUtil.Store(resp.CpuUtil)
	metaNode.updateMetric(resp, c.cfg.MetaNodeThreshold)
	metaNode.setNodeActive()
	metaNode.updateRocksdbDisks(resp)

	if err = c.t.putMetaNode(metaNode); err != nil {
		log.LogErrorf("action[dealMetaNodeHeartbeatResp],metaNode[%v] error[%v]", metaNode.Addr, err)
	}
	c.updateMetaNode(metaNode, resp.MetaPartitionReports, metaNode.reachesThreshold())
	// todo remove, this no need set metaNode.metaPartitionInfos = nil
	// metaNode.metaPartitionInfos = nil
	logMsg = fmt.Sprintf("action[dealMetaNodeHeartbeatResp],metaNode:%v,zone[%v], ReportTime:%v  success", metaNode.Addr, metaNode.ZoneName, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, errors.Stack(err))
	log.LogError(logMsg)
	return
}

func (c *Cluster) adjustMetaNode(metaNode *MetaNode) {
	c.mnMutex.Lock()
	defer c.mnMutex.Unlock()
	oldNodeSetID := metaNode.NodeSetID
	var err error
	defer func() {
		if err != nil {
			err = fmt.Errorf("action[adjustMetaNode],clusterID[%v] addr:%v,zone[%v] rack[%v] err:%v ",
				c.Name, metaNode.Addr, metaNode.ZoneName, metaNode.Rack, err.Error())
			log.LogError(errors.Stack(err))
			Warn(c.Name, err.Error())
		}
	}()
	var zone *Zone
	zone, err = c.t.getZone(metaNode.ZoneName)
	if err != nil {
		zone = newZone(metaNode.ZoneName, proto.MediaType_Unspecified)
		c.t.putZone(zone)
	}
	c.nsMutex.Lock()
	ns := zone.getAvailNodeSetForMetaNode(metaNode.Rack) // Use rack field
	if ns == nil {
		if ns, err = zone.createNodeSet(c); err != nil {
			c.nsMutex.Unlock()
			return
		}
	}
	c.nsMutex.Unlock()

	metaNode.NodeSetID = ns.ID
	if err = c.syncUpdateMetaNode(metaNode); err != nil {
		metaNode.NodeSetID = oldNodeSetID
		return
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		return
	}
	err = c.t.putMetaNode(metaNode)
}

func (c *Cluster) handleDataNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		log.LogInfof("action[handleDataNodeTaskResponse] receive addr[%v] task response,but task is nil", nodeAddr)
		return
	}
	if log.EnableDebug() {
		log.LogDebugf("action[handleDataNodeTaskResponse] receive addr[%v] task response:%v", nodeAddr, task.ToString())
	}
	var (
		err      error
		dataNode *DataNode
	)

	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		goto errHandler
	}
	dataNode.TaskManager.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpDeleteDataPartition:
		response := task.Response.(*proto.DeleteDataPartitionResponse)
		err = c.dealDeleteDataPartitionResponse(task.OperatorAddr, response)
	case proto.OpLoadDataPartition:
		response := task.Response.(*proto.LoadDataPartitionResponse)
		err = c.handleResponseToLoadDataPartition(task.OperatorAddr, response)
	case proto.OpDataNodeHeartbeat:
		response := task.Response.(*proto.DataNodeHeartbeatResponse)
		err = c.handleDataNodeHeartbeatResp(task.OperatorAddr, response, task.RequestID)
	case proto.OpVersionOperation:
		response := task.Response.(*proto.MultiVersionOpResponse)
		err = c.dealOpDataNodeMultiVerResp(task.OperatorAddr, response)
	default:
		err = fmt.Errorf("unknown operate code %d", task.OpCode)
		goto errHandler
	}

	if err != nil {
		goto errHandler
	}
	return

errHandler:
	log.LogErrorf("process task[%v] failed,err:%v", task.ToString(), err)
}

func (c *Cluster) dealDeleteDataPartitionResponse(nodeAddr string, resp *proto.DeleteDataPartitionResponse) (err error) {
	var dp *DataPartition
	if resp.Status == proto.TaskSucceeds {
		if dp, err = c.getDataPartitionByID(resp.PartitionId); err != nil {
			return
		}
		dp.Lock()
		defer dp.Unlock()
		dp.removeReplicaByAddr(nodeAddr)

	} else {
		Warn(c.Name, fmt.Sprintf("clusterID[%v] delete data partition[%v] failed,err[%v]", c.Name, nodeAddr, resp.Result))
	}

	return
}

func (c *Cluster) handleResponseToLoadDataPartition(nodeAddr string, resp *proto.LoadDataPartitionResponse) (err error) {
	if resp.Status == proto.TaskFailed || resp.PartitionSnapshot == nil {
		return
	}
	var (
		dataNode *DataNode
		dp       *DataPartition
		vol      *Vol
	)
	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		return
	}
	if resp.VolName != "" {
		vol, err = c.getVol(resp.VolName)
		if err != nil {
			return
		}
		dp, err = vol.getDataPartitionByID(resp.PartitionId)
	} else {
		dp, err = c.getDataPartitionByID(resp.PartitionId)
	}
	if err != nil {
		return
	}
	dp.loadFile(dataNode, resp)

	return
}

func (c *Cluster) handleDataNodeHeartbeatResp(nodeAddr string, resp *proto.DataNodeHeartbeatResponse, reqId string) (err error) {
	var (
		dataNode *DataNode
		logMsg   string
	)
	log.LogInfof("action[handleDataNodeHeartbeatResp] clusterID[%v] receive dataNode[%v] heartbeat %v ",
		c.Name, nodeAddr, reqId)
	if resp.Status != proto.TaskSucceeds {
		Warn(c.Name, fmt.Sprintf("action[handleDataNodeHeartbeatResp] clusterID[%v] dataNode[%v] heartbeat task failed",
			c.Name, nodeAddr))
		return
	}

	if dataNode, err = c.dataNode(nodeAddr); err != nil {
		goto errHandler
	}
	if dataNode.ToBeOffline {
		log.LogInfof("action[handleDataNodeHeartbeatResp] dataNode is toBeOffline, addr[%s]", nodeAddr)
		// return
	}
	if resp.ZoneName == "" {
		resp.ZoneName = DefaultZoneName
	}
	if dataNode.ZoneName != resp.ZoneName {
		c.t.deleteDataNode(dataNode)
		oldZoneName := dataNode.ZoneName
		dataNode.ZoneName = resp.ZoneName
		c.dnMutex.Lock()
		c.adjustDataNode(dataNode)
		c.dnMutex.Unlock()
		log.LogWarnf("dataNode [%v] zone changed from [%v] to [%v]", dataNode.Addr, oldZoneName, resp.ZoneName)
	}
	// change cpu util and io used
	dataNode.CpuUtil.Store(resp.CpuUtil)
	dataNode.SetIoUtils(resp.IoUtils)

	dataNode.updateNodeMetric(c, resp)

	if err = c.t.putDataNode(dataNode); err != nil {
		log.LogErrorf("action[handleDataNodeHeartbeatResp] dataNode[%v],zone[%v],node set[%v], err[%v]", dataNode.Addr, dataNode.ZoneName, dataNode.NodeSetID, err)
	}
	c.updateDataNode(dataNode, resp.PartitionReports)

	dataNode.ReceivedForbidWriteOpOfProtoVer0 = resp.ReceivedForbidWriteOpOfProtoVer0
	if dataNode.ReceivedForbidWriteOpOfProtoVer0 != c.cfg.forbidWriteOpOfProtoVer0 {
		log.LogWarnf("[handleDataNodeHeartbeatResp] dataNode[%v] receivedForbiddenWriteOpVerBitmask(%v) is different from master forbidWriteOpOfProtoVer0(%v)",
			dataNode.Addr, dataNode.ReceivedForbidWriteOpOfProtoVer0, c.cfg.forbidWriteOpOfProtoVer0)
	}

	logMsg = fmt.Sprintf("action[handleDataNodeHeartbeatResp],dataNode:%v,zone[%v], ReportTime:%v  success", dataNode.Addr, dataNode.ZoneName, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, err.Error())
	log.LogError(logMsg)
	return
}

func (c *Cluster) adjustDataNode(dataNode *DataNode) {
	oldNodeSetID := dataNode.NodeSetID
	var err error
	defer func() {
		if err != nil {
			err = fmt.Errorf("action[adjustDataNode],clusterID[%v] dataNodeAddr:%v,zone[%v] err:%v ", c.Name, dataNode.Addr, dataNode.ZoneName, err.Error())
			log.LogError(errors.Stack(err))
			Warn(c.Name, err.Error())
		}
	}()
	var zone *Zone
	zone, err = c.t.getZone(dataNode.ZoneName)
	if err != nil {
		zone = newZone(dataNode.ZoneName, dataNode.MediaType)
		c.t.putZone(zone)
	}

	c.nsMutex.Lock()
	ns := zone.getAvailNodeSetForDataNode(dataNode.Rack)
	if ns == nil {
		if ns, err = zone.createNodeSet(c); err != nil {
			c.nsMutex.Unlock()
			return
		}
	}
	c.nsMutex.Unlock()

	if _, err = c.checkSetZoneMediaTypePersist(zone, dataNode.MediaType); err != nil {
		return
	}

	dataNode.NodeSetID = ns.ID
	if err = c.syncUpdateDataNode(dataNode); err != nil {
		dataNode.NodeSetID = oldNodeSetID
		return
	}
	if err = c.syncUpdateNodeSet(ns); err != nil {
		return
	}
	err = c.t.putDataNode(dataNode)
}

/*if node report data partition infos,so range data partition infos,then update data partition info*/
func (c *Cluster) updateDataNode(dataNode *DataNode, dps []*proto.DataPartitionReport) {
	for _, vr := range dps {
		if vr == nil {
			continue
		}
		if vr.VolName != "" {
			vol, err := c.getVol(vr.VolName)
			if err != nil {
				continue
			}
			//if vol.Status == proto.VolStatusMarkDelete {
			//	continue
			//}
			if dp, err := vol.getDataPartitionByID(vr.PartitionID); err == nil {
				dp.updateMetric(vr, dataNode, c)
			}
		} else {
			if dp, err := c.getDataPartitionByID(vr.PartitionID); err == nil {
				dp.updateMetric(vr, dataNode, c)
			}
		}
	}
}

func (c *Cluster) updateMetaNode(metaNode *MetaNode, metaPartitions []*proto.MetaPartitionReport, threshold bool) {
	var (
		vol *Vol
		err error
	)
	for _, mr := range metaPartitions {
		if mr == nil {
			continue
		}
		var mp *MetaPartition
		if mr.VolName != "" {

			vol, err = c.getVol(mr.VolName)
			if err != nil {
				continue
			}

			//if vol.Status == proto.VolStatusMarkDelete {
			//	continue
			//}

			mp, err = vol.metaPartition(mr.PartitionID)
			if err != nil {
				continue
			}

		} else {
			mp, err = c.getMetaPartitionByID(mr.PartitionID)
			if err != nil {
				continue
			}
		}

		// send latest end to replica metanode, including updating the end after MaxMP split when the old MaxMP is unavailable
		if mr.End != mp.End {
			mp.addUpdateMetaReplicaTask(c)
		}

		mp.updateMetaPartition(mr, metaNode, c)
		vol.uidSpaceManager.pushUidMsg(mr)
		vol.quotaManager.quotaUpdate(mr)
		c.updateInodeIDUpperBound(mp, mr, threshold, metaNode)
	}
}

func (c *Cluster) updateInodeIDUpperBound(mp *MetaPartition, mr *proto.MetaPartitionReport, hasArriveThreshold bool, metaNode *MetaNode) (err error) {
	if !hasArriveThreshold {
		return
	}
	var vol *Vol
	if vol, err = c.getVol(mp.volName); err != nil {
		log.LogWarnf("action[updateInodeIDRange] vol[%v] not found", mp.volName)
		return
	}

	maxPartitionID := vol.maxMetaPartitionID()
	if mr.PartitionID < maxPartitionID {
		return
	}
	var end uint64
	metaPartitionInodeIdStep := gConfig.MetaPartitionInodeIdStep
	if mr.MaxInodeID <= 0 {
		end = mr.Start + metaPartitionInodeIdStep
	} else {
		end = mr.MaxInodeID + metaPartitionInodeIdStep
	}
	log.LogWarnf("mpId[%v],start[%v],end[%v],addr[%v],used[%v]", mp.PartitionID, mp.Start, mp.End, metaNode.Addr, metaNode.Used)
	if c.cfg.DisableAutoCreate {
		log.LogWarnf("updateInodeIDUpperBound: disable auto create meta partition, mp %d", mp.PartitionID)
		return
	}
	if err = vol.splitMetaPartition(c, mp, end, metaPartitionInodeIdStep, false, mp.Region); err != nil {
		log.LogErrorf("mpId[%v], splitMetaPartition err %v", mp.PartitionID, err)
	}
	return
}

func IsExcessiveReplicaMetaPartition(mp *MetaPartition) bool {
	count := uint8(0)
	for _, peer := range mp.Peers {
		if peer.Type == raftProto.PeerLearner {
			continue
		}
		count++
	}
	return count > mp.ReplicaNum
}

func getMetaReplicaLearnerInfo(mp *MetaPartition, learnerAddr string) (isLearner bool, manualPromote bool, err error) {
	for _, peer := range mp.Peers {
		if peer.Addr == learnerAddr {
			isLearner = peer.Type == raftProto.PeerLearner
			manualPromote = peer.ManualPromote
			return
		}
	}

	return false, false, fmt.Errorf("learnerAddr[%s] not found in mp[%v]", learnerAddr, mp.PartitionID)
}

func hasLearnerFlagMismatch(mp *MetaPartition) bool {
	masterLearner := make(map[string]bool)
	for _, p := range mp.Peers {
		masterLearner[p.Addr] = p.Type == raftProto.PeerLearner
	}
	for _, r := range mp.Replicas {
		if isLearner, ok := masterLearner[r.Addr]; ok && isLearner != r.IsLearner {
			return true
		}
	}
	return false
}

func isLackReplicaMetaPartition(mp *MetaPartition) bool {
	nonLearner := uint8(0)
	for _, peer := range mp.Peers {
		if peer.Type != raftProto.PeerLearner {
			nonLearner++
		}
	}
	return nonLearner < mp.ReplicaNum
}

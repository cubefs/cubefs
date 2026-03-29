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
	"runtime/debug"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

func (c *Cluster) scheduleToLoadMetaPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkLoadMetaPartitions()
				}
			}
			time.Sleep(2 * time.Second * defaultIntervalToCheckDataPartition)
		}
	}()
}

func (c *Cluster) checkLoadMetaPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDiskRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkDiskRecoveryProgress occurred panic")
		}
	}()
	vols := c.allVols()
	for _, vol := range vols {
		mps := vol.cloneMetaPartitionMap()
		for _, mp := range mps {
			c.doLoadMetaPartition(mp)
		}
	}
}

func (mp *MetaPartition) checkPeerDiffWithRaft(c *Cluster) {
	if len(mp.LoadResponse) == 0 {
		return
	}
	if !mp.doCompare() {
		return
	}

	// master view: peerID -> addr
	masterPeers := make(map[uint64]string)
	for _, peer := range mp.Peers {
		masterPeers[peer.ID] = peer.Addr
	}

	// Find leader replica
	var leaderInfo *proto.MetaPartitionLoadResponse
	for _, info := range mp.LoadResponse {
		if info.RaftInfo.RaftStatus.Leader == info.RaftInfo.RaftStatus.NodeID {
			leaderInfo = info
			if len(info.RaftInfo.PendingPeers) != 0 || len(info.RaftInfo.DownReplicas) != 0 {
				c.AbnormalRaftMP.Store(mp.PartitionID, mp)
				return
			}
			break
		}
	}

	// Check all replicas: replicas in Raft that are additional compared to master
	for _, info := range mp.LoadResponse {
		raftPeers := make(map[uint64]string)
		for _, p := range info.RaftInfo.Hosts {
			raftPeers[p.ID] = p.Addr
		}

		for peerID := range raftPeers {
			if _, ok := masterPeers[peerID]; !ok {
				c.AbnormalRaftMP.Store(mp.PartitionID, mp)
				return
			}
		}
	}

	// No leader found, cannot compare
	if leaderInfo == nil {
		return
	}

	// Check only leader: peers that the master has are additional compared to raft leader
	leaderRaftPeers := make(map[uint64]string)
	for _, p := range leaderInfo.RaftInfo.Hosts {
		leaderRaftPeers[p.ID] = p.Addr
	}

	for peerID := range masterPeers {
		if _, ok := leaderRaftPeers[peerID]; !ok {
			c.AbnormalRaftMP.Store(mp.PartitionID, mp)
			return
		}
	}

	c.AbnormalRaftMP.Delete(mp.PartitionID)
}

func (mp *MetaPartition) checkSnapshot(c *Cluster) {
	if len(mp.LoadResponse) == 0 {
		return
	}
	if !mp.doCompare() {
		return
	}
	if !mp.isSameApplyID() {
		return
	}
	ckInode := mp.checkInodeCount(c)
	ckDentry := mp.checkDentryCount(c)
	if ckInode && ckDentry {
		mp.EqualCheckPass = true
	} else {
		mp.EqualCheckPass = false
	}
}

func (mp *MetaPartition) doCompare() bool {
	for _, lr := range mp.LoadResponse {
		if !lr.DoCompare {
			return false
		}
	}
	return true
}

func (mp *MetaPartition) isSameApplyID() bool {
	rst := true
	applyID := mp.LoadResponse[0].ApplyID
	for _, loadResponse := range mp.LoadResponse {
		if applyID != loadResponse.ApplyID {
			rst = false
		}
	}
	return rst
}

func (mp *MetaPartition) checkInodeCount(c *Cluster) (isEqual bool) {
	isEqual = true
	maxInode := mp.LoadResponse[0].MaxInode
	maxInodeCount := mp.LoadResponse[0].InodeCount
	inodeEqual := true
	maxInodeEqual := true
	if mp.IsRecover.Load() {
		return
	}
	for _, loadResponse := range mp.LoadResponse {
		diff := math.Abs(float64(loadResponse.MaxInode) - float64(maxInode))
		if diff > defaultRangeOfCountDifferencesAllowed {
			isEqual = false
			inodeEqual = false
			break
		}
		diff = math.Abs(float64(loadResponse.InodeCount) - float64(maxInodeCount))
		if diff > defaultRangeOfCountDifferencesAllowed {
			isEqual = false
			maxInodeEqual = false
			break
		}
	}
	if !isEqual {
		msg := fmt.Sprintf("inode count is not equal,vol[%v],mpID[%v],", mp.volName, mp.PartitionID)
		for _, lr := range mp.LoadResponse {
			lrMsg := fmt.Sprintf(msg+"addr[%s],applyId[%d],committedId[%d],maxInode[%d],InodeCnt[%d]", lr.Addr, lr.ApplyID, lr.CommittedID, lr.MaxInode, lr.InodeCount)
			Warn(c.Name, lrMsg)
		}
		if !maxInodeEqual {
			c.inodeCountNotEqualMP.Store(mp.PartitionID, mp)
		}
		if !inodeEqual {
			c.maxInodeNotEqualMP.Store(mp.PartitionID, mp)
		}

	} else {
		if _, ok := c.inodeCountNotEqualMP.Load(mp.PartitionID); ok {
			c.inodeCountNotEqualMP.Delete(mp.PartitionID)
		}
		if _, ok := c.maxInodeNotEqualMP.Load(mp.PartitionID); ok {
			c.maxInodeNotEqualMP.Delete(mp.PartitionID)
		}
	}
	return
}

func (mp *MetaPartition) checkDentryCount(c *Cluster) (isEqual bool) {
	isEqual = true
	if mp.IsRecover.Load() {
		return
	}
	dentryCount := mp.LoadResponse[0].DentryCount
	for _, loadResponse := range mp.LoadResponse {
		diff := math.Abs(float64(loadResponse.DentryCount) - float64(dentryCount))
		if diff > defaultRangeOfCountDifferencesAllowed {
			isEqual = false
		}
	}

	if !isEqual {
		msg := fmt.Sprintf("dentry count is not equal,vol[%v],mpID[%v],", mp.volName, mp.PartitionID)
		for _, lr := range mp.LoadResponse {
			lrMsg := fmt.Sprintf(msg+"addr[%s],applyId[%d],committedId[%d],dentryCount[%d]", lr.Addr, lr.ApplyID, lr.CommittedID, lr.DentryCount)
			Warn(c.Name, lrMsg)
		}
		c.dentryCountNotEqualMP.Store(mp.PartitionID, mp)
	} else {
		if _, ok := c.dentryCountNotEqualMP.Load(mp.PartitionID); ok {
			c.dentryCountNotEqualMP.Delete(mp.PartitionID)
		}
	}
	return
}

func (c *Cluster) scheduleToCheckMetaPartitionRecoveryProgress() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				if c.vols != nil {
					c.checkMetaPartitionRecoveryProgress()
				}
			}
			time.Sleep(time.Second * defaultIntervalToCheck)
		}
	}()
}

func (c *Cluster) checkMetaPartitionRecoveryProgress() {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			log.LogErrorf("checkMetaPartitionRecoveryProgress occurred panic,err[%v],stack:\n%v", r, stack)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				fmt.Sprintf("checkMetaPartitionRecoveryProgress occurred panic,err[%v]", r))
		}

		cost := time.Since(start)
		msg := fmt.Sprintf("checkMetaPartitionRecoveryProgress duration[%v]", cost.String())
		if cost > time.Second*5 {
			log.LogWarn(msg)
		} else {
			log.LogInfo(msg)
		}
	}()

	c.badPartitionMutex.Lock()
	defer c.badPartitionMutex.Unlock()

	c.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badMetaPartitionIds := value.([]uint64)
		newBadMpIds := make([]uint64, 0)
		for _, partitionID := range badMetaPartitionIds {
			partition, err := c.getMetaPartitionByID(partitionID)
			if err != nil {
				Warn(c.Name, fmt.Sprintf("checkMetaPartitionRecoveryProgress clusterID[%v], partitionID[%v] is not exist", c.Name, partitionID))
				continue
			}

			if !partition.IsRecover.Load() {
				continue
			}

			vol, err := c.getVol(partition.volName)
			if err != nil {
				Warn(c.Name, fmt.Sprintf("checkMetaPartitionRecoveryProgress clusterID[%v],vol[%v] partitionID[%v]is not exist",
					c.Name, partition.volName, partitionID))
				continue
			}

			if vol.isUnavailable() {
				continue
			}

			// Check if it's learner mode decommission
			partition.RLock()
			isLearnerMode := partition.RecoverDst != ""
			partition.RUnlock()

			if isLearnerMode {
				// Learner mode decommission check
				if err = c.checkLearnerModeRecovery(partition, &partition.RecoverPair, false); err != nil {
					log.LogWarnf("checkMetaPartitionRecoveryProgress learner mode check failed,vol[%v],partitionID[%v],err[%v]",
						partition.volName, partitionID, err)
					newBadMpIds = append(newBadMpIds, partitionID)
					continue
				}
			} else {
				// Normal mode check
				if partition.getMinusOfMaxInodeID() < defaultMinusOfMaxInodeID {
					partition.IsRecover.Store(false)
					partition.setRestoreReplicaStatus(RestoreReplicaMetaStop)
					partition.RLock()
					c.syncUpdateMetaPartition(partition)
					partition.RUnlock()
					Warn(c.Name, fmt.Sprintf("checkMetaPartitionRecoveryProgress clusterID[%v],vol[%v] partitionID[%v] has recovered success",
						c.Name, partition.volName, partitionID))
				} else {
					newBadMpIds = append(newBadMpIds, partitionID)
				}
			}
		}

		if len(newBadMpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("checkMetaPartitionRecoveryProgress clusterID[%v],node[%v] has recovered success", c.Name, key))
			c.BadMetaPartitionIds.Delete(key)
		} else {
			c.BadMetaPartitionIds.Store(key, newBadMpIds)
			log.LogInfof("checkMetaPartitionRecoveryProgress BadMetaPartitionIds there is still (%d) mp in recover, addr (%s)", len(newBadMpIds), key)
		}

		return true
	})

	c.RecoverMetaPartitionIds.Range(func(key, _ interface{}) bool {
		mpId := key.(uint64)
		mp, err := c.getMetaPartitionByID(mpId)
		if err != nil {
			Warn(c.Name, fmt.Sprintf("checkMetaPartitionRecoveryProgress clusterID[%v], partitionID[%v] is not exist", c.Name, mpId))
			c.RecoverMetaPartitionIds.Delete(mpId)
			return true
		}

		recoverLearners := mp.RecoverLearners
		for _, info := range recoverLearners {
			log.LogWarnf("checkMetaPartitionRecoveryProgress learner mode check,vol[%v],partitionID[%v],info[%v]", mp.volName, mpId, info)
			if err = c.checkLearnerModeRecovery(mp, info, true); err != nil {
				log.LogWarnf("checkMetaPartitionRecoveryProgress learner mode check failed,vol[%v],partitionID[%v],err[%v]",
					mp.volName, mpId, err)
			}
		}

		if len(mp.RecoverLearners) == 0 {
			c.RecoverMetaPartitionIds.Delete(mpId)
			Warn(c.Name, fmt.Sprintf("checkMetaPartitionRecoveryProgress clusterID[%v],vol[%v] partitionID[%v] has recovered success",
				c.Name, mp.volName, mpId))
		}

		if len(mp.RecoverLearners) > 0 {
			log.LogWarnf("checkMetaPartitionRecoveryProgress learner mode check,vol[%v],partitionID[%v],recoverLearners[%v]", mp.volName, mpId, mp.RecoverLearners[0])
		}
		return true
	})
}

// markLearnerRecoverFailed marks learner mode recovery as failed and clears recovery flags
func (c *Cluster) markLearnerRecoverFailed(mp *MetaPartition, info *proto.RecoverPair) {
	mp.Lock()
	defer mp.Unlock()
	info.IsRecover.Store(false)
	info.RecoverState = proto.RecoverStateFailed
	mp.setRestoreReplicaStatus(RestoreReplicaMetaStop)
	c.syncUpdateMetaPartition(mp)
	log.LogWarnf("markLearnerRecoverFailed mp[%v] marked as failed, recovery stopped, info[%v]", mp.PartitionID, info)
}

// recordRecoveryFailure records failure time for retry cooldown
func (c *Cluster) recordRecoveryFailure(mp *MetaPartition, info *proto.RecoverPair) {
	mp.Lock()
	info.RecoverRetryTime = time.Now().Unix()
	info.RecoverRetryCnt++
	c.syncUpdateMetaPartition(mp)
	mp.Unlock()
}

func (c *Cluster) clearRecoveryState(mp *MetaPartition, info *proto.RecoverPair, manualPromote bool) (err error) {
	if manualPromote {
		return c.clearStateForRecoverLearners(mp, info)
	}
	return c.clearLearnerRecoveryState(mp)
}

func (c *Cluster) clearStateForRecoverLearners(mp *MetaPartition, info *proto.RecoverPair) (err error) {
	mp.Lock()
	defer mp.Unlock()

	oldLearners := mp.RecoverLearners
	newLearners := make([]*proto.RecoverPair, 0)
	for _, learner := range oldLearners {
		if learner.RecoverDst == info.RecoverDst {
			log.LogWarnf("clearLearnerRecoveryStateFromRecoverLearners learner[%v] is already in recover learners, info[%v]", learner.RecoverDst, learner)
			continue
		}

		newLearners = append(newLearners, learner)
	}
	mp.RecoverLearners = newLearners
	mp.setRestoreReplicaStatus(RestoreReplicaMetaStop)

	err = c.syncUpdateMetaPartition(mp)
	if err != nil {
		log.LogWarnf("clearLearnerRecoveryStateFromRecoverLearners mp[%v] cleared learner[%v] from recover learners, info[%v], err[%v]", mp.PartitionID, info.RecoverDst, info, err)
		mp.RecoverLearners = oldLearners
		mp.setRestoreReplicaStatus(RestoreReplicaMetaForbidden)
		return
	}

	auditMsg := fmt.Sprintf("clearLearnerRecoveryStateFromRecoverLearners: vol[%v] mp[%v] cleared learner[%v] from recover learners, info[%v]", mp.volName, mp.PartitionID, info.RecoverDst, info)
	auditlog.LogMasterOp("clearLearnerRecoveryStateFromRecoverLearners", auditMsg, nil)
	return nil
}

// clearLearnerRecoveryState clears learner recovery state and persists the change
// If persistence fails, restores the original state
func (c *Cluster) clearLearnerRecoveryState(mp *MetaPartition) (err error) {
	mp.Lock()
	defer mp.Unlock()

	srcAddr := mp.RecoverSrc
	dstAddr := mp.RecoverDst
	recoverStartTime := mp.RecoverStart
	recoverState := mp.RecoverState
	decommissionType := mp.DecommissionType

	mp.RecoverSrc = ""
	mp.RecoverDst = ""
	mp.DecommissionType = proto.InitialDecommission
	mp.RecoverStart = 0
	mp.RecoverRetryCnt = 0
	mp.RecoverRetryTime = 0
	mp.RecoverState = proto.RecoverStateInit
	mp.IsRecover.Store(false)

	mp.setRestoreReplicaStatus(RestoreReplicaMetaStop)

	err = c.syncUpdateMetaPartition(mp)
	if err != nil {
		mp.IsRecover.Store(true)
		mp.setRestoreReplicaStatus(RestoreReplicaMetaForbidden)
		// Restore state on update failure
		mp.DecommissionType = decommissionType
		mp.RecoverSrc = srcAddr
		mp.RecoverDst = dstAddr
		mp.RecoverStart = recoverStartTime
		mp.RecoverState = recoverState
		log.LogWarnf("clearLearnerRecoveryState restore state on update failure, mp[%v]", mp.PartitionID)
		return
	}

	auditMsg := fmt.Sprintf("clearLearnerRecoveryState: vol[%v] mp[%v] clear learner recovery state, src[%v] dst[%v] recoverStartTime[%v]",
		mp.volName, mp.PartitionID, srcAddr, dstAddr, recoverStartTime)
	auditlog.LogMasterOp("clearLearnerRecoveryState", auditMsg, nil)
	return nil
}

// validateLearnerRecoveryStatus validates if learner is ready for promotion
func (c *Cluster) validateLearnerRecoveryStatus(mp *MetaPartition, dstAddr string) (err error) {
	// Load and find responses
	c.doLoadMetaPartition(mp)
	mp.RLock()
	var leaderResponse, learnerResponse *proto.MetaPartitionLoadResponse
	for _, lr := range mp.LoadResponse {
		if lr.Addr == dstAddr {
			learnerResponse = lr
		}
		if lr.RaftInfo.RaftStatus.Leader == lr.RaftInfo.RaftStatus.NodeID {
			leaderResponse = lr
		}
	}
	mp.RUnlock()

	if leaderResponse == nil || learnerResponse == nil {
		return fmt.Errorf("leader[%v] or learner[%v] response not found for mp[%v]", leaderResponse == nil, learnerResponse == nil, mp.PartitionID)
	}

	// Validate learner status
	if learnerResponse.ApplyID == 0 {
		return fmt.Errorf("learner applyId is 0 for mp[%v]", mp.PartitionID)
	}

	if learnerResponse.RaftInfo.RaftStatus.RestoringSnapshot {
		return fmt.Errorf("learner[%v] is in snapshot mode for mp[%v]", dstAddr, mp.PartitionID)
	}

	learnerMetaNode, err1 := c.metaNode(dstAddr)
	if err1 != nil {
		return fmt.Errorf("get learner metaNode[%v] failed: %v", dstAddr, err1)
	}

	// Check snapshot mode
	if leaderResponse.RaftInfo.RaftStatus.Replicas == nil {
		return fmt.Errorf("leader's replicas is nil for mp[%v]", mp.PartitionID)
	}
	learnerReplicaStatus, exists := leaderResponse.RaftInfo.RaftStatus.Replicas[learnerMetaNode.ID]
	if !exists {
		return fmt.Errorf("learner[%v] not found in leader's replicas for mp[%v]", dstAddr, mp.PartitionID)
	}
	if learnerReplicaStatus.Snapshoting {
		return fmt.Errorf("learner[%v] is in snapshot mode for mp[%v]", dstAddr, mp.PartitionID)
	}

	// Check sync progress
	var commitDiff uint64
	if leaderResponse.RaftInfo.RaftStatus.Commit >= learnerReplicaStatus.Commit {
		commitDiff = leaderResponse.RaftInfo.RaftStatus.Commit - learnerReplicaStatus.Commit
	} else {
		commitDiff = learnerReplicaStatus.Commit - leaderResponse.RaftInfo.RaftStatus.Commit
	}
	if commitDiff >= defaultMinusOfCommit {
		return fmt.Errorf("applyId difference[%v] >= 1000 for mp[%v]", commitDiff, mp.PartitionID)
	}

	return nil
}

// checkLearnerModeRecovery checks if learner mode decommission is ready to complete
// Returns error if not ready, nil if ready to promote
func (c *Cluster) checkLearnerModeRecovery(mp *MetaPartition, info *proto.RecoverPair, manualPromote bool) (err error) {
	if !contains(mp.Hosts, info.RecoverDst) {
		log.LogWarnf("checkLearnerModeRecovery dstAddr[%v] is not in mp[%v] hosts, info[%v]", info.RecoverDst, mp.PartitionID, info)
		return c.clearRecoveryState(mp, info, manualPromote)
	}

	if info.RecoverState == proto.RecoverStateFailed {
		return fmt.Errorf("learner recovery failed for mp[%v]", mp.PartitionID)
	}

	srcAddr := info.RecoverSrc
	dstAddr := info.RecoverDst

	// Get recovery status
	mp.Lock()
	recoverStartTime := info.RecoverStart
	failCount := info.RecoverRetryCnt
	lastFailTime := info.RecoverRetryTime
	if recoverStartTime == 0 {
		recoverStartTime = time.Now().Unix()
		info.RecoverStart = recoverStartTime
		c.syncUpdateMetaPartition(mp)
	}
	mp.Unlock()

	// Log entry with key info only
	recoverDuration := time.Now().Unix() - recoverStartTime
	log.LogWarnf("checkLearnerModeRecovery: vol[%v] mp[%v] src[%v] dst[%v] duration[%v]s failCount[%v]",
		mp.volName, mp.PartitionID, srcAddr, dstAddr, recoverDuration, failCount)

	// Wait at least one heartbeat interval before acting, so that the first
	// heartbeat response has been collected and mp autoHealing can get the latest status.
	if recoverDuration < defaultIntervalToCheckHeartbeat {
		return fmt.Errorf("mp[%v] recovery just started, wait for next check", mp.PartitionID)
	}

	// Check timeout and failure count
	timeoutSeconds := c.cfg.LearnerRecoverTimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = defaultLearnerRecoverTimeout
	}

	if recoverDuration > timeoutSeconds {
		auditMsg := fmt.Sprintf("checkLearnerModeRecovery: vol[%v] mp[%v] timeout[%vs] exceeds[%vs], marking failed, info[%v]",
			mp.volName, mp.PartitionID, recoverDuration, timeoutSeconds, info)
		auditlog.LogMasterOp("checkLearnerModeRecovery", auditMsg, nil)
		c.markLearnerRecoverFailed(mp, info)
		return fmt.Errorf("learner recovery timeout for mp[%v]", mp.PartitionID)
	}

	// Check retry cooldown
	if lastFailTime > 0 {
		if timeSinceLastFail := time.Now().Unix() - lastFailTime; timeSinceLastFail < learnerRecoverRetryInterval {
			err = fmt.Errorf("retry cooldown, wait %vs, info[%v]", learnerRecoverRetryInterval-timeSinceLastFail, info)
			return
		}
	}

	if failCount >= learnerRecoverMaxFailCount {
		log.LogWarnf("checkLearnerModeRecovery: vol[%v] mp[%v] failCount[%v] exceeds[%v], marking failed, info[%v]",
			mp.volName, mp.PartitionID, failCount, learnerRecoverMaxFailCount, info)
		c.markLearnerRecoverFailed(mp, info)
		return fmt.Errorf("learner recovery failure count exceeds limit for mp[%v]", mp.PartitionID)
	}

	// Validate learner recovery status
	if err = c.validateLearnerRecoveryStatus(mp, dstAddr); err != nil {
		if strings.Contains(err.Error(), "response not found") {
			c.recordRecoveryFailure(mp, info)
			log.LogWarnf("checkLearnerModeRecovery: vol[%v] mp[%v] learner recovery status validation failed, err[%v], info[%v]",
				mp.volName, mp.PartitionID, err, info)
		}
		return
	}

	if !manualPromote {
		// Promote learner to voter
		if err = c.promoteMetaReplicaToVoter(mp, dstAddr, false); err != nil {
			c.recordRecoveryFailure(mp, info)
			auditlog.LogMasterOp("checkLearnerModeRecovery", fmt.Sprintf("promote learner[%v] failed: %v, info[%v]", dstAddr, err, info), err)
			return
		}
	}

	// Delete source replica
	if srcAddr != "" {
		if err = c.deleteMetaReplica(mp, srcAddr, false, false); err != nil {
			c.recordRecoveryFailure(mp, info)
			auditlog.LogMasterOp("checkLearnerModeRecovery", fmt.Sprintf("delete source replica[%v] failed: %v, info[%v]", srcAddr, err, info), err)
			return
		}
		auditMsg := fmt.Sprintf("checkLearnerModeRecovery: vol[%v] mp[%v] delete source replica[%v], info[%v]", mp.volName, mp.PartitionID, srcAddr, info)
		auditlog.LogMasterOp("checkLearnerModeRecovery", auditMsg, nil)
	}

	// Clear recovery state
	if err = c.clearRecoveryState(mp, info, manualPromote); err != nil {
		return
	}

	auditMsg := fmt.Sprintf("checkLearnerModeRecovery: vol[%v] mp[%v] decommission success, src[%v] dst[%v] duration[%vs], info[%v]",
		mp.volName, mp.PartitionID, srcAddr, dstAddr, time.Now().Unix()-recoverStartTime, info)
	auditlog.LogMasterOp("checkLearnerModeRecovery", auditMsg, nil)
	Warn(c.Name, auditMsg)
	return nil
}

func (c *Cluster) scheduleToCheckMetaReplicaMeta() {
	c.runTask(&cTask{
		tickTime: time.Second * time.Duration(c.cfg.IntervalToCheckMetaPartition),
		name:     "scheduleToCheckMetaReplicaMeta",
		function: func() (fin bool) {
			// Replica meta check and auto-healing only works in learner mode
			if !c.EnableMpDecommissionByLearner {
				return
			}
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkMetaReplicaMeta()
			}
			return
		},
	})
}

func (c *Cluster) checkMetaReplicaMeta() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMetaReplicaMeta occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMetaReplicaMeta occurred panic")
		}
	}()

	vols := c.allVols()
	for _, vol := range vols {
		vol.checkMetaReplicaMeta(c)
	}
}

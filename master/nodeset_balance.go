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
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

type nodeSetResource struct {
	ID                  uint64
	CanAllocDataNodeCnt int
	TotalDpCapacity     uint64
	TotalDpAvail        uint64
	TotalCapacity       uint64
	TotalAvail          uint64
}

const (
	// concurrent Balance DP Count
	concurrentBalancedDPCount = 400

	// Task execution interval (4 hours)
	// NodesetBalanceInterval = 4 * time.Hour
	NodesetBalanceInterval = 2 * time.Minute
)

// scheduleToNodesetBalance registers auto nodeset balance task
func (c *Cluster) scheduleToNodesetBalance() {
	c.runTask(
		&cTask{
			tickTime: NodesetBalanceInterval,
			name:     "nodesetBalanceController",
			function: func() (fin bool) {
				if c.partition == nil || !c.partition.IsRaftLeader() {
					return
				}
				// Check if nodeset balance is enabled before execution
				if !c.getEnableAutoNodesetBalance() {
					log.LogDebugf("action[nodesetBalanceController] nodeset balance is disabled, skip execution")
					return
				}
				c.executeNodesetBalanceMigrations()
				return
			},
		},
	)
}

func (c *Cluster) executeNodesetBalanceMigrations() {
	begin := time.Now()
	log.LogInfof("action[executeNodesetBalanceMigrations] starting nodeset balance migration execution")

	defer func() {
		log.LogInfof("action[executeNodesetBalanceMigrations] migration execution completed in %v", time.Since(begin))
	}()

	activeTasks := c.countActiveNodesetBalanceTasks()
	if activeTasks >= concurrentBalancedDPCount {
		log.LogInfof("action[executeNodesetBalanceMigrations] already have %d active tasks, skipping execution", activeTasks)
		return
	}

	dpHost2Ns := c.buildDpHostToNodeSet()
	vols := c.copyVols()
	processedCount := 0
	availableSlots := concurrentBalancedDPCount - activeTasks

outerLoop:
	for _, vol := range vols {
		partitions := vol.dataPartitions.clonePartitions()
		for _, dp := range partitions {
			if processedCount >= availableSlots {
				log.LogInfof("action[executeNodesetBalanceMigrations] reached available slots limit (%d)", availableSlots)
				break outerLoop
			}

			if dp.IsDiscard {
				continue
			}

			_, isBalanced := getDpNodesetDistribution(dp, dpHost2Ns)
			if isBalanced {
				continue
			}

			if dp.DecommissionType == proto.NodesetBalance && dp.DecommissionStatus != DecommissionFail {
				continue
			}

			c.processPartitionMigration(dp, dp.Hosts)
			processedCount++
		}
	}

	log.LogInfof("action[executeNodesetBalanceMigrations] completed, processed %d DPs", processedCount)
}

func (c *Cluster) countActiveNodesetBalanceTasks() int {
	count := 0
	vols := c.copyVols()

	for _, vol := range vols {
		partitions := vol.dataPartitions.clonePartitions()
		for _, dp := range partitions {
			if dp.IsDiscard {
				continue
			}

			if dp.DecommissionType == proto.NodesetBalance && dp.DecommissionStatus != DecommissionFail {
				count++
			}
		}
	}

	log.LogInfof("action[countActiveNodesetBalanceTasks] total active tasks: %d", count)
	return count
}

func getDpNodesetDistribution(dp *DataPartition, dpHost2Ns map[string]uint64) (map[uint64]int, bool) {
	domainCnts := make(map[uint64]int)
	for _, host := range dp.Hosts {
		if nsID, ok := dpHost2Ns[host]; ok {
			domainCnts[nsID]++
		}
	}
	isBalanced := len(domainCnts) <= 1
	return domainCnts, isBalanced
}

func (c *Cluster) processPartitionMigration(dp *DataPartition, hosts []string) {
	if len(hosts) == 0 {
		return
	}

	if dp.DecommissionStatus == markDecommission ||
		dp.DecommissionStatus == DecommissionRunning ||
		dp.DecommissionStatus == DecommissionPrepare {
		return
	}

	var (
		srcAddrs []string
		dstAddrs []string
		targetNs *nodeSet
		err      error
	)

	if targetNs, srcAddrs, dstAddrs, err = selectTargetHostsInNodesetBalance(hosts, int(dp.ReplicaNum), c, dp.MediaType); err != nil {
		log.LogWarnf("action[executeReplicaMigration] dp(%v) select Target hosts failed", dp.PartitionID)
		return
	}

	dp.DecommissionSrcAddrs = srcAddrs
	dp.DecommissionDstAddrs = dstAddrs
	dp.DecommissionDstNodeSet = targetNs.ID
	dp.DecommissionType = proto.NodesetBalance
	dp.DecommissionWeight = 1

	log.LogDebugf("action[executeReplicaMigration] dp %v srcAddrs %v dstAddrs %v", dp.PartitionID, dp.DecommissionSrcAddrs, dp.DecommissionDstAddrs)

	if err = c.addDataReservedResource(dstAddrs, dp); err != nil {
		log.LogWarnf("action[executeReplicaMigration] dp %v simulate resource change failed: %v", dp.PartitionID, err)
		return
	}

	if !dp.ProcessNextDecommissionSrcHost(c) {
		log.LogWarnf("action[executeReplicaMigration] submitted decommission: dp(%v) replicas(%v) failed",
			dp.PartitionID, dp.Hosts)
		c.releaseDataReservedResource(dstAddrs, dp)
		return
	}
	// if err := c.markDecommissionDataPartition(dp, lastSrcNode, targetNs.ID, false, proto.NodesetBalance, 1, srcHosts, targetHosts); err != nil {
	// 	log.LogWarnf("action[executeReplicaMigration] decommission failed for dp(%v) replicas(%v) : %v",
	// 		dp.PartitionID, srcHosts, err)
	// 	return false
	// }
	// if err := dp.MarkDecommissionStatus(srcHosts[lastIndex], targetHosts[0], "", targetNs.ID, false, uint64(time.Now().Unix()), proto.NodesetBalance, 1, c, srcHosts, targetHosts); err != nil {
	// 	log.LogWarnf("action[executeReplicaMigration] decommission failed for dp(%v) replicas(%v) : %v",
	// 		dp.PartitionID, srcHosts, err)
	// 	return false
	// }

	log.LogInfof("action[executeReplicaMigration] submitted decommission: dp(%v) replicas(%v)",
		dp.PartitionID, dp.Hosts)
	return
}

func (c *Cluster) buildDpHostToNodeSet() map[string]uint64 {
	m := make(map[string]uint64)
	c.dataNodes.Range(func(key, value interface{}) bool {
		dn := value.(*DataNode)
		m[dn.Addr] = dn.NodeSetID
		return true
	})
	return m
}

func (c *Cluster) getNodesetBalanceStatus() *proto.NodesetBalanceStatus {
	status := &proto.NodesetBalanceStatus{
		DecommissioningDPIDs: make([]uint64, 0),
		SingleMigrationLimit: concurrentBalancedDPCount,
		EnableNodesetBalance: c.getEnableAutoNodesetBalance(),
		DomainDistribution: &proto.DomainDistributionInfo{
			SingleDomainDPs: 0,
			TwoDomainDPs:    0,
			ThreeDomainDPs:  0,
		},
	}

	dpHost2Ns := c.buildDpHostToNodeSet()

	vols := c.copyVols()
	for _, vol := range vols {
		partitions := vol.dataPartitions.clonePartitions()
		for _, dp := range partitions {
			if dp.IsDiscard {
				continue
			}

			if dp.DecommissionType == proto.NodesetBalance {
				status.DecommissioningDPIDs = append(status.DecommissioningDPIDs, dp.PartitionID)
			}

			domainCnts, isBalanced := getDpNodesetDistribution(dp, dpHost2Ns)
			domainCount := len(domainCnts)

			switch domainCount {
			case 0, 1:
				status.DomainDistribution.SingleDomainDPs++
			case 2:
				status.DomainDistribution.TwoDomainDPs++
			case 3:
				status.DomainDistribution.ThreeDomainDPs++
			}

			if !isBalanced {
				status.TotalUnbalancedDPs++
			}
		}
	}

	return status
}

func (c *Cluster) getAllNodesetBalanceDataPartition() (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if dp.DecommissionType == proto.NodesetBalance {
				partitions = append(partitions, dp)
			}
		}
	}
	return
}

// cancelDpNodesetBalance cancels all ongoing nodeset balance decommission tasks
func (c *Cluster) cancelDpNodesetBalance() (err error) {
	var (
		dstNs *nodeSet
		srcNs *nodeSet
		dps   []*DataPartition
		dpWg  sync.WaitGroup
		mu    sync.Mutex
	)
	begin := time.Now()
	defer func() {
		log.LogInfof("action[cancelDpNodesetBalance] cancel dp NodesetBalance using time(%v)", time.Since(begin))
	}()

	dpCh := make(chan *DataPartition, 1024)
	dpIds := make([]uint64, 0)
	failedDpIds := make([]uint64, 0)
	dps = c.getAllNodesetBalanceDataPartition()

	for ii := 0; ii < 10; ii++ {
		go func() {
			for dp := range dpCh {
				if dp.GetDecommissionStatus() == DecommissionSuccess || dp.IsRollbackFailed() || dp.DecommissionType != proto.NodesetBalance {
					dpWg.Done()
					continue
				}
				if dp.DecommissionDstAddr != "" {
					dstNs, _, err = getTargetNodeset(dp.DecommissionDstAddr, c)
					if err != nil {
						log.LogWarnf("action[cancelDpNodesetBalance] dp %v find dst(%v) nodeset failed:%v",
							dp.PartitionID, dp.DecommissionDstAddr, err.Error())
						mu.Lock()
						failedDpIds = append(failedDpIds, dp.PartitionID)
						mu.Unlock()
						dpWg.Done()
						continue
					}
					if dstNs.HasDecommissionToken(dp.PartitionID) {
						if dp.IsDecommissionPrepare() || dp.IsMarkDecommission() {
							dpCh <- dp
							continue
						}
						if dp.isSpecialReplicaCnt() && !dp.DecommissionRaftForce {
							if (dp.IsDecommissionRunning() && dp.GetSpecialReplicaDecommissionStep() == SpecialDecommissionWaitAddRes) || dp.IsDecommissionFailed() {
								log.LogDebugf("action[cancelDpNodesetBalance] try delete dp[%v] replica %v",
									dp.PartitionID, dp.DecommissionDstAddr)

								if dp.IsDecommissionRunning() && dp.GetSpecialReplicaDecommissionStep() == SpecialDecommissionWaitAddRes {
									dp.SpecialReplicaDecommissionStop <- false
								}

								// delete it from BadDataPartitionIds
								err = c.removeDPFromBadDataPartitionIDs(dp.DecommissionSrcAddr, dp.DecommissionSrcDiskPath, dp.PartitionID)
								if err != nil {
									log.LogWarnf("action[cancelDpNodesetBalance] dp[%v] delete from bad dataPartitionIDs failed:%v", dp.PartitionID, err)
								}
								removeAddr := dp.DecommissionDstAddr
								// when special replica partition enter SpecialDecommissionWaitAddResFin, new replica is recoverd, so only
								// need to delete DecommissionSrcAddr
								if dp.isSpecialReplicaCnt() && dp.IsDecommissionFailed() && dp.GetSpecialReplicaDecommissionStep() >= SpecialDecommissionWaitAddResFin {
									removeAddr = dp.DecommissionSrcAddr
								}
								err = dp.removeReplicaByForce(c, removeAddr, true, false)
								if err != nil {
									log.LogWarnf("action[cancelDpNodesetBalance] dp[%v] remove decommission dst replica %v failed: %v",
										dp.PartitionID, removeAddr, err)
								}
							} else if dp.IsDecommissionRunning() && dp.GetSpecialReplicaDecommissionStep() >= SpecialDecommissionWaitAddResFin {
								// new replica has been repaired,  let it continue with the subsequent decommission process, skip it this time
								dpWg.Done()
								continue
							}
						} else {
							if dp.IsDecommissionRunning() || dp.IsDecommissionFailed() {
								log.LogDebugf("action[cancelDpNodesetBalance] try delete dp[%v] replica %v",
									dp.PartitionID, dp.DecommissionDstAddr)
								// delete it from BadDataPartitionIds
								err = c.removeDPFromBadDataPartitionIDs(dp.DecommissionSrcAddr, dp.DecommissionSrcDiskPath, dp.PartitionID)
								if err != nil {
									log.LogWarnf("action[cancelDpNodesetBalance] dp[%v] delete from bad dataPartitionIDs failed:%v", dp.PartitionID, err)
								}
								removeAddr := dp.DecommissionDstAddr
								err = dp.removeReplicaByForce(c, removeAddr, true, false)
								if err != nil {
									log.LogWarnf("action[cancelDpNodesetBalance] dp[%v] remove decommission dst replica %v failed: %v",
										dp.PartitionID, removeAddr, err)
								}
							}
						}
						dp.ReleaseDecommissionToken(c)
						dp.ReleaseDecommissionFirstHostToken(c)
					}
				}
				msg := fmt.Sprintf("dp(%v) cancel decommission", dp.decommissionInfo())
				dp.ResetDecommissionStatus()
				dp.setRestoreReplicaStop()
				srcNs, _, err = getTargetNodeset(dp.DecommissionSrcAddr, c)
				if err != nil {
					log.LogWarnf("action[cancelDpNodesetBalance] dp %v find src(%v) nodeset failed:%v",
						dp.PartitionID, dp.DecommissionSrcAddr, err.Error())
					mu.Lock()
					failedDpIds = append(failedDpIds, dp.PartitionID)
					mu.Unlock()
					dpWg.Done()
					continue
				}
				srcNs.decommissionDataPartitionList.Remove(dp)
				c.syncUpdateDataPartition(dp)
				auditlog.LogMasterOp("CancelDpNodesetBalance", msg, nil)
				mu.Lock()
				dpIds = append(dpIds, dp.PartitionID)
				mu.Unlock()
				dpWg.Done()
			}
		}()
	}

	for _, dp := range dps {
		dpWg.Add(1)
		dpCh <- dp
	}
	dpWg.Wait()
	close(dpCh)

	msg := fmt.Sprintf("cluster(%v) cancel dp nodesetBalance len(dps)(%v) with len(faileddps)(%v)", c.Name, len(dpIds), len(failedDpIds))
	auditlog.LogMasterOp("CancelDpNodesetBalance", msg, err)
	return err
}

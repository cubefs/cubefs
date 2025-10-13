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

const (
	// concurrent Balance DP Count (default fallback; actual value comes from cluster config)
	concurrentBalancedDPCountDefault = 400
)

// scheduleToDistributionOptimization registers auto distribution optimization task
func (c *Cluster) scheduleToDistributionOptimization() {
	c.runTask(
		&cTask{
			tickTime: time.Second * time.Duration(c.DistributionOptimizationIntervalSec.Load()),
			name:     "distributionOptimizationController",
			function: func() (fin bool) {
				if c.partition == nil || !c.partition.IsRaftLeader() {
					return
				}
				// Check if distribution optimization is enabled before execution
				if !c.getEnableAutoDistributionOptimization() {
					log.LogDebugf("action[distributionOptimizationController] distribution optimization is disabled, skip execution")
					return
				}
				c.executeDistributionOptimizationMigrations()
				return
			},
		},
	)
}

func (c *Cluster) executeDistributionOptimizationMigrations() {
	begin := time.Now()
	log.LogInfof("action[executeDistributionOptimizationMigrations] starting unified distribution optimization (NodeSet + Rack)")

	defer func() {
		log.LogInfof("action[executeDistributionOptimizationMigrations] migration execution completed in %v", time.Since(begin))
	}()

	activeTasks := c.countActiveDistributionOptimizationTasks()
	limit := c.DistributionOptimizationConcurrentDpCount.Load()
	if limit <= 0 {
		limit = concurrentBalancedDPCountDefault
	}
	if int64(activeTasks) >= limit {
		log.LogInfof("action[executeDistributionOptimizationMigrations] already have %d active tasks, skipping execution", activeTasks)
		return
	}

	dpHost2Ns := c.buildDpHostToNodeSet()
	vols := c.copyVols()
	processedCount := 0
	availableSlots := int(limit) - activeTasks

outerLoop:
	for _, vol := range vols {
		partitions := vol.dataPartitions.clonePartitions()
		for _, dp := range partitions {
			if processedCount >= availableSlots {
				log.LogInfof("action[executeDistributionOptimizationMigrations] reached available slots limit (%d)", availableSlots)
				break outerLoop
			}

			if dp.IsDiscard {
				continue
			}

			if isOptimalDistribution(dp, dpHost2Ns, c) {
				continue
			}

			if dp.DecommissionType == proto.DistributionOptimization && dp.DecommissionStatus != DecommissionFail {
				continue
			}

			c.processPartitionMigration(dp, dp.Hosts)
			processedCount++
		}
	}

	log.LogInfof("action[executeDistributionOptimizationMigrations] completed, processed %d DPs", processedCount)
}

func (c *Cluster) countActiveDistributionOptimizationTasks() int {
	count := 0
	vols := c.copyVols()

	for _, vol := range vols {
		partitions := vol.dataPartitions.clonePartitions()
		for _, dp := range partitions {
			if dp.IsDiscard {
				continue
			}

			if dp.DecommissionType == proto.DistributionOptimization && dp.DecommissionStatus != DecommissionFail {
				count++
			}
		}
	}

	log.LogInfof("action[countActiveDistributionOptimizationTasks] total active tasks: %d", count)
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

// isOptimalDistribution checks if DP has optimal distribution (single NodeSet + no rack conflicts)
func isOptimalDistribution(dp *DataPartition, dpHost2Ns map[string]uint64, c *Cluster) bool {
	_, nodesetBalanced := getDpNodesetDistribution(dp, dpHost2Ns)

	// If not in single NodeSet, it's not optimal
	if !nodesetBalanced {
		return false
	}

	// If rack awareness is disabled, NodeSet balance is sufficient
	if c.getRackAwareLevel() == proto.RackAwareNone {
		return true
	}

	// Check rack distribution within the single NodeSet
	return !hasRackConflict(dp, c)
}

// hasRackConflict checks if DP has rack conflicts within its NodeSet
func hasRackConflict(dp *DataPartition, c *Cluster) bool {
	rackDistribution := make(map[string]int)

	for _, host := range dp.Hosts {
		dataNode, err := c.dataNode(host)
		if err != nil {
			continue
		}
		rackDistribution[dataNode.Rack]++
	}

	// Calculate conflict score
	conflictScore := 0
	for _, count := range rackDistribution {
		if count > 1 {
			conflictScore += count - 1
		}
	}

	// Determine if there's conflict based on rack aware level
	rackLevel := c.getRackAwareLevel()
	switch rackLevel {
	case proto.RackAwareStrong:
		return conflictScore > 0
	case proto.RackAwareWeak:
		return conflictScore > int(dp.ReplicaNum)/3
	default:
		return false
	}
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

	if targetNs, srcAddrs, dstAddrs, err = selectTargetHostsInDistributionOptimization(hosts, int(dp.ReplicaNum), c, dp.MediaType); err != nil {
		log.LogWarnf("action[executeReplicaMigration] dp(%v) select Target hosts failed", dp.PartitionID)
		return
	}

	dp.DecommissionSrcAddrs = srcAddrs
	dp.DecommissionDstAddrs = dstAddrs
	dp.DecommissionDstNodeSet = targetNs.ID
	dp.DecommissionType = proto.DistributionOptimization
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
	// if err := c.markDecommissionDataPartition(dp, lastSrcNode, targetNs.ID, false, proto.DistributionOptimization, 1, srcHosts, targetHosts); err != nil {
	// 	log.LogWarnf("action[executeReplicaMigration] decommission failed for dp(%v) replicas(%v) : %v",
	// 		dp.PartitionID, srcHosts, err)
	// 	return false
	// }
	// if err := dp.MarkDecommissionStatus(srcHosts[lastIndex], targetHosts[0], "", targetNs.ID, false, uint64(time.Now().Unix()), proto.DistributionOptimization, 1, c, srcHosts, targetHosts); err != nil {
	// 	log.LogWarnf("action[executeReplicaMigration] decommission failed for dp(%v) replicas(%v) : %v",
	// 		dp.PartitionID, srcHosts, err)
	// 	return false
	// }

	log.LogInfof("action[executeReplicaMigration] submitted distribution optimization: dp(%v) replicas(%v)",
		dp.PartitionID, dp.Hosts)
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

func (c *Cluster) getDistributionOptimizationStatus() *proto.DistributionOptimizationStatus {
	status := &proto.DistributionOptimizationStatus{
		DecommissioningDPIDs:           make([]uint64, 0),
		ConcurrentDpCount:              c.DistributionOptimizationConcurrentDpCount.Load(),
		BalanceIntervalSec:             c.DistributionOptimizationIntervalSec.Load(),
		BalanceThreshold:               c.DistributionOptimizationThreshold.Load(),
		EnableDistributionOptimization: c.getEnableAutoDistributionOptimization(),
		DomainDistribution: &proto.DomainDistributionInfo{
			SingleDomainDPs: 0,
			TwoDomainDPs:    0,
			ThreeDomainDPs:  0,
		},
		RackDistribution: &proto.RackDistributionInfo{
			NoRackConflictDPs:    0,
			MinorRackConflictDPs: 0,
			MajorRackConflictDPs: 0,
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

			if dp.DecommissionType == proto.DistributionOptimization {
				status.DecommissioningDPIDs = append(status.DecommissioningDPIDs, dp.PartitionID)
			}

			// Analyze NodeSet distribution
			domainCnts, isNodeSetBalanced := getDpNodesetDistribution(dp, dpHost2Ns)
			domainCount := len(domainCnts)

			switch domainCount {
			case 0, 1:
				status.DomainDistribution.SingleDomainDPs++
			case 2:
				status.DomainDistribution.TwoDomainDPs++
			case 3:
				status.DomainDistribution.ThreeDomainDPs++
			}

			// Analyze rack distribution
			noRackConflict, rackConflictLevel := analyzeRackDistribution(dp, c)
			switch rackConflictLevel {
			case 0: // No conflict
				status.RackDistribution.NoRackConflictDPs++
			case 1: // Minor conflict
				status.RackDistribution.MinorRackConflictDPs++
			case 2: // Major conflict
				status.RackDistribution.MajorRackConflictDPs++
			}

			// Count different types of unbalanced DPs
			if !isNodeSetBalanced {
				status.NodeSetUnbalancedDPs++
				status.TotalUnbalancedDPs++
			} else if !noRackConflict {
				// NodeSet is balanced but has rack conflicts
				status.RackConflictDPs++
				status.TotalUnbalancedDPs++
			}
		}
	}

	return status
}

func (c *Cluster) getAllDistributionOptimizationDataPartition() (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.partitions {
			if dp.DecommissionType == proto.DistributionOptimization {
				partitions = append(partitions, dp)
			}
		}
	}
	return
}

// cancelDpNodesetBalance cancels all ongoing nodeset balance decommission tasks
func (c *Cluster) cancelDpDistributionOptimization() (err error) {
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
	dps = c.getAllDistributionOptimizationDataPartition()

	for ii := 0; ii < 10; ii++ {
		go func() {
			for dp := range dpCh {
				if dp.GetDecommissionStatus() == DecommissionSuccess || dp.IsRollbackFailed() || dp.DecommissionType != proto.DistributionOptimization {
					dpWg.Done()
					continue
				}
				if dp.DecommissionDstAddr != "" {
					dstNs, _, err = getTargetNodeset(dp.DecommissionDstAddr, c)
					if err != nil {
						log.LogWarnf("action[cancelDpDistributionOptimization] dp %v find dst(%v) nodeset failed:%v",
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
								log.LogDebugf("action[cancelDpDistributionOptimization] try delete dp[%v] replica %v",
									dp.PartitionID, dp.DecommissionDstAddr)

								if dp.IsDecommissionRunning() && dp.GetSpecialReplicaDecommissionStep() == SpecialDecommissionWaitAddRes {
									dp.SpecialReplicaDecommissionStop <- false
								}

								// delete it from BadDataPartitionIds
								err = c.removeDPFromBadDataPartitionIDs(dp.DecommissionSrcAddr, dp.DecommissionSrcDiskPath, dp.PartitionID)
								if err != nil {
									log.LogWarnf("action[cancelDpDistributionOptimization] dp[%v] delete from bad dataPartitionIDs failed:%v", dp.PartitionID, err)
								}
								removeAddr := dp.DecommissionDstAddr
								// when special replica partition enter SpecialDecommissionWaitAddResFin, new replica is recoverd, so only
								// need to delete DecommissionSrcAddr
								if dp.isSpecialReplicaCnt() && dp.IsDecommissionFailed() && dp.GetSpecialReplicaDecommissionStep() >= SpecialDecommissionWaitAddResFin {
									removeAddr = dp.DecommissionSrcAddr
								}
								err = dp.removeReplicaByForce(c, removeAddr, true, false)
								if err != nil {
									log.LogWarnf("action[cancelDpDistributionOptimization] dp[%v] remove decommission dst replica %v failed: %v",
										dp.PartitionID, removeAddr, err)
								}
							} else if dp.IsDecommissionRunning() && dp.GetSpecialReplicaDecommissionStep() >= SpecialDecommissionWaitAddResFin {
								// new replica has been repaired,  let it continue with the subsequent decommission process, skip it this time
								dpWg.Done()
								continue
							}
						} else {
							if dp.IsDecommissionRunning() || dp.IsDecommissionFailed() {
								log.LogDebugf("action[cancelDpDistributionOptimization] try delete dp[%v] replica %v",
									dp.PartitionID, dp.DecommissionDstAddr)
								// delete it from BadDataPartitionIds
								err = c.removeDPFromBadDataPartitionIDs(dp.DecommissionSrcAddr, dp.DecommissionSrcDiskPath, dp.PartitionID)
								if err != nil {
									log.LogWarnf("action[cancelDpDistributionOptimization] dp[%v] delete from bad dataPartitionIDs failed:%v", dp.PartitionID, err)
								}
								removeAddr := dp.DecommissionDstAddr
								err = dp.removeReplicaByForce(c, removeAddr, true, false)
								if err != nil {
									log.LogWarnf("action[cancelDpDistributionOptimization] dp[%v] remove decommission dst replica %v failed: %v",
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

				// Check if DecommissionSrcAddr is not empty before trying to find the nodeset
				if dp.DecommissionSrcAddr != "" {
					srcNs, _, err = getTargetNodeset(dp.DecommissionSrcAddr, c)
					if err != nil {
						log.LogWarnf("action[cancelDpDistributionOptimization] dp %v find src(%v) nodeset failed:%v",
							dp.PartitionID, dp.DecommissionSrcAddr, err.Error())
						mu.Lock()
						failedDpIds = append(failedDpIds, dp.PartitionID)
						mu.Unlock()
						dpWg.Done()
						continue
					}
					srcNs.decommissionDataPartitionList.Remove(dp)
				} else {
					log.LogWarnf("action[cancelDpDistributionOptimization] dp %v has empty DecommissionSrcAddr, skip nodeset removal",
						dp.PartitionID)
				}

				c.syncUpdateDataPartition(dp)
				auditlog.LogMasterOp("CancelDpDistributionOptimization", msg, nil)
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

	msg := fmt.Sprintf("cluster(%v) cancel dp distributionOptimization len(dps)(%v) with len(faileddps)(%v)", c.Name, len(dpIds), len(failedDpIds))
	auditlog.LogMasterOp("CancelDpNodesetBalance", msg, err)
	return err
}

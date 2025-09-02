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
	// Single migration target: 5000 DPs per day
	singleMigrationTarget = 5000
	// Direct execution batch size limit per cycle
	directExecutionBatchSize = 1000
	// Task execution check interval (4 hours)
	taskExecutionCheckInterval = 4 * time.Hour

	// Resource admission thresholds (configurable)
	// Conservative admission thresholds
	conservativeMinAllocNodes         = 3
	conservativeDpRatioThreshold      = 0.3
	conservativeStorageRatioThreshold = 0.2

	// Normal admission thresholds
	normalMinAllocNodes         = 2
	normalDpRatioThreshold      = 0.2
	normalStorageRatioThreshold = 0.15

	// Target selection scoring weights
	dpRatioWeight      = 0.4
	storageRatioWeight = 0.6

	// Cluster-wide migration thresholds (全集群迁移阈值)
	clusterWideDpRatioThreshold      = 0.5 // 50% DP 可用率才考虑全集群迁移
	clusterWideStorageRatioThreshold = 0.4 // 40% 存储可用率才考虑全集群迁移
)

// scheduleToNodesetBalance registers auto nodeset balance task
func (c *Cluster) scheduleToNodesetBalance() {
	c.runTask(
		&cTask{
			tickTime: taskExecutionCheckInterval, // 4 hours
			name:     "nodesetBalanceController",
			function: func() (fin bool) {
				if c.partition == nil || !c.partition.IsRaftLeader() {
					return
				}
				// Check if nodeset balance is enabled before execution
				if !c.getEnableNodesetBalance() {
					log.LogDebugf("action[nodesetBalanceController] nodeset balance is disabled, skip execution")
					return
				}
				c.executeNodesetBalanceController()
				return
			},
		},
	)
}

func (c *Cluster) executeNodesetBalanceController() {
	begin := time.Now()
	log.LogInfof("action[executeNodesetBalanceController] starting balance controller cycle")

	defer func() {
		log.LogInfof("action[executeNodesetBalanceController] balance controller cycle completed in %v", time.Since(begin))
	}()

	// Check if cluster is fully balanced first
	balanceStatus := c.getNodesetBalanceStatus()
	if balanceStatus.TotalUnbalancedDPs == 0 {
		log.LogDebugf("action[executeNodesetBalanceController] cluster is fully balanced, no action needed")
		return
	}

	c.executeNodesetBalanceMigrations()
}

func (c *Cluster) executeNodesetBalanceMigrations() {
	begin := time.Now()
	log.LogInfof("action[executeNodesetBalanceMigrations] starting nodeset balance migration execution")

	defer func() {
		log.LogInfof("action[executeNodesetBalanceMigrations] migration execution completed in %v", time.Since(begin))
	}()

	dpHost2Ns := c.buildDpHostToNodeSet()
	if len(dpHost2Ns) == 0 {
		log.LogWarnf("action[executeNodesetBalanceMigrations] empty host->nodeset mapping, skip execution")
		return
	}

	vols := c.copyVols()
	processedCount := 0

	for _, vol := range vols {
		partitions := vol.dataPartitions.clonePartitions()
		for _, dp := range partitions {
			if dp.IsDiscard {
				continue
			}

			if dp.DecommissionType == proto.NodesetBalance {
				continue
			}

			c.processPartitionMigration(dp, dp.Hosts, dpHost2Ns)
			processedCount++

			if processedCount >= directExecutionBatchSize {
				log.LogInfof("action[executeNodesetBalanceMigrations] processed %d DPs, stopping", processedCount)
				break
			}
		}
	}

	log.LogInfof("action[executeNodesetBalanceMigrations] completed, processed %d DPs", processedCount)
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

func (c *Cluster) processPartitionMigration(dp *DataPartition, srcHosts []string, dpHost2Ns map[string]uint64) {
	if len(srcHosts) == 0 {
		log.LogDebugf("action[processPartitionMigration] dp(%v) already balanced", dp.PartitionID)
		return
	}

	c.executeReplicaMigration(dp, srcHosts)
}

func (c *Cluster) executeReplicaMigration(dp *DataPartition, srcHosts []string) bool {
	if dp.DecommissionStatus == markDecommission ||
		dp.DecommissionStatus == DecommissionRunning ||
		dp.DecommissionStatus == DecommissionPrepare {
		return true
	}

	if err := c.markDecommissionDataPartition(dp, nil, 0, false, proto.NodesetBalance, 1, srcHosts); err != nil {
		log.LogWarnf("action[executeReplicaMigration] batch decommission failed for dp(%v) replicas(%v) : %v",
			dp.PartitionID, srcHosts, err)
		return false
	}

	log.LogInfof("action[executeReplicaMigration] submitted batch decommission: dp(%v) replicas(%v)",
		dp.PartitionID, srcHosts)
	return true
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

// NodesetBalanceStatus represents the status of nodeset balance operation
type NodesetBalanceStatus struct {
	TotalUnbalancedDPs   int                     `json:"total_unbalanced_dps"`   // 总未均衡DP数量
	DecommissioningDPIDs []uint64                `json:"decommissioning_dp_ids"` // 正在下线的DP ID列表
	LastBalanceTime      int64                   `json:"last_balance_time"`      // 上次均衡时间戳
	SingleMigrationLimit int                     `json:"single_migration_limit"` // 单轮迁移限制
	EnableNodesetBalance bool                    `json:"enable_nodeset_balance"` // 是否启用nodeset均衡
	DomainDistribution   *DomainDistributionInfo `json:"domain_distribution"`    // 域分布统计
}

// DomainDistributionInfo represents the distribution of DPs across different numbers of domains
type DomainDistributionInfo struct {
	SingleDomainDPs int `json:"single_domain_dps"`
	TwoDomainDPs    int `json:"two_domain_dps"`
	ThreeDomainDPs  int `json:"three_domain_dps"`
}

func (c *Cluster) getNodesetBalanceStatus() *NodesetBalanceStatus {
	status := &NodesetBalanceStatus{
		DecommissioningDPIDs: make([]uint64, 0),
		SingleMigrationLimit: singleMigrationTarget,
		EnableNodesetBalance: c.getEnableNodesetBalance(),
		DomainDistribution: &DomainDistributionInfo{
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

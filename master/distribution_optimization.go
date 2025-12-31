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
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

// scheduleToDistributionOptimization registers auto distribution optimization task
func (c *Cluster) scheduleToDistributionOptimization() {
	task := &cTask{
		tickTime: time.Second * time.Duration(defaultDistributionOptimizationIntervalSec),
		name:     "distributionOptimizationController",
	}
	task.function = func() (fin bool) {
		if c.partition == nil || !c.partition.IsRaftLeader() {
			return
		}
		// Check if distribution optimization is enabled before execution
		if !c.getEnableDistributionOptimization() {
			log.LogDebugf("action[distributionOptimizationController] distribution optimization is disabled, skip execution")
			return
		}
		_ = c.executeDistributionOptimizationMigrations()
		return
	}
	c.runTask(task)
}

func (c *Cluster) executeDistributionOptimizationMigrations() int {
	begin := time.Now()
	processedCount := 0
	log.LogWarnf("action[executeDistributionOptimizationMigrations] starting unified distribution optimization (NodeSet + Rack)")

	defer func() {
		duration := time.Since(begin)
		log.LogWarnf("action[executeDistributionOptimizationMigrations] migration execution completed in %v, processed %d DPs", duration, processedCount)
	}()

	activeTasks := c.countActiveDistributionOptimizationTasks()
	limit := c.DistributionOptimizationConDpCnt.Load()
	if int64(activeTasks) >= limit {
		log.LogWarnf("action[executeDistributionOptimizationMigrations] already have %d active tasks, skipping execution", activeTasks)
		return processedCount
	}

	abnormalDpSet := c.getAbnormalDps(true)

	vols := c.copyVols()
	availableSlots := int(limit) - activeTasks

	for _, vol := range vols {
		partitions := vol.dataPartitions.clonePartitions()
		for _, dp := range partitions {
			if processedCount >= availableSlots {
				log.LogWarnf("action[executeDistributionOptimizationMigrations] reached available slots limit (%d)", availableSlots)
				return processedCount
			}

			if dp.IsDiscard {
				continue
			}

			// Skip abnormal DPs detected by checkReplicaOfDataPartitions
			if _, exists := abnormalDpSet[dp.PartitionID]; exists {
				continue
			}

			if dp.isPerformingDecommission(c) {
				continue
			}

			dp.RLock()
			hosts, isOptimal := isOptimalDistribution(dp, c)
			dp.RUnlock()
			if isOptimal {
				continue
			}

			err := c.processPartitionDistributionOptimization(dp, hosts)
			if err != nil {
				log.LogWarnf("action[executeDistributionOptimizationMigrations] process partition migration failed: %v", err)
				continue
			}
			processedCount++
		}
	}
	return processedCount
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

			if dp.DecommissionType == proto.DistributionOptimization && dp.isPerformingDecommission(c) {
				count++
			}
		}
	}

	log.LogInfof("action[countActiveDistributionOptimizationTasks] total active tasks: %d", count)
	return count
}

func getDpNodesetDistribution(dp *DataPartition) (map[string]map[uint64]int, bool) {
	zoneNsDistribution := make(map[string]map[uint64]int)

	for _, replica := range dp.Replicas {
		dataNode := replica.getReplicaNode()
		if dataNode == nil {
			continue
		}
		zone := dataNode.ZoneName
		nsID := dataNode.NodeSetID
		if zoneNsDistribution[zone] == nil {
			zoneNsDistribution[zone] = make(map[uint64]int)
		}
		zoneNsDistribution[zone][nsID]++
	}

	isBalanced := true
	for _, nsCnts := range zoneNsDistribution {
		if len(nsCnts) > 1 {
			isBalanced = false
			break
		}
	}

	return zoneNsDistribution, isBalanced
}

// isOptimalDistribution checks if DP has optimal distribution (single NodeSet + no rack conflicts)
func isOptimalDistribution(dp *DataPartition, c *Cluster) ([]string, bool) {
	zoneNsDistribution, nodesetBalanced := getDpNodesetDistribution(dp)

	// If not in single NodeSet, it's not optimal
	if !nodesetBalanced {
		for zone, nsCnts := range zoneNsDistribution {
			if len(nsCnts) > 1 {
				var zoneHosts []string
				for _, replica := range dp.Replicas {
					dataNode := replica.getReplicaNode()
					if dataNode == nil {
						continue
					}
					if dataNode.ZoneName == zone {
						zoneHosts = append(zoneHosts, replica.Addr)
					}
				}
				return zoneHosts, false
			}
		}
	}

	// If rack awareness is disabled, NodeSet balance is sufficient
	if c.getRackAwareLevel() == proto.RackAwareNone {
		return nil, true
	}

	if isConflict, zone := hasRackConflict(dp); isConflict {
		var zoneHosts []string
		for _, replica := range dp.Replicas {
			dataNode := replica.getReplicaNode()
			if dataNode == nil {
				continue
			}
			if dataNode.ZoneName == zone {
				zoneHosts = append(zoneHosts, replica.Addr)
			}
		}
		return zoneHosts, false
	}

	return nil, true
}

// hasRackConflict checks if there are any rack conflicts in the data partition
// Returns true if any rack contains multiple replicas, regardless of rack aware level
func hasRackConflict(dp *DataPartition) (bool, string) {
	if len(dp.Replicas) == 0 {
		return false, ""
	}

	// Group hosts by zone and then by rack
	zoneRackHosts := make(map[string]map[string][]string)

	for _, replica := range dp.Replicas {
		dataNode := replica.getReplicaNode()
		if dataNode == nil {
			continue
		}
		zone := dataNode.ZoneName
		rack := dataNode.Rack

		if _, exists := zoneRackHosts[zone]; !exists {
			zoneRackHosts[zone] = make(map[string][]string)
		}

		zoneRackHosts[zone][rack] = append(zoneRackHosts[zone][rack], replica.Addr)
	}

	// Check each zone for rack conflicts
	for zone, rackHosts := range zoneRackHosts {
		for _, hosts := range rackHosts {
			if len(hosts) > 1 {
				// Found a rack with multiple hosts in this zone
				return true, zone
			}
		}
	}

	return false, ""
}

func (c *Cluster) processPartitionDistributionOptimization(dp *DataPartition, hosts []string) error {
	if len(hosts) == 0 {
		return fmt.Errorf("hosts is empty")
	}

	var (
		srcAddrs []string
		dstAddrs []string
		targetNs *nodeSet
		err      error
	)

	if targetNs, srcAddrs, dstAddrs, err = selectTargetHostsInDistributionOptimization(hosts, len(hosts), c); err != nil {
		log.LogWarnf("action[executeReplicaMigration] dp(%v) select Target hosts failed", dp.PartitionID)
		return err
	}

	if len(srcAddrs) == 0 || len(dstAddrs) == 0 {
		return fmt.Errorf("srcAddrs or dstAddrs is empty, no migration needed")
	}

	if dp.isPerformingDecommission(c) {
		return fmt.Errorf("dp is decommissing")
	}

	dp.DecommissionSrcAddrs = srcAddrs
	dp.DecommissionDstAddrs = dstAddrs
	dp.DecommissionDstNodeSet = targetNs.ID
	dp.DecommissionType = proto.DistributionOptimization
	dp.DecommissionWeight = 1

	auditlog.LogMasterOp("DistributionOptimization", fmt.Sprintf("dp %v srcAddrs %v dstAddrs %v", dp.PartitionID, dp.DecommissionSrcAddrs, dp.DecommissionDstAddrs), nil)

	if err = c.addDataReservedResource(dstAddrs, dp); err != nil {
		log.LogWarnf("action[executeReplicaMigration] dp %v simulate resource change failed: %v", dp.PartitionID, err)
		return err
	}

	if !dp.ProcessNextDecommissionSrcHost(c) {
		log.LogWarnf("action[executeReplicaMigration] submitted decommission: dp(%v) replicas(%v) failed",
			dp.PartitionID, dp.Hosts)
		c.releaseDataReservedResource(dstAddrs, dp)
		return fmt.Errorf("submitted decommission: dp(%v) replicas(%v) failed", dp.PartitionID, dp.Hosts)
	}

	log.LogInfof("action[executeReplicaMigration] submitted distribution optimization: dp(%v) replicas(%v)",
		dp.PartitionID, dp.Hosts)
	return nil
}

func (c *Cluster) getDistributionOptimizationStatus() *proto.DistributionOptimizationStatus {
	status := &proto.DistributionOptimizationStatus{
		DecommissioningDPIDs:           make([]uint64, 0),
		ConcurrentDpCount:              c.DistributionOptimizationConDpCnt.Load(),
		BalanceIntervalSec:             defaultDistributionOptimizationIntervalSec,
		BalanceThreshold:               getDistributionOptimizationThreshold(),
		EnableDistributionOptimization: c.getEnableDistributionOptimization(),
		SSDStats: &proto.MediaTypeDistributionStats{
			DomainDistribution: &proto.DomainDistributionInfo{},
			RackDistribution:   &proto.RackDistributionInfo{},
		},
		HDDStats: &proto.MediaTypeDistributionStats{
			DomainDistribution: &proto.DomainDistributionInfo{},
			RackDistribution:   &proto.RackDistributionInfo{},
		},
	}

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

			// Determine MediaType for this DP
			mediaType := dp.MediaType
			var mediaStats *proto.MediaTypeDistributionStats
			if mediaType == proto.MediaType_HDD {
				mediaStats = status.HDDStats
			} else if mediaType == proto.MediaType_SSD {
				mediaStats = status.SSDStats
			} else {
				// For unspecified or other types, skip media-specific stats
				continue
			}

			// Analyze NodeSet distribution
			zoneNsDistribution, isNodeSetBalanced := getDpNodesetDistribution(dp)
			zoneCount := len(zoneNsDistribution)
			if zoneCount > 1 {
				mediaStats.CrossZoneDPs++
			}

			// Statistics of nodeset distribution - each DP is counted only once.
			maxNodesetCount := 0
			for _, nsCnts := range zoneNsDistribution {
				nodesetCount := len(nsCnts)
				if nodesetCount > maxNodesetCount {
					maxNodesetCount = nodesetCount
				}
			}

			switch maxNodesetCount {
			case 0, 1:
				mediaStats.DomainDistribution.SingleDomainDPs++
			case 2:
				mediaStats.DomainDistribution.TwoDomainDPs++
			case 3:
				mediaStats.DomainDistribution.ThreeDomainDPs++
			}

			// Analyze rack distribution
			isConflict, conflictZone := hasRackConflict(dp)
			var rackConflictLevel int

			if isConflict {
				var zoneHosts []string
				for _, replica := range dp.Replicas {
					dataNode := replica.getReplicaNode()
					if dataNode == nil {
						continue
					}
					if dataNode.ZoneName == conflictZone {
						zoneHosts = append(zoneHosts, replica.Addr)
					}
				}
				rackConflictLevel = getRackConflictLevel(zoneHosts, c)

				// Use conflict levels for classification
				switch rackConflictLevel {
				case 1:
					mediaStats.RackDistribution.MinorRackConflictDPs++
				case 2:
					mediaStats.RackDistribution.MajorRackConflictDPs++
				}
			} else {
				mediaStats.RackDistribution.NoRackConflictDPs++
			}

			// Count different types of unbalanced DPs
			isUnbalanced := false
			if !isNodeSetBalanced {
				mediaStats.NodeSetUnbalancedDPs++
				isUnbalanced = true
			}
			if isConflict {
				mediaStats.RackConflictDPs++
				isUnbalanced = true
			}
			if isUnbalanced {
				mediaStats.TotalUnbalancedDPs++
			}
		}
	}

	return status
}

func (c *Cluster) getAllDistributionOptimizationDataPartition() (dps []*DataPartition) {
	dps = make([]*DataPartition, 0)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		partitions := vol.dataPartitions.clonePartitions()
		for _, dp := range partitions {
			if dp.DecommissionType == proto.DistributionOptimization {
				dps = append(partitions, dp)
			}
		}
	}
	return
}

// cancelDpDistributionOptimization cancels all ongoing nodeset balance decommission tasks
func (c *Cluster) cancelDpDistributionOptimization() (err error) {
	begin := time.Now()
	defer func() {
		log.LogInfof("action[cancelDpDistributionOptimization] cancel dp DistributionOptimization using time(%v)", time.Since(begin))
	}()

	dps := c.getAllDistributionOptimizationDataPartition()
	success, failed := c.cancelDecommissionWorker(dps, nil, "cancelDpDistributionOptimization")

	msg := fmt.Sprintf("cluster(%v) cancel dp distributionOptimization len(dps)(%v) with len(faileddps)(%v)",
		c.Name, len(success), len(failed))
	auditlog.LogMasterOp("CancelDpDistributionOptimization", msg, nil)
	return nil
}

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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// GetMetaPartitionDecommissionCount returns the current decommission count for the given type
// by counting MPs in BadMetaPartitionIds with the specified DecommissionType
func (c *Cluster) GetMetaPartitionDecommissionCount(decommissionType uint32) uint32 {
	var count uint32 = 0

	c.badPartitionMutex.RLock()
	defer c.badPartitionMutex.RUnlock()

	c.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badMetaPartitionIds := value.([]uint64)
		for _, partitionID := range badMetaPartitionIds {
			partition, err := c.getMetaPartitionByID(partitionID)
			if err != nil {
				continue
			}

			partition.RLock()
			mpDecommissionType := partition.DecommissionType
			partition.RUnlock()

			if mpDecommissionType == decommissionType {
				count++
			}
		}
		return true
	})

	c.RecoverMetaPartitionIds.Range(func(key, value interface{}) bool {
		recoverMetaPartitionId := key.(uint64)
		partition, err := c.getMetaPartitionByID(recoverMetaPartitionId)
		if err != nil {
			return true
		}

		partition.RLock()
		defer partition.RUnlock()
		for _, learner := range partition.RecoverLearners {
			if learner.DecommissionType == decommissionType {
				count++
				break
			}
		}
		return true
	})

	return count
}

// CheckMPDecommissionLimit checks if the decommission limit is reached for the given type
func (c *Cluster) CheckMPDecommissionLimit(decommissionType uint32) error {
	currentCount := c.GetMetaPartitionDecommissionCount(decommissionType)
	limit := c.GetMetaPartitionDecommissionLimit(decommissionType)

	if limit == 0 {
		// No limit set, allow operation
		return nil
	}

	if currentCount >= limit {
		return fmt.Errorf("meta partition decommission limit reached for type %s: current=%d, limit=%d",
			GetMetaPartitionDecommissionTypeName(decommissionType), currentCount, limit)
	}

	return nil
}

// SetMetaPartitionDecommissionLimit sets the decommission limit for the given type
func (c *Cluster) SetMetaPartitionDecommissionLimit(decommissionType uint32, limit uint32) error {
	switch decommissionType {
	case proto.AutoAddReplica:
		c.MetaAutoAddReplicaLimit.Store(limit)
		log.LogInfof("action[SetMetaPartitionDecommissionLimit] AutoAddReplica limit set to: %d", limit)
	case proto.ManualDecommission:
		c.MetaManualDecommissionLimit.Store(limit)
		log.LogInfof("action[SetMetaPartitionDecommissionLimit] ManualDecommission limit set to: %d", limit)
	case proto.MpBalance:
		c.MetaBalanceLimit.Store(limit)
		log.LogInfof("action[SetMetaPartitionDecommissionLimit] MpBalance limit set to: %d", limit)
	case proto.ManualAddReplica:
		c.MetaManualAddReplicaLimit.Store(limit)
		log.LogInfof("action[SetMetaPartitionDecommissionLimit] ManualAddReplica limit set to: %d", limit)
	case proto.MpManumalLearner:
		c.MetaManualLearnerLimit.Store(limit)
		log.LogInfof("action[SetMetaPartitionDecommissionLimit] MpManumalLearner limit set to: %d", limit)
	default:
		return fmt.Errorf("unknown meta partition decommission type: %d", decommissionType)
	}

	if err := c.syncPutCluster(); err != nil {
		return fmt.Errorf("action[SetMetaPartitionDecommissionLimit] failed to sync put cluster, err: %v", err)
	}
	return nil
}

// GetMetaPartitionDecommissionLimit returns the decommission limit for the given type
func (c *Cluster) GetMetaPartitionDecommissionLimit(decommissionType uint32) uint32 {
	switch decommissionType {
	case proto.AutoAddReplica:
		return c.MetaAutoAddReplicaLimit.Load()
	case proto.ManualDecommission:
		return c.MetaManualDecommissionLimit.Load()
	case proto.MpBalance:
		return c.MetaBalanceLimit.Load()
	case proto.ManualAddReplica:
		return c.MetaManualAddReplicaLimit.Load()
	case proto.MpManumalLearner:
		return c.MetaManualLearnerLimit.Load()
	default:
		return 0
	}
}

// GetMetaPartitionDecommissionTypeName returns the name of the decommission type
func GetMetaPartitionDecommissionTypeName(decommissionType uint32) string {
	switch decommissionType {
	case proto.AutoAddReplica:
		return "AutoAddReplica"
	case proto.ManualDecommission:
		return "ManualDecommission"
	case proto.MpBalance:
		return "MpBalance"
	case proto.ManualAddReplica:
		return "ManualAddReplica"
	case proto.MpManumalLearner:
		return "MpManumalLearner"
	default:
		return fmt.Sprintf("Unknown(%d)", decommissionType)
	}
}

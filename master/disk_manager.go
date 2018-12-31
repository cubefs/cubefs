// Copyright 2018 The CFS Authors.
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
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"math"
	"time"
)

// TODO start recovering bad disks?
// 检查disk下线的进度
func (c *Cluster) startCheckBadDiskRecovery() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsLeader() {
				if c.vols != nil {
					c.checkBadDiskRecovery()
				}
			}
			time.Sleep(time.Second * defaultCheckDataPartitionIntervalSeconds)
		}
	}()
}


// TODO recovery bad disk?
func (c *Cluster) checkBadDiskRecovery() {
	var minus float64
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		newBadDpIds := make([]uint64, 0)
		for _, partitionID := range badDataPartitionIds {
			partition, err := c.getDataPartitionByID(partitionID)
			if err != nil {
				continue
			}
			vol, err := c.getVol(partition.VolName)
			if err != nil {
				continue
			}
			if len(partition.Replicas) == 0 || len(partition.Replicas) < int(vol.dpReplicaNum) {
				continue
			}
			used := partition.Replicas[0].Used
			for _, replica := range partition.Replicas {
				// TODO same problem here
				if math.Abs(float64(replica.Used)-float64(used)) > minus {
					minus = math.Abs(float64(replica.Used) - float64(used))
				}
			}

			// TODO what does minus mean here?
			if minus < util.GB {
				Warn(c.Name, fmt.Sprintf("clusterID[%v],partitionID[%v] has recovered success", c.Name, partitionID))
				c.BadDataPartitionIds.Delete(key)
			} else {
				newBadDpIds = append(newBadDpIds, partitionID)
			}
		}

		if len(newBadDpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("clusterID[%v],node:disk[%v] has recovered success", c.Name, key))
		} else {
			c.BadDataPartitionIds.Store(key, newBadDpIds)
		}

		return true
	})
}


// TODO take the disk off?
// decompression hadoop 有一个专有名字
// takeDiskOff
func (c *Cluster) diskOffLine(dataNode *DataNode, badDiskPath string, badPartitionIds []uint64) (err error) {
	msg := fmt.Sprintf("action[diskOffLine], Node[%v] OffLine,disk[%v]", dataNode.Addr, badDiskPath)
	log.LogWarn(msg)

	// TODO what are safeVols and what are normalVols?  safeVols -> vols
	safeVols := c.getAllNormalVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.dataPartitions {
			for _, bad := range badPartitionIds {
				if bad == dp.PartitionID {
					if err = c.dataPartitionOffline(dataNode.Addr, vol.Name, dp, diskOfflineInfo); err != nil {
						return
					}
				}
			}
		}
	}
	msg = fmt.Sprintf("action[diskOffLine],clusterID[%v] Node[%v] OffLine success",
		c.Name, dataNode.Addr)
	Warn(c.Name, msg)
	return
}

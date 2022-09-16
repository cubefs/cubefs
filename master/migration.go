package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"time"
)

func (c *Cluster) checkMigratedDataPartitionsRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMigratedDataPartitionsRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMigratedDataPartitionsRecoveryProgress occurred panic")
		}
	}()
	unrecoverPartitionIDs := make(map[uint64]int64, 0)
	c.MigratedDataPartitionIds.Range(func(key, value interface{}) bool {
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
			if partition.isDataCatchUpInStrictMode() && partition.allReplicaHasRecovered() {
				partition.RLock()
				if partition.isRecover {
					partition.isRecover = false
					c.syncUpdateDataPartition(partition)
				}
				partition.RUnlock()
			} else {
				newBadDpIds = append(newBadDpIds, partitionID)
				if time.Now().Unix()-partition.modifyTime > defaultUnrecoverableDuration {
					unrecoverPartitionIDs[partitionID] = partition.modifyTime
				}
			}
		}

		if len(newBadDpIds) == 0 {
			c.MigratedDataPartitionIds.Delete(key)
		} else {
			c.MigratedDataPartitionIds.Store(key, newBadDpIds)
		}

		return true
	})
	if len(unrecoverPartitionIDs) != 0 {
		Warn(c.Name, fmt.Sprintf("action[checkMigratedDpRecoveryProgress] clusterID[%v],has[%v] has migrated more than 24 hours,still not recovered,ids[%v]", c.Name, len(unrecoverPartitionIDs), unrecoverPartitionIDs))
	}
}

func (c *Cluster) putMigratedDataPartitionIDs(replica *DataReplica, addr string, partitionID uint64) {
	var key string
	newMigratedPartitionIDs := make([]uint64, 0)
	if replica != nil {
		key = fmt.Sprintf("%s:%s", addr, replica.DiskPath)
	} else {
		key = fmt.Sprintf("%s:%s", addr, "")
	}
	migratedPartitionIDs, ok := c.MigratedDataPartitionIds.Load(key)
	if ok {
		newMigratedPartitionIDs = migratedPartitionIDs.([]uint64)
	}
	newMigratedPartitionIDs = append(newMigratedPartitionIDs, partitionID)
	c.MigratedDataPartitionIds.Store(key, newMigratedPartitionIDs)
}

func (c *Cluster) putMigratedMetaPartitions(addr string, partitionID uint64) {
	newMigratedPartitionIDs := make([]uint64, 0)
	migratedPartitionIDs, ok := c.MigratedMetaPartitionIds.Load(addr)
	if ok {
		newMigratedPartitionIDs = migratedPartitionIDs.([]uint64)
	}
	newMigratedPartitionIDs = append(newMigratedPartitionIDs, partitionID)
	c.MigratedMetaPartitionIds.Store(addr, newMigratedPartitionIDs)
}

func (c *Cluster) checkMigratedMetaPartitionRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMigratedMetaPartitionRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMigratedMetaPartitionRecoveryProgress occurred panic")
		}
	}()

	c.MigratedMetaPartitionIds.Range(func(key, value interface{}) bool {
		badMetaPartitionIds := value.([]uint64)
		for _, partitionID := range badMetaPartitionIds {
			partition, err := c.getMetaPartitionByID(partitionID)
			if err != nil {
				continue
			}
			c.doLoadMetaPartition(partition)
		}
		return true
	})

	var (
		dentryDiff  float64
		applyIDDiff float64
	)
	unrecoverMpIDs := make(map[uint64]int64, 0)
	c.MigratedMetaPartitionIds.Range(func(key, value interface{}) bool {
		badMetaPartitionIds := value.([]uint64)
		newBadMpIds := make([]uint64, 0)
		for _, partitionID := range badMetaPartitionIds {
			partition, err := c.getMetaPartitionByID(partitionID)
			if err != nil {
				continue
			}
			vol, err := c.getVol(partition.volName)
			if err != nil {
				continue
			}
			if len(partition.Replicas) == 0 || len(partition.Replicas) < int(vol.mpReplicaNum) {
				continue
			}
			dentryDiff = partition.getMinusOfDentryCount()
			//inodeDiff = partition.getMinusOfInodeCount()
			//inodeDiff = partition.getPercentMinusOfInodeCount()
			applyIDDiff = partition.getMinusOfApplyID()
			if dentryDiff == 0 && applyIDDiff == 0 {
				partition.RLock()
				partition.IsRecover = false
				c.syncUpdateMetaPartition(partition)
				partition.RUnlock()
			} else {
				newBadMpIds = append(newBadMpIds, partitionID)
				if time.Now().Unix()-partition.modifyTime > defaultUnrecoverableDuration {
					unrecoverMpIDs[partitionID] = partition.modifyTime
				}
			}
		}

		if len(newBadMpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("action[checkMigratedMpRecoveryProgress] clusterID[%v],node[%v] has recovered success", c.Name, key))
			c.MigratedMetaPartitionIds.Delete(key)
		} else {
			c.MigratedMetaPartitionIds.Store(key, newBadMpIds)
		}

		return true
	})
	if len(unrecoverMpIDs) != 0 {
		Warn(c.Name, fmt.Sprintf("action[checkMetaPartitionRecoveryProgress] clusterID[%v],[%v] has migrated more than 24 hours,still not recovered,ids[%v]", c.Name, len(unrecoverMpIDs), unrecoverMpIDs))
	}
}

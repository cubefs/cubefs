package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util"
)

func (c *Cluster) checkMigratedDataPartitionsRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMigratedDataPartitionsRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMigratedDataPartitionsRecoveryProgress occurred panic")
		}
	}()

	var (
		fileCountDiff float64
		usedSizeDiff  float64
	)
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
			usedSizeDiff = partition.getMinus()
			fileCountDiff = partition.getMinusOfFileCount()
			if usedSizeDiff < util.GB && fileCountDiff == 0 {
				partition.isRecover = false
				partition.RLock()
				c.syncUpdateDataPartition(partition)
				partition.RUnlock()
			} else {
				newBadDpIds = append(newBadDpIds, partitionID)
			}
		}

		if len(newBadDpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("clusterID[%v],node:disk[%v] has recovered success", c.Name, key))
			c.MigratedDataPartitionIds.Delete(key)
		} else {
			c.MigratedDataPartitionIds.Store(key, newBadDpIds)
		}

		return true
	})
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

	var (
		dentryDiff float64
		inodeDiff  float64
	)
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
			inodeDiff = partition.getMinusOfInodeCount()
			if dentryDiff == 0 && inodeDiff == 0 {
				partition.IsRecover = false
				partition.RLock()
				c.syncUpdateMetaPartition(partition)
				partition.RUnlock()
			} else {
				newBadMpIds = append(newBadMpIds, partitionID)
			}
		}

		if len(newBadMpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("clusterID[%v],node[%v] has recovered success", c.Name, key))
			c.MigratedMetaPartitionIds.Delete(key)
		} else {
			c.MigratedMetaPartitionIds.Store(key, newBadMpIds)
		}

		return true
	})
}

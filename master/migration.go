package master

import (
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"strings"
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
		partitionID := value.(uint64)
		partition, err := c.getDataPartitionByID(partitionID)
		if err != nil {
			return true
		}
		vol, err := c.getVol(partition.VolName)
		if err != nil {
			return true
		}
		if len(partition.Replicas) == 0 || len(partition.Replicas) < int(vol.dpReplicaNum) {
			return true
		}
		if partition.isDataCatchUpInStrictMode() && partition.allReplicaHasRecovered() {
			partition.RLock()
			if partition.isRecover {
				partition.isRecover = false
				c.syncUpdateDataPartition(partition)
			}
			partition.RUnlock()
			c.MigratedDataPartitionIds.Delete(key)
		} else {
			if time.Now().Unix()-partition.modifyTime > defaultUnrecoverableDuration {
				unrecoverPartitionIDs[partitionID] = partition.modifyTime
			}
		}

		return true
	})
	if len(unrecoverPartitionIDs) != 0 {
		Warn(c.Name, fmt.Sprintf("action[checkMigratedDpRecoveryProgress] clusterID[%v],has[%v] has migrated more than 24 hours,still not recovered,ids[%v]", c.Name, len(unrecoverPartitionIDs), unrecoverPartitionIDs))
	}
}

func (c *Cluster) putMigratedDataPartitionIDs(replica *DataReplica, addr string, partitionID uint64) {
	var diskPath string
	if replica != nil {
		diskPath = replica.DiskPath
	}
	key := decommissionDataPartitionKey(addr, diskPath, partitionID)
	c.MigratedDataPartitionIds.Store(key, partitionID)
}

func decommissionDataPartitionKey(addr, diskPath string, partitionID uint64) string {
	return fmt.Sprintf("%s%v%s%v%v", addr, keySeparator, diskPath, keySeparator, partitionID)
}
func getAddrFromDecommissionDataPartitionKey(key string) string {
	return strings.Split(key, keySeparator)[0]
}

func (c *Cluster) putMigratedMetaPartitions(addr string, partitionID uint64) {
	key := decommissionMetaPartitionKey(addr, partitionID)
	c.MigratedMetaPartitionIds.Store(key, partitionID)
}

func decommissionMetaPartitionKey(addr string, partitionID uint64) string {
	return fmt.Sprintf("%v%v%v", addr, keySeparator, partitionID)
}
func getAddrFromDecommissionMetaPartitionKey(key string) string {
	return strings.Split(key, keySeparator)[0]
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
		partitionID := value.(uint64)
		partition, err := c.getMetaPartitionByID(partitionID)
		if err != nil {
			return true
		}
		c.doLoadMetaPartition(partition)
		return true
	})

	var (
		dentryDiff  float64
		applyIDDiff float64
	)
	unrecoverMpIDs := make(map[uint64]int64, 0)
	c.MigratedMetaPartitionIds.Range(func(key, value interface{}) bool {
		partitionID := value.(uint64)
		partition, err := c.getMetaPartitionByID(partitionID)
		if err != nil {
			return true
		}
		vol, err := c.getVol(partition.volName)
		if err != nil {
			return true
		}
		if len(partition.Replicas) == 0 || len(partition.Replicas) < int(vol.mpReplicaNum) {
			return true
		}
		dentryDiff = partition.getMinusOfDentryCount()
		//inodeDiff = partition.getMinusOfInodeCount()
		//inodeDiff = partition.getPercentMinusOfInodeCount()
		applyIDDiff = partition.getMinusOfApplyID()
		if dentryDiff == 0 && applyIDDiff == 0 && partition.allReplicaHasRecovered() {
			partition.RLock()
			partition.IsRecover = false
			c.syncUpdateMetaPartition(partition)
			partition.RUnlock()
			c.MigratedMetaPartitionIds.Delete(key)
		} else {
			if time.Now().Unix()-partition.modifyTime > defaultUnrecoverableDuration {
				unrecoverMpIDs[partitionID] = partition.modifyTime
			}
		}

		return true
	})
	if len(unrecoverMpIDs) != 0 {
		Warn(c.Name, fmt.Sprintf("action[checkMetaPartitionRecoveryProgress] clusterID[%v],[%v] has migrated more than 24 hours,still not recovered,ids[%v]", c.Name, len(unrecoverMpIDs), unrecoverMpIDs))
	}
}

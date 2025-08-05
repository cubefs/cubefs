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
	"strconv"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

func (partition *DataPartition) checkStatus(clusterName string, needLog bool, dpTimeOutSec int64, c *Cluster,
	shouldDpInhibitWriteByVolFull bool, forbiddenVol bool,
) {
	partition.Lock()
	defer partition.Unlock()
	var liveReplicas []*DataReplica

	if proto.IsNormalDp(partition.PartitionType) {
		liveReplicas = partition.getLiveReplicasFromHosts(dpTimeOutSec)
		if len(partition.Replicas) > len(partition.Hosts) {
			partition.Status = proto.ReadOnly
			msg := fmt.Sprintf("action[extractStatus],partitionID:%v has exceed repica, replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
				partition.PartitionID, partition.ReplicaNum, len(liveReplicas), partition.Status, partition.Hosts)
			Warn(clusterName, msg)
			return
		}
	} else {
		liveReplicas = partition.getLiveReplicas(dpTimeOutSec)
	}

	switch len(liveReplicas) {
	case (int)(partition.ReplicaNum):
		partition.Status = proto.ReadOnly
		if partition.checkReplicaEqualStatus(liveReplicas, proto.ReadWrite) &&
			partition.hasEnoughAvailableSpace() &&
			!shouldDpInhibitWriteByVolFull {

			writable := false
			if proto.IsNormalDp(partition.PartitionType) {
				if partition.getLeaderAddr() != "" {
					writable = true
				}
			} else {
				// cold volume has no leader
				writable = true
			}
			// if the volume is not forbidden
			// set status to ReadWrite
			if writable && !forbiddenVol {
				partition.Status = proto.ReadWrite
			}
		}
	default:
		partition.Status = proto.ReadOnly
	}
	// keep readonly if special replica is still decommission
	if partition.isSpecialReplicaCnt() && partition.GetSpecialReplicaDecommissionStep() > 0 {
		log.LogInfof("action[checkStatus] partition %v with Special replica cnt %v on decommison status %v, live replicacnt %v",
			partition.PartitionID, partition.ReplicaNum, partition.Status, len(liveReplicas))
		partition.Status = proto.ReadOnly
	}

	if partition.checkReplicaEqualStatus(liveReplicas, proto.Unavailable) {
		log.LogWarnf("action[checkStatus] partition %v bet set Unavailable", partition.PartitionID)
		partition.Status = proto.Unavailable
	}

	if needLog && len(liveReplicas) != int(partition.ReplicaNum) {
		msg := fmt.Sprintf("action[extractStatus],partitionID:%v  replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			partition.PartitionID, partition.ReplicaNum, len(liveReplicas), partition.Status, partition.Hosts)
		log.LogInfo(msg)
		if time.Now().Unix()-partition.lastWarnTime > intervalToWarnDataPartition {
			Warn(clusterName, msg)
			partition.lastWarnTime = time.Now().Unix()
		}
	}
}

func (partition *DataPartition) hasEnoughAvailableSpace() bool {
	avail := partition.total - partition.used
	return int64(avail) > 10*util.GB
}

func (partition *DataPartition) checkReplicaEqualStatus(liveReplicas []*DataReplica, status int8) (equal bool) {
	for _, replica := range liveReplicas {
		if replica.Status != status {
			log.LogDebugf("action[checkReplicaEqualStatus] partition %v replica %v status %v dst status %v",
				partition.PartitionID, replica.Addr, replica.Status, status)
			return
		}
	}

	return true
}

func (partition *DataPartition) checkReplicaStatus(timeOutSec int64) {
	partition.Lock()
	defer partition.Unlock()
	for _, replica := range partition.Replicas {
		if !replica.isLive(partition.PartitionID, timeOutSec) {
			log.LogInfof("action[checkReplicaStatus] partition %v replica %v be set status ReadOnly", partition.PartitionID, replica.Addr)
			if replica.Status == proto.ReadWrite {
				replica.Status = proto.ReadOnly
			}
			if partition.isSpecialReplicaCnt() {
				return
			}
			continue
		}

		if (replica.dataNode.RdOnly || partition.RdOnly) && replica.Status == proto.ReadWrite {
			replica.Status = proto.ReadOnly
		}
	}
}

func (partition *DataPartition) checkLeader(c *Cluster, clusterID string, timeOut int64) {
	partition.Lock()
	for _, dr := range partition.Replicas {
		if !dr.isLive(partition.PartitionID, timeOut) {
			dr.IsLeader = false
		}
	}

	if !proto.IsNormalDp(partition.PartitionType) {
		partition.Unlock()
		return
	}

	var report bool
	if partition.getLeaderAddr() == "" {
		report = true
	}
	if WarnMetrics != nil {
		WarnMetrics.WarnDpNoLeader(clusterID, partition.PartitionID, partition.ReplicaNum, report)
	}
	partition.Unlock()
}

// Check if there is any missing replica for a data partition.
func (partition *DataPartition) checkMissingReplicas(clusterID, leaderAddr string, dataPartitionMissSec, dataPartitionWarnInterval int64) {
	partition.Lock()
	defer partition.Unlock()

	id := strconv.FormatUint(partition.PartitionID, 10)

	WarnMetrics.dpMissingReplicaMutex.Lock()
	_, ok := WarnMetrics.dpMissingReplicaInfo[id]
	oldMissingReplicaNum := 0
	if ok {
		oldMissingReplicaNum = len(WarnMetrics.dpMissingReplicaInfo[id].addrs)
	}
	WarnMetrics.dpMissingReplicaMutex.Unlock()

	for _, replica := range partition.Replicas {
		if partition.hasHost(replica.Addr) && replica.isMissing(dataPartitionMissSec) && !partition.IsDiscard {
			if partition.needToAlarmMissingDataPartition(replica.Addr, dataPartitionWarnInterval) {
				dataNode := replica.getReplicaNode()
				var lastReportTime time.Time
				isActive := true
				if dataNode != nil {
					lastReportTime = dataNode.ReportTime
					isActive = dataNode.isActive
				}
				msg := fmt.Sprintf("action[checkMissErr],clusterID[%v] paritionID:%v  on node:%v  "+
					"miss time > %v  lastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v So Migrate by manual",
					clusterID, partition.PartitionID, replica.Addr, dataPartitionMissSec, replica.ReportTime, lastReportTime, isActive)
				// msg = msg + fmt.Sprintf(" decommissionDataPartitionURL is http://%v/dataPartition/decommission?id=%v&addr=%v", leaderAddr, partition.PartitionID, replica.Addr)
				Warn(clusterID, msg)
				WarnMetrics.WarnMissingDp(clusterID, replica.Addr, partition.PartitionID, true)
			}
		} else {
			WarnMetrics.WarnMissingDp(clusterID, replica.Addr, partition.PartitionID, false)
		}
	}
	WarnMetrics.CleanObsoleteDpMissing(clusterID, partition)
	WarnMetrics.dpMissingReplicaMutex.Lock()
	replicaInfo, ok := WarnMetrics.dpMissingReplicaInfo[id]
	if ok {
		MissingReplicaNum := len(replicaInfo.addrs)
		oldDpReplicaAliveNum := ""
		if MissingReplicaNum != oldMissingReplicaNum && oldMissingReplicaNum != 0 {
			oldDpReplicaAliveNum = WarnMetrics.dpMissingReplicaInfo[id].replicaAlive
		}
		dpReplicaMissingNum := uint8(len(WarnMetrics.dpMissingReplicaInfo[id].addrs))
		dpReplicaAliveNum := partition.ReplicaNum - dpReplicaMissingNum
		replicaInfo.replicaNum = strconv.FormatUint(uint64(partition.ReplicaNum), 10)
		replicaInfo.replicaAlive = strconv.FormatUint(uint64(dpReplicaAliveNum), 10)
		WarnMetrics.dpMissingReplicaInfo[id] = replicaInfo
		for missingReplicaAddr := range WarnMetrics.dpMissingReplicaInfo[id].addrs {
			if WarnMetrics.missingDp != nil {
				if oldDpReplicaAliveNum != "" {
					WarnMetrics.missingDp.DeleteLabelValues(clusterID, id, missingReplicaAddr, oldDpReplicaAliveNum, replicaInfo.replicaNum)
				}
				// WarnMetrics.missingDp.SetWithLabelValues(1, clusterID, id, missingReplicaAddr, replicaInfo.replicaAlive, replicaInfo.replicaNum)
			}
		}
	}
	WarnMetrics.dpMissingReplicaMutex.Unlock()
	if !proto.IsNormalDp(partition.PartitionType) {
		return
	}

	for _, addr := range partition.Hosts {
		if partition.hasMissingDataPartition(addr) && partition.needToAlarmMissingDataPartition(addr, dataPartitionWarnInterval) && !partition.IsDiscard {
			msg := fmt.Sprintf("action[checkMissErr],clusterID[%v] partitionID:%v  on node:%v  "+
				"miss time  > :%v  but server not exsit So Migrate", clusterID, partition.PartitionID, addr, dataPartitionMissSec)
			msg = msg + fmt.Sprintf(" decommissionDataPartitionURL is http://%v/dataPartition/decommission?id=%v&addr=%v", leaderAddr, partition.PartitionID, addr)
			Warn(clusterID, msg)
		}
	}
}

func (partition *DataPartition) needToAlarmMissingDataPartition(addr string, interval int64) (shouldAlarm bool) {
	t, ok := partition.MissingNodes[addr]
	if !ok {
		partition.MissingNodes[addr] = time.Now().Unix()
		shouldAlarm = true
	} else {
		if time.Now().Unix()-t > interval {
			shouldAlarm = true
			partition.MissingNodes[addr] = time.Now().Unix()
		}
	}

	return
}

func (partition *DataPartition) hasMissingDataPartition(addr string) (isMissing bool) {
	_, ok := partition.hasReplica(addr)

	if !ok {
		isMissing = true
	}

	return
}

func (partition *DataPartition) checkDiskError(clusterID, leaderAddr string) {
	diskErrorAddrs := make(map[string]string)

	partition.Lock()
	defer partition.Unlock()

	for _, addr := range partition.Hosts {
		replica, ok := partition.hasReplica(addr)
		if !ok {
			continue
		}

		if replica.Status == proto.Unavailable {
			if partition.isSpecialReplicaCnt() && len(partition.Hosts) > 1 {
				log.LogWarnf("action[%v],clusterID[%v],partitionID:%v,diskPath:%v  On :%v status Unavailable",
					checkDataPartitionDiskErr, clusterID, partition.PartitionID, replica.DiskPath, addr)
				continue
			}
			diskErrorAddrs[replica.Addr] = replica.DiskPath
		}
	}

	if len(diskErrorAddrs) != (int)(partition.ReplicaNum) && len(diskErrorAddrs) > 0 {
		partition.Status = proto.ReadOnly
	}

	for addr, diskPath := range diskErrorAddrs {
		msg := fmt.Sprintf("action[%v],clusterID[%v],partitionID:%v,diskPath:%v  On :%v  status Unavailable",
			checkDataPartitionDiskErr, clusterID, partition.PartitionID, diskPath, addr)
		Warn(clusterID, msg)
	}
}

func (partition *DataPartition) checkReplicationTask(clusterID string, dataPartitionSize uint64) {
	var msg string

	if excessAddr, excessErr := partition.deleteIllegalReplica(); excessErr != nil {
		msg = fmt.Sprintf("action[%v], partitionID:%v  Excess Replication On :%v  Err:%v  rocksDBRecords:%v",
			deleteIllegalReplicaErr, partition.PartitionID, excessAddr, excessErr.Error(), partition.Hosts)
		Warn(clusterID, msg)
		partition.Lock()
		partition.removeReplicaByAddr(excessAddr)
		partition.Unlock()
	}

	if partition.Status == proto.ReadWrite {
		return
	}

	if !partition.IsDiscard {
		if lackAddr, lackErr := partition.missingReplicaAddress(dataPartitionSize); lackErr != nil {
			msg = fmt.Sprintf("action[%v], partitionID:%v  Lack Replication On :%v  Err:%v  Hosts:%v  new task to create DataReplica",
				addMissingReplicaErr, partition.PartitionID, lackAddr, lackErr.Error(), partition.Hosts)
			Warn(clusterID, msg)
		}
	}
}

func (partition *DataPartition) deleteIllegalReplica() (excessAddr string, err error) {
	partition.Lock()
	defer partition.Unlock()

	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if ok := partition.hasHost(replica.Addr); !ok {
			excessAddr = replica.Addr
			err = proto.ErrIllegalDataReplica
			break
		}
	}

	return
}

func (partition *DataPartition) missingReplicaAddress(dataPartitionSize uint64) (addr string, err error) {
	partition.Lock()
	defer partition.Unlock()

	if time.Now().Unix()-partition.createTime < 120 {
		return
	}

	// go through all the hosts to find the missing replica
	for _, host := range partition.Hosts {
		if _, ok := partition.hasReplica(host); !ok {
			log.LogError(fmt.Sprintf("action[missingReplicaAddress],partitionID:%v lack replication:%v",
				partition.PartitionID, host))
			err = proto.ErrMissingReplica
			addr = host
			break
		}
	}

	return
}

func (partition *DataPartition) checkReplicaSize(clusterID string, diffSpaceUsage uint64) {
	partition.RLock()
	defer partition.RUnlock()
	if len(partition.Replicas) == 0 {
		return
	}
	diff := 0.0
	sentry := float64(partition.Replicas[0].Used)
	for _, dr := range partition.Replicas {
		temp := math.Abs(float64(dr.Used) - sentry)
		if temp > diff {
			diff = temp
		}
	}
	if diff > float64(diffSpaceUsage) {
		msg := fmt.Sprintf("action[checkReplicaSize] vol[%v],partition[%v] difference space usage [%v] larger than %v, ",
			partition.VolName, partition.PartitionID, diff, diffSpaceUsage)
		for _, dr := range partition.Replicas {
			msg = msg + fmt.Sprintf("replica[%v],used[%v];", dr.Addr, dr.Used)
		}
		Warn(clusterID, msg)
	}
}

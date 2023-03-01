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
	"github.com/cubefs/cubefs/datanode"
	"math"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

func (partition *DataPartition) checkStatus(clusterName string, needLog bool, dpTimeOutSec int64, c *Cluster) {
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
		if partition.checkReplicaEqualStatus(liveReplicas, proto.ReadWrite) == true && partition.canWrite() && partition.getLeaderAddr() != "" {
			partition.Status = proto.ReadWrite
		}
	default:
		partition.Status = proto.ReadOnly
	}

	if partition.isSpecialReplicaCnt() && partition.SingleDecommissionStatus > 0 {
		log.LogInfof("action[checkStatus] partition %v with Special replica cnt %v on decommison status %v, live replicacnt %v",
			partition.PartitionID, partition.ReplicaNum, partition.Status, len(liveReplicas))
		partition.Status = proto.ReadOnly
		if partition.SingleDecommissionStatus == datanode.DecommsionWaitAddRes {
			if len(liveReplicas) == int(partition.ReplicaNum+1) && partition.checkReplicaNotHaveStatus(liveReplicas, proto.Unavailable) == true {
				partition.SingleDecommissionStatus = datanode.DecommsionWaitAddResFin
				log.LogInfof("action[checkStatus] partition %v with single replica on decommison and continue to remove old replica",
					partition.PartitionID)
				// partition.Status = proto.ReadWrite
				c.syncUpdateDataPartition(partition)
				partition.singleDecommissionChan <- true
			}
		}
	}

	if partition.checkReplicaEqualStatus(liveReplicas, proto.Unavailable) {
		log.LogWarnf("action[checkStatus] partition %v bet set Unavailable", partition.PartitionID)
		partition.Status = proto.Unavailable
	}

	if needLog == true && len(liveReplicas) != int(partition.ReplicaNum) {
		msg := fmt.Sprintf("action[extractStatus],partitionID:%v  replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			partition.PartitionID, partition.ReplicaNum, len(liveReplicas), partition.Status, partition.Hosts)
		log.LogInfo(msg)
		if time.Now().Unix()-partition.lastWarnTime > intervalToWarnDataPartition {
			Warn(clusterName, msg)
			partition.lastWarnTime = time.Now().Unix()
		}
	}
}

func (partition *DataPartition) canWrite() bool {
	avail := partition.total - partition.used
	if int64(avail) > 10*util.GB {
		return true
	}
	return false
}

func (partition *DataPartition) checkReplicaNotHaveStatus(liveReplicas []*DataReplica, status int8) (equal bool) {
	for _, replica := range liveReplicas {
		if replica.Status == status {
			log.LogInfof("action[checkReplicaNotHaveStatus] partition %v replica %v status %v dst status %v",
				partition.PartitionID, replica.Addr, replica.Status, status)
			return
		}
	}

	return true
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
		if !replica.isLive(timeOutSec) {
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

func (partition *DataPartition) checkLeader(clusterID string, timeOut int64) {
	partition.Lock()
	defer partition.Unlock()
	for _, dr := range partition.Replicas {
		if !dr.isLive(timeOut) {
			dr.IsLeader = false
		}
	}

	if partition.getLeaderAddr() == "" {
		WarnMetrics.WarnDpNoLeader(clusterID, partition.PartitionID)
	}
	return
}

// Check if there is any missing replica for a data partition.
func (partition *DataPartition) checkMissingReplicas(clusterID, leaderAddr string, dataPartitionMissSec, dataPartitionWarnInterval int64) {
	partition.Lock()
	defer partition.Unlock()

	for _, replica := range partition.Replicas {
		if partition.hasHost(replica.Addr) && replica.isMissing(dataPartitionMissSec) && partition.needToAlarmMissingDataPartition(replica.Addr, dataPartitionWarnInterval) {
			dataNode := replica.getReplicaNode()
			var (
				lastReportTime time.Time
			)
			isActive := true
			if dataNode != nil {
				lastReportTime = dataNode.ReportTime
				isActive = dataNode.isActive
			}
			msg := fmt.Sprintf("action[checkMissErr],clusterID[%v] paritionID:%v  on node:%v  "+
				"miss time > %v  lastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v So Migrate by manual",
				clusterID, partition.PartitionID, replica.Addr, dataPartitionMissSec, replica.ReportTime, lastReportTime, isActive)
			//msg = msg + fmt.Sprintf(" decommissionDataPartitionURL is http://%v/dataPartition/decommission?id=%v&addr=%v", leaderAddr, partition.PartitionID, replica.Addr)
			Warn(clusterID, msg)
			WarnMetrics.WarnMissingDp(clusterID, replica.Addr, partition.PartitionID)
		}
	}

	if !proto.IsNormalDp(partition.PartitionType) {
		return
	}

	for _, addr := range partition.Hosts {
		if partition.hasMissingDataPartition(addr) && partition.needToAlarmMissingDataPartition(addr, dataPartitionWarnInterval) {
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
	diskErrorAddrs := make(map[string]string, 0)

	partition.Lock()
	defer partition.Unlock()

	for _, addr := range partition.Hosts {
		replica, ok := partition.hasReplica(addr)
		if !ok {
			continue
		}

		if replica.Status == proto.Unavailable {
			if partition.isSpecialReplicaCnt() && len(partition.Hosts) > 1 {
				log.LogWarnf("action[%v],clusterID[%v],partitionID:%v  On :%v status Unavailable",
					checkDataPartitionDiskErr, clusterID, partition.PartitionID, addr)
				continue
			}
			diskErrorAddrs[replica.Addr] = replica.DiskPath
		}
	}

	if len(diskErrorAddrs) != (int)(partition.ReplicaNum) && len(diskErrorAddrs) > 0 {
		partition.Status = proto.ReadOnly
	}

	for addr, diskPath := range diskErrorAddrs {
		msg := fmt.Sprintf("action[%v],clusterID[%v],partitionID:%v  On :%v  Disk Error,So Remove it From RocksDBHost, decommissionDiskURL is http://%v/disk/decommission?addr=%v&disk=%v",
			checkDataPartitionDiskErr, clusterID, partition.PartitionID, addr, leaderAddr, addr, diskPath)
		Warn(clusterID, msg)
	}

	return
}

func (partition *DataPartition) checkReplicationTask(clusterID string, dataPartitionSize uint64) (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
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

	if lackAddr, lackErr := partition.missingReplicaAddress(dataPartitionSize); lackErr != nil {
		msg = fmt.Sprintf("action[%v], partitionID:%v  Lack Replication On :%v  Err:%v  Hosts:%v  new task to create DataReplica",
			addMissingReplicaErr, partition.PartitionID, lackAddr, lackErr.Error(), partition.Hosts)
		Warn(clusterID, msg)
	}

	return
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

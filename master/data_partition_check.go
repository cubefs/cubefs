// Copyright 2018 The Chubao Authors.
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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"time"
	"github.com/chubaofs/chubaofs/util"
)

func (partition *DataPartition) checkStatus(clusterName string, needLog bool, dpTimeOutSec int64) {
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.getLiveReplicasFromHosts(dpTimeOutSec)
	if len(partition.Replicas) > len(liveReplicas) {
		partition.Status = proto.ReadOnly
		msg := fmt.Sprintf("action[extractStatus],partitionID:%v has exceed repica, replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			partition.PartitionID, partition.ReplicaNum, len(liveReplicas), partition.Status, partition.Hosts)
		Warn(clusterName, msg)
		return
	}

	switch len(liveReplicas) {
	case (int)(partition.ReplicaNum):
		partition.Status = proto.ReadOnly
		if partition.checkReplicaStatusOnLiveNode(liveReplicas) == true && partition.isReplicaSizeAligned() && partition.canWrite() {
			partition.Status = proto.ReadWrite
		}
	default:
		partition.Status = proto.ReadOnly
	}
	if needLog == true {
		msg := fmt.Sprintf("action[extractStatus],partitionID:%v  replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			partition.PartitionID, partition.ReplicaNum, len(liveReplicas), partition.Status, partition.Hosts)
		log.LogInfo(msg)
	}
}

func (partition *DataPartition) canWrite() bool {
	avail := partition.total - partition.used
	if int64(avail) > 10*util.GB {
		return true
	}
	return false
}

func (partition *DataPartition) checkReplicaStatusOnLiveNode(liveReplicas []*DataReplica) (equal bool) {
	for _, replica := range liveReplicas {
		if replica.Status != proto.ReadWrite {
			return
		}
	}

	return true
}

func (partition *DataPartition) checkReplicaStatus(timeOutSec int64) {
	partition.RLock()
	defer partition.RUnlock()
	for _, replica := range partition.Replicas {
		replica.isLive(timeOutSec)
	}
}

// Check if there is any missing replica for a data partition.
func (partition *DataPartition) checkMissingReplicas(clusterID string, dataPartitionMissSec, dataPartitionWarnInterval int64) {
	partition.Lock()
	defer partition.Unlock()
	for _, replica := range partition.Replicas {
		if partition.hasHost(replica.Addr) && replica.isMissing(dataPartitionMissSec) == true && partition.needToAlarmMissingDataPartition(replica.Addr, dataPartitionWarnInterval) {
			dataNode := replica.getReplicaNode()
			var (
				lastReportTime time.Time
			)
			isActive := true
			if dataNode != nil {
				lastReportTime = dataNode.ReportTime
				isActive = dataNode.isActive
			}
			msg := fmt.Sprintf("action[checkMissErr],clusterID[%v] paritionID:%v  on Node:%v  "+
				"miss time > %v  lastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v So Migrate by manual",
				clusterID, partition.PartitionID, replica.Addr, dataPartitionMissSec, replica.ReportTime, lastReportTime, isActive)
			Warn(clusterID, msg)
		}
	}

	for _, addr := range partition.Hosts {
		if partition.hasMissingDataPartition(addr) == true && partition.needToAlarmMissingDataPartition(addr, dataPartitionWarnInterval) {
			msg := fmt.Sprintf("action[checkMissErr],clusterID[%v] partitionID:%v  on Node:%v  "+
				"miss time  > :%v  but server not exsit So Migrate", clusterID, partition.PartitionID, addr, dataPartitionMissSec)
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

	if ok == false {
		isMissing = true
	}

	return
}

func (partition *DataPartition) checkDiskError(clusterID string) (diskErrorAddrs []string) {
	diskErrorAddrs = make([]string, 0)
	partition.Lock()
	defer partition.Unlock()
	for _, addr := range partition.Hosts {
		replica, ok := partition.hasReplica(addr)
		if !ok {
			continue
		}
		if replica.Status == proto.Unavailable {
			diskErrorAddrs = append(diskErrorAddrs, addr)
		}
	}

	if len(diskErrorAddrs) != (int)(partition.ReplicaNum) && len(diskErrorAddrs) > 0 {
		partition.Status = proto.ReadOnly
	}

	for _, diskAddr := range diskErrorAddrs {
		msg := fmt.Sprintf("action[%v],clusterID[%v],partitionID:%v  On :%v  Disk Error,So Remove it From RocksDBHost",
			checkDataPartitionDiskErr, clusterID, partition.PartitionID, diskAddr)
		Warn(clusterID, msg)
	}

	return
}

func (partition *DataPartition) checkReplicationTask(clusterID string, dataPartitionSize uint64) (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if excessAddr, task, excessErr := partition.deleteIllegalReplica(); excessErr != nil {
		tasks = append(tasks, task)
		msg = fmt.Sprintf("action[%v], partitionID:%v  Excess Replication"+
			" On :%v  Err:%v  rocksDBRecords:%v",
			deleteIllegalReplicaErr, partition.PartitionID, excessAddr, excessErr.Error(), partition.Hosts)
		Warn(clusterID, msg)

	}
	if partition.Status == proto.ReadWrite {
		return
	}
	if lackAddr, lackErr := partition.missingReplicaAddress(dataPartitionSize); lackErr != nil {
		msg = fmt.Sprintf("action[%v], partitionID:%v  Lack Replication"+
			" On :%v  Err:%v  Hosts:%v  new task to create DataReplica",
			addMissingReplicaErr, partition.PartitionID, lackAddr, lackErr.Error(), partition.Hosts)
		Warn(clusterID, msg)
	} else {
		partition.setToNormal()
	}

	return
}

func (partition *DataPartition) deleteIllegalReplica() (excessAddr string, task *proto.AdminTask, err error) {
	partition.Lock()
	defer partition.Unlock()
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if ok := partition.hasHost(replica.Addr); !ok {
			excessAddr = replica.Addr
			log.LogError(fmt.Sprintf("action[removeIllegalReplica],partitionID:%v,has excess replication:%v",
				partition.PartitionID, excessAddr))
			err = proto.ErrIllegalDataReplica
			task = partition.createTaskToDeleteDataPartition(excessAddr)
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
	for _, addr := range partition.Hosts {
		if _, ok := partition.hasReplica(addr); !ok {
			log.LogError(fmt.Sprintf("action[missingReplicaAddress],partitionID:%v lack replication:%v",
				partition.PartitionID, addr))
			err = proto.ErrMissingReplica
			addr = addr
			break
		}
	}

	return
}

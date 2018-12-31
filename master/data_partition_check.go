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
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"time"
)

func (partition *DataPartition) checkStatus(clusterName string, needLog bool, dpTimeOutSec int64) {
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.getLiveReplicasByPersistenceHosts(dpTimeOutSec)
	if len(partition.Replicas) > len(liveReplicas) {
		partition.Status = proto.ReadOnly
		msg := fmt.Sprintf("action[checkStatus],partitionID:%v has exceed repica, replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			partition.PartitionID, partition.ReplicaNum, len(liveReplicas), partition.Status, partition.PersistenceHosts)
		Warn(clusterName, msg)
		return
	}

	switch len(liveReplicas) {
	case (int)(partition.ReplicaNum):
		partition.Status = proto.ReadOnly
		if partition.checkReplicaStatusOnLiveNode(liveReplicas) == true && partition.isReplicaSizeAligned() {
			partition.Status = proto.ReadWrite
		}
	default:
		partition.Status = proto.ReadOnly
	}
	if needLog == true {
		msg := fmt.Sprintf("action[checkStatus],partitionID:%v  replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			partition.PartitionID, partition.ReplicaNum, len(liveReplicas), partition.Status, partition.PersistenceHosts)
		log.LogInfo(msg)
	}
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

func (partition *DataPartition) checkMiss(clusterID string, dataPartitionMissSec, dataPartitionWarnInterval int64) {
	partition.Lock()
	defer partition.Unlock()
	for _, replica := range partition.Replicas {
		if partition.isInPersistenceHosts(replica.Addr) && replica.checkMiss(dataPartitionMissSec) == true && partition.needWarnMissDataPartition(replica.Addr, dataPartitionWarnInterval) {
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

	for _, addr := range partition.PersistenceHosts {
		if partition.missDataPartition(addr) == true && partition.needWarnMissDataPartition(addr, dataPartitionWarnInterval) {
			msg := fmt.Sprintf("action[checkMissErr],clusterID[%v] partitionID:%v  on Node:%v  "+
				"miss time  > :%v  but server not exsit So Migrate", clusterID, partition.PartitionID, addr, dataPartitionMissSec)
			Warn(clusterID, msg)
		}
	}
}

func (partition *DataPartition) needWarnMissDataPartition(addr string, dataPartitionWarnInterval int64) (isWarn bool) {
	warnTime, ok := partition.MissNodes[addr]
	if !ok {
		partition.MissNodes[addr] = time.Now().Unix()
		isWarn = true
	} else {
		if time.Now().Unix()-warnTime > dataPartitionWarnInterval {
			isWarn = true
			partition.MissNodes[addr] = time.Now().Unix()
		}
	}

	return
}

func (partition *DataPartition) missDataPartition(addr string) (isMiss bool) {
	_, ok := partition.isInReplicas(addr)

	if ok == false {
		isMiss = true
	}

	return
}

func (partition *DataPartition) checkDiskError(clusterID string) (diskErrorAddrs []string) {
	diskErrorAddrs = make([]string, 0)
	partition.Lock()
	defer partition.Unlock()
	for _, addr := range partition.PersistenceHosts {
		replica, ok := partition.isInReplicas(addr)
		if !ok {
			continue
		}
		if replica.Status == proto.Unavaliable {
			diskErrorAddrs = append(diskErrorAddrs, addr)
		}
	}

	if len(diskErrorAddrs) != (int)(partition.ReplicaNum) && len(diskErrorAddrs) > 0 {
		partition.Status = proto.ReadOnly
	}

	for _, diskAddr := range diskErrorAddrs {
		msg := fmt.Sprintf("action[%v],clusterID[%v],partitionID:%v  On :%v  Disk Error,So Remove it From RocksDBHost",
			checkDataPartitionDiskErrorErr, clusterID, partition.PartitionID, diskAddr)
		Warn(clusterID, msg)
	}

	return
}

func (partition *DataPartition) checkReplicationTask(clusterID string, randomWrite bool, dataPartitionSize uint64) (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if excessAddr, task, excessErr := partition.deleteExcessReplication(); excessErr != nil {
		tasks = append(tasks, task)
		msg = fmt.Sprintf("action[%v], partitionID:%v  Excess Replication"+
			" On :%v  Err:%v  rocksDBRecords:%v",
			deleteExcessReplicationErr, partition.PartitionID, excessAddr, excessErr.Error(), partition.PersistenceHosts)
		Warn(clusterID, msg)

	}
	if partition.Status == proto.ReadWrite {
		return
	}
	if lackAddr, lackErr := partition.addLackReplication(dataPartitionSize); lackErr != nil {
		msg = fmt.Sprintf("action[%v], partitionID:%v  Lack Replication"+
			" On :%v  Err:%v  PersistenceHosts:%v  new task to create DataReplica",
			addLackReplicationErr, partition.PartitionID, lackAddr, lackErr.Error(), partition.PersistenceHosts)
		Warn(clusterID, msg)
	} else {
		partition.setToNormal()
	}

	return
}

/*delete data replica excess replication ,range all data replicas
if data replica not in persistenceHosts then generator task to delete the replica*/
func (partition *DataPartition) deleteExcessReplication() (excessAddr string, task *proto.AdminTask, err error) {
	partition.Lock()
	defer partition.Unlock()
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if ok := partition.isInPersistenceHosts(replica.Addr); !ok {
			excessAddr = replica.Addr
			log.LogError(fmt.Sprintf("action[deleteExcessReplication],partitionID:%v,has excess replication:%v",
				partition.PartitionID, excessAddr))
			err = errDataReplicaExcess
			task = partition.generateDeleteTask(excessAddr)
			break
		}
	}

	return
}

// TODO what does addLackReplication mean? what is lackAddr
/*add data partition lack replication,range all RocksDBHost if Hosts not in Replicas,
then generator a task to OpRecoverCreateDataPartition to a new Node*/
// missingReplica
// getMissingReplicaAddress
func (partition *DataPartition) addLackReplication(dataPartitionSize uint64) (lackAddr string, err error) {
	partition.Lock()
	defer partition.Unlock()

	// TODO why 120 here?
	if time.Now().Unix() - partition.createTime < 120 {
		return
	}

	for _, addr := range partition.PersistenceHosts {
		if _, ok := partition.isInReplicas(addr); !ok {
			log.LogError(fmt.Sprintf("action[addLackReplication],partitionID:%v lack replication:%v",
				partition.PartitionID, addr))
			err = errDataReplicaLack
			lackAddr = addr
			partition.isRecover = true
			break
		}
	}

	return
}

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
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"sync"
)

// Vol represents a set of meta partitions and data partitions
type Vol struct {
	ID                uint64
	Name              string
	RandomWrite       bool
	dpReplicaNum      uint8
	mpReplicaNum      uint8
	Status            uint8
	threshold         float32
	dataPartitionSize uint64
	Capacity          uint64 //GB
	MetaPartitions    map[uint64]*MetaPartition
	mpsLock           sync.RWMutex
	dataPartitions    *DataPartitionMap
	sync.RWMutex
}

func newVol(id uint64, name string, replicaNum uint8, randomWrite bool, dpSize, capacity uint64) (vol *Vol) {
	vol = &Vol{ID: id, Name: name, MetaPartitions: make(map[uint64]*MetaPartition, 0)}
	vol.RandomWrite = randomWrite
	vol.dataPartitions = newDataPartitionMap(name)
	vol.dpReplicaNum = replicaNum
	vol.threshold = defaultMetaPartitionThreshold
	if replicaNum%2 == 0 {
		vol.mpReplicaNum = replicaNum + 1
	} else {
		vol.mpReplicaNum = replicaNum
	}
	if dpSize == 0 {
		dpSize = util.DefaultDataPartitionSize
	}
	if dpSize < util.GB {
		dpSize = util.DefaultDataPartitionSize
	}
	vol.dataPartitionSize = dpSize
	vol.Capacity = capacity
	return
}

func (vol *Vol) addMetaPartition(mp *MetaPartition) {
	vol.mpsLock.Lock()
	defer vol.mpsLock.Unlock()
	if _, ok := vol.MetaPartitions[mp.PartitionID]; !ok {
		vol.MetaPartitions[mp.PartitionID] = mp
		return
	}
	// replace the old partition in the map with mp
	vol.MetaPartitions[mp.PartitionID] = mp
}

func (vol *Vol) getMetaPartition(partitionID uint64) (mp *MetaPartition, err error) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	mp, ok := vol.MetaPartitions[partitionID]
	if !ok {
		err = metaPartitionNotFound(partitionID)
	}
	return
}

func (vol *Vol) getMaxPartitionID() (maxPartitionID uint64) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for id := range vol.MetaPartitions {
		if id > maxPartitionID {
			maxPartitionID = id
		}
	}
	return
}

func (vol *Vol) getDataPartitionsView(liveRate float32) (body []byte, err error) {
	if liveRate < nodesAliveRate {
		body = make([]byte, 0)
		return
	}
	return vol.dataPartitions.updateDataPartitionResponseCache(false, 0)
}

func (vol *Vol) getDataPartitionByID(partitionID uint64) (dp *DataPartition, err error) {
	return vol.dataPartitions.get(partitionID)
}

func (vol *Vol) initMetaPartitions(c *Cluster) {
	// initialize k meta partitions at a time
	var (
		start uint64
		end   uint64
	)
	for index := 0; index < defaultInitMetaPartitionCount; index++ {
		start = end + 1
		end = defaultMetaPartitionInodeIDStep * uint64(index+1)
		if index == defaultInitMetaPartitionCount-1 {
			end = defaultMaxMetaPartitionInodeID
		}
		if err := c.createMetaPartition(vol.Name, start, end); err != nil {
			log.LogErrorf("action[initMetaPartitions] vol[%v] init meta partition err[%v]", err)
		}
	}
}

func (vol *Vol) initDataPartitions(c *Cluster) {
	// initialize k data partitions at a time
	for i := 0; i < defaultInitDataPartitions; i++ {
		c.createDataPartition(vol.Name)
	}
	return
}
func (vol *Vol) checkDataPartitionStatus(c *Cluster) (readWriteDataPartitions int) {
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	for _, dp := range vol.dataPartitions.dataPartitionMap {
		dp.checkStatus(c.Name, true, c.cfg.DataPartitionTimeOutSec)
		if dp.Status == proto.ReadWrite {
			readWriteDataPartitions++
		}
	}
	return
}

func (vol *Vol) checkDataPartitions(c *Cluster) (readWriteDataPartitions int) {
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	for _, dp := range vol.dataPartitions.dataPartitionMap {
		dp.checkReplicaStatus(c.cfg.DataPartitionTimeOutSec)
		dp.checkStatus(c.Name, true, c.cfg.DataPartitionTimeOutSec)
		dp.checkMiss(c.Name, c.cfg.DataPartitionMissSec, c.cfg.DataPartitionWarnInterval)
		dp.checkReplicaNum(c, vol.Name)
		if dp.Status == proto.ReadWrite {
			readWriteDataPartitions++
		}
		diskErrorAddrs := dp.checkDiskError(c.Name)
		if diskErrorAddrs != nil {
			for _, addr := range diskErrorAddrs {
				c.dataPartitionOffline(addr, vol.Name, dp, checkDataPartitionDiskErrorErr)
			}
		}
		tasks := dp.checkReplicationTask(c.Name, vol.RandomWrite, vol.dataPartitionSize)
		if len(tasks) != 0 {
			c.addDataNodeTasks(tasks)
		}
	}
	return
}

func (vol *Vol) loadDataPartition(c *Cluster) {
	needCheckDataPartitions, startIndex := vol.dataPartitions.getNeedCheckDataPartitions(c.cfg.LoadDataPartitionFrequencyTime)
	if len(needCheckDataPartitions) == 0 {
		return
	}
	c.waitLoadDataPartitionResponse(needCheckDataPartitions)
	msg := fmt.Sprintf("action[loadDataPartition] vol[%v],checkStartIndex:%v checkCount:%v",
		vol.Name, startIndex, len(needCheckDataPartitions))
	log.LogInfo(msg)
}

func (vol *Vol) releaseDataPartitions(releaseCount int, afterLoadSeconds int64) {
	needReleaseDataPartitions, startIndex := vol.dataPartitions.getNeedReleaseDataPartitions(releaseCount, afterLoadSeconds)
	if len(needReleaseDataPartitions) == 0 {
		return
	}
	vol.dataPartitions.releaseDataPartitions(needReleaseDataPartitions)
	msg := fmt.Sprintf("action[releaseDataPartitions] vol[%v] release data partition start:%v releaseCount:%v",
		vol.Name, startIndex, len(needReleaseDataPartitions))
	log.LogInfo(msg)
}

func (vol *Vol) checkMetaPartitions(c *Cluster) {
	var tasks []*proto.AdminTask
	maxPartitionID := vol.getMaxPartitionID()
	mps := vol.cloneMetaPartitionMap()
	for _, mp := range mps {
		mp.checkStatus(true, int(vol.mpReplicaNum))
		mp.checkReplicaLeader()
		mp.checkReplicaNum(c, vol.Name, vol.mpReplicaNum)
		mp.checkEnd(c, maxPartitionID)
		mp.checkReplicaMiss(c.Name, defaultMetaPartitionTimeOutSec, defaultMetaPartitionWarnInterval)
		tasks = append(tasks, mp.generateReplicaTask(c.Name, vol.Name)...)
	}
	c.addMetaNodeTasks(tasks)
}

func (vol *Vol) cloneMetaPartitionMap() (mps map[uint64]*MetaPartition) {
	mps = make(map[uint64]*MetaPartition, 0)
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		mps[mp.PartitionID] = mp
	}
	return
}

func (vol *Vol) setStatus(status uint8) {
	vol.Lock()
	defer vol.Unlock()
	vol.Status = status
}

func (vol *Vol) getStatus() uint8 {
	vol.RLock()
	defer vol.RUnlock()
	return vol.Status
}

func (vol *Vol) setCapacity(capacity uint64) {
	vol.Lock()
	defer vol.Unlock()
	vol.Capacity = capacity
}

func (vol *Vol) getCapacity() uint64 {
	vol.RLock()
	defer vol.RUnlock()
	return vol.Capacity
}

func (vol *Vol) checkNeedAutoCreateDataPartitions(c *Cluster) {
	if vol.getStatus() == volMarkDelete {
		return
	}
	if vol.getCapacity() == 0 {
		return
	}
	usedSpace := vol.getTotalUsedSpace()
	usedSpace = usedSpace / util.GB
	if usedSpace >= vol.getCapacity() {
		vol.setAllDataPartitionsToReadOnly()
		return
	}
	vol.setStatus(volNormal)
	if vol.getStatus() == volNormal && !c.DisableAutoAlloc {
		vol.autoCreateDataPartitions(c)
	}
}

func (vol *Vol) autoCreateDataPartitions(c *Cluster) {
	if vol.dataPartitions.readWriteDataPartitions < minReadWriteDataPartitions {
		count := vol.calculateExpandNum()
		log.LogInfof("action[autoCreateDataPartitions] vol[%v] count[%v]", vol.Name, count)
		for i := 0; i < count; i++ {
			c.createDataPartition(vol.Name)
		}
	}
}

func (vol *Vol) calculateExpandNum() (count int) {
	calCount := float64(vol.Capacity) * float64(volExpandDataPartitionStepRatio) * float64(util.GB) / float64(util.DefaultDataPartitionSize)
	switch {
	case calCount < minReadWriteDataPartitions:
		count = minReadWriteDataPartitions
	case calCount > volMaxExpandDataPartitionCount:
		count = volMaxExpandDataPartitionCount
	default:
		count = int(calCount)
	}
	return
}

func (vol *Vol) setAllDataPartitionsToReadOnly() {
	vol.dataPartitions.setAllDataPartitionsToReadOnly()
}

func (vol *Vol) getTotalUsedSpace() uint64 {
	return vol.dataPartitions.getTotalUsedSpace()
}

func (vol *Vol) checkStatus(c *Cluster) {
	vol.Lock()
	defer vol.Unlock()
	if vol.Status != volMarkDelete {
		return
	}
	log.LogInfof("action[volCheckStatus] vol[%v],status[%v]", vol.Name, vol.Status)
	metaTasks := vol.getDeleteMetaTasks()
	dataTasks := vol.getDeleteDataTasks()
	if len(metaTasks) == 0 && len(dataTasks) == 0 {
		vol.deleteVolFromStore(c)
	}
	c.addMetaNodeTasks(metaTasks)
	c.addDataNodeTasks(dataTasks)
	return
}

func (vol *Vol) deleteVolFromStore(c *Cluster) {

	if err := c.syncDeleteVol(vol); err != nil {
		return
	}

	// delete the metadata of the meta and data partitions first
	vol.deleteDataPartitionsFromStore(c)
	vol.deleteMetaPartitionsFromStore(c)

	// then delete the volume
	c.deleteVol(vol.Name)
}

func (vol *Vol) deleteMetaPartitionsFromStore(c *Cluster) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		c.syncDeleteMetaPartition(mp)
	}
	return
}

func (vol *Vol) deleteDataPartitionsFromStore(c *Cluster) {
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	for _, dp := range vol.dataPartitions.dataPartitions {
		c.syncDeleteDataPartition(dp)
	}

}

func (vol *Vol) getDeleteMetaTasks() (tasks []*proto.AdminTask) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	tasks = make([]*proto.AdminTask, 0)

	for _, mp := range vol.MetaPartitions {
		for _, replica := range mp.Replicas {
			tasks = append(tasks, replica.generateDeleteReplicaTask(mp.PartitionID))
		}
	}
	return
}

func (vol *Vol) getDeleteDataTasks() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()

	for _, dp := range vol.dataPartitions.dataPartitions {
		for _, replica := range dp.Replicas {
			tasks = append(tasks, dp.generateDeleteTask(replica.Addr))
		}
	}
	return
}

func (vol *Vol) String() string {
	return fmt.Sprintf("name[%v],dpNum[%v],mpNum[%v],cap[%v],status[%v]",
		vol.Name, vol.dpReplicaNum, vol.mpReplicaNum, vol.Capacity, vol.Status)
}

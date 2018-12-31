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
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"runtime"
	"sync"
	"time"
)

// DataPartitionMap stores all the data partitions
type DataPartitionMap struct {
	sync.RWMutex
	dataPartitionMap           map[uint64]*DataPartition
	dataPartitionCount         int
	readWriteDataPartitions    int  // TODO what are readWriteDataPartitions 可以读写的data partition 个数
	lastCheckIndex             uint64  // TODO what is lastCheckIndex 上次load data partition 结束的位置 lastLoadDPIndex
	lastReleaseIndex           uint64  // TODO what is lastReleaseIndex release callMap -> lastReleaseDPIndex
	dataPartitions             []*DataPartition
	cacheDataPartitionResponse []byte  // TODO what is cacheDataPartitionResponse  返回给客户端 responseCache
	volName                    string
}

func newDataPartitionMap(volName string) (dpMap *DataPartitionMap) {
	dpMap = new(DataPartitionMap)
	dpMap.dataPartitionMap = make(map[uint64]*DataPartition, 0)
	dpMap.dataPartitionCount = 1
	dpMap.dataPartitions = make([]*DataPartition, 0)
	dpMap.volName = volName
	return
}

func (dpMap *DataPartitionMap) get(ID uint64) (*DataPartition, error) {
	dpMap.RLock()
	defer dpMap.RUnlock()
	if v, ok := dpMap.dataPartitionMap[ID]; ok {
		return v, nil
	}
	return nil, errors.Annotatef(dataPartitionNotFound(ID), "[%v] not found in [%v]", ID, dpMap.volName)
}

func (dpMap *DataPartitionMap) put(dp *DataPartition) {
	dpMap.Lock()
	defer dpMap.Unlock()
	_, ok := dpMap.dataPartitionMap[dp.PartitionID]
	if !ok {
		dpMap.dataPartitions = append(dpMap.dataPartitions, dp)
		dpMap.dataPartitionMap[dp.PartitionID] = dp
		return
	}

	// replace the old partition with dp in the map and array
	dpMap.dataPartitionMap[dp.PartitionID] = dp
	dataPartitions := make([]*DataPartition, 0)
	for index, partition := range dpMap.dataPartitions {
		if partition.PartitionID == dp.PartitionID {
			dataPartitions = append(dataPartitions, dpMap.dataPartitions[:index]...)
			dataPartitions = append(dataPartitions, dp)
			dataPartitions = append(dataPartitions, dpMap.dataPartitions[index+1:]...)
			dpMap.dataPartitions = dataPartitions
			break
		}
	}
}

func (dpMap *DataPartitionMap) setReadWriteDataPartitions(readWrites int, clusterName string) {
	dpMap.Lock()
	defer dpMap.Unlock()
	dpMap.readWriteDataPartitions = readWrites
}


// TODO what is updateDataPartitionResponseCache?
func (dpMap *DataPartitionMap) updateDataPartitionResponseCache(needUpdate bool, minPartitionID uint64) (body []byte, err error) {
	dpMap.Lock()
	defer dpMap.Unlock()
	if dpMap.cacheDataPartitionResponse == nil || needUpdate || len(dpMap.cacheDataPartitionResponse) == 0 {
		dpMap.cacheDataPartitionResponse = make([]byte, 0)
		dpResps := dpMap.getDataPartitionsView(minPartitionID)
		if len(dpResps) == 0 {
			log.LogError(fmt.Sprintf("action[updateDpResponseCache],volName[%v] minPartitionID:%v,err:%v",
				dpMap.volName, minPartitionID, errNoAvailDataPartition))
			return nil, errors.Annotatef(errNoAvailDataPartition, "volName[%v]", dpMap.volName)
		}
		cv := newDataPartitionsView()
		cv.DataPartitions = dpResps
		if body, err = json.Marshal(cv); err != nil {
			log.LogError(fmt.Sprintf("action[updateDpResponseCache],minPartitionID:%v,err:%v",
				minPartitionID, err.Error()))
			return nil, errors.Annotatef(err, "volName[%v],marshal err", dpMap.volName)
		}
		dpMap.cacheDataPartitionResponse = body
		return
	}
	body = make([]byte, len(dpMap.cacheDataPartitionResponse))
	copy(body, dpMap.cacheDataPartitionResponse)

	return
}

func (dpMap *DataPartitionMap) getDataPartitionsView(minPartitionID uint64) (dpResps []*DataPartitionResponse) {
	dpResps = make([]*DataPartitionResponse, 0)
	log.LogDebugf("volName[%v] DataPartitionMapLen[%v],DataPartitionsLen[%v],minPartitionID[%v]",
		dpMap.volName, len(dpMap.dataPartitionMap), len(dpMap.dataPartitions), minPartitionID)
	for _, dp := range dpMap.dataPartitionMap {
		if dp.PartitionID <= minPartitionID {
			continue
		}
		dpResp := dp.convertToDataPartitionResponse()
		dpResps = append(dpResps, dpResp)
	}

	return
}

func (dpMap *DataPartitionMap) getNeedReleaseDataPartitions(everyReleaseDataPartitionCount int, releaseDataPartitionAfterLoadSeconds int64) (partitions []*DataPartition, startIndex uint64) {
	partitions = make([]*DataPartition, 0)
	dpMap.RLock()
	defer dpMap.RUnlock()
	dpLen := len(dpMap.dataPartitions)
	if dpLen == 0 {
		return
	}
	startIndex = dpMap.lastReleaseIndex
	needReleaseCount := everyReleaseDataPartitionCount
	if dpLen < everyReleaseDataPartitionCount {
		needReleaseCount = dpLen
	}
	for i := 0; i < needReleaseCount; i++ {
		if dpMap.lastReleaseIndex >= uint64(dpLen) {
			dpMap.lastReleaseIndex = 0
		}
		dp := dpMap.dataPartitions[dpMap.lastReleaseIndex]
		dpMap.lastReleaseIndex++
		if time.Now().Unix()-dp.LastLoadTime >= releaseDataPartitionAfterLoadSeconds {
			partitions = append(partitions, dp)
		}
	}

	return
}

func (dpMap *DataPartitionMap) releaseDataPartitions(partitions []*DataPartition) {
	var wg sync.WaitGroup
	for _, dp := range partitions {
		wg.Add(1)
		go func(dp *DataPartition) {
			defer func() {
				wg.Done()
				if err := recover(); err != nil {
					const size = runtimeStackBufSize
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					log.LogError(fmt.Sprintf("[%v] releaseDataPartitions panic %v: %s\n", dpMap.volName, err, buf))
				}
			}()
			dp.releaseDataPartition()
		}(dp)
	}
	wg.Wait()

}

func (dpMap *DataPartitionMap) getNeedCheckDataPartitions(loadFrequencyTime int64) (partitions []*DataPartition, startIndex uint64) {
	partitions = make([]*DataPartition, 0)
	dpMap.RLock()
	defer dpMap.RUnlock()
	dpLen := len(dpMap.dataPartitions)
	if dpLen == 0 {
		return
	}
	startIndex = dpMap.lastCheckIndex
	needLoadCount := dpLen / loadDataPartitionPeriod
	if needLoadCount == 0 {
		needLoadCount = 1
	}
	for i := 0; i < needLoadCount; i++ {
		if dpMap.lastCheckIndex >= (uint64)(len(dpMap.dataPartitions)) {
			dpMap.lastCheckIndex = 0
		}
		dp := dpMap.dataPartitions[dpMap.lastCheckIndex]
		dpMap.lastCheckIndex++
		if time.Now().Unix()-dp.LastLoadTime >= loadFrequencyTime {
			partitions = append(partitions, dp)
		}
	}

	return
}

func (dpMap *DataPartitionMap) getTotalUsedSpace() (totalUsed uint64) {
	dpMap.RLock()
	defer dpMap.RUnlock()
	for _, dp := range dpMap.dataPartitions {
		totalUsed = totalUsed + dp.getMaxUsedSpace()
	}
	return
}

func (dpMap *DataPartitionMap) setAllDataPartitionsToReadOnly() {
	dpMap.Lock()
	defer dpMap.Unlock()
	for _, dp := range dpMap.dataPartitions {
		dp.Status = proto.ReadOnly
	}
}

// Copyright 2018 The Containerfs Authors.
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
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"runtime"
	"sync"
	"time"
	"github.com/juju/errors"
)

type DataPartitionMap struct {
	sync.RWMutex
	dataPartitionMap           map[uint64]*DataPartition
	dataPartitionCount         int
	readWriteDataPartitions    int
	lastCheckIndex             uint64
	lastReleaseIndex           uint64
	dataPartitions             []*DataPartition
	cacheDataPartitionResponse []byte
	volName                    string
}

func NewDataPartitionMap(volName string) (dpMap *DataPartitionMap) {
	dpMap = new(DataPartitionMap)
	dpMap.dataPartitionMap = make(map[uint64]*DataPartition, 0)
	dpMap.dataPartitionCount = 1
	dpMap.dataPartitions = make([]*DataPartition, 0)
	dpMap.volName = volName
	return
}

func (dpMap *DataPartitionMap) getDataPartition(ID uint64) (*DataPartition, error) {
	dpMap.RLock()
	defer dpMap.RUnlock()
	if v, ok := dpMap.dataPartitionMap[ID]; ok {
		return v, nil
	}
	return nil, errors.Annotatef(DataPartitionNotFound, "[%v] not found in [%v]", ID, dpMap.volName)
}

func (dpMap *DataPartitionMap) putDataPartition(dp *DataPartition) {
	dpMap.Lock()
	defer dpMap.Unlock()
	_, ok := dpMap.dataPartitionMap[dp.PartitionID]
	if !ok {
		dpMap.dataPartitions = append(dpMap.dataPartitions, dp)
		dpMap.dataPartitionMap[dp.PartitionID] = dp
		return
	}
	//use dp replace old partition in the map and array
	dpMap.dataPartitionMap[dp.PartitionID] = dp
	dpMap.dataPartitions = append(dpMap.dataPartitions, dp)
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

func (dpMap *DataPartitionMap) updateDataPartitionResponseCache(needUpdate bool, minPartitionID uint64) (body []byte, err error) {
	dpMap.Lock()
	defer dpMap.Unlock()
	if dpMap.cacheDataPartitionResponse == nil || needUpdate || len(dpMap.cacheDataPartitionResponse) == 0 {
		dpMap.cacheDataPartitionResponse = make([]byte, 0)
		dpResps := dpMap.GetDataPartitionsView(minPartitionID)
		if len(dpResps) == 0 {
			log.LogError(fmt.Sprintf("action[updateDpResponseCache],volName[%v] minPartitionID:%v,err:%v",
				dpMap.volName, minPartitionID, NoAvailDataPartition))
			return nil, errors.Annotatef(NoAvailDataPartition, "volName[%v]", dpMap.volName)
		}
		cv := NewDataPartitionsView()
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

func (dpMap *DataPartitionMap) GetDataPartitionsView(minPartitionID uint64) (dpResps []*DataPartitionResponse) {
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
	defer func() {
		if err := recover(); err != nil {
			const size = RuntimeStackBufSize
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.LogError(fmt.Sprintf("[%v] releaseDataPartitions panic %v: %s\n", dpMap.volName, err, buf))
		}
	}()
	var wg sync.WaitGroup
	for _, dp := range partitions {
		wg.Add(1)
		go func(dp *DataPartition) {
			dp.ReleaseDataPartition()
			wg.Done()
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
	needLoadCount := dpLen / LoadDataPartitionPeriod
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
		totalUsed = totalUsed + dp.getMaxUsedSize()
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

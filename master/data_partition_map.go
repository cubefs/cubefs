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
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/compressor"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/timeutil"
)

const (
	updateMaxDpIdInterval = 10
)

// DataPartitionMap stores all the data partitionMap
type DataPartitionMap struct {
	sync.RWMutex
	partitionMap           map[uint64]*DataPartition
	readableAndWritableCnt int    // number of readable and writable partitionMap
	lastLoadedIndex        uint64 // last loaded partition index
	lastReleasedIndex      uint64 // last released partition index
	partitions             []*DataPartition

	responseCache             []byte
	responseCachePool         []byte
	responseCompressCache     []byte
	ResponseCompressCachePool []byte

	lastAutoCreateTime    time.Time
	volName               string
	readMutex             sync.RWMutex
	partitionMapByPoolId  map[uint8]map[uint64]struct{} // level-1 key: poolId, level-2 key: dpId
	rwCntByMediaType      map[uint32]int                // readable and writable dp count by mediaType
	rwCntByPoolId         map[uint8]int                 // readable and writable dp count by poolId
	maxDpId               uint64
	lastUpdateMaxDpIdTime int64
}

func newDataPartitionMap(volName string) (dpMap *DataPartitionMap) {
	dpMap = new(DataPartitionMap)
	dpMap.partitionMap = make(map[uint64]*DataPartition)
	dpMap.partitionMapByPoolId = make(map[uint8]map[uint64]struct{})
	dpMap.rwCntByMediaType = make(map[uint32]int)
	dpMap.rwCntByPoolId = make(map[uint8]int)
	dpMap.partitions = make([]*DataPartition, 0)
	dpMap.responseCache = make([]byte, 0)
	dpMap.responseCachePool = make([]byte, 0)
	dpMap.responseCompressCache = make([]byte, 0)
	dpMap.ResponseCompressCachePool = make([]byte, 0)
	dpMap.volName = volName
	dpMap.lastAutoCreateTime = time.Now()
	return
}

// attention: it's not deep clone for element, dataPartition
func (dpMap *DataPartitionMap) clonePartitions() []*DataPartition {
	dpMap.RLock()
	partitions := make([]*DataPartition, 0, len(dpMap.partitions))
	partitions = append(partitions, dpMap.partitions...)
	dpMap.RUnlock()
	return partitions
}

func (dpMap *DataPartitionMap) get(ID uint64) (*DataPartition, error) {
	dpMap.RLock()
	defer dpMap.RUnlock()
	if v, ok := dpMap.partitionMap[ID]; ok {
		return v, nil
	}
	return nil, proto.ErrDataPartitionNotExists
}

func (dpMap *DataPartitionMap) del(dp *DataPartition) {
	dpMap.Lock()
	defer dpMap.Unlock()
	_, ok := dpMap.partitionMap[dp.PartitionID]
	if !ok {
		return
	}

	dataPartitions := make([]*DataPartition, 0)
	for index, partition := range dpMap.partitions {
		if partition.PartitionID == dp.PartitionID {
			dataPartitions = append(dataPartitions, dpMap.partitions[:index]...)
			dataPartitions = append(dataPartitions, dpMap.partitions[index+1:]...)
			dpMap.partitions = dataPartitions
			break
		}
	}
	delete(dpMap.partitionMap, dp.PartitionID)
	dpMap.delByPoolId(dp)
}

func (dpMap *DataPartitionMap) delByPoolId(dp *DataPartition) {
	dpIdSet, ok := dpMap.partitionMapByPoolId[dp.PoolId]
	if !ok {
		log.LogCriticalf("[DataPartitionMap] delByPoolId: not record of poolId(%v) when trying to del dpId(%v)",
			dp.PoolId, dp.PartitionID)
		return
	}
	delete(dpIdSet, dp.PartitionID)
	log.LogDebugf("[DataPartitionMap] delByPoolId: poolId(%v), dpId(%v)", dp.PoolId, dp.PartitionID)
}

func (dpMap *DataPartitionMap) put(dp *DataPartition) {
	dpMap.Lock()
	defer dpMap.Unlock()

	_, ok := dpMap.partitionMap[dp.PartitionID]
	if !ok {
		dpMap.partitions = append(dpMap.partitions, dp)
		dpMap.partitionMap[dp.PartitionID] = dp
		dpMap.putByPoolId(dp)
		return
	}

	// replace the old partition with dp in the map and array
	dpMap.partitionMap[dp.PartitionID] = dp
	dataPartitions := make([]*DataPartition, 0)
	for index, partition := range dpMap.partitions {
		if partition.PartitionID == dp.PartitionID {
			dataPartitions = append(dataPartitions, dpMap.partitions[:index]...)
			dataPartitions = append(dataPartitions, dp)
			dataPartitions = append(dataPartitions, dpMap.partitions[index+1:]...)
			dpMap.partitions = dataPartitions
			break
		}
	}
}

func (dpMap *DataPartitionMap) putByPoolId(dp *DataPartition) {
	dpIdSet, ok := dpMap.partitionMapByPoolId[dp.PoolId]
	if !ok {
		dpIdSet = make(map[uint64]struct{})
		dpMap.partitionMapByPoolId[dp.PoolId] = dpIdSet
	}
	dpIdSet[dp.PartitionID] = struct{}{}
}

func (dpMap *DataPartitionMap) getReadWriteDataPartitionCntByPoolId() map[uint8]int {
	dpMap.RLock()
	defer dpMap.RUnlock()

	newMap := make(map[uint8]int)
	for poolId, dpIdSet := range dpMap.partitionMapByPoolId {
		newMap[poolId] = len(dpIdSet)
	}
	return newMap
}

func (dpMap *DataPartitionMap) getDataPartitionsCountOfPool(poolId uint8) int {
	dpMap.RLock()
	defer dpMap.RUnlock()

	dpIdSet, ok := dpMap.partitionMapByPoolId[poolId]
	if !ok {
		return 0
	}

	return len(dpIdSet)
}

func (dpMap *DataPartitionMap) refreshReadWriteDataPartitionCnt() {
	var cntAllPoolId int
	for _, cntOfPoolId := range dpMap.rwCntByPoolId {
		cntAllPoolId = cntAllPoolId + cntOfPoolId
	}
	dpMap.readableAndWritableCnt = cntAllPoolId
}

func (dpMap *DataPartitionMap) setReadWriteDataPartitionCntByMediaType(rwDpCnt int, mediaType uint32) {
	dpMap.Lock()
	defer dpMap.Unlock()

	dpMap.rwCntByMediaType[mediaType] = rwDpCnt

	dpMap.refreshReadWriteDataPartitionCnt()
}

func (dpMap *DataPartitionMap) setReadWriteCntByPoolId(cntByPoolId map[uint8]int) {
	dpMap.Lock()
	defer dpMap.Unlock()

	dpMap.rwCntByPoolId = cntByPoolId

	dpMap.refreshReadWriteDataPartitionCnt()
}

func (dpMap *DataPartitionMap) getReadWriteCntByPoolId() map[uint8]int {
	dpMap.RLock()
	defer dpMap.RUnlock()
	newMap := make(map[uint8]int)
	for poolId, cnt := range dpMap.rwCntByPoolId {
		newMap[poolId] = cnt
	}
	return newMap
}

func (dpMap *DataPartitionMap) IncReadWriteDataPartitionCntByPoolId(incCnt int, poolId uint8) {
	dpMap.Lock()
	defer dpMap.Unlock()

	var oldCnt int
	if cnt, ok := dpMap.rwCntByPoolId[poolId]; ok {
		oldCnt = cnt
	}

	newCnt := oldCnt + incCnt
	dpMap.rwCntByPoolId[poolId] = newCnt

	dpMap.refreshReadWriteDataPartitionCnt()
}

func (dpMap *DataPartitionMap) getReadWriteDataPartitionCntByMediaType(mediaType uint32) (rwDpCnt int) {
	dpMap.Lock()
	defer dpMap.Unlock()

	if cnt, ok := dpMap.rwCntByMediaType[mediaType]; ok {
		rwDpCnt = cnt
	}

	return rwDpCnt
}

func (dpMap *DataPartitionMap) getReadWriteDataPartitionCntByPool(poolId uint8) int {
	dpMap.RLock()
	defer dpMap.RUnlock()

	if cnt, ok := dpMap.rwCntByPoolId[poolId]; ok {
		return cnt
	}
	return 0
}

func (dpMap *DataPartitionMap) getDataPartitionResponseCache(poolAware bool) []byte {
	dpMap.RLock()
	defer dpMap.RUnlock()
	if poolAware {
		return dpMap.responseCachePool
	}
	return dpMap.responseCache
}

func (dpMap *DataPartitionMap) getDataPartitionCompressCache(poolAware bool) []byte {
	dpMap.RLock()
	defer dpMap.RUnlock()
	if poolAware {
		return dpMap.ResponseCompressCachePool
	}
	return dpMap.responseCompressCache
}

func (dpMap *DataPartitionMap) setDataPartitionResponseCache(responseCache []byte, poolAware bool) {
	dpMap.Lock()
	defer dpMap.Unlock()
	if responseCache != nil {
		if poolAware {
			dpMap.responseCachePool = responseCache
		} else {
			dpMap.responseCache = responseCache
		}
	}
}

func (dpMap *DataPartitionMap) setDataPartitionCompressCache(responseCompress []byte, poolAware bool) {
	dpMap.Lock()
	defer dpMap.Unlock()
	if responseCompress != nil {
		if poolAware {
			dpMap.ResponseCompressCachePool = responseCompress
		} else {
			dpMap.responseCompressCache = responseCompress
		}
	}
}

func (dpMap *DataPartitionMap) updateResponseCache(needsUpdate bool, minPartitionID uint64, vol *Vol, poolAware bool) (body []byte, err error) {
	log.LogDebugf("[updateResponseCache] get vol(%v) dp cache", vol.Name)

	responseCache := dpMap.getDataPartitionResponseCache(poolAware)
	if responseCache == nil || needsUpdate || len(responseCache) == 0 {
		dpMap.readMutex.Lock()
		defer dpMap.readMutex.Unlock()
		responseCache = dpMap.getDataPartitionResponseCache(poolAware)
		if !(responseCache == nil || needsUpdate || len(responseCache) == 0) {
			body = responseCache
			return
		}

		updateCache := func(tmpPoolAware bool) (body []byte, err error) {
			dpResps := dpMap.getDataPartitionsView(minPartitionID, tmpPoolAware)
			log.LogDebugf("[updateResponseCache] vol(%v) needsUpdate(%v) minPartitionID(%v) volType(%v)  dpNum(%v)",
				dpMap.volName, needsUpdate, minPartitionID, vol.VolType, len(dpResps))
			if len(dpResps) == 0 && proto.IsHot(vol.VolType) {
				log.LogError(fmt.Sprintf("action[updateDpResponseCache],volName[%v] minPartitionID:%v,err:%v",
					dpMap.volName, minPartitionID, proto.ErrNoAvailDataPartition))
				return nil, proto.ErrNoAvailDataPartition
			}
			cv := proto.NewDataPartitionsView()
			cv.DataPartitions = dpResps
			if vol.IsReadOnlyForVolFull() || vol.Forbidden {
				cv.VolReadOnly = true
			}

			if vol.DpReadOnlyWhenVolFull {
				cv.StatByClass = vol.StatByStorageClass
				cv.StatByPool = vol.StatByPool
			}

			reply := newSuccessHTTPReply(cv)
			if body, err = json.Marshal(reply); err != nil {
				log.LogError(fmt.Sprintf("action[updateDpResponseCache],minPartitionID:%v,err:%v",
					minPartitionID, err.Error()))
				return nil, proto.ErrMarshalData
			}
			dpMap.setDataPartitionResponseCache(body, tmpPoolAware)
			log.LogInfof("[updateResponseCache] update vol(%v) dp cache cnt(%v)", vol.Name, len(dpResps))
			return body, nil
		}

		var fullBody, body []byte

		if body, err = updateCache(false); err != nil {
			log.LogErrorf("action[updateResponseCache]updateCache failed,err:%+v", err)
			return nil, err
		}

		if fullBody, err = updateCache(true); err != nil {
			log.LogErrorf("action[updateResponseCache]updateCache failed,err:%+v", err)
			return nil, err
		}

		if poolAware {
			body = fullBody
		}

		return body, nil
	}

	body = responseCache
	return
}

func (dpMap *DataPartitionMap) updateCompressCache(needsUpdate bool, minPartitionID uint64, vol *Vol, poolAware bool) (body []byte, err error) {
	cachedBody := dpMap.getDataPartitionCompressCache(poolAware)
	if len(cachedBody) != 0 && !needsUpdate {
		body = cachedBody
		return
	}
	if body, err = dpMap.updateResponseCache(needsUpdate, minPartitionID, vol, poolAware); err != nil {
		log.LogErrorf("action[updateCompressCache]updateResponseCache failed,err:%+v", err)
		return
	}

	if body, err = compressor.New(compressor.EncodingGzip).Compress(body); err != nil {
		log.LogErrorf("action[updateCompressCache]GzipCompressor.Compress failed,err:%+v", err)
		err = proto.ErrCompressFailed
		return
	}
	dpMap.setDataPartitionCompressCache(body, poolAware)

	// set compress cache for non-pool aware
	newBody := dpMap.getDataPartitionResponseCache(!poolAware)
	if len(newBody) == 0 {
		log.LogErrorf("action[updateCompressCache]newBody is nil or empty")
		return
	}

	if newBody, err = compressor.New(compressor.EncodingGzip).Compress(newBody); err != nil {
		log.LogErrorf("action[updateCompressCache]GzipCompressor.Compress failed,err:%+v", err)
		err = proto.ErrCompressFailed
		return
	}
	dpMap.setDataPartitionCompressCache(newBody, !poolAware)

	return
}

func (dpMap *DataPartitionMap) getDataPartitionsView(minPartitionID uint64, poolAware bool) (dpResps []*proto.DataPartitionResponse) {
	dpResps = make([]*proto.DataPartitionResponse, 0)
	dpCache := make([]*DataPartition, 0)
	log.LogDebugf("volName[%v] DataPartitionMapLen[%v],DataPartitionsLen[%v],minPartitionID[%v]",
		dpMap.volName, len(dpMap.partitionMap), len(dpMap.partitions), minPartitionID)
	dpMap.RLock()
	for _, dp := range dpMap.partitionMap {
		if len(dp.Hosts) == 0 {
			log.LogErrorf("getDataPartitionsView. dp %v host nil", dp.PartitionID)
			continue
		}
		if dp.PartitionID <= minPartitionID {
			continue
		}
		dpCache = append(dpCache, dp)
	}
	dpMap.RUnlock()
	for _, dp := range dpCache {
		dpResp := dp.convertToDataPartitionResponse()
		if dp.PoolId >= proto.MaxDefaultPoolId && !poolAware && dp.Status == proto.ReadWrite {
			dpResp.Status = proto.ReadOnly
		}
		dpResps = append(dpResps, dpResp)
	}
	return
}

func (dpMap *DataPartitionMap) getDataPartitionsToBeReleased(numberOfDataPartitionsToFree int, secondsToFreeDataPartitionAfterLoad int64) (partitions []*DataPartition, startIndex uint64) {
	partitions = make([]*DataPartition, 0)
	dpMap.RLock()
	defer dpMap.RUnlock()
	dpLen := len(dpMap.partitions)
	if dpLen == 0 {
		return
	}
	startIndex = dpMap.lastReleasedIndex
	count := numberOfDataPartitionsToFree
	if dpLen < numberOfDataPartitionsToFree {
		count = dpLen
	}
	for i := 0; i < count; i++ {
		if dpMap.lastReleasedIndex >= uint64(dpLen) {
			dpMap.lastReleasedIndex = 0
		}
		dp := dpMap.partitions[dpMap.lastReleasedIndex]
		dpMap.lastReleasedIndex++
		if time.Now().Unix()-dp.LastLoadedTime >= secondsToFreeDataPartitionAfterLoad {
			partitions = append(partitions, dp)
		}
	}

	return
}

func (dpMap *DataPartitionMap) freeMemOccupiedByDataPartitions(partitions []*DataPartition) {
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
					log.LogError(fmt.Sprintf("[%v] freeMemOccupiedByDataPartitions panic %v: %s\n", dpMap.volName, err, buf))
				}
			}()
			dp.releaseDataPartition()
		}(dp)
	}
	wg.Wait()
}

func (dpMap *DataPartitionMap) getDataPartitionsToBeChecked(loadFrequencyTime int64) (partitions []*DataPartition, startIndex uint64) {
	partitions = make([]*DataPartition, 0)
	dpMap.RLock()
	defer dpMap.RUnlock()
	dpLen := len(dpMap.partitions)
	if dpLen == 0 {
		return
	}
	startIndex = dpMap.lastLoadedIndex

	// determine the number of data partitions to load
	count := dpLen / intervalToLoadDataPartition
	if count == 0 {
		count = 1
	}

	for i := 0; i < count; i++ {
		if dpMap.lastLoadedIndex >= (uint64)(len(dpMap.partitions)) {
			dpMap.lastLoadedIndex = 0
		}
		dp := dpMap.partitions[dpMap.lastLoadedIndex]
		dpMap.lastLoadedIndex++

		if time.Now().Unix()-dp.LastLoadedTime >= loadFrequencyTime {
			partitions = append(partitions, dp)
		}
	}

	return
}

func (dpMap *DataPartitionMap) totalUsedSpace() (totalUsed uint64) {
	dpMap.RLock()
	defer dpMap.RUnlock()
	for _, dp := range dpMap.partitions {
		totalUsed = totalUsed + dp.getMaxUsedSpace()
	}
	return
}

func (dpMap *DataPartitionMap) setAllDataPartitionsToReadOnly() {
	dpMap.Lock()
	defer dpMap.Unlock()
	changedCnt := 0
	for _, dp := range dpMap.partitions {
		if proto.ReadWrite == dp.Status {
			dp.applyDataPartitionStatus(proto.ReadOnly, "setAllDataPartitionsToReadOnly")
			changedCnt++
		}
	}
	log.LogDebugf("action[setAllDataPartitionsToReadOnly] ReadWrite->ReadOnly dp cnt: %v", changedCnt)
}

func (dpMap *DataPartitionMap) checkBadDiskDataPartitions(diskPath, nodeAddr string, ignoreDiscard bool) (partitions []*DataPartition) {
	dpMapCache := dpMap.clonePartitions()
	partitions = make([]*DataPartition, 0)
	for _, dp := range dpMapCache {
		if ignoreDiscard && dp.IsDiscard {
			continue
		}
		if dp.containsBadDisk(diskPath, nodeAddr) {
			partitions = append(partitions, dp)
		}
	}
	return
}

func (dpMap *DataPartitionMap) Range(f func(dp *DataPartition) bool) {
	dpMap.RLock()
	defer dpMap.RUnlock()

	for _, dp := range dpMap.partitions {
		if !f(dp) {
			return
		}
	}
}

func (dpMap *DataPartitionMap) getReplicaDiskPaths(nodeAddr string) (diskPaths []string) {
	dps := dpMap.clonePartitions()
	diskPaths = make([]string, 0)
	for _, dp := range dps {
		disk := dp.getReplicaDisk(nodeAddr)
		if len(disk) != 0 && !inStingList(disk, diskPaths) {
			diskPaths = append(diskPaths, disk)
		}
	}
	return
}

func (dpMap *DataPartitionMap) getMaxDataPartitionID() (maxPartitionID uint64) {
	dpMap.Lock()
	defer dpMap.Unlock()
	curtime := timeutil.GetCurrentTimeUnix()
	if curtime < dpMap.lastUpdateMaxDpIdTime+updateMaxDpIdInterval {
		return dpMap.maxDpId
	}
	for id := range dpMap.partitionMap {
		if id > maxPartitionID {
			maxPartitionID = id
		}
	}
	dpMap.maxDpId = maxPartitionID
	dpMap.lastUpdateMaxDpIdTime = curtime
	return
}

func inStingList(target string, strArray []string) bool {
	for _, element := range strArray {
		if target == element {
			return true
		}
	}
	return false
}

func (dpMap *DataPartitionMap) CheckReadWritableCntUnderLimit(limit int, poolId uint8) error {
	dpMap.Lock()
	defer dpMap.Unlock()

	cntOfPoolId, ok := dpMap.rwCntByPoolId[poolId]
	if !ok {
		err := fmt.Errorf("CheckReadWritableCntUnderLimit: poolId(%d) is not in dpMap.rwCntByPoolId", poolId)
		return err
	}
	if cntOfPoolId >= limit {
		err := fmt.Errorf("CheckReadWritableCntUnderLimit: poolId(%d) RWDpCnt(%d) reach limit(%d)", poolId, cntOfPoolId, limit)
		return err
	}
	return nil
}

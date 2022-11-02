package storage

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"sync"
)

const (
	reduceThresholdSize = 1024
)

// MapSlice for GC optimization
type MapSlice struct {
	partitionID   uint64
	mu            sync.RWMutex
	idxMap        map[uint64]uint64
	objSlice      []ExtentInfoBlock
	objDeletedCnt uint64
	tinyExtents   [proto.TinyExtentCount + 1]ExtentInfoBlock
}

func NewMapSlice(partitionID uint64) *MapSlice {
	return &MapSlice{
		partitionID: partitionID,
		idxMap:      make(map[uint64]uint64, 0),
		objSlice:    make([]ExtentInfoBlock, 0),
	}
}

func (ms *MapSlice) Store(extentId uint64, extentBlock ExtentInfoBlock) {
	if proto.IsTinyExtent(extentId) {
		ms.tinyExtents[extentId] = extentBlock
		return
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if extentBlock[FileID] != extentId {
		return
	}
	if idx, ok := ms.idxMap[extentId]; ok {
		ms.objSlice[idx] = extentBlock
		return
	}
	ms.objSlice = append(ms.objSlice, extentBlock)
	ms.idxMap[extentId] = uint64(len(ms.objSlice) - 1)
}

func (ms *MapSlice) Load(extentId uint64) (extentBlock *ExtentInfoBlock, ok bool) {
	if proto.IsTinyExtent(extentId) {
		extentBlock = &ms.tinyExtents[extentId]
		if extentBlock[FileID] <= 0 {
			extentBlock = nil
			return
		}
		ok = true
		if extentBlock[FileID] != extentId {
			err := fmt.Errorf("LoadMapSlice error:partitionID(%v) MapSlice ,"+
				"loadExtent(%v) extentblock(%v)", ms.partitionID, extentId, ms.tinyExtents[extentId])
			log.LogErrorf(err.Error())
			log.LogFlush()
			panic(err.Error())
		}
		return
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	idx, ok := ms.idxMap[extentId]
	if !ok {
		return
	}
	extentBlock = &ms.objSlice[idx]
	if extentBlock[FileID] != extentId {
		err := fmt.Errorf("LoadMapSlice error:partitionID(%v) MapSlice ,"+
			"loadExtent(%v) extentblock(%v)", ms.partitionID, extentId, ms.objSlice[idx])
		log.LogErrorf(err.Error())
		log.LogFlush()
		panic(err.Error())
	}
	return
}

func (ms *MapSlice) Range(f func(extentID uint64, ei *ExtentInfoBlock)) {
	ms.RangeTinyExtent(f)
	ms.RangeNormalExtent(f)
}

func (ms *MapSlice) RangeNormalExtent(f func(extentID uint64, ei *ExtentInfoBlock)) {
	ms.mu.Lock()
	tmpObjSlice := ms.objSlice
	ms.mu.Unlock()
	for i := 0; i < len(tmpObjSlice); i++ {
		if tmpObjSlice[i][FileID] != 0 {
			f(tmpObjSlice[i][FileID], &tmpObjSlice[i])
		}
	}
}

func (ms *MapSlice) RangeTinyExtent(f func(extentID uint64, ei *ExtentInfoBlock)) {
	for i := 0; i < len(ms.tinyExtents); i++ {
		extentID := uint64(i)
		if ms.tinyExtents[i][FileID] != 0 {
			f(extentID, &ms.tinyExtents[i])
		}
	}
}

func (ms *MapSlice) RangeDist(extentType uint8, f func(extentID uint64, ei *ExtentInfoBlock)) {
	if extentType == proto.NormalExtentType {
		ms.RangeNormalExtent(f)
	} else if extentType == proto.TinyExtentType {
		ms.RangeTinyExtent(f)
	} else {
		ms.Range(f)
	}
}

func (ms *MapSlice) Len() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var tinyCount int
	for _, tinyExtent := range ms.tinyExtents {
		if tinyExtent[FileID] != 0 {
			tinyCount++
		}
	}
	return len(ms.idxMap) + tinyCount
}

func (ms *MapSlice) Delete(extentID uint64) {
	if proto.IsTinyExtent(extentID) {
		return
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if curIdx, ok := ms.idxMap[extentID]; ok {
		delete(ms.idxMap, extentID)
		ms.objSlice[curIdx] = EmptyExtentBlock
		ms.objDeletedCnt++
	}
	ms.reduceObjSlice()
}

func (ms *MapSlice) reduceObjSlice() {
	if ms.objDeletedCnt >= reduceThresholdSize {
		newObjSlice := make([]ExtentInfoBlock, 0, len(ms.idxMap))
		newIdxMap := make(map[uint64]uint64, len(ms.idxMap))
		var idxCount uint64 = 0
		for _, obj := range ms.objSlice {
			if obj[FileID] != 0 {
				newObjSlice = append(newObjSlice, obj)
				newIdxMap[obj[FileID]] = idxCount
				idxCount++
			}
		}
		ms.objSlice = nil
		ms.objSlice = newObjSlice
		ms.idxMap = nil
		ms.idxMap = newIdxMap
		ms.objDeletedCnt = 0
	}
}

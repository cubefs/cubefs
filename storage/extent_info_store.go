package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	reduceThresholdSize = 1024
)

const (
	FileID = iota
	Size
	Crc
	ModifyTime
)

type ExtentInfoBlock [4]uint64

var EmptyExtentBlock = ExtentInfoBlock{}

func (eiBlock ExtentInfoBlock) String() string {
	return fmt.Sprintf("%v_%v_%v_%v", eiBlock[FileID], eiBlock[Size], eiBlock[Crc], eiBlock[ModifyTime])
}

// ExtentInfoStore for GC optimization
type ExtentInfoStore struct {
	partitionID      uint64
	mu               sync.RWMutex
	normalIndex      map[uint64]uint64
	normalExtents    []ExtentInfoBlock
	normalDeletedCnt uint64
	normalUsed       uint64
	tinyExtents      [proto.TinyExtentCount + 1]ExtentInfoBlock
}

func NewExtentInfoStore(partitionID uint64) *ExtentInfoStore {
	return &ExtentInfoStore{
		partitionID:   partitionID,
		normalIndex:   make(map[uint64]uint64, 0),
		normalExtents: make([]ExtentInfoBlock, 0),
	}
}

// Deprecated: 现在ExtentInfoStore中Info的生命周期由 Create, Update, Delete 控制,
// 请不要使用 Store 方法存储信息，否则可能导致NormalExtent的使用总量信息不准确。
func (ms *ExtentInfoStore) Store(extentId uint64, extentBlock ExtentInfoBlock) {
	if proto.IsTinyExtent(extentId) {
		ms.tinyExtents[extentId] = extentBlock
		return
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if extentBlock[FileID] != extentId {
		return
	}
	if idx, ok := ms.normalIndex[extentId]; ok {
		ms.normalExtents[idx] = extentBlock
		return
	}
	ms.normalExtents = append(ms.normalExtents, extentBlock)
	ms.normalIndex[extentId] = uint64(len(ms.normalExtents) - 1)
}

func (ms *ExtentInfoStore) Load(extentId uint64) (extentBlock *ExtentInfoBlock, ok bool) {
	if proto.IsTinyExtent(extentId) {
		extentBlock = &ms.tinyExtents[extentId]
		if extentBlock[FileID] <= 0 {
			extentBlock = nil
			return
		}
		ok = true
		if extentBlock[FileID] != extentId {
			err := fmt.Errorf("LoadMapSlice error:partitionID(%v) ExtentInfoStore ,"+
				"loadExtent(%v) extentblock(%v)", ms.partitionID, extentId, ms.tinyExtents[extentId])
			log.LogErrorf(err.Error())
			log.LogFlush()
			panic(err.Error())
		}
		return
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	idx, ok := ms.normalIndex[extentId]
	if !ok {
		return
	}
	extentBlock = &ms.normalExtents[idx]
	if extentBlock[FileID] != extentId {
		err := fmt.Errorf("LoadMapSlice error:partitionID(%v) ExtentInfoStore ,"+
			"loadExtent(%v) extentblock(%v)", ms.partitionID, extentId, ms.normalExtents[idx])
		log.LogErrorf(err.Error())
		log.LogFlush()
		panic(err.Error())
	}
	return
}

func (ms *ExtentInfoStore) NormalUsed() uint64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.normalUsed
}

func (ms *ExtentInfoStore) Create(extentID uint64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if proto.IsTinyExtent(extentID) && ms.tinyExtents[extentID] == EmptyExtentBlock {
		var info = ExtentInfoBlock{}
		info[FileID] = extentID
		ms.tinyExtents[extentID] = info
		return
	}
	if _, ok := ms.normalIndex[extentID]; !ok {
		var info = ExtentInfoBlock{}
		info[FileID] = extentID
		ms.normalExtents = append(ms.normalExtents, info)
		ms.normalIndex[extentID] = uint64(len(ms.normalExtents) - 1)
	}
}

func (ms *ExtentInfoStore) Update(extentID, size, modTime, crc uint64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if proto.IsTinyExtent(extentID) {
		var info = &ms.tinyExtents[extentID]
		info[Size] = size
		info[ModifyTime] = modTime
		info[Crc] = crc
		return
	}
	if curIdx, ok := ms.normalIndex[extentID]; ok {
		var info = &ms.normalExtents[curIdx]
		if time.Now().Unix()-int64(modTime) <= UpdateCrcInterval {
			crc = 0
		}
		var prevSize = info[Size]
		if size >= prevSize {
			ms.normalUsed += size - prevSize
		} else {
			ms.normalUsed -= prevSize - size
		}
		info[Size] = size
		if !proto.IsTinyExtent(extentID) {
			info[Crc] = crc
		}
		info[ModifyTime] = modTime
	}
}

func (ms *ExtentInfoStore) UpdateInfoFromExtent(extent *Extent, crc uint32) {
	ms.Update(extent.extentID, uint64(extent.dataSize), uint64(extent.ModifyTime()), uint64(crc))
}

func (ms *ExtentInfoStore) Range(f func(extentID uint64, ei *ExtentInfoBlock)) {
	ms.RangeTinyExtent(f)
	ms.RangeNormalExtent(f)
}

func (ms *ExtentInfoStore) RangeNormalExtent(f func(extentID uint64, ei *ExtentInfoBlock)) {
	ms.mu.Lock()
	tmpObjSlice := ms.normalExtents
	ms.mu.Unlock()
	for i := 0; i < len(tmpObjSlice); i++ {
		if tmpObjSlice[i][FileID] != 0 {
			f(tmpObjSlice[i][FileID], &tmpObjSlice[i])
		}
	}
}

func (ms *ExtentInfoStore) RangeTinyExtent(f func(extentID uint64, ei *ExtentInfoBlock)) {
	for i := 0; i < len(ms.tinyExtents); i++ {
		extentID := uint64(i)
		if ms.tinyExtents[i][FileID] != 0 {
			f(extentID, &ms.tinyExtents[i])
		}
	}
}

func (ms *ExtentInfoStore) RangeDist(extentType uint8, f func(extentID uint64, ei *ExtentInfoBlock)) {
	if extentType == proto.NormalExtentType {
		ms.RangeNormalExtent(f)
	} else if extentType == proto.TinyExtentType {
		ms.RangeTinyExtent(f)
	} else if extentType == proto.AllExtentType {
		ms.Range(f)
	}
}

func (ms *ExtentInfoStore) Len() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var tinyCount int
	for _, tinyExtent := range ms.tinyExtents {
		if tinyExtent[FileID] != 0 {
			tinyCount++
		}
	}
	return len(ms.normalIndex) + tinyCount
}

func (ms *ExtentInfoStore) Delete(extentID uint64) {
	if proto.IsTinyExtent(extentID) {
		return
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if curIdx, ok := ms.normalIndex[extentID]; ok {
		delete(ms.normalIndex, extentID)
		var deletedSize = ms.normalExtents[curIdx][Size]
		ms.normalExtents[curIdx] = EmptyExtentBlock
		ms.normalDeletedCnt++
		ms.normalUsed -= deletedSize
	}
	ms.reduceObjSlice()
}

func (ms *ExtentInfoStore) reduceObjSlice() {
	if ms.normalDeletedCnt >= reduceThresholdSize {
		newObjSlice := make([]ExtentInfoBlock, 0, len(ms.normalIndex))
		newIdxMap := make(map[uint64]uint64, len(ms.normalIndex))
		var idxCount uint64 = 0
		for _, obj := range ms.normalExtents {
			if obj[FileID] != 0 {
				newObjSlice = append(newObjSlice, obj)
				newIdxMap[obj[FileID]] = idxCount
				idxCount++
			}
		}
		ms.normalExtents = nil
		ms.normalExtents = newObjSlice
		ms.normalIndex = nil
		ms.normalIndex = newIdxMap
		ms.normalDeletedCnt = 0
	}
}

package metanode

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/bitmap"
	"sync"
)

const (
	bitPerRegion             = 8 * 1024
)

type inoAllocatorV2 struct {
	mu           sync.RWMutex
	L1Bits		 bitmap.U64BitMap
	Bits         []bitmap.U64BitMap
	Start        uint64
	End          uint64
	Cnt          uint64
	Used         uint64
	LastBitIndex int
	Status       int8
}

func NewInoAllocatorV2(start, end uint64)  *inoAllocatorV2 {
	if end <= start {
		panic(fmt.Errorf("error inode section, start: %v, end: %v", start, end))
	}

	cnt := end - start
	if cnt > proto.DefaultMetaPartitionInodeIDStep {
		cnt = proto.DefaultMetaPartitionInodeIDStep
	}
	allocator := &inoAllocatorV2{
		Cnt:   cnt,
		Start: start,
		End:   start + cnt,
	}
	l1BitsCnt := cnt / bitPerRegion
	if cnt % bitPerRegion != 0 {
		l1BitsCnt += bitPerU64
	}
	allocator.L1Bits = make([]uint64, l1BitsCnt / bitPerU64)
	allocator.Bits = make([]bitmap.U64BitMap, l1BitsCnt)
	for i := 0; i < int(l1BitsCnt); i++{
		//128 * 64 = 8 * 1024
		allocator.Bits[i] = make([]uint64, 128)
	}

	if cnt % bitPerRegion != 0 {
		for l2Index := int(cnt) % bitPerRegion; l2Index < bitPerRegion; l2Index++ {
			allocator.Bits[l1BitsCnt - 1].SetBit(l2Index)
		}
	}
	return allocator
}

func (allocator *inoAllocatorV2)AllocateId() (id uint64, err error) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	if allocator.Status != allocatorStatusAvailable {
		return 0, fmt.Errorf("allocator not start")
	}

	l1LastIndex := allocator.LastBitIndex / bitPerRegion
	l1Index := l1LastIndex
	l2Index := 0

	//find cur region
	if allocator.L1Bits.IsBitFree(l1LastIndex) {
		l2Index, _ = allocator.Bits[l1LastIndex].GetFirstFreeBit(allocator.LastBitIndex % bitPerRegion, false)
		if l2Index != -1 {
			//find free
			goto CalId
		}
	}

	//find other region
	for ; ; {
		l1LastIndex = l1Index + 1
		l1Index, _ = allocator.L1Bits.GetFirstFreeBit(l1LastIndex, true)
		if l1Index == -1 {
			//no free bit
			break
		}

		l2Index, _ = allocator.Bits[l1Index].GetFirstFreeBit(0, false)
		if l2Index == -1 {
			//this region is full set l1 bit
			allocator.L1Bits.SetBit(l1Index)
			continue
		}
		goto CalId
	}
	return 0, fmt.Errorf("bit map is full")

CalId:
	allocator.LastBitIndex = l1Index * bitPerRegion + l2Index
	id = uint64(allocator.LastBitIndex) + allocator.Start
	allocator.Bits[l1Index].SetBit(l2Index)
	return
}

func (allocator *inoAllocatorV2)InnerSetId(id uint64) (updateUsed bool) {
	allocator.LastBitIndex = int (id - allocator.Start)
	l1Index := allocator.LastBitIndex / bitPerRegion
	l2Index := allocator.LastBitIndex % bitPerRegion
	if allocator.Bits[l1Index].IsBitFree(l2Index) {
		allocator.Bits[l1Index].SetBit(l2Index)
		updateUsed = true
	}
	return
}

func (allocator *inoAllocatorV2)InnerClearId(id uint64) (updateUsed bool){
	l1Index := int(id - allocator.Start) / bitPerRegion
	l2Index := int(id - allocator.Start) % bitPerRegion
	allocator.L1Bits.ClearBit(l1Index)
	if !allocator.Bits[l1Index].IsBitFree(l2Index) {
		allocator.Bits[l1Index].ClearBit(int((id - allocator.Start) % bitPerRegion))
		updateUsed = true
	}
	return
}

func (allocator *inoAllocatorV2)SetId(id uint64) {
	if id >= allocator.End || id < allocator.Start {
		return
	}
	allocator.mu.Lock()
	allocator.mu.Unlock()
	if allocator.Status == allocatorStatusUnavailable {
		return
	}
	if allocator.InnerSetId(id) {
		allocator.Used++
	}
}

func (allocator *inoAllocatorV2)ClearId(id uint64) {
	if id >= allocator.End || id < allocator.Start{
		return
	}
	allocator.mu.Lock()
	allocator.mu.Unlock()
	if allocator.Status == allocatorStatusUnavailable {
		return
	}
	if allocator.InnerClearId(id) {
		allocator.Used--
	}
}


func (allocator *inoAllocatorV2)GetUsed() uint64{
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()
	return allocator.Used
}

func (allocator *inoAllocatorV2)ReleaseBitMapMemory() {
	if allocator.Cnt != 0 {
		allocator.Bits = make([]bitmap.U64BitMap, 0)
		allocator.L1Bits = make([]uint64, 0)
		allocator.Cnt = 0
		allocator.End = 0
	}
}

func (allocator *inoAllocatorV2) changeStatusToUnavailable() (err error) {
	allocator.Status = allocatorStatusUnavailable
	allocator.ReleaseBitMapMemory()
	return
}

func (allocator *inoAllocatorV2) changeStatusToInit() (err error) {
	if allocator.Status == allocatorStatusAvailable {
		return fmt.Errorf("can not change status available to init")
	}

	allocator.Status = allocatorStatusInit
	return
}

func (allocator *inoAllocatorV2) changeStatusToAvailable() (err error) {
	if allocator.Status == allocatorStatusUnavailable {
		return fmt.Errorf("can not change status unavailable to available")
	}

	allocator.Status = allocatorStatusAvailable
	return
}

func (allocator *inoAllocatorV2) SetStatus(newStatus int8) (err error) {
	if newStatus > allocatorStatusAvailable || newStatus < allocatorStatusUnavailable {
		err = fmt.Errorf("unknown status %v", newStatus)
		return
	}
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	switch newStatus {
	case allocatorStatusUnavailable:
		err = allocator.changeStatusToUnavailable()
	case allocatorStatusInit:
		err = allocator.changeStatusToInit()
	case allocatorStatusAvailable:
		err = allocator.changeStatusToAvailable()
	default:
		err = fmt.Errorf("unknown new status:%d", newStatus)
	}
	return
}

func (allocator *inoAllocatorV2)GetStatus() int8 {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()
	return allocator.Status
}

func (allocator *inoAllocatorV2)ResetLastBitIndex() {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()
	allocator.LastBitIndex = 0
}

func (allocator *inoAllocatorV2)IsBitFree(id uint64) bool {
	if id >= allocator.End || id < allocator.Start {
		return false
	}
	l1Index  := int(id - allocator.Start) / bitPerRegion
	l2Index  := int(id - allocator.Start) % bitPerRegion
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()
	return allocator.Bits[l1Index].IsBitFree(l2Index)
}
package metanode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sync"

	"github.com/cubefs/cubefs/proto"
)

type SortedObjExtents struct {
	sync.RWMutex
	eks []proto.ObjExtentKey
}

func NewSortedObjExtents() *SortedObjExtents {
	return &SortedObjExtents{
		eks: make([]proto.ObjExtentKey, 0),
	}
}

func (se *SortedObjExtents) String() string {
	se.RLock()
	data, err := json.Marshal(se.eks)
	se.RUnlock()
	if err != nil {
		return ""
	}
	return string(data)
}

func (se *SortedObjExtents) MarshalBinary() ([]byte, error) {
	var data []byte

	se.RLock()
	defer se.RUnlock()

	for _, ek := range se.eks {
		ekdata, err := ek.MarshalBinary()
		if err != nil {
			return nil, err
		}
		data = append(data, ekdata...)
	}
	return data, nil
}

func (se *SortedObjExtents) UnmarshalBinary(data []byte) error {
	var ek proto.ObjExtentKey

	se.Lock()
	defer se.Unlock()

	buf := bytes.NewBuffer(data)
	for {
		if buf.Len() == 0 {
			break
		}
		if err := ek.UnmarshalBinary(buf); err != nil {
			return err
		}
		// Don't use se.Append here, since we need to retain the raw ek order.
		se.eks = append(se.eks, ek)
	}
	return nil
}

// Append will return error if the objextentkey exist overlap.
func (se *SortedObjExtents) Append(ek proto.ObjExtentKey) (err error) {
	se.Lock()
	defer se.Unlock()
	// 1. list is empty
	if len(se.eks) <= 0 {
		se.eks = append(se.eks, ek)
		return
	}
	// 2. last key's (fileoffset+size) is equal to new one
	lastKey := se.eks[len(se.eks)-1]
	if (lastKey.FileOffset + lastKey.Size) == ek.FileOffset {
		se.eks = append(se.eks, ek)
		return
	}

	// fix: find one key is equals to the new one, if not, return error.
	for i := len(se.eks) - 1; i >= 0; i-- {
		if ek.IsEquals(&se.eks[i]) {
			return
		}
		if se.eks[i].FileOffset < ek.FileOffset {
			break
		}
	}

	err = fmt.Errorf("obj extentkeys exist overlay! the new obj extent key must be appended to the last position with offset [%d], new(%s)",
		lastKey.FileOffset, ek.String())
	return
}

func (se *SortedObjExtents) Clone() *SortedObjExtents {
	newSe := NewSortedObjExtents()

	se.RLock()
	defer se.RUnlock()

	newSe.eks = se.doCopyExtents()
	return newSe
}

func (se *SortedObjExtents) doCopyExtents() []proto.ObjExtentKey {
	eks := make([]proto.ObjExtentKey, len(se.eks))
	copy(eks, se.eks)
	return eks
}

func (se *SortedObjExtents) CopyExtents() []proto.ObjExtentKey {
	se.RLock()
	defer se.RUnlock()
	return se.doCopyExtents()
}

// Returns the file size
func (se *SortedObjExtents) Size() uint64 {
	se.RLock()
	defer se.RUnlock()

	last := len(se.eks)
	if last <= 0 {
		return uint64(0)
	}
	// TODO: maybe we should use ebs location's Size?
	return se.eks[last-1].FileOffset + se.eks[last-1].Size
}

func (se *SortedObjExtents) Range(f func(ek proto.ObjExtentKey) bool) {
	se.RLock()
	defer se.RUnlock()

	for _, ek := range se.eks {
		if !f(ek) {
			break
		}
	}
}

func (se *SortedObjExtents) FindOffsetExist(fileOffset uint64) (bool, int) {
	se.RLock()
	defer se.RUnlock()

	if len(se.eks) <= 0 {
		return false, 0
	}
	left, right, mid := 0, len(se.eks)-1, 0
	for {
		mid = int(math.Floor(float64((left + right) / 2)))
		if se.eks[mid].FileOffset > fileOffset {
			right = mid - 1
		} else if se.eks[mid].FileOffset < fileOffset {
			left = mid + 1
		} else {
			return true, mid
		}
		if left > right {
			break
		}
	}
	return false, 0
}

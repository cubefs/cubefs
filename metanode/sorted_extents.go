package metanode

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/chubaofs/chubaofs/proto"
)

type SortedExtents struct {
	sync.RWMutex
	eks []proto.ExtentKey
}

func NewSortedExtents() *SortedExtents {
	return &SortedExtents{
		eks: make([]proto.ExtentKey, 0),
	}
}

func (se *SortedExtents) String() string {
	se.RLock()
	data, err := json.Marshal(se.eks)
	se.RUnlock()
	if err != nil {
		return ""
	}
	return string(data)
}

func (se *SortedExtents) MarshalBinary() ([]byte, error) {
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

func (se *SortedExtents) UnmarshalBinary(data []byte) error {
	var ek proto.ExtentKey

	buf := bytes.NewBuffer(data)
	for {
		if buf.Len() == 0 {
			break
		}
		if err := ek.UnmarshalBinary(buf); err != nil {
			return err
		}
		se.Append(ek)
	}
	return nil
}

func (se *SortedExtents) Append(ek proto.ExtentKey) (deleteExtents []proto.ExtentKey) {
	deleteExtents = make([]proto.ExtentKey, 0)
	endOffset := ek.FileOffset + uint64(ek.Size)

	se.Lock()
	defer se.Unlock()

	if len(se.eks) <= 0 {
		se.eks = append(se.eks, ek)
		return
	}
	lastKey := se.eks[len(se.eks)-1]
	if lastKey.FileOffset+uint64(lastKey.Size) <= ek.FileOffset {
		se.eks = append(se.eks, ek)
		return
	}
	firstKey := se.eks[0]
	if firstKey.FileOffset >= endOffset {
		eks := se.doCopyExtents()
		se.eks = se.eks[:0]
		se.eks = append(se.eks, ek)
		se.eks = append(se.eks, eks...)
		return
	}

	var startIndex, endIndex int

	for idx, key := range se.eks {
		if ek.FileOffset >= key.FileOffset+uint64(key.Size) {
			startIndex = idx + 1
			continue
		}
		if endOffset >= key.FileOffset+uint64(key.Size) {
			deleteExtents = append(deleteExtents, key)
			continue
		}
		break
	}

	endIndex = startIndex + len(deleteExtents)
	upperExtents := make([]proto.ExtentKey, len(se.eks)-endIndex)
	copy(upperExtents, se.eks[endIndex:])
	se.eks = se.eks[:startIndex]
	se.eks = append(se.eks, ek)
	se.eks = append(se.eks, upperExtents...)
	return
}

func (se *SortedExtents) Truncate(offset uint64) (deleteExtents []proto.ExtentKey) {
	var endIndex int

	se.Lock()
	defer se.Unlock()

	endIndex = -1
	for idx, key := range se.eks {
		if key.FileOffset >= offset {
			endIndex = idx
			break
		}
	}

	if endIndex < 0 {
		deleteExtents = make([]proto.ExtentKey, 0)
	} else {
		deleteExtents = make([]proto.ExtentKey, len(se.eks)-endIndex)
		copy(deleteExtents, se.eks[endIndex:])
		se.eks = se.eks[:endIndex]
	}

	numKeys := len(se.eks)
	if numKeys > 0 {
		lastKey := &se.eks[numKeys-1]
		if lastKey.FileOffset+uint64(lastKey.Size) > offset {
			lastKey.Size = uint32(offset - lastKey.FileOffset)
		}
	}
	return
}

func (se *SortedExtents) Len() int {
	se.RLock()
	defer se.RUnlock()
	return len(se.eks)
}

// Returns the file size
func (se *SortedExtents) Size() uint64 {
	se.RLock()
	defer se.RUnlock()

	last := len(se.eks)
	if last <= 0 {
		return uint64(0)
	}
	return se.eks[last-1].FileOffset + uint64(se.eks[last-1].Size)
}

func (se *SortedExtents) Range(f func(ek proto.ExtentKey) bool) {
	se.RLock()
	defer se.RUnlock()

	for _, ek := range se.eks {
		if !f(ek) {
			break
		}
	}
}

func (se *SortedExtents) Clone() *SortedExtents {
	newSe := NewSortedExtents()

	se.RLock()
	defer se.RUnlock()

	newSe.eks = se.doCopyExtents()
	return newSe
}

func (se *SortedExtents) CopyExtents() []proto.ExtentKey {
	se.RLock()
	defer se.RUnlock()
	return se.doCopyExtents()
}

func (se *SortedExtents) doCopyExtents() []proto.ExtentKey {
	eks := make([]proto.ExtentKey, len(se.eks))
	copy(eks, se.eks)
	return eks
}

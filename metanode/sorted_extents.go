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

func (se *SortedExtents) Append(ek proto.ExtentKey) (deleteExtents []proto.ExtentKey) {
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

	invalidExtents := make([]proto.ExtentKey, 0)
	for idx, key := range se.eks {
		if ek.FileOffset > key.FileOffset {
			startIndex = idx + 1
			continue
		}
		if endOffset >= key.FileOffset+uint64(key.Size) {
			invalidExtents = append(invalidExtents, key)
			continue
		}
		break
	}

	endIndex = startIndex + len(invalidExtents)
	upperExtents := make([]proto.ExtentKey, len(se.eks)-endIndex)
	copy(upperExtents, se.eks[endIndex:])
	se.eks = se.eks[:startIndex]
	se.eks = append(se.eks, ek)
	se.eks = append(se.eks, upperExtents...)
	// check if ek and key are the same extent file with size extented
	deleteExtents = make([]proto.ExtentKey, 0, len(invalidExtents))
	for _, key := range invalidExtents {
		if key.PartitionId != ek.PartitionId || key.ExtentId != ek.ExtentId {
			deleteExtents = append(deleteExtents, key)
		}
	}
	return
}

func (se *SortedExtents) AppendWithCheck(ek proto.ExtentKey, discard []proto.ExtentKey) (deleteExtents []proto.ExtentKey, status uint8) {
	status = proto.OpOk
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

	invalidExtents := make([]proto.ExtentKey, 0)
	for idx, key := range se.eks {
		if ek.FileOffset > key.FileOffset {
			startIndex = idx + 1
			continue
		}
		if endOffset >= key.FileOffset+uint64(key.Size) {
			invalidExtents = append(invalidExtents, key)
			continue
		}
		break
	}

	// check if ek and key are the same extent file with size extented
	deleteExtents = make([]proto.ExtentKey, 0, len(invalidExtents))
	for _, key := range invalidExtents {
		if key.PartitionId != ek.PartitionId || key.ExtentId != ek.ExtentId || key.ExtentOffset != ek.ExtentOffset {
			deleteExtents = append(deleteExtents, key)
		}
	}

	//log.LogInfof("invalidExtents(%v) deleteExtents(%v) discardExtents(%v)", invalidExtents, deleteExtents, discard)

	if discard != nil {
		if len(deleteExtents) != len(discard) {
			return deleteExtents, proto.OpConflictExtentsErr
		}
		for i := 0; i < len(discard); i++ {
			if deleteExtents[i].PartitionId != discard[i].PartitionId || deleteExtents[i].ExtentId != discard[i].ExtentId || deleteExtents[i].ExtentOffset != discard[i].ExtentOffset {
				return deleteExtents, proto.OpConflictExtentsErr
			}
		}
	} else if len(deleteExtents) != 0 {
		return deleteExtents, proto.OpConflictExtentsErr
	}

	endIndex = startIndex + len(invalidExtents)
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

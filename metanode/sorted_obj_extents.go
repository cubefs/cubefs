package metanode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/proto"
)

// SortedObjExtents manages a sorted collection of object extent keys
// with thread-safe operations for concurrent access
type SortedObjExtents struct {
	sync.RWMutex
	eks []proto.ObjExtentKey
}

// NewSortedObjExtents creates a new empty SortedObjExtents instance
func NewSortedObjExtents() *SortedObjExtents {
	return &SortedObjExtents{
		eks: make([]proto.ObjExtentKey, 0),
	}
}

// NewSortedObjExtentsFromObjEks creates a new SortedObjExtents from existing extent keys
func NewSortedObjExtentsFromObjEks(eks []proto.ObjExtentKey) *SortedObjExtents {
	keys := make([]proto.ObjExtentKey, len(eks))
	copy(keys, eks)
	return &SortedObjExtents{
		eks: keys,
	}
}

// String returns JSON representation of the extent keys
func (se *SortedObjExtents) String() string {
	se.RLock()
	data, err := json.Marshal(se.eks)
	se.RUnlock()

	if err != nil {
		return ""
	}
	return string(data)
}

// IsEmpty checks if the extent keys collection is empty
func (se *SortedObjExtents) IsEmpty() bool {
	se.RLock()
	defer se.RUnlock()
	return len(se.eks) == 0
}

// MarshalBinary serializes the extent keys to binary format
func (se *SortedObjExtents) MarshalBinary() ([]byte, error) {
	var data []byte

	se.RLock()
	defer se.RUnlock()

	for _, ek := range se.eks {
		ekData, err := ek.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal extent key: %w", err)
		}
		data = append(data, ekData...)
	}
	return data, nil
}

// UnmarshalBinary deserializes extent keys from binary format
func (se *SortedObjExtents) UnmarshalBinary(data []byte) error {
	se.Lock()
	defer se.Unlock()

	// Clear existing data
	se.eks = se.eks[:0]

	buf := bytes.NewBuffer(data)
	var ek proto.ObjExtentKey

	for buf.Len() > 0 {
		if err := ek.UnmarshalBinary(buf); err != nil {
			return fmt.Errorf("failed to unmarshal extent key: %w", err)
		}
		// Don't use se.Append here, since we need to retain the raw ek order
		se.eks = append(se.eks, ek)
	}
	return nil
}

// Append adds a new extent key to the collection
// Returns error if the extent key overlaps with existing ones
func (se *SortedObjExtents) Append(ek proto.ObjExtentKey) error {
	se.Lock()
	defer se.Unlock()

	// Handle empty collection
	if len(se.eks) == 0 {
		se.eks = append(se.eks, ek)
		return nil
	}

	// Check if new key can be appended to the end (consecutive)
	lastKey := se.eks[len(se.eks)-1]
	if lastKey.FileOffset+lastKey.Size == ek.FileOffset {
		se.eks = append(se.eks, ek)
		return nil
	}

	// Check for duplicates and validate ordering
	for i := len(se.eks) - 1; i >= 0; i-- {
		if ek.IsEquals(&se.eks[i]) {
			return nil // Duplicate found, no error
		}
		if se.eks[i].FileOffset < ek.FileOffset {
			break
		}
	}

	// Return error for overlapping extent keys
	return fmt.Errorf("extent keys overlap detected: new extent key must be appended to last position with offset [%d], new key: %s",
		lastKey.FileOffset, ek.String())
}

// Clone creates a deep copy of the SortedObjExtents
func (se *SortedObjExtents) Clone() *SortedObjExtents {
	se.RLock()
	defer se.RUnlock()

	newSe := &SortedObjExtents{
		eks: se.doCopyExtents(),
	}
	return newSe
}

// doCopyExtents creates a copy of the extent keys slice
func (se *SortedObjExtents) doCopyExtents() []proto.ObjExtentKey {
	eks := make([]proto.ObjExtentKey, len(se.eks))
	copy(eks, se.eks)
	return eks
}

// CopyExtents returns a copy of the extent keys
func (se *SortedObjExtents) CopyExtents() []proto.ObjExtentKey {
	se.RLock()
	defer se.RUnlock()
	return se.doCopyExtents()
}

// Size returns the total file size based on the last extent key
func (se *SortedObjExtents) Size() uint64 {
	se.RLock()
	defer se.RUnlock()

	if len(se.eks) == 0 {
		return 0
	}

	lastKey := se.eks[len(se.eks)-1]
	return lastKey.FileOffset + lastKey.Size
}

// LayerSize returns the sum of all extent key sizes
func (se *SortedObjExtents) LayerSize() uint64 {
	se.RLock()
	defer se.RUnlock()

	var layerSize uint64
	for _, ek := range se.eks {
		layerSize += ek.Size
	}
	return layerSize
}

// Range iterates over extent keys and calls the provided function
// Stops iteration if the function returns false
func (se *SortedObjExtents) Range(f func(ek proto.ObjExtentKey) bool) {
	se.RLock()
	defer se.RUnlock()

	for _, ek := range se.eks {
		if !f(ek) {
			break
		}
	}
}

// FindOffsetExist performs binary search to find if a file offset exists
// Returns true and index if found, false and 0 if not found
func (se *SortedObjExtents) FindOffsetExist(fileOffset uint64) (bool, int) {
	se.RLock()
	defer se.RUnlock()

	if len(se.eks) == 0 {
		return false, 0
	}

	left, right := 0, len(se.eks)-1

	for left <= right {
		mid := left + (right-left)/2 // Avoid potential overflow
		midOffset := se.eks[mid].FileOffset

		if midOffset > fileOffset {
			right = mid - 1
		} else if midOffset < fileOffset {
			left = mid + 1
		} else {
			return true, mid
		}
	}

	return false, 0
}

// Equals compares two SortedObjExtents for equality
func (se *SortedObjExtents) Equals(other *SortedObjExtents) bool {
	se.RLock()
	defer se.RUnlock()

	if other == nil {
		return false
	}

	if len(se.eks) != len(other.eks) {
		return false
	}

	for i, seKey := range se.eks {
		if !seKey.IsEquals(&other.eks[i]) {
			return false
		}
	}

	return true
}

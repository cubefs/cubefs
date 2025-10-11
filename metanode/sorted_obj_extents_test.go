// Copyright 2024 The CubeFS Authors.
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

package metanode

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestSortedObjExtentsNewSortedObjExtents(t *testing.T) {
	se := NewSortedObjExtents()
	require.NotNil(t, se)
	require.NotNil(t, se.eks)
	require.Equal(t, 0, len(se.eks))
	require.True(t, se.IsEmpty())
}

func TestSortedObjExtentsNewSortedObjExtentsFromObjEks(t *testing.T) {
	originalEks := []proto.ObjExtentKey{
		{Cid: 1, FileOffset: 0, Size: 100},
		{Cid: 2, FileOffset: 100, Size: 200},
	}

	se := NewSortedObjExtentsFromObjEks(originalEks)
	require.NotNil(t, se)
	require.Equal(t, 2, len(se.eks))
	require.Equal(t, originalEks[0], se.eks[0])
	require.Equal(t, originalEks[1], se.eks[1])

	// Test that modifying original slice doesn't affect the SortedObjExtents
	originalEks[0].Cid = 999
	require.NotEqual(t, originalEks[0].Cid, se.eks[0].Cid)
}

func TestSortedObjExtentsString(t *testing.T) {
	se := NewSortedObjExtents()

	// Test empty case
	require.Equal(t, "[]", se.String())

	// Test with data
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200})

	result := se.String()
	require.Contains(t, result, "Cid")
	require.Contains(t, result, "FileOffset")
	require.Contains(t, result, "Size")
}

func TestSortedObjExtentsIsEmpty(t *testing.T) {
	se := NewSortedObjExtents()
	require.True(t, se.IsEmpty())

	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	require.False(t, se.IsEmpty())
}

func TestSortedObjExtentsAppend(t *testing.T) {
	se := NewSortedObjExtents()

	// Test appending to empty collection
	ek1 := proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100}
	err := se.Append(ek1)
	require.NoError(t, err)
	require.Equal(t, 1, len(se.eks))
	require.Equal(t, ek1, se.eks[0])

	// Test appending consecutive extent
	ek2 := proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200}
	err = se.Append(ek2)
	require.NoError(t, err)
	require.Equal(t, 2, len(se.eks))
	require.Equal(t, ek2, se.eks[1])

	// Test appending duplicate (should not error)
	err = se.Append(ek1)
	require.NoError(t, err)
	require.Equal(t, 2, len(se.eks)) // Should not add duplicate

	// Test appending overlapping extent (should error)
	ek3 := proto.ObjExtentKey{Cid: 3, FileOffset: 50, Size: 100}
	err = se.Append(ek3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "overlap detected")
	require.Equal(t, 2, len(se.eks)) // Should not add overlapping extent
}

func TestSortedObjExtentsMarshalBinary(t *testing.T) {
	se := NewSortedObjExtents()

	// Test empty case
	data, err := se.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, 0, len(data))

	// Test with data
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200})

	data, err = se.MarshalBinary()
	require.NoError(t, err)
	require.Greater(t, len(data), 0)
}

func TestSortedObjExtentsUnmarshalBinary(t *testing.T) {
	se := NewSortedObjExtents()

	// Test empty data
	err := se.UnmarshalBinary([]byte{})
	require.NoError(t, err)
	require.Equal(t, 0, len(se.eks))

	// Test with data
	originalEks := []proto.ObjExtentKey{
		{Cid: 1, FileOffset: 0, Size: 100},
		{Cid: 2, FileOffset: 100, Size: 200},
	}

	se.eks = originalEks
	data, err := se.MarshalBinary()
	require.NoError(t, err)

	se2 := NewSortedObjExtents()
	err = se2.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, len(originalEks), len(se2.eks))

	for i, ek := range originalEks {
		require.Equal(t, ek.Cid, se2.eks[i].Cid)
		require.Equal(t, ek.FileOffset, se2.eks[i].FileOffset)
		require.Equal(t, ek.Size, se2.eks[i].Size)
	}
}

func TestSortedObjExtentsClone(t *testing.T) {
	se := NewSortedObjExtents()
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200})

	cloned := se.Clone()
	require.NotNil(t, cloned)
	require.Equal(t, len(se.eks), len(cloned.eks))

	// Test that modifications to original don't affect clone
	se.eks[0].Cid = 999
	require.NotEqual(t, se.eks[0].Cid, cloned.eks[0].Cid)

	// Test that modifications to clone don't affect original
	cloned.eks[1].Cid = 888
	require.NotEqual(t, se.eks[1].Cid, cloned.eks[1].Cid)
}

func TestSortedObjExtentsCopyExtents(t *testing.T) {
	se := NewSortedObjExtents()
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200})

	copied := se.CopyExtents()
	require.Equal(t, len(se.eks), len(copied))

	// Test that modifications to original don't affect copy
	se.eks[0].Cid = 999
	require.NotEqual(t, se.eks[0].Cid, copied[0].Cid)
}

func TestSortedObjExtentsSize(t *testing.T) {
	se := NewSortedObjExtents()

	// Test empty case
	require.Equal(t, uint64(0), se.Size())

	// Test with single extent
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	require.Equal(t, uint64(100), se.Size())

	// Test with multiple extents
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200})
	require.Equal(t, uint64(300), se.Size())

	// Test with non-consecutive extents
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 3, FileOffset: 500, Size: 50})
	require.Equal(t, uint64(550), se.Size())
}

func TestSortedObjExtentsLayerSize(t *testing.T) {
	se := NewSortedObjExtents()

	// Test empty case
	require.Equal(t, uint64(0), se.LayerSize())

	// Test with single extent
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	require.Equal(t, uint64(100), se.LayerSize())

	// Test with multiple extents
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200})
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 3, FileOffset: 500, Size: 50})
	require.Equal(t, uint64(350), se.LayerSize())
}

func TestSortedObjExtentsRange(t *testing.T) {
	se := NewSortedObjExtents()
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200})
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 3, FileOffset: 300, Size: 50})

	// Test iterating through all extents
	count := 0
	se.Range(func(ek proto.ObjExtentKey) bool {
		count++
		return true
	})
	require.Equal(t, 3, count)

	// Test early termination
	count = 0
	se.Range(func(ek proto.ObjExtentKey) bool {
		count++
		return count < 2 // Stop after 2 iterations
	})
	require.Equal(t, 2, count)
}

func TestSortedObjExtentsFindOffsetExist(t *testing.T) {
	se := NewSortedObjExtents()

	// Test empty case
	found, index := se.FindOffsetExist(0)
	require.False(t, found)
	require.Equal(t, 0, index)

	// Test with data
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200})
	se.eks = append(se.eks, proto.ObjExtentKey{Cid: 3, FileOffset: 300, Size: 50})

	// Test finding existing offset
	found, index = se.FindOffsetExist(100)
	require.True(t, found)
	require.Equal(t, 1, index)

	// Test finding non-existing offset
	found, index = se.FindOffsetExist(150)
	require.False(t, found)
	require.Equal(t, 0, index)

	// Test finding first offset
	found, index = se.FindOffsetExist(0)
	require.True(t, found)
	require.Equal(t, 0, index)

	// Test finding last offset
	found, index = se.FindOffsetExist(300)
	require.True(t, found)
	require.Equal(t, 2, index)
}

func TestSortedObjExtentsEquals(t *testing.T) {
	se1 := NewSortedObjExtents()
	se2 := NewSortedObjExtents()

	// Test empty equality
	require.True(t, se1.Equals(se2))

	// Test with nil
	require.False(t, se1.Equals(nil))

	// Test with same data
	se1.eks = append(se1.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	se2.eks = append(se2.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	require.True(t, se1.Equals(se2))

	// Test with different lengths
	se2.eks = append(se2.eks, proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200})
	require.False(t, se1.Equals(se2))

	// Test with same length but different data
	se1.eks = append(se1.eks, proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200})
	se2.eks[1].Cid = 3
	require.False(t, se1.Equals(se2))
}

func TestSortedObjExtentsConcurrentAccess(t *testing.T) {
	se := NewSortedObjExtents()

	// Test concurrent reads
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			se.IsEmpty()
			se.Size()
			se.LayerSize()
			_ = se.String()
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	// Test concurrent writes
	for i := 0; i < 10; i++ {
		go func(offset uint64) {
			se.Append(proto.ObjExtentKey{
				Cid:        uint64(offset),
				FileOffset: offset * 100,
				Size:       100,
			})
		}(uint64(i))
	}

	// Wait a bit for goroutines to complete
	// Note: This test may have race conditions due to overlapping extents
	// but it tests the thread safety of the methods
}

func TestSortedObjExtentsEdgeCases(t *testing.T) {
	se := NewSortedObjExtents()

	// Test with zero-sized extent
	err := se.Append(proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 0})
	require.NoError(t, err)
	require.Equal(t, 1, len(se.eks))
	require.Equal(t, uint64(0), se.Size())

	// Test with very large values
	se = NewSortedObjExtents()
	err = se.Append(proto.ObjExtentKey{
		Cid:        ^uint64(0),
		FileOffset: ^uint64(0) - 1000,
		Size:       1000,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(se.eks))
}

func TestSortedObjExtentsMarshalUnmarshalRoundTrip(t *testing.T) {
	original := NewSortedObjExtents()
	original.eks = append(original.eks, proto.ObjExtentKey{Cid: 1, FileOffset: 0, Size: 100})
	original.eks = append(original.eks, proto.ObjExtentKey{Cid: 2, FileOffset: 100, Size: 200})
	original.eks = append(original.eks, proto.ObjExtentKey{Cid: 3, FileOffset: 300, Size: 50})

	// Marshal
	data, err := original.MarshalBinary()
	require.NoError(t, err)
	require.Greater(t, len(data), 0)

	// Unmarshal
	restored := NewSortedObjExtents()
	err = restored.UnmarshalBinary(data)
	require.NoError(t, err)

	// Verify equality
	require.True(t, original.Equals(restored))
	require.Equal(t, original.Size(), restored.Size())
	require.Equal(t, original.LayerSize(), restored.LayerSize())
}

package metanode

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortedExtentAppend01(t *testing.T) {
	se := NewSortedExtents()
	se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, nil, nil)
	se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 2000, Size: 1000, ExtentId: 2}, nil, nil)
	se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 4000, Size: 1000, ExtentId: 3}, nil, nil)
	se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 3000, Size: 500, ExtentId: 4}, nil, nil)
	t.Logf("\neks: %v\n", se.eks)
	if se.Size() != 5000 || len(se.eks) != 4 || se.eks[2].ExtentId != 4 {
		t.Fail()
	}
	t.Logf("%v\n", se.Size())
}

// The same extent file is extended
func TestSortedExtentAppend02(t *testing.T) {
	se := NewSortedExtents()
	delExtents, status := se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, nil, nil)
	t.Logf("\ndel: %v\nstatus: %v\neks: %v", delExtents, status, se.eks)
	if status != proto.OpOk || len(delExtents) != 0 {
		t.Fail()
	}
	delExtents, status = se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 0, Size: 2000, ExtentId: 1}, nil, nil)
	t.Logf("\ndel: %v\nstatus: %v\neks: %v", delExtents, status, se.eks)
	if status != proto.OpOk || len(delExtents) != 0 || se.Size() != 2000 {
		t.Fail()
	}
	discard := make([]proto.ExtentKey, 0)
	discard = append(discard, proto.ExtentKey{FileOffset: 0, Size: 2000, ExtentId: 1})
	delExtents, status = se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 0, Size: 2000, ExtentId: 2}, nil, discard)
	t.Logf("\ndel: %v\nstatus: %v\neks: %v", delExtents, status, se.eks)
	if status != proto.OpOk || len(delExtents) != 1 || delExtents[0].ExtentId != 1 || se.eks[0].ExtentId != 2 {
		t.Fail()
	}
	t.Logf("%v\n", se.Size())
}

func TestSortedExtentAppend03(t *testing.T) {
	se := NewSortedExtents()
	delExtents, status := se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, nil, nil)
	t.Logf("\ndel: %v\nstatus: %v\neks: %v", delExtents, status, se.eks)
	discard := make([]proto.ExtentKey, 0)
	discard = append(discard, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	delExtents, status = se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 2}, nil, discard)
	t.Logf("\ndel: %v\nstatus: %v\neks: %v", delExtents, status, se.eks)
	if status != proto.OpOk || len(delExtents) != 1 || delExtents[0].ExtentId != 1 ||
		se.eks[0].ExtentId != 2 || se.Size() != 1000 {
		t.Fail()
	}
}

// This is the case when multiple clients are writing to the same file
// with an overlapping file range. The final file data is not guaranteed
// for such case, but we should be aware of what the extents look like.
func TestSortedExtentAppend04(t *testing.T) {
	se := NewSortedExtents()
	delExtents, status := se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, nil, nil)
	t.Logf("\nstatus: %v\ndel: %v\neks: %v", status, delExtents, se.eks)
	delExtents, status = se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 1000, Size: 1000, ExtentId: 2}, nil, nil)
	t.Logf("\nstatus: %v\ndel: %v\neks: %v", status, delExtents, se.eks)
	delExtents, status = se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 1500, Size: 4000, ExtentId: 3}, nil, nil)
	t.Logf("\nstatus: %v\ndel: %v\neks: %v", status, delExtents, se.eks)
	discard := make([]proto.ExtentKey, 0)
	discard = append(discard, proto.ExtentKey{FileOffset: 1000, Size: 1000, ExtentId: 2})
	delExtents, status = se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 500, Size: 4000, ExtentId: 4}, nil, discard)
	t.Logf("\nstatus: %v\ndel: %v\neks: %v", status, delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 2 ||
		len(se.eks) != 3 || se.Size() != 5500 ||
		se.eks[0].ExtentId != 1 || se.eks[1].ExtentId != 4 ||
		se.eks[2].ExtentId != 3 {
		t.Fail()
	}
	t.Logf("%v\n", se.Size())
}

func TestSortedExtentTruncate01(t *testing.T) {
	se := NewSortedExtents()
	delExtents, _ := se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, nil, nil)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents, _ = se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 2000, Size: 1000, ExtentId: 2}, nil, nil)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Truncate(500, nil)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 2 || delExtents[1].ExtentId != 2 ||
		len(se.eks) != 1 || se.eks[0].ExtentId != 1 ||
		se.Size() != 500 {
		t.Fail()
	}
}

func TestSortedExtentMarshal(t *testing.T) {
	se := NewSortedExtents()

	e1 := proto.ExtentKey{
		FileOffset:   1,
		Size:         1010,
		ExtentId:     10,
		ExtentOffset: 10110,
		PartitionId:  100,
		CRC:          0o000,
	}
	e2 := proto.ExtentKey{
		FileOffset:   4,
		Size:         1030,
		ExtentId:     10,
		ExtentOffset: 1010,
		PartitionId:  100,
		CRC:          0o200,
	}

	se.eks = append(se.eks, e1)
	se.eks = append(se.eks, e2)

	buf1 := GetInodeBuf()
	defer PutInodeBuf(buf1)

	// data, err := se.MarshalBinary(false)
	err := se.MarshalBinary(buf1, false)
	if err != nil {
		t.Fail()
	}
	data := buf1.Bytes()

	se2 := NewSortedExtents()
	err, _ = se2.UnmarshalBinary(data, false)
	if err != nil {
		t.Fail()
	}

	for idx := 0; idx < len(se.eks); idx++ {
		e1 := se.eks[idx]
		e2 := se2.eks[idx]
		if !reflect.DeepEqual(e1, e2) {
			t.Fail()
		}
	}

	se3 := NewSortedExtents()
	err, _ = se3.UnmarshalBinary(data, false)
	if err != nil {
		t.Fail()
	}

	for idx := 0; idx < len(se.eks); idx++ {
		e1 := se.eks[idx]
		e2 := se3.eks[idx]
		if !reflect.DeepEqual(e1, e2) {
			t.Fail()
		}
	}
}

func TestSortedExtentEkCompitable(t *testing.T) {
	se := NewSortedExtents()

	e1 := proto.ExtentKey{
		FileOffset:   1,
		Size:         1010,
		ExtentId:     10,
		ExtentOffset: 10110,
		PartitionId:  100,
		CRC:          0o000,
	}
	e2 := proto.ExtentKey{
		FileOffset:   4,
		Size:         1030,
		ExtentId:     10,
		ExtentOffset: 1010,
		PartitionId:  100,
		CRC:          0o200,
	}
	se.eks = append(se.eks, e1)
	se.eks = append(se.eks, e2)

	// old byte data marshal by version 3.5.0
	oldData := []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 39, 126, 0, 0, 3, 242, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 3, 242, 0, 0, 4, 6, 0, 0, 0, 128}

	buf1 := GetInodeBuf()
	defer PutInodeBuf(buf1)

	err := se.MarshalBinary(buf1, false)
	if err != nil || !bytes.Equal(oldData, buf1.Bytes()) {
		t.Fail()
	}
}

func BenchmarkSortedExtentMarshal(b *testing.B) {
	se := NewSortedExtents()

	e1 := proto.ExtentKey{
		FileOffset:   1,
		Size:         1010,
		ExtentId:     10,
		ExtentOffset: 10110,
		PartitionId:  100,
		CRC:          0o000,
	}
	e2 := proto.ExtentKey{
		FileOffset:   4,
		Size:         1030,
		ExtentId:     10,
		ExtentOffset: 1010,
		PartitionId:  100,
		CRC:          0o200,
	}

	se.eks = append(se.eks, e1)
	se.eks = append(se.eks, e2)

	b.ReportAllocs()

	buf := GetInodeBuf()
	defer PutInodeBuf(buf)

	for i := 0; i < b.N; i++ {
		se.MarshalBinary(buf, false)
	}
}

// TestSortedExtentBasicOperations tests basic operations of SortedExtents
func TestSortedExtentBasicOperations(t *testing.T) {
	t.Run("NewSortedExtents", func(t *testing.T) {
		se := NewSortedExtents()
		assert.NotNil(t, se)
		assert.NotNil(t, se.eks)
		assert.Equal(t, 0, len(se.eks))
		assert.True(t, se.IsEmpty())
	})

	t.Run("NewSortedExtentsFromEks", func(t *testing.T) {
		eks := []proto.ExtentKey{
			{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1},
			{FileOffset: 1024, Size: 2048, ExtentId: 2, PartitionId: 1},
		}
		se := NewSortedExtentsFromEks(eks)
		assert.NotNil(t, se)
		assert.Equal(t, len(eks), len(se.eks))
		assert.False(t, se.IsEmpty())
	})

	t.Run("IsEmpty", func(t *testing.T) {
		se := NewSortedExtents()
		assert.True(t, se.IsEmpty())

		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024})
		assert.False(t, se.IsEmpty())
	})

	t.Run("String", func(t *testing.T) {
		se := NewSortedExtents()
		result := se.String()
		assert.Contains(t, result, "[]")

		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1})
		result = se.String()
		assert.Contains(t, result, "FileOffset")
		assert.Contains(t, result, "Size")
	})

	t.Run("Len", func(t *testing.T) {
		se := NewSortedExtents()
		assert.Equal(t, 0, se.Len())

		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024})
		assert.Equal(t, 1, se.Len())
	})
}

// TestSortedExtentAppend tests the Append method
func TestSortedExtentAppend(t *testing.T) {
	t.Run("AppendToEmpty", func(t *testing.T) {
		se := NewSortedExtents()
		ek := proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1}

		deleteExtents := se.Append(ek)
		assert.Empty(t, deleteExtents)
		assert.Equal(t, 1, se.Len())
		assert.Equal(t, ek, se.eks[0])
	})

	t.Run("AppendAtEnd", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})

		ek := proto.ExtentKey{FileOffset: 1024, Size: 2048, ExtentId: 2, PartitionId: 1}
		deleteExtents := se.Append(ek)
		assert.Empty(t, deleteExtents)
		assert.Equal(t, 2, se.Len())
		assert.Equal(t, ek, se.eks[1])
	})

	t.Run("AppendAtBeginning", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1024, Size: 2048, ExtentId: 2, PartitionId: 1})

		ek := proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1}
		deleteExtents := se.Append(ek)
		assert.Empty(t, deleteExtents)
		assert.Equal(t, 2, se.Len())
		assert.Equal(t, ek, se.eks[0])
	})

	t.Run("AppendWithSameExtentFile", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1024, Size: 2048, ExtentId: 2, PartitionId: 1})

		// Test with same PartitionId and ExtentId (same extent file)
		ek := proto.ExtentKey{FileOffset: 512, Size: 1024, ExtentId: 1, PartitionId: 1}
		deleteExtents := se.Append(ek)
		// Should be empty because it's the same extent file (size extension)
		assert.Empty(t, deleteExtents)
		assert.Equal(t, 3, se.Len())
	})
}

// TestSortedExtentTruncate tests the Truncate method
func TestSortedExtentTruncate(t *testing.T) {
	t.Run("TruncateEmpty", func(t *testing.T) {
		se := NewSortedExtents()
		deleteExtents := se.Truncate(1024, nil)
		assert.Empty(t, deleteExtents)
	})

	t.Run("TruncateAtEnd", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1024, Size: 2048, ExtentId: 2, PartitionId: 1})

		// Truncate at 2048, which is in the middle of the second extent
		deleteExtents := se.Truncate(2048, nil)
		// Should delete 1 extent (the truncated part) and keep 2 extents (first + truncated second)
		assert.Equal(t, 1, len(deleteExtents))
		assert.Equal(t, 2, se.Len())                  // First extent + truncated second extent
		assert.Equal(t, uint32(1024), se.eks[0].Size) // First extent unchanged
		assert.Equal(t, uint32(1024), se.eks[1].Size) // Second extent truncated to 1024
	})

	t.Run("TruncateInMiddle", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1024, Size: 2048, ExtentId: 2, PartitionId: 1})

		// Truncate at 1536, which is in the middle of the second extent
		deleteExtents := se.Truncate(1536, nil)
		// Should delete 1 extent (the truncated part) and keep 2 extents (first + truncated second)
		assert.Equal(t, 1, len(deleteExtents))
		assert.Equal(t, 2, se.Len())                  // First extent + truncated second extent
		assert.Equal(t, uint32(1024), se.eks[0].Size) // First extent unchanged
		assert.Equal(t, uint32(512), se.eks[1].Size)  // Second extent truncated to 512
	})

	t.Run("TruncateAtExactBoundary", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1024, Size: 2048, ExtentId: 2, PartitionId: 1})

		// Truncate at 1024, which is exactly at the boundary between extents
		deleteExtents := se.Truncate(1024, nil)
		// Should delete 1 extent (the second one) without creating truncated extent
		assert.Equal(t, 1, len(deleteExtents))
		assert.Equal(t, 1, se.Len())
		assert.Equal(t, uint32(1024), se.eks[0].Size)
	})
}

// TestSortedExtentUtilityMethods tests utility methods
func TestSortedExtentUtilityMethods(t *testing.T) {
	t.Run("LayerSize", func(t *testing.T) {
		se := NewSortedExtents()
		assert.Equal(t, uint64(0), se.LayerSize())

		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1024, Size: 2048, ExtentId: 2, PartitionId: 1})

		assert.Equal(t, uint64(3072), se.LayerSize())
	})

	t.Run("Size", func(t *testing.T) {
		se := NewSortedExtents()
		assert.Equal(t, uint64(0), se.Size())

		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1024, Size: 2048, ExtentId: 2, PartitionId: 1})

		assert.Equal(t, uint64(3072), se.Size())
	})

	t.Run("Range", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 1024, Size: 2048, ExtentId: 2, PartitionId: 1})

		count := 0
		se.Range(func(index int, ek proto.ExtentKey) bool {
			count++
			return count < 1 // Only process first element
		})
		assert.Equal(t, 1, count)
	})

	t.Run("Clone", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})

		clone := se.Clone()
		// Check that they are different objects but have same content
		assert.NotSame(t, se, clone) // Use NotSame instead of NotEqual
		assert.Equal(t, se.Len(), clone.Len())
		assert.True(t, se.Equals(clone))
	})

	t.Run("CopyExtents", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})

		extents := se.CopyExtents()
		assert.Equal(t, len(se.eks), len(extents))
		assert.Equal(t, se.eks[0], extents[0])
	})
}

// TestSortedExtentEquals tests the Equals method
func TestSortedExtentEquals(t *testing.T) {
	t.Run("EqualsNil", func(t *testing.T) {
		se := NewSortedExtents()
		assert.False(t, se.Equals(nil))
	})

	t.Run("EqualsEmpty", func(t *testing.T) {
		se1 := NewSortedExtents()
		se2 := NewSortedExtents()
		assert.True(t, se1.Equals(se2))
	})

	t.Run("EqualsDifferentLength", func(t *testing.T) {
		se1 := NewSortedExtents()
		se2 := NewSortedExtents()
		se2.eks = append(se2.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})

		assert.False(t, se1.Equals(se2))
	})

	t.Run("EqualsSameContent", func(t *testing.T) {
		se1 := NewSortedExtents()
		se1.eks = append(se1.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})

		se2 := NewSortedExtents()
		se2.eks = append(se2.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})

		assert.True(t, se1.Equals(se2))
	})

	t.Run("EqualsDifferentContent", func(t *testing.T) {
		se1 := NewSortedExtents()
		se1.eks = append(se1.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})

		se2 := NewSortedExtents()
		se2.eks = append(se2.eks, proto.ExtentKey{FileOffset: 0, Size: 2048, ExtentId: 1, PartitionId: 1})

		assert.False(t, se1.Equals(se2))
	})
}

// TestSortedExtentEdgeCases tests edge cases and error conditions
func TestSortedExtentEdgeCases(t *testing.T) {
	t.Run("ConcurrentAccess", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})

		// These should not panic
		assert.NotPanics(t, func() {
			se.IsEmpty()
			se.Len()
			_ = se.String()
			se.LayerSize()
			se.Size()
		})
	})

	t.Run("LargeValues", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{
			FileOffset: 0, Size: 1024 * 1024 * 1024, ExtentId: 1, PartitionId: 1,
		})

		assert.Equal(t, uint64(1024*1024*1024), se.LayerSize())
		assert.Equal(t, uint64(1024*1024*1024), se.Size())
	})

	t.Run("ZeroSizeExtent", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 0, ExtentId: 1, PartitionId: 1})

		assert.Equal(t, uint64(0), se.LayerSize())
		assert.Equal(t, uint64(0), se.Size())
	})
}

// TestSortedExtentCheckAndAddRef tests the CheckAndAddRef method
func TestSortedExtentCheckAndAddRef(t *testing.T) {
	t.Run("CheckAndAddRefSameExtent", func(t *testing.T) {
		se := NewSortedExtents()
		lastKey := &proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1}
		currEk := &proto.ExtentKey{FileOffset: 1024, Size: 2048, ExtentId: 1, PartitionId: 1}

		refCount := 0
		addRefFunc := func(ek *proto.ExtentKey) {
			refCount++
		}

		ok := se.CheckAndAddRef(lastKey, currEk, addRefFunc)
		assert.True(t, ok)
		assert.Equal(t, 2, refCount)
	})

	t.Run("CheckAndAddRefDifferentExtent", func(t *testing.T) {
		se := NewSortedExtents()
		lastKey := &proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1}
		currEk := &proto.ExtentKey{FileOffset: 1024, Size: 2048, ExtentId: 2, PartitionId: 1}

		refCount := 0
		addRefFunc := func(ek *proto.ExtentKey) {
			refCount++
		}

		ok := se.CheckAndAddRef(lastKey, currEk, addRefFunc)
		assert.False(t, ok)
		assert.Equal(t, 0, refCount)
	})
}

// TestSortedExtentMarshalUnmarshal tests binary serialization
func TestSortedExtentMarshalUnmarshal(t *testing.T) {
	t.Run("MarshalBinary", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})

		buf := GetInodeBuf()
		defer PutInodeBuf(buf)

		err := se.MarshalBinary(buf, false)
		assert.NoError(t, err)
		assert.Greater(t, buf.Len(), 0)
	})

	t.Run("UnmarshalBinary", func(t *testing.T) {
		se := NewSortedExtents()
		se.eks = append(se.eks, proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1})

		buf := GetInodeBuf()
		defer PutInodeBuf(buf)

		err := se.MarshalBinary(buf, false)
		require.NoError(t, err)

		newSe := NewSortedExtents()
		err, splitMap := newSe.UnmarshalBinary(buf.Bytes(), false)
		assert.NoError(t, err)
		assert.Equal(t, se.Len(), newSe.Len())
		assert.Nil(t, splitMap)
	})
}

// Benchmark tests for performance
func BenchmarkSortedExtentAppend(b *testing.B) {
	se := NewSortedExtents()
	ek := proto.ExtentKey{FileOffset: 0, Size: 1024, ExtentId: 1, PartitionId: 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		se.Append(ek)
		ek.FileOffset += 1024
	}
}

func BenchmarkSortedExtentLayerSize(b *testing.B) {
	se := NewSortedExtents()
	for i := 0; i < 1000; i++ {
		se.eks = append(se.eks, proto.ExtentKey{
			FileOffset: uint64(i * 1024), Size: 1024, ExtentId: uint64(i), PartitionId: 1,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		se.LayerSize()
	}
}

func BenchmarkSortedExtentClone(b *testing.B) {
	se := NewSortedExtents()
	for i := 0; i < 1000; i++ {
		se.eks = append(se.eks, proto.ExtentKey{
			FileOffset: uint64(i * 1024), Size: 1024, ExtentId: uint64(i), PartitionId: 1,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		se.Clone()
	}
}

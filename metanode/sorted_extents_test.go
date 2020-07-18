package metanode

import (
	"testing"

	"github.com/chubaofs/chubaofs/proto"
)

func TestAppend01(t *testing.T) {
	se := NewSortedExtents()
	se.Append(proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	se.Append(proto.ExtentKey{FileOffset: 2000, Size: 1000, ExtentId: 2})
	se.Append(proto.ExtentKey{FileOffset: 4000, Size: 1000, ExtentId: 3})
	se.Append(proto.ExtentKey{FileOffset: 3000, Size: 500, ExtentId: 4})
	t.Logf("\neks: %v\n", se.eks)
	if se.Size() != 5000 || len(se.eks) != 4 || se.eks[2].ExtentId != 4 {
		t.Fail()
	}
	t.Logf("%v\n", se.Size())
}

// The same extent file is extended
func TestAppend02(t *testing.T) {
	se := NewSortedExtents()
	delExtents := se.Append(proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	delExtents = se.Append(proto.ExtentKey{FileOffset: 0, Size: 2000, ExtentId: 1})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 0 || se.Size() != 2000 {
		t.Fail()
	}
	delExtents = se.Append(proto.ExtentKey{FileOffset: 0, Size: 2000, ExtentId: 2})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 1 || se.eks[0].ExtentId != 2 {
		t.Fail()
	}
	t.Logf("%v\n", se.Size())
}

func TestAppend03(t *testing.T) {
	se := NewSortedExtents()
	delExtents := se.Append(proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 2})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 1 ||
		se.eks[0].ExtentId != 2 || se.Size() != 1000 {
		t.Fail()
	}
}

// This is the case when multiple clients are writing to the same file
// with an overlapping file range. The final file data is not guaranteed
// for such case, but we should be aware of what the extents look like.
func TestAppend04(t *testing.T) {
	se := NewSortedExtents()
	delExtents := se.Append(proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(proto.ExtentKey{FileOffset: 1000, Size: 1000, ExtentId: 2})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(proto.ExtentKey{FileOffset: 1500, Size: 4000, ExtentId: 3})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(proto.ExtentKey{FileOffset: 500, Size: 4000, ExtentId: 4})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 2 ||
		len(se.eks) != 3 || se.Size() != 5500 ||
		se.eks[0].ExtentId != 1 || se.eks[1].ExtentId != 4 ||
		se.eks[2].ExtentId != 3 {
		t.Fail()
	}
	t.Logf("%v\n", se.Size())
}

func TestTruncate01(t *testing.T) {
	se := NewSortedExtents()
	delExtents := se.Append(proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Append(proto.ExtentKey{FileOffset: 2000, Size: 1000, ExtentId: 2})
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Truncate(500)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 1 || delExtents[0].ExtentId != 2 ||
		len(se.eks) != 1 || se.eks[0].ExtentId != 1 ||
		se.Size() != 500 {
		t.Fail()
	}
}

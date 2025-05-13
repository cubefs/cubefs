package metanode

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/cubefs/cubefs/proto"
)

func TestAppend01(t *testing.T) {
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
func TestAppend02(t *testing.T) {
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

func TestAppend03(t *testing.T) {
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
func TestAppend04(t *testing.T) {
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

func TestTruncate01(t *testing.T) {
	se := NewSortedExtents()
	delExtents, _ := se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 0, Size: 1000, ExtentId: 1}, nil, nil)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents, _ = se.AppendWithCheck(0, proto.ExtentKey{FileOffset: 2000, Size: 1000, ExtentId: 2}, nil, nil)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	delExtents = se.Truncate(500, nil, nil)
	t.Logf("\ndel: %v\neks: %v", delExtents, se.eks)
	if len(delExtents) != 2 || delExtents[1].ExtentId != 2 ||
		len(se.eks) != 1 || se.eks[0].ExtentId != 1 ||
		se.Size() != 500 {
		t.Fail()
	}
}

func TestSortedMarshal(t *testing.T) {
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

func TestSortedEkCompitable(t *testing.T) {
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

	oldData := []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 39, 126, 0, 0, 3, 242, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 3, 242, 0, 0, 4, 6, 0, 0, 0, 128}

	buf1 := GetInodeBuf()
	defer PutInodeBuf(buf1)

	err := se.MarshalBinary(buf1, false)
	if err != nil || !bytes.Equal(oldData, buf1.Bytes()) {
		t.Fail()
	}
}

func BenchmarkSortedMarshal(b *testing.B) {
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

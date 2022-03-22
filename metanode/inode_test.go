package metanode

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	se "github.com/chubaofs/chubaofs/util/sortedextent"
)

var buffer = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func genRandBuffer(size uint32) (b []byte, crc uint32) {
	rand.Seed(time.Now().UnixNano())
	b = make([]byte, size)
	for i := range b {
		b[i] = buffer[rand.Intn(len(buffer))]
	}
	return b, crc32.ChecksumIEEE(b[:size])
}

func TestInode_V1Marshal(t *testing.T) {
	i := &Inode{
		Inode:        1,
		Type:         2147484159,
		Uid:          0,
		Gid:          0,
		Size:         1111,
		Generation:   1,
		CreateTime:   1638191474,
		AccessTime:   1640333705,
		ModifyTime:   1638191474,
		LinkTarget:   []byte{1, 2, 3},
		NLink:        5,
		Flag:         0,
		Reserved:     0,
		Extents:      se.NewSortedExtents(),
	}
	var ctx = context.Background()

	i.Extents.Append(ctx, proto.ExtentKey{FileOffset: 1000, PartitionId: 12, ExtentId: 1, ExtentOffset: 100, Size: 1000, CRC: 0, StoreType: proto.NormalData})
	i.Extents.Append(ctx, proto.ExtentKey{FileOffset: 2000, PartitionId: 12, ExtentId: 2, ExtentOffset: 100, Size: 1000, CRC: 0, StoreType: proto.NormalData})
	raw, _ := i.Marshal()

	inodeRestoreExpect := &Inode{}
	err := inodeRestoreExpect.UnmarshalV2(ctx, raw)
	if err != nil {
		t.Fatalf("UnmarshalV2 failed, err: %s", err.Error())
	}

	inodeKV := &Inode{}
	err = inodeKV.UnmarshalV2WithKeyAndValue(ctx, i.MarshalKey(), i.MarshalValue())
	if err != nil {
		t.Fatalf("UnmarshalV2WithKeyAndValue failed, err: %s", err.Error())
	}
	if reflect.DeepEqual(i, inodeRestoreExpect) && reflect.DeepEqual(i, inodeKV) {
		t.Logf("inodeMarshal---->inodeUnmarshalV2: success,")
	} else {
		t.Errorf("Failed to test, error: len:%d \n src=\n[%v]\n res=\n[%v]\n inodeKV=\n[%v] ", len(raw), i, inodeRestoreExpect, inodeKV)
	}
}

func TestInode_V2Marshal(t *testing.T) {
	i := &Inode{
		Inode:        1,
		Type:         2147484159,
		Uid:          0,
		Gid:          0,
		Size:         0,
		Generation:   1,
		CreateTime:   1638191474,
		AccessTime:   1640333705,
		ModifyTime:   1638191474,
		LinkTarget:   []byte{1, 2, 3},
		NLink:        5,
		Flag:         0,
		Reserved:     0,
		Extents:      se.NewSortedExtents(),
	}
	var ctx = context.Background()
	i.Extents.Append(ctx, proto.ExtentKey{FileOffset: 1000, PartitionId: 12, ExtentId: 1, ExtentOffset: 100, Size: 1000, CRC: 0, StoreType: proto.NormalData})
	i.Extents.Append(ctx, proto.ExtentKey{FileOffset: 2000, PartitionId: 12, ExtentId: 2, ExtentOffset: 100, Size: 1000, CRC: 0, StoreType: proto.NormalData})

	raw, _ := i.MarshalV2()
	inodeRestore := &Inode{}
	err := inodeRestore.Unmarshal(ctx, raw)

	inodeRestoreExpect := &Inode{}
	err = inodeRestoreExpect.UnmarshalV2(ctx, raw)
	if err != nil {
		t.Fatalf("UnmarshalV2 failed, err: %v", err)
	}
	inodeKV := &Inode{}
	err = inodeKV.UnmarshalKey(raw[BaseInodeKeyOffset : BaseInodeKeyOffset+BaseInodeKeyLen])
	if err != nil {
		t.Fatalf("UnmarshalKey failed, err: %v", err)
	}
	err = inodeKV.UnmarshalValue(ctx, raw[BaseInodeValueOffset:])
	if err != nil {
		t.Fatalf("UnmarshalKey failed, err: %v", err)
	}
	if reflect.DeepEqual(i, inodeRestore) && reflect.DeepEqual(i, inodeRestoreExpect) && reflect.DeepEqual(i, inodeKV) {
		t.Logf("inodeMarshalV2---->inodeUnmarshal: success")
	} else {
		t.Errorf("Failed to test, error:len:%d \nsrc=\n[%v] res=\n[%v], expectRes=\n[%v]\n", len(raw), i, inodeRestore, inodeRestoreExpect)
	}
}

func TestInodeMergeMarshal(t *testing.T) {
	newExtents := make([]proto.ExtentKey, 0)
	oldExtents := make([]proto.ExtentKey, 0)
	for i := 0; i < 2; i++ {
		ek := proto.ExtentKey{
			FileOffset:   uint64(i),
			PartitionId:  uint64(i),
			ExtentId:     uint64(i),
			ExtentOffset: uint64(i),
			Size:         uint32(i),
		}
		newExtents = append(newExtents, ek)
	}
	for i := 0; i < 100; i++ {
		ek := proto.ExtentKey{
			FileOffset:   uint64(i),
			PartitionId:  uint64(i),
			ExtentId:     uint64(i),
			ExtentOffset: uint64(i),
			Size:         uint32(i),
		}
		oldExtents = append(oldExtents, ek)
	}
	im := &InodeMerge{
		Inode:       1,
		NewExtents:  newExtents,
		OldExtents:  oldExtents,
	}
	raw, err := im.Marshal()
	if err != nil {
		t.Fatalf("InodeMerge Marshal failed, err: %v", err)
	}
	inodeMerge, err := InodeMergeUnmarshal(raw)
	if reflect.DeepEqual(im, inodeMerge) {
		t.Logf("TestInodeMerge---->InodeMerge.Marshal: success")
	} else {
		t.Errorf("Failed to test, error:len:%d \nsrc=[%v] res=\n[%v]\n", len(raw), im, inodeMerge)
	}
}


func TestInode_MarshalWithVersionCase01(t *testing.T) {
	//mock inode, inode marshal, inode unmarshal
	i := &Inode{
		Inode:        1,
		Type:         2147484159,
		Uid:          0,
		Gid:          0,
		Size:         0,
		Generation:   1,
		CreateTime:   1638191474,
		AccessTime:   1640333705,
		ModifyTime:   1638191474,
		LinkTarget:   []byte{1, 2, 3},
		NLink:        5,
		Flag:         0,
		Reserved:     0,
		version:      InodeMarshalVersion3,
		Extents:      se.NewSortedExtents(),
		InnerDataSet: NewSortedInnerDataSet(),
	}
	var ctx = context.Background()
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 1000, PartitionId: 12, ExtentId: 1, ExtentOffset: 100, Size: 1000, CRC: 0, StoreType: proto.NormalData})
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 2000, PartitionId: 12, ExtentId: 2, ExtentOffset: 100, Size: 1000, CRC: 0, StoreType: proto.NormalData})
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 0, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 0, Size: 128, CRC: 0, StoreType: proto.InnerData})
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 128, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 128, Size: 128, CRC: 0, StoreType: proto.InnerData})

	d, _ := genRandBuffer(128)
	i.InnerDataSet.Insert(&proto.InnerDataSt{FileOffset: 0, Size: 128, CRC: 0, Data: d})

	d, _ = genRandBuffer(128)
	i.InnerDataSet.Insert(&proto.InnerDataSt{FileOffset: 128, Size: 128, CRC: 0, Data: d})

	_ = i.InnerDataSet.GenCrc()
	bytes, err := i.MarshalByVersion()
	if err != nil {
		t.Logf("marshal failed:%v\n", err)
		t.FailNow()
	}

	inode := NewInode(0, 0)
	if err = inode.UnmarshalByVersion(ctx, bytes); err != nil {
		t.Logf("unmarshal failed:%v\n", err)
		t.FailNow()
	}

	if !reflect.DeepEqual(i, inode) {
		t.Errorf("inode mismatch\n expect:\n%v,\n actual:\n%v", i, inode)
		t.FailNow()
	}
}

func TestInode_UnmarshalWithVersionCase01(t *testing.T) {
	i := &Inode{
		Inode:        1,
		Type:         2147484159,
		Uid:          0,
		Gid:          0,
		Size:         0,
		Generation:   1,
		CreateTime:   1638191474,
		AccessTime:   1640333705,
		ModifyTime:   1638191474,
		LinkTarget:   []byte{1, 2, 3},
		NLink:        5,
		Flag:         0,
		Reserved:     0,
		version:      InodeMarshalVersion3,
		Extents:      se.NewSortedExtents(),
		InnerDataSet: NewSortedInnerDataSet(),
	}
	var ctx = context.Background()
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 1000, PartitionId: 12, ExtentId: 1, ExtentOffset: 100, Size: 1000, CRC: 0, StoreType: proto.NormalData})
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 2000, PartitionId: 12, ExtentId: 2, ExtentOffset: 100, Size: 1000, CRC: 0, StoreType: proto.NormalData})

	bytes, err := i.MarshalByVersion()
	if err != nil {
		t.Logf("inode marshal failed:%v", err)
		t.FailNow()
	}

	inode := NewInode(0, 0)
	if err = inode.UnmarshalByVersion(ctx, bytes); err != nil {
		t.Logf("inode unmarshal failed:%v", err)
		t.FailNow()
	}
	if !reflect.DeepEqual(i, inode) {
		t.Errorf("inode mismatch\n expect:\n%v,\n actual:\n%v", i, inode)
		t.FailNow()
	}
}

func TestInode_UnmarshalWithVersionCase02(t *testing.T) {
	i := &Inode{
		Inode:        1,
		Type:         1,
		Uid:          0,
		Gid:          0,
		Size:         0,
		Generation:   1,
		CreateTime:   1638191474,
		AccessTime:   1640333705,
		ModifyTime:   1638191474,
		LinkTarget:   []byte{1, 2, 3},
		NLink:        5,
		Flag:         0,
		Reserved:     0,
		version:      InodeMarshalVersion,
		Extents:      se.NewSortedExtents(),
		InnerDataSet: NewSortedInnerDataSet(),
	}
	var ctx = context.Background()
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 0, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 0, Size: 128, CRC: 0, StoreType: proto.InnerData})
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 128, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 128, Size: 128, CRC: 0, StoreType: proto.InnerData})

	d, _ := genRandBuffer(128)
	i.InnerDataSet.Insert(&proto.InnerDataSt{FileOffset: 0, Size: 128, CRC: 0, Data: d})

	d, _ = genRandBuffer(128)
	i.InnerDataSet.Insert(&proto.InnerDataSt{FileOffset: 128, Size: 128, CRC: 0, Data: d})

	_ = i.InnerDataSet.GenCrc()
	bytes, err := i.MarshalByVersion()
	if err != nil {
		t.Logf("inode marshal failed:%v", err)
		t.FailNow()
	}

	inode := NewInode(0, 0)
	if err = inode.UnmarshalByVersion(ctx, bytes); err != nil {
		t.Logf("inode unmarshal failed:%v\n", err)
		t.FailNow()
	}
	if !reflect.DeepEqual(i, inode) {
		t.Errorf("inode mismatch\n expect:\n%v,\n actual:\n%v", i, inode)
		t.FailNow()
	}
}

func TestInode_MarshalKeyAndValue(t *testing.T) {
	i := &Inode{
		Inode:        1,
		Type:         2147484159,
		Uid:          0,
		Gid:          0,
		Size:         0,
		Generation:   1,
		CreateTime:   1638191474,
		AccessTime:   1640333705,
		ModifyTime:   1638191474,
		LinkTarget:   []byte{1, 2, 3},
		NLink:        5,
		Flag:         0,
		Reserved:     0,
		version:      InodeMarshalVersion3,
		Extents:      se.NewSortedExtents(),
		InnerDataSet: NewSortedInnerDataSet(),
	}
	var ctx = context.Background()
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 1000, PartitionId: 12, ExtentId: 1, ExtentOffset: 100, Size: 1000, CRC: 0, StoreType: proto.NormalData})
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 2000, PartitionId: 12, ExtentId: 2, ExtentOffset: 100, Size: 1000, CRC: 0, StoreType: proto.NormalData})
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 0, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 0, Size: 128, CRC: 0, StoreType: proto.InnerData})
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 128, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 128, Size: 128, CRC: 0, StoreType: proto.InnerData})

	d, _ := genRandBuffer(128)
	i.InnerDataSet.Insert(&proto.InnerDataSt{FileOffset: 0, Size: 128, CRC: 0, Data: d})

	d, _ = genRandBuffer(128)
	i.InnerDataSet.Insert(&proto.InnerDataSt{FileOffset: 128, Size: 128, CRC: 0, Data: d})

	_ = i.InnerDataSet.GenCrc()
	bytes, err := i.MarshalByVersion()
	if err != nil {
		t.Logf("inode marshal failed:%v", err)
		t.FailNow()
	}

	inode := NewInode(0, 0)
	keyLen := binary.BigEndian.Uint32(bytes[0:4])
	if err = inode.UnmarshalKeyV3(bytes[4:4+keyLen]); err != nil {
		t.Logf("unmarshal inode key failed:%v", err)
		t.FailNow()
	}

	valueLen := binary.BigEndian.Uint32(bytes[4+keyLen:8+keyLen])
	if err = inode.UnmarshalValueV3(ctx, bytes[keyLen+8:keyLen+8+valueLen]); err != nil {
		t.Logf("unmarshal inode value failed:%v", err)
		t.FailNow()
	}
}

func TestInode_MarshalAndUnmarshalInnerData(t *testing.T) {
	i := NewInode(1, 2147484159)
	var ctx = context.Background()
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 0, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 0, Size: 128, CRC: 0, StoreType: proto.InnerData})
	i.Extents.Insert(ctx, proto.ExtentKey{FileOffset: 128, PartitionId: math.MaxUint64, ExtentId: math.MaxUint64, ExtentOffset: 128, Size: 128, CRC: 0, StoreType: proto.InnerData})

	d, _ := genRandBuffer(128)
	i.InnerDataSet.Insert(&proto.InnerDataSt{FileOffset: 0, Size: 128, CRC: 0, Data: d})

	d, _ = genRandBuffer(128)
	i.InnerDataSet.Insert(&proto.InnerDataSt{FileOffset: 128, Size: 128, CRC: 0, Data: d})

	_ = i.InnerDataSet.GenCrc()
	bytes, err := i.MarshalInnerData()
	if err != nil {
		t.Logf("inode marshal extend field failed:%v", err)
		t.FailNow()
	}

	inode := NewInode(0, 0)
	if err = inode.UnmarshalInnerData(bytes); err != nil {
		t.Logf("inode unmarshal extend field failed:%v", err)
		t.FailNow()
	}

	if !reflect.DeepEqual(i.InnerDataSet, inode.InnerDataSet) {
		t.Errorf("inode mismatch\n expect:\n%v,\n actual:\n%v", i, inode)
		t.FailNow()
	}
}
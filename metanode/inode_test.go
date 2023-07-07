package metanode

import (
	"context"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"

	"github.com/cubefs/cubefs/proto"
	se "github.com/cubefs/cubefs/util/sortedextent"
)

func TestInode_V1Marshal(t *testing.T) {
	i := &Inode{
		Inode:      1,
		Type:       2147484159,
		Uid:        0,
		Gid:        0,
		Size:       1111,
		Generation: 1,
		CreateTime: 1638191474,
		AccessTime: 1640333705,
		ModifyTime: 1638191474,
		LinkTarget: []byte{1, 2, 3},
		NLink:      5,
		Flag:       0,
		Reserved:   0,
		Extents:    se.NewSortedExtents(),
	}
	var ctx = context.Background()

	i.Extents.Append(ctx, proto.ExtentKey{FileOffset: 0, PartitionId: 12, ExtentId: 1, ExtentOffset: 100, Size: 1000, CRC: 0}, i.Inode)
	i.Extents.Append(ctx, proto.ExtentKey{FileOffset: 1000, PartitionId: 12, ExtentId: 2, ExtentOffset: 100, Size: 1000, CRC: 0}, i.Inode)
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
		t.Errorf("Failed to test, error: len:%d \n src=\n[%v] res=\n[%v] ", len(raw), i, inodeRestoreExpect)
	}
}

func TestInode_V2Marshal(t *testing.T) {
	i := &Inode{
		Inode:      1,
		Type:       2147484159,
		Uid:        0,
		Gid:        0,
		Size:       0,
		Generation: 1,
		CreateTime: 1638191474,
		AccessTime: 1640333705,
		ModifyTime: 1638191474,
		LinkTarget: []byte{1, 2, 3},
		NLink:      5,
		Flag:       0,
		Reserved:   0,
		Extents:    se.NewSortedExtents(),
	}
	var ctx = context.Background()
	i.Extents.Append(ctx, proto.ExtentKey{FileOffset: 0, PartitionId: 12, ExtentId: 1, ExtentOffset: 100, Size: 1000, CRC: 0}, i.Inode)
	i.Extents.Append(ctx, proto.ExtentKey{FileOffset: 1000, PartitionId: 12, ExtentId: 2, ExtentOffset: 100, Size: 1000, CRC: 0}, i.Inode)

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
		t.Errorf("Failed to test, error:len:%d \nsrc=[%v] res=\n[%v], expectRes=\n[%v]\n", len(raw), i, inodeRestore, inodeRestoreExpect)
	}
}

func TestInode_EncodeBinary(t *testing.T) {
	inodeExpect := &Inode{
		Inode:      1,
		Type:       2147484159,
		Uid:        0,
		Gid:        0,
		Size:       0,
		Generation: 1,
		CreateTime: 1638191474,
		AccessTime: 1640333705,
		ModifyTime: 1638191474,
		LinkTarget: []byte{1, 2, 3},
		NLink:      5,
		Flag:       0,
		Reserved:   0,
		Extents:    se.NewSortedExtents(),
	}
	var ctx = context.Background()
	inodeExpect.Extents.Append(ctx, proto.ExtentKey{FileOffset: 0, PartitionId: 12, ExtentId: 1, ExtentOffset: 100, Size: 1000, CRC: 0}, inodeExpect.Inode)
	inodeExpect.Extents.Append(ctx, proto.ExtentKey{FileOffset: 1000, PartitionId: 12, ExtentId: 2, ExtentOffset: 100, Size: 1000, CRC: 0}, inodeExpect.Inode)

	data := make([]byte, inodeExpect.BinaryDataLen())
	_, _ = inodeExpect.EncodeBinary(data)

	inode := NewInode(0, 0)
	if err := inode.Unmarshal(context.Background(), data); err != nil {
		t.Fatalf("unmarshal failed:%v", err)
		return
	}
	assert.Equal(t, inodeExpect, inode)
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
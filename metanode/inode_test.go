package metanode

import (
	"bytes"
	"os"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmptyV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_HDD
	data, err := ino.Marshal()
	require.NoError(t, err)
	targetIno := NewInode(0, 0)
	err = targetIno.Unmarshal(data)
	require.NoError(t, err)
	assert.True(t, ino.Equal(targetIno))
}

func TestHDDV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 1)
	ino.StorageClass = proto.MediaType_HDD
	ino.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})

	buf1 := GetInodeBuf()
	defer PutInodeBuf(buf1)

	var data []byte
	err := ino.MarshalV2(buf1)
	if err != nil {
		t.Fail()
	}
	data = buf1.Bytes()

	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))

	targetInoV2 := NewInode(0, 0)
	err = targetInoV2.Unmarshal(data)
	if err != nil {
		panic(err)
	}
	assert.True(t, ino.Equal(targetInoV2))
}

func TestEmptyEBSV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_BlobStore
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
}

func TestEBSV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_BlobStore

	ino.HybridCloudExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})

	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
}

func TestSDDToHDDV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_SSD
	ino.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	ino.HybridCloudExtentsMigration.storageClass = proto.MediaType_HDD
	ino.HybridCloudExtentsMigration.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 14,
		ExtentId: 16, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
	ino.Copy()
}

func TestSDDToEBSV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_SSD
	ino.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	ino.HybridCloudExtentsMigration.storageClass = proto.StorageClass_BlobStore
	ino.HybridCloudExtentsMigration.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
}

func TestV4InodeCopy(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_BlobStore

	ino.HybridCloudExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.Copy().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4InodeCopyDirectly(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_BlobStore

	ino.HybridCloudExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.CopyDirectly().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4MigrationInodeCopy(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_SSD
	ino.HybridCloudExtentsMigration.storageClass = proto.MediaType_HDD
	ino.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})

	ino.HybridCloudExtentsMigration.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 21, PartitionId: 22,
		ExtentId: 23, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	temp := ino.Copy().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4MigrationInodeCopyDirectly(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_SSD
	ino.HybridCloudExtentsMigration.storageClass = proto.StorageClass_BlobStore
	ino.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})

	ino.HybridCloudExtentsMigration.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.CopyDirectly().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestInodeMarshal(t *testing.T) {

	checkInodeMarshal := func(a *Inode, t *testing.T) {
		data, err := a.Marshal()
		if err != nil {
			t.Fail()
		}

		b := NewInode(0, 0)
		err = b.Unmarshal(data)
		if err != nil {
			t.Fail()
		}

		buff := GetInodeBuf()
		defer PutInodeBuf(buff)
		err = b.MarshalV2(buff)
		if err != nil {
			t.Fail()
		}

		if !bytes.Equal(data, buff.Bytes()) {
			t.Fail()
		}
	}

	oldIno := NewInode(1024, uint32(os.ModeDir))
	oldIno.Uid = 101
	oldIno.Gid = 102
	oldIno.Generation = 104
	oldIno.CreateTime = 105
	oldIno.AccessTime = 106
	oldIno.ModifyTime = 107
	oldIno.LinkTarget = []byte("test op")
	oldIno.NLink = 108
	oldIno.Flag = 109
	oldIno.StorageClass = proto.StorageClass_Replica_SSD

	// dir
	oldIno.Type = uint32(os.ModeDir)
	checkInodeMarshal(oldIno, t)

	// empty file
	oldIno.Type = 0
	checkInodeMarshal(oldIno, t)

	oldIno.LinkTarget = []byte("test link")
	checkInodeMarshal(oldIno, t)
	oldIno.LinkTarget = nil

	// old ebs file
	oldIno.HybridCloudExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{
			{Size: uint64(100), FileOffset: uint64(100)},
		},
	)
	oldIno.StorageClass = proto.StorageClass_BlobStore
	checkInodeMarshal(oldIno, t)

	// replica file
	oldIno.StorageClass = proto.StorageClass_Replica_HDD
	oldIno.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 100}})
	checkInodeMarshal(oldIno, t)

	legacyReplicaStorageClass = proto.StorageClass_Replica_HDD
	checkInodeMarshal(oldIno, t)

	// check for migration empty
	oldIno.HybridCloudExtentsMigration = &SortedHybridCloudExtentsMigration{
		storageClass: proto.MediaType_SSD,
	}
	checkInodeMarshal(oldIno, t)

	oldIno.HybridCloudExtentsMigration = &SortedHybridCloudExtentsMigration{
		sortedEks:    NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 1024}}),
		storageClass: proto.MediaType_SSD,
	}
	checkInodeMarshal(oldIno, t)

	eks := NewSortedExtents()
	for i := 0; i < 2048; i++ {
		eks.Append(proto.ExtentKey{
			FileOffset: uint64(i + 1024),
			Size:       uint32(i),
		})
	}
	oldIno.HybridCloudExtents.sortedEks = eks
	checkInodeMarshal(oldIno, t)
}

// old bytes marshal from version 3.5.0
func TestInodeMarshalCompitable(t *testing.T) {
	// normal(dir, file, empty file), ebs(dir, file, empty file)
	checkInodeCompatibility := func(raw []byte, oldIno *Inode, t *testing.T) {
		data, err := oldIno.Marshal()
		require.NoError(t, err)
		if bytes.Equal(raw, data) {
			return
		}

		t.FailNow()
	}

	oldIno := NewInode(1024, uint32(os.ModeDir))
	oldIno.Uid = 101
	oldIno.Gid = 102
	oldIno.Generation = 104
	oldIno.CreateTime = 105
	oldIno.AccessTime = 106
	oldIno.ModifyTime = 107
	oldIno.LinkTarget = []byte("test op")
	oldIno.NLink = 108
	oldIno.Flag = 109
	oldIno.StorageClass = proto.StorageClass_Replica_SSD

	// dir
	oldIno.Type = uint32(os.ModeDir)
	// old data is marshal bytes by old version inode marshal
	oldData := []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 99, 128, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 0, 0, 0, 0, 105, 0, 0, 0, 0, 0, 0, 0, 106, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 7, 116, 101, 115, 116, 32, 111, 112, 0, 0, 0, 108, 0, 0, 0, 109, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	checkInodeCompatibility(oldData, oldIno, t)

	// empty file
	oldIno.Type = 0
	oldData = []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 99, 0, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 0, 0, 0, 0, 105, 0, 0, 0, 0, 0, 0, 0, 106, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 7, 116, 101, 115, 116, 32, 111, 112, 0, 0, 0, 108, 0, 0, 0, 109, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	checkInodeCompatibility(oldData, oldIno, t)

	// old ebs file
	oldIno.HybridCloudExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{
			{Size: uint64(100), FileOffset: uint64(100)},
		},
	)
	oldIno.StorageClass = proto.StorageClass_BlobStore
	oldData = []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 140, 0, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 0, 0, 0, 0, 105, 0, 0, 0, 0, 0, 0, 0, 106, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 7, 116, 101, 115, 116, 32, 111, 112, 0, 0, 0, 108, 0, 0, 0, 109, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	checkInodeCompatibility(oldData, oldIno, t)

	// replica file
	oldIno.StorageClass = proto.StorageClass_Replica_SSD
	oldIno.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 100}})
	oldData = []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 139, 0, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 0, 0, 0, 0, 105, 0, 0, 0, 0, 0, 0, 0, 106, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 7, 116, 101, 115, 116, 32, 111, 112, 0, 0, 0, 108, 0, 0, 0, 109, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	checkInodeCompatibility(oldData, oldIno, t)

	// check for migration
	oldIno.StorageClass = proto.StorageClass_Replica_HDD
	oldIno.HybridCloudExtentsMigration = &SortedHybridCloudExtentsMigration{
		sortedEks:    NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 1024}}),
		storageClass: proto.MediaType_SSD,
	}
	oldData = []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 195, 0, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 0, 0, 0, 0, 105, 0, 0, 0, 0, 0, 0, 0, 106, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 7, 116, 101, 115, 116, 32, 111, 112, 0, 0, 0, 108, 0, 0, 0, 109, 0, 0, 0, 0, 0, 0, 0, 72, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	checkInodeCompatibility(oldData, oldIno, t)
}

func BenchmarkInodeMarshal(b *testing.B) {
	oldIno := NewInode(1024, 0)
	oldIno.Uid = 101
	oldIno.Gid = 102
	oldIno.Generation = 104
	oldIno.CreateTime = 105
	oldIno.AccessTime = 106
	oldIno.ModifyTime = 107
	oldIno.NLink = 108
	oldIno.Flag = 109
	oldIno.StorageClass = proto.StorageClass_Replica_SSD
	oldIno.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 100}})

	buff := GetInodeBuf()
	defer PutInodeBuf(buff)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := oldIno.MarshalV2(buff)
		if err != nil {
			b.Fatal(err)
		}
	}
}

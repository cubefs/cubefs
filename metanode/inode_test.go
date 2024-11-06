package metanode

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmptyV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_HDD
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	err := targetIno.Unmarshal(data)
	require.NoError(t, err)
	assert.True(t, ino.Equal(targetIno))
}

func TestHDDV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_HDD
	ino.HybridCouldExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
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

	ino.HybridCouldExtents.sortedEks = NewSortedObjExtentsFromObjEks(
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
	ino.HybridCouldExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	ino.HybridCouldExtentsMigration.storageClass = proto.MediaType_HDD
	ino.HybridCouldExtentsMigration.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
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
	ino.HybridCouldExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	ino.HybridCouldExtentsMigration.storageClass = proto.StorageClass_BlobStore
	ino.HybridCouldExtentsMigration.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
}

func TestV1InodeStoreInSSDToV4Inode_UnMarshal(t *testing.T) {
	legacyReplicaStorageClass = proto.StorageClass_Replica_SSD
	//
	ino := NewOldVersionInode(1024, 0)
	ino.Extents = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	ino.Size = 10

	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, targetIno.StorageClass == proto.StorageClass_Replica_SSD)
	assert.True(t, reflect.DeepEqual(targetIno.HybridCouldExtents.sortedEks, ino.Extents))
	var data2 []byte
	data2, _ = targetIno.Marshal()
	targetIno2 := NewInode(0, 0)
	targetIno2.Unmarshal(data2)
	assert.True(t, targetIno2.StorageClass == proto.StorageClass_Replica_SSD)
	assert.True(t, targetIno2.Reserved|V4EnableHybridCloud > 0)
	assert.True(t, targetIno2.Reserved|V3EnableSnapInodeFlag > 0)
}

func TestV1InodeStoreInEBSWithoutCacheToV4Inode_UnMarshal(t *testing.T) {
	legacyReplicaStorageClass = proto.StorageClass_BlobStore
	//
	ino := NewOldVersionInode(1024, 0)
	ino.ObjExtents = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, targetIno.StorageClass == proto.StorageClass_BlobStore)
	assert.True(t, reflect.DeepEqual(targetIno.HybridCouldExtents.sortedEks, ino.ObjExtents))
	var data2 []byte
	data2, _ = targetIno.Marshal()
	targetIno2 := NewInode(0, 0)
	targetIno2.Unmarshal(data2)
	assert.True(t, targetIno2.StorageClass == proto.StorageClass_BlobStore)
	assert.True(t, targetIno2.Reserved|V4EnableHybridCloud > 0)
	assert.True(t, targetIno2.Reserved|V3EnableSnapInodeFlag > 0)
}

func TestV1InodeStoreInEBSWithCacheToV4Inode_UnMarshal(t *testing.T) {
	legacyReplicaStorageClass = proto.StorageClass_BlobStore
	//
	ino := NewOldVersionInode(1024, 0)
	ino.ObjExtents = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	ino.Extents = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, targetIno.StorageClass == proto.StorageClass_BlobStore)
	assert.True(t, reflect.DeepEqual(targetIno.HybridCouldExtents.sortedEks, ino.ObjExtents))
	assert.True(t, reflect.DeepEqual(targetIno.Extents, ino.Extents))
	var data2 []byte
	data2, _ = targetIno.Marshal()
	targetIno2 := NewInode(0, 0)
	targetIno2.Unmarshal(data2)
	assert.True(t, targetIno2.StorageClass == proto.StorageClass_BlobStore)
	assert.True(t, targetIno2.Reserved|V4EnableHybridCloud > 0)
	assert.True(t, targetIno2.Reserved|V3EnableSnapInodeFlag > 0)
}

func TestV4InodeCopy(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_BlobStore

	ino.HybridCouldExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.Copy().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4InodeCopyDirectly(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.StorageClass_BlobStore

	ino.HybridCouldExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.CopyDirectly().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4MigrationInodeCopy(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_SSD
	ino.HybridCouldExtentsMigration.storageClass = proto.MediaType_HDD
	ino.HybridCouldExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})

	ino.HybridCouldExtentsMigration.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 21, PartitionId: 22,
		ExtentId: 23, ExtentOffset: 0, Size: 0, CRC: 0,
	}})
	temp := ino.Copy().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4MigrationInodeCopyDirectly(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_SSD
	ino.HybridCouldExtentsMigration.storageClass = proto.StorageClass_BlobStore
	ino.HybridCouldExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0,
	}})

	ino.HybridCouldExtentsMigration.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.CopyDirectly().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestCompitableWithOldVersion(t *testing.T) {
	// normal(dir, file, empty file), ebs(dir, file, empty file)
	legacyReplicaStorageClass = proto.MediaType_Unspecified
	checkInodeCompatibility := func(oldIno *OldVersionInode, t *testing.T) {
		newIno := NewInode(0, 0)
		data, err := oldIno.Marshal()
		require.NoError(t, err)

		err = newIno.Unmarshal(data)
		require.NoError(t, err)
		require.True(t, oldIno.EqualNew(newIno))

		data, err = newIno.Marshal()
		require.NoError(t, err)

		oldIno2 := NewOldVersionInode(0, 0)
		err = oldIno2.Unmarshal(data)
		require.NoError(t, err)
		require.True(t, oldIno2.EqualNew(newIno))
	}

	oldIno := NewOldVersionInode(1024, uint32(os.ModeDir))
	oldIno.Uid = 101
	oldIno.Gid = 102
	oldIno.Generation = 104
	oldIno.CreateTime = 105
	oldIno.AccessTime = 106
	oldIno.ModifyTime = 107
	oldIno.LinkTarget = []byte("test op")
	oldIno.NLink = 108
	oldIno.Flag = 109

	// dir
	oldIno.Type = uint32(os.ModeDir)
	checkInodeCompatibility(oldIno, t)

	// empty file
	oldIno.Type = 0
	checkInodeCompatibility(oldIno, t)

	// old ebs file
	oldIno.ObjExtents = NewSortedObjExtentsFromObjEks([]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	checkInodeCompatibility(oldIno, t)

	// file
	oldIno.Extents = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 100}})
	oldIno.ObjExtents = nil
	data, err := oldIno.Marshal()
	require.NoError(t, err)
	newIno := NewInode(0, 0)
	err = newIno.Unmarshal(data)
	require.Error(t, err)

	legacyReplicaStorageClass = proto.StorageClass_Replica_HDD
	checkInodeCompatibility(oldIno, t)
}

func NewOldVersionInode(ino uint64, t uint32) *OldVersionInode {
	ts := timeutil.GetCurrentTimeUnix()
	i := &OldVersionInode{
		Inode:      ino,
		Type:       t,
		Generation: 1,
		CreateTime: ts,
		AccessTime: ts,
		ModifyTime: ts,
		NLink:      1,
		Extents:    NewSortedExtents(),
		ObjExtents: NewSortedObjExtents(),
	}
	if proto.IsDir(t) {
		i.NLink = 2
	}
	return i
}

type OldVersionInode struct {
	sync.RWMutex
	Inode      uint64 // Inode ID
	Type       uint32
	Uid        uint32
	Gid        uint32
	Size       uint64
	Generation uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	LinkTarget []byte // SymLink target name
	NLink      uint32 // NodeLink counts
	Flag       int32
	Reserved   uint64 // reserved space
	// Extents    *ExtentsTree
	Extents    *SortedExtents
	ObjExtents *SortedObjExtents
	multiSnap  *InodeMultiSnap
}

func (i *OldVersionInode) EqualNew(i1 *Inode) bool {
	if i.Inode != i1.Inode || i.Type != i1.Type || i.Uid != i1.Uid || i.Gid != i1.Gid || i.Size != i1.Size {
		return false
	}

	if i.Generation != i1.Generation || i.CreateTime != i1.CreateTime || i.AccessTime != i1.AccessTime {
		return false
	}

	if i.AccessTime != i1.AccessTime || i.ModifyTime != i1.ModifyTime || !bytes.Equal(i.LinkTarget, i1.LinkTarget) {
		return false
	}

	if i.NLink != i1.NLink || i.Flag != i1.Flag {
		return false
	}

	if i.Extents != nil && !i.Extents.IsEmpty() {
		ext := i1.HybridCouldExtents.sortedEks.(*SortedExtents)
		if !i.Extents.Equals(ext) {
			return false
		}
	}

	if i.ObjExtents != nil && !i.ObjExtents.IsEmpty() {
		ext := i1.HybridCouldExtents.sortedEks.(*SortedObjExtents)
		if !i.ObjExtents.Equals(ext) {
			return false
		}
	}

	return true
}

func (i *OldVersionInode) Marshal() (result []byte, err error) {
	keyBytes := i.MarshalKey()
	valBytes := i.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	if err = binary.Write(buff, binary.BigEndian, keyLen); err != nil {
		return
	}
	if _, err = buff.Write(keyBytes); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, valLen); err != nil {
		return
	}
	if _, err = buff.Write(valBytes); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

func (i *OldVersionInode) MarshalKey() (k []byte) {
	k = make([]byte, 8)
	binary.BigEndian.PutUint64(k, i.Inode)
	return
}

// MarshalValue marshals the value to bytes.
func (i *OldVersionInode) MarshalValue() []byte {
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	buff.Grow(64)

	var err error
	if err = binary.Write(buff, binary.BigEndian, &i.Type); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Uid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Gid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Size); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Generation); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.CreateTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.AccessTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		panic(err)
	}
	// write SymLink
	symSize := uint32(len(i.LinkTarget))
	if err = binary.Write(buff, binary.BigEndian, &symSize); err != nil {
		panic(err)
	}
	if _, err = buff.Write(i.LinkTarget); err != nil {
		panic(err)
	}

	if err = binary.Write(buff, binary.BigEndian, &i.NLink); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Flag); err != nil {
		panic(err)
	}

	i.Reserved = 0
	if i.ObjExtents != nil && len(i.ObjExtents.eks) > 0 {
		i.Reserved |= V2EnableEbsFlag
	}
	if i.multiSnap != nil {
		i.Reserved |= V3EnableSnapInodeFlag
	}

	// log.LogInfof("action[MarshalInodeValue] inode[%v] Reserved %v", i.Inode, i.Reserved)
	if err = binary.Write(buff, binary.BigEndian, &i.Reserved); err != nil {
		panic(err)
	}

	// marshal ExtentsKey
	extData, err := i.Extents.MarshalBinary(i.Reserved&V3EnableSnapInodeFlag > 0)
	if err != nil {
		panic(err)
	}
	if i.Reserved != 0 {
		if err = binary.Write(buff, binary.BigEndian, uint32(len(extData))); err != nil {
			panic(err)
		}
	}
	if _, err = buff.Write(extData); err != nil {
		panic(err)
	}

	if i.Reserved&V2EnableEbsFlag > 0 {
		// marshal ObjExtentsKey
		objExtData, err := i.ObjExtents.MarshalBinary()
		if err != nil {
			panic(err)
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(objExtData))); err != nil {
			panic(err)
		}
		if _, err = buff.Write(objExtData); err != nil {
			panic(err)
		}
	}
	if i.multiSnap != nil {
		if err = binary.Write(buff, binary.BigEndian, i.multiSnap.verSeq); err != nil {
			panic(err)
		}
	}
	return buff.Bytes()
}

func (i *OldVersionInode) Unmarshal(raw []byte) (err error) {
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		return
	}
	if err = i.UnmarshalKey(keyBytes); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	err = i.UnmarshalValue(valBytes)
	return
}

func (i *OldVersionInode) UnmarshalKey(k []byte) (err error) {
	i.Inode = binary.BigEndian.Uint64(k)
	return
}

// UnmarshalValue unmarshals the value from bytes.
func (i *OldVersionInode) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)
	if err = binary.Read(buff, binary.BigEndian, &i.Type); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Uid); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Gid); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Generation); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.CreateTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.AccessTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		return
	}
	// read symLink
	symSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &symSize); err != nil {
		return
	}
	if symSize > 0 {
		i.LinkTarget = make([]byte, symSize)
		if _, err = io.ReadFull(buff, i.LinkTarget); err != nil {
			return
		}
	}

	if err = binary.Read(buff, binary.BigEndian, &i.NLink); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Flag); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Reserved); err != nil {
		return
	}

	// unmarshal ExtentsKey
	if i.Extents == nil {
		i.Extents = NewSortedExtents()
	}
	if i.ObjExtents == nil {
		i.ObjExtents = NewSortedObjExtents()
	}

	if i.Reserved == 0 {
		if err, _ = i.Extents.UnmarshalBinary(buff.Bytes(), false); err != nil {
			return
		}
		return
	}

	v3 := i.Reserved&V3EnableSnapInodeFlag > 0
	v2 := i.Reserved&V2EnableEbsFlag > 0

	extSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &extSize); err != nil {
		return
	}

	if extSize > 0 {
		extBytes := make([]byte, extSize)
		if _, err = io.ReadFull(buff, extBytes); err != nil {
			return
		}
		var ekRef *sync.Map
		if err, ekRef = i.Extents.UnmarshalBinary(extBytes, v3); err != nil {
			return
		}
		// log.LogDebugf("inode[%v] ekRef %v", i.Inode, ekRef)
		if ekRef != nil {
			if i.multiSnap == nil {
				i.multiSnap = NewMultiSnap(0)
			}
			// log.LogDebugf("inode[%v] ekRef %v", i.Inode, ekRef)
			i.multiSnap.ekRefMap = ekRef
		}
	}

	if v2 {
		// unmarshal ObjExtentsKey
		ObjExtSize := uint32(0)
		if err = binary.Read(buff, binary.BigEndian, &ObjExtSize); err != nil {
			return
		}
		if ObjExtSize > 0 {
			objExtBytes := make([]byte, ObjExtSize)
			if _, err = io.ReadFull(buff, objExtBytes); err != nil {
				return
			}
			if err = i.ObjExtents.UnmarshalBinary(objExtBytes); err != nil {
				return
			}
		}
	}

	if v3 {
		var seq uint64
		if err = binary.Read(buff, binary.BigEndian, &seq); err != nil {
			return
		}
		if seq != 0 {
			// i.setVer(seq)
		}
	}

	return
}

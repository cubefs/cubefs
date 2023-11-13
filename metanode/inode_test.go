package metanode

import (
	"bytes"
	"encoding/binary"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/timeutil"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync"
	"testing"
)

func TestEmptyV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_HDD
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
}

func TestHDDV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_HDD
	ino.HybridCouldExtents.sortedEks =
		NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 11, PartitionId: 12,
			ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
}

func TestEmptyEBSV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_EBS
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
}

func TestEBSV4Inode_Marshal(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_EBS

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
	ino.HybridCouldExtents.sortedEks =
		NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 11, PartitionId: 12,
			ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0}})
	ino.HybridCouldExtentsMigration.storageClass = proto.MediaType_HDD
	ino.HybridCouldExtentsMigration.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 11, PartitionId: 14,
		ExtentId: 16, ExtentOffset: 0, Size: 0, CRC: 0}})
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
	ino.HybridCouldExtents.sortedEks =
		NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 11, PartitionId: 12,
			ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0}})
	ino.HybridCouldExtentsMigration.storageClass = proto.MediaType_EBS
	ino.HybridCouldExtentsMigration.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, ino.Equal(targetIno))
}

func TestV1InodeStoreInSSDToV4Inode_UnMarshal(t *testing.T) {
	defaultMediaType = uint64(proto.MediaType_SSD)
	//
	ino := NewOldVersionInode(1024, 0)
	ino.Extents = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, targetIno.StorageClass == proto.MediaType_SSD)
	assert.True(t, reflect.DeepEqual(targetIno.HybridCouldExtents.sortedEks, ino.Extents))
	var data2 []byte
	data2, _ = targetIno.Marshal()
	targetIno2 := NewInode(0, 0)
	targetIno2.Unmarshal(data2)
	assert.True(t, targetIno2.StorageClass == proto.MediaType_SSD)
	assert.True(t, targetIno2.Reserved|V4EnableHybridCloud > 0)
	assert.True(t, targetIno2.Reserved|V3EnableSnapInodeFlag > 0)
}

func TestV1InodeStoreInEBSWithoutCacheToV4Inode_UnMarshal(t *testing.T) {
	defaultMediaType = uint64(proto.MediaType_EBS)
	//
	ino := NewOldVersionInode(1024, 0)
	ino.ObjExtents = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, targetIno.StorageClass == proto.MediaType_EBS)
	assert.True(t, reflect.DeepEqual(targetIno.HybridCouldExtents.sortedEks, ino.ObjExtents))
	var data2 []byte
	data2, _ = targetIno.Marshal()
	targetIno2 := NewInode(0, 0)
	targetIno2.Unmarshal(data2)
	assert.True(t, targetIno2.StorageClass == proto.MediaType_EBS)
	assert.True(t, targetIno2.Reserved|V4EnableHybridCloud > 0)
	assert.True(t, targetIno2.Reserved|V3EnableSnapInodeFlag > 0)
}

func TestV1InodeStoreInEBSWithCacheToV4Inode_UnMarshal(t *testing.T) {
	defaultMediaType = uint64(proto.MediaType_EBS)
	//
	ino := NewOldVersionInode(1024, 0)
	ino.ObjExtents = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	ino.Extents = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0}})
	var data []byte
	data, _ = ino.Marshal()
	targetIno := NewInode(0, 0)
	targetIno.Unmarshal(data)
	assert.True(t, targetIno.StorageClass == proto.MediaType_EBS)
	assert.True(t, reflect.DeepEqual(targetIno.HybridCouldExtents.sortedEks, ino.ObjExtents))
	assert.True(t, reflect.DeepEqual(targetIno.Extents, ino.Extents))
	var data2 []byte
	data2, _ = targetIno.Marshal()
	targetIno2 := NewInode(0, 0)
	targetIno2.Unmarshal(data2)
	assert.True(t, targetIno2.StorageClass == proto.MediaType_EBS)
	assert.True(t, targetIno2.Reserved|V4EnableHybridCloud > 0)
	assert.True(t, targetIno2.Reserved|V3EnableSnapInodeFlag > 0)
}

func TestV4InodeCopy(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_EBS

	ino.HybridCouldExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.Copy().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4InodeCopyDirectly(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_EBS

	ino.HybridCouldExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.CopyDirectly().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4MigrationInodeCopy(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_SSD
	ino.HybridCouldExtentsMigration.storageClass = proto.MediaType_HDD
	ino.HybridCouldExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0}})

	ino.HybridCouldExtentsMigration.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 21, PartitionId: 22,
		ExtentId: 23, ExtentOffset: 0, Size: 0, CRC: 0}})
	temp := ino.Copy().(*Inode)
	assert.True(t, ino.Equal(temp))
}

func TestV4MigrationInodeCopyDirectly(t *testing.T) {
	ino := NewInode(1024, 0)
	ino.StorageClass = proto.MediaType_SSD
	ino.HybridCouldExtentsMigration.storageClass = proto.MediaType_EBS
	ino.HybridCouldExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: 11, PartitionId: 12,
		ExtentId: 13, ExtentOffset: 0, Size: 0, CRC: 0}})

	ino.HybridCouldExtentsMigration.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{Size: uint64(100), FileOffset: uint64(100)}})
	temp := ino.CopyDirectly().(*Inode)
	assert.True(t, ino.Equal(temp))
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
	//Extents    *ExtentsTree
	Extents    *SortedExtents
	ObjExtents *SortedObjExtents
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

func (i *OldVersionInode) MarshalValue() (val []byte) {
	var err error
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	buff.Grow(64)
	i.RLock()
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
	if i.ObjExtents != nil && len(i.ObjExtents.eks) > 0 {
		i.Reserved = V2EnableColdInodeFlag
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Reserved); err != nil {
		panic(err)
	}

	if i.Reserved == V2EnableColdInodeFlag {
		// marshal ExtentsKey
		extData, err := i.Extents.MarshalBinary(false)
		if err != nil {
			panic(err)
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(extData))); err != nil {
			panic(err)
		}
		if _, err = buff.Write(extData); err != nil {
			panic(err)
		}
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
	} else {
		// marshal ExtentsKey
		extData, err := i.Extents.MarshalBinary(false)
		if err != nil {
			panic(err)
		}
		if _, err = buff.Write(extData); err != nil {
			panic(err)
		}
	}

	val = buff.Bytes()
	i.RUnlock()
	return
}

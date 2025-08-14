package metanode

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestStoreDentry(t *testing.T) {
	dTree := NewBtree()
	for i := 0; i < 100; i++ {
		dTree.ReplaceOrInsert(&Dentry{
			ParentId: uint64(101 + i),
			Inode:    uint64(201 + i),
			Name:     fmt.Sprintf("test_%d", i),
		}, true)
	}

	sm := &storeMsg{
		dentryTree: dTree,
	}

	rootDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fail()
	}
	defer func() {
		err = os.RemoveAll(rootDir)
		if err != nil {
			t.Fail()
		}
	}()

	mp := newMetaPartition(1024, nil)
	crc, err := mp.storeDentry(rootDir, sm)
	require.NoError(t, err)

	err = mp.loadDentry(rootDir, crc)
	require.NoError(t, err)
}

func TestStoreDentryCompitable(t *testing.T) {
	oldCrc := uint32(713290320)
	// old date is marshal byte for dtree by old version
	oldData := []byte{0, 0, 0, 34, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 101, 116, 101, 115, 116, 95, 48, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 201, 0, 0, 0, 0, 0, 0, 0, 34, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 102, 116, 101, 115, 116, 95, 49, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 202, 0, 0, 0, 0, 0, 0, 0, 34, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 103, 116, 101, 115, 116, 95, 50, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 203, 0, 0, 0, 0, 0, 0, 0, 34, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 104, 116, 101, 115, 116, 95, 51, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 204, 0, 0, 0, 0, 0, 0, 0, 34, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 105, 116, 101, 115, 116, 95, 52, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 205, 0, 0, 0, 0}

	dTree := NewBtree()
	for i := 0; i < 5; i++ {
		dTree.ReplaceOrInsert(&Dentry{
			ParentId: uint64(101 + i),
			Inode:    uint64(201 + i),
			Name:     fmt.Sprintf("test_%d", i),
		}, true)
	}

	rootDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fail()
	}

	fileName := filepath.Join(rootDir, dentryFile)
	err = os.WriteFile(fileName, oldData, 0o755)
	if err != nil {
		t.Fail()
	}

	mp := newMetaPartition(1024, nil)
	err = mp.loadDentry(rootDir, oldCrc)
	if err != nil {
		t.Fail()
	}

	require.True(t, mp.dentryTree.Len() == dTree.Len())
}

func TestStoreInode(t *testing.T) {
	dTree := NewBtree()
	baseInode := NewInode(1024, uint32(os.ModeDir))
	baseInode.Uid = 101
	baseInode.Gid = 102
	baseInode.Generation = 104
	baseInode.CreateTime = 105
	baseInode.AccessTime = 106
	baseInode.ModifyTime = 107
	baseInode.LinkTarget = []byte("test op")
	baseInode.NLink = 108
	baseInode.Flag = 109
	baseInode.StorageClass = proto.StorageClass_Replica_SSD

	for i := 0; i < 100; i++ {
		newInode := baseInode.Copy().(*Inode)
		newInode.Inode = uint64(i + 104)
		newInode.Size = uint64(100 + i)
		newInode.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: uint64(100 + i)}})
		dTree.ReplaceOrInsert(newInode, true)
	}

	sm := &storeMsg{
		inodeTree: dTree,
	}

	rootDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fail()
	}
	defer func() {
		err = os.RemoveAll(rootDir)
		if err != nil {
			t.Fail()
		}
	}()

	mp := newMetaPartition(1024, nil)
	crc, err := mp.storeInode(rootDir, sm)
	require.NoError(t, err)

	err = mp.loadInode(rootDir, crc)
	require.NoError(t, err)
}

func TestStoreInodeCompitable(t *testing.T) {
	oldCrc := uint32(4195506950)
	// old date is marshal byte for dtree by version 3.5.0
	oldData := []byte{0, 0, 0, 155, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 139, 128, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 102, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 0, 0, 0, 0, 105, 0, 0, 0, 0, 0, 0, 0, 106, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 7, 116, 101, 115, 116, 32, 111, 112, 0, 0, 0, 108, 0, 0, 0, 109, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 155, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 105, 0, 0, 0, 139, 128, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 102, 0, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 0, 0, 0, 0, 105, 0, 0, 0, 0, 0, 0, 0, 106, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 7, 116, 101, 115, 116, 32, 111, 112, 0, 0, 0, 108, 0, 0, 0, 109, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 155, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 106, 0, 0, 0, 139, 128, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 102, 0, 0, 0, 0, 0, 0, 0, 102, 0, 0, 0, 0, 0, 0, 0, 104, 0, 0, 0, 0, 0, 0, 0, 105, 0, 0, 0, 0, 0, 0, 0, 106, 0, 0, 0, 0, 0, 0, 0, 107, 0, 0, 0, 7, 116, 101, 115, 116, 32, 111, 112, 0, 0, 0, 108, 0, 0, 0, 109, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	baseInode := NewInode(1024, uint32(os.ModeDir))
	baseInode.Uid = 101
	baseInode.Gid = 102
	baseInode.Generation = 104
	baseInode.CreateTime = 105
	baseInode.AccessTime = 106
	baseInode.ModifyTime = 107
	baseInode.LinkTarget = []byte("test op")
	baseInode.NLink = 108
	baseInode.Flag = 109
	baseInode.StorageClass = proto.StorageClass_Replica_SSD

	dTree := NewBtree()
	for i := 0; i < 3; i++ {
		newInode := baseInode.Copy().(*Inode)
		newInode.Inode = uint64(i + 104)
		newInode.Size = uint64(100 + i)
		newInode.HybridCloudExtents.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{FileOffset: uint64(100 + i)}})
		dTree.ReplaceOrInsert(newInode, true)
	}

	rootDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fail()
	}

	fileName := filepath.Join(rootDir, inodeFile)
	err = os.WriteFile(fileName, oldData, 0o755)
	if err != nil {
		t.Fail()
	}

	mp := newMetaPartition(1024, nil)
	err = mp.loadInode(rootDir, oldCrc)
	if err != nil {
		t.Fail()
	}

	require.True(t, mp.inodeTree.Len() == dTree.Len())

	err = os.RemoveAll(rootDir)
	if err != nil {
		t.Fail()
	}
}

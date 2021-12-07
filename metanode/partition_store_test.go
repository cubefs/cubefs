package metanode

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"
)

const (
	rootdir = "."
)

func mockDeletedDentry(parentID, ino uint64, name string, dtype uint32, timestamp int64) *DeletedDentry {
	dd := newPrimaryDeletedDentry(parentID, name, timestamp, ino)
	dd.Type = dtype
	return dd
}

func TestMetaPartition_storeDeletedDentry(t *testing.T) {
	var (
		parentID  uint64
		ino       uint64
		dtype     uint32
		timestamp int64
		i         uint64
	)

	filename := path.Join(rootdir, dentryDeletedFile)
	_, err := os.Stat(filename)
	if err == nil {
		os.Remove(filename)
	}

	tree := NewBtree()
	timestamp = time.Now().UnixNano() / 1000
	timestamp2 := timestamp
	for parentID = 2; parentID < 10; parentID++ {
		timestamp += int64(parentID)
		name := fmt.Sprintf("d_%v", parentID)
		dd := mockDeletedDentry(1, parentID, name, dtype, timestamp)
		tree.ReplaceOrInsert(dd, false)
		for i = 1; i < 3; i++ {
			ino = parentID * i * 100
			timestamp += int64(ino)

			name = fmt.Sprintf("d_%v_%v", parentID, parentID)
			dd = mockDeletedDentry(parentID, ino, name, dtype, timestamp)
			tree.ReplaceOrInsert(dd, false)

			name := fmt.Sprintf("d_%v_%v", parentID, ino)
			dd := mockDeletedDentry(parentID, ino, name, dtype, timestamp)
			tree.ReplaceOrInsert(dd, false)
		}
	}

	storeMsg := new(storeMsg)
	storeMsg.dentryDeletedTree = tree
	mp := new(metaPartition)
	mp.config = new(MetaPartitionConfig)
	mp.config.PartitionId = 1
	mp.config.VolName = "ltptest"
	_, err = mp.storeDeletedDentry(rootdir, storeMsg)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	mp.dentryDeletedTree = NewBtree()
	err = mp.loadDeletedDentry(rootdir)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	if mp.dentryDeletedTree.Len() != tree.Len() {
		t.FailNow()
	}

	timestamp = timestamp2
	for parentID = 2; parentID < 10; parentID++ {
		timestamp += int64(parentID)
		name := fmt.Sprintf("d_%v", parentID)
		dd := mockDeletedDentry(1, parentID, name, dtype, timestamp)
		checkTestDeletedDentry(mp, dd, t)
		for i = 1; i < 3; i++ {
			ino = parentID * i * 100
			timestamp += int64(ino)

			name = fmt.Sprintf("d_%v_%v", parentID, parentID)
			dd = mockDeletedDentry(parentID, ino, name, dtype, timestamp)
			checkTestDeletedDentry(mp, dd, t)

			name := fmt.Sprintf("d_%v_%v", parentID, ino)
			dd := mockDeletedDentry(parentID, ino, name, dtype, timestamp)
			checkTestDeletedDentry(mp, dd, t)
		}
	}
}

func checkTestDeletedDentry(mp *metaPartition, dd *DeletedDentry, t *testing.T) {
	item := mp.dentryDeletedTree.Get(dd)
	if item == nil {
		t.Errorf("Not found deleted dentry: %v ", dd)
		t.FailNow()
	}

	if compareDeletedDentry(item.(*DeletedDentry), dd) == false {
		t.Errorf("failed to compare the two deleted dentry: %v, %v", item.(*DeletedDentry), dd)
		t.FailNow()
	}
}

func TestMetaPartition_storeDeletedInode(t *testing.T) {
	var (
		ino uint64
	)
	filename := path.Join(rootdir, inodeDeletedFile)
	_, err := os.Stat(filename)
	if err == nil {
		os.Remove(filename)
	}

	tree := NewBtree()
	for ino = 1; ino < 1000; ino++ {
		inode := mockINode(ino)
		dino := NewDeletedInode(inode, ts+msFactor*int64(ino))
		tree.ReplaceOrInsert(dino, false)
	}

	storeMsg := new(storeMsg)
	storeMsg.inodeDeletedTree = tree
	mp := new(metaPartition)
	mp.config = new(MetaPartitionConfig)
	mp.config.PartitionId = 1
	mp.config.VolName = "ltptest"
	_, err = mp.storeDeletedInode(rootdir, storeMsg)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	mp.inodeDeletedTree = NewBtree()
	err = mp.loadDeletedInode(context.Background(), rootdir)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	if mp.inodeDeletedTree.Len() != tree.Len() {
		t.FailNow()
	}

	for ino = 1; ino < 1000; ino++ {
		dino := NewDeletedInodeByID(ino)
		item := mp.inodeDeletedTree.Get(dino)
		if item == nil {
			t.Errorf("Not found deleted inode: %v", dino)
			t.FailNow()
		}

		olditem := tree.Get(dino)
		compareTestInode(item.(*DeletedINode), olditem.(*DeletedINode), t)
	}
}

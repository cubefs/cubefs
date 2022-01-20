package metanode

import (
	"context"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"os"
	"path"
	"reflect"
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
	defer os.Remove(filename)

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

			name = fmt.Sprintf("d_%v_%v", parentID, ino)
			dd = mockDeletedDentry(parentID, ino, name, dtype, timestamp)
			tree.ReplaceOrInsert(dd, false)
		}
	}

	msg := new(storeMsg)
	msg.snap = &BTreeSnapShot{
		delDentry: &DeletedDentryBTree{tree},
	}

	mp := new(metaPartition)
	mp.config = new(MetaPartitionConfig)
	mp.config.PartitionId = 1
	mp.config.VolName = "ltptest"
	mp.config.StoreMode = proto.StoreModeMem
	mp.initMemoryTree()
	_, err = mp.storeDeletedDentry(rootdir, msg)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	err = mp.loadDeletedDentry(rootdir)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	if mp.dentryDeletedTree.Count() != uint64(tree.Len()) {
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
	item, _ := mp.dentryDeletedTree.Get(dd.ParentId, dd.Name, dd.Timestamp)
	if item == nil {
		t.Errorf("Not found deleted dentry: %v ", dd)
		t.FailNow()
	}

	if compareDeletedDentry(item, dd) == false {
		t.Errorf("failed to compare the two deleted dentry: %v, %v", item, dd)
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
	defer os.Remove(filename)

	tree := NewBtree()
	for ino = 1; ino < 1000; ino++ {
		inode := mockINode(ino)
		dino := NewDeletedInode(inode, ts+msFactor*int64(ino))
		tree.ReplaceOrInsert(dino, false)
	}

	storeMsg := new(storeMsg)
	storeMsg.snap = &BTreeSnapShot{
		delInode: &DeletedInodeBTree{tree},
	}
	mp := new(metaPartition)
	mp.config = new(MetaPartitionConfig)
	mp.config.PartitionId = 1
	mp.config.VolName = "ltptest"
	mp.config.StoreMode = proto.StoreModeMem
	mp.initMemoryTree()
	_, err = mp.storeDeletedInode(rootdir, storeMsg)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	mp.inodeDeletedTree = &DeletedInodeBTree{NewBtree()}
	err = mp.loadDeletedInode(context.Background(), rootdir)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	if mp.inodeDeletedTree.Count() != uint64(tree.Len()) {
		t.FailNow()
	}

	for ino = 1; ino < 1000; ino++ {
		dino := NewDeletedInodeByID(ino)
		item, _ := mp.inodeDeletedTree.Get(dino.Inode.Inode)
		if item == nil {
			t.Errorf("Not found deleted inode: %v", dino)
			t.FailNow()
		}

		olditem := tree.Get(dino)
		compareTestInode(item, olditem.(*DeletedINode), t)
	}
}

func TestMetaPartition_storeInode(t *testing.T) {
	var (
		ino uint64
	)
	filename := path.Join(rootdir, inodeFile)
	_, err := os.Stat(filename)
	if err == nil {
		os.Remove(filename)
	}
	defer os.Remove(filename)

	tree := NewBtree()
	for ino = 1; ino < 1000; ino++ {
		inode := mockINode(ino)
		tree.ReplaceOrInsert(inode, false)
	}

	sMsg := new(storeMsg)
	sMsg.snap = &BTreeSnapShot{
		inode: &InodeBTree{tree},
	}
	mp := new(metaPartition)
	mp.config = new(MetaPartitionConfig)
	mp.config.PartitionId = 1
	mp.config.VolName = "ltptest"
	mp.config.StoreMode = proto.StoreModeMem
	mp.initMemoryTree()
	_, err = mp.storeInode(rootdir, sMsg)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	mp.inodeTree = &InodeBTree{NewBtree()}
	err = mp.loadInode(context.Background(), rootdir)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	if mp.inodeTree.Count() != uint64(tree.Len()) {
		t.FailNow()
	}

	for ino = 1; ino < 1000; ino++ {
		inode, _ := mp.inodeTree.Get(ino)
		if inode == nil {
			t.Errorf("Not found inode: %v", ino)
			t.FailNow()
		}

		oldInode := tree.Get(inode).(*Inode)
		compareTestInode2(inode, oldInode, t)
	}
}

func TestMetaPartition_storeDentry(t *testing.T) {
	var (
		ino       uint64
		i         uint64
	)

	filename := path.Join(rootdir, dentryFile)
	_, err := os.Stat(filename)
	if err == nil {
		os.Remove(filename)
	}
	defer os.Remove(filename)

	tree := NewBtree()
	for ino = 2; ino < 10; ino++ {
		name := fmt.Sprintf("test_dentry_store_%v", ino)
		dentry := &Dentry{ParentId: 1, Inode: ino, Name: name}
		tree.ReplaceOrInsert(dentry, false)
		for i = 1; i < 5; i++ {
			ino2 := ino * i + 1000
			name = fmt.Sprintf("test_dentry_store_%v_%v", ino, ino2)
			dentry = &Dentry{ParentId: ino, Inode: ino2, Name: name}
			tree.ReplaceOrInsert(dentry, false)
		}
	}

	msg := new(storeMsg)
	msg.snap = &BTreeSnapShot{
		dentry: &DentryBTree{tree},
	}

	mp := new(metaPartition)
	mp.config = new(MetaPartitionConfig)
	mp.config.PartitionId = 1
	mp.config.VolName = "ltptest"
	mp.config.StoreMode = proto.StoreModeMem
	mp.config.Cursor = 10000
	mp.config.Start = 0
	mp.initMemoryTree()
	_, err = mp.storeDentry(rootdir, msg)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	mp.dentryTree = &DentryBTree{NewBtree()}
	err = mp.loadDentry(rootdir)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	if mp.dentryTree.Count() != uint64(tree.Len()) {
		t.FailNow()
	}

	for ino = 2; ino < 10; ino++ {
		name := fmt.Sprintf("test_dentry_store_%v", ino)
		dentry, _ := mp.dentryTree.Get(1, name)
		if dentry == nil {
			t.Errorf("Not found dentry[parent id:%v, name:%s]", ino, name)
			t.FailNow()
		}
		expectDentry := tree.Get(dentry).(*Dentry)
		if !reflect.DeepEqual(dentry, expectDentry) {
			t.Errorf("dentry info mismatch, expect:%v, actual:%v", expectDentry, dentry)
			t.FailNow()
		}
		for i = 1; i < 5; i++ {
			ino2 := ino * i + 1000
			name = fmt.Sprintf("test_dentry_store_%v_%v", ino, ino2)
			dentry, _ = mp.dentryTree.Get(ino, name)
			if dentry == nil {
				t.Errorf("Not found dentry[parent id:%v, name:%s]", ino, name)
				t.FailNow()
			}
			expectDentry = tree.Get(dentry).(*Dentry)
			if !reflect.DeepEqual(dentry, expectDentry) {
				t.Errorf("dentry info mismatch, expect:%v, actual:%v", expectDentry, dentry)
				t.FailNow()
			}
		}
	}
}

func TestMetaPartition_storeExtend(t *testing.T) {
	var ino uint64
	filename := path.Join(rootdir, extendFile)
	_, err := os.Stat(filename)
	if err == nil {
		os.Remove(filename)
	}
	defer os.Remove(filename)

	tree := NewBtree()
	for ino = 1; ino < 100; ino++ {
		extend := NewExtend(ino)
		for index := 1; index <= 10; index++ {
			key := []byte(fmt.Sprintf("test_store_extend_key_%v_%v", ino, index))
			value := []byte(fmt.Sprintf("test_store_extend_value_%v_%v", ino, index))
			extend.Put(key, value)
		}
		tree.ReplaceOrInsert(extend, false)
	}

	msg := new(storeMsg)
	msg.snap = &BTreeSnapShot{
		extend: &ExtendBTree{tree},
	}

	mp := new(metaPartition)
	mp.config = new(MetaPartitionConfig)
	mp.config.PartitionId = 1
	mp.config.VolName = "ltptest"
	mp.config.StoreMode = proto.StoreModeMem
	mp.config.Cursor = 10000
	mp.config.Start = 0
	mp.initMemoryTree()
	_, err = mp.storeExtend(rootdir, msg)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	mp.extendTree = &ExtendBTree{NewBtree()}
	err = mp.loadExtend(rootdir)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	if mp.extendTree.Count() != uint64(tree.Len()) {
		t.FailNow()
	}

	for ino = 1; ino < 100; ino++ {
		extend, _ := mp.extendTree.Get(ino)
		if extend == nil {
			t.Errorf("Not found extent[ino:%v]", ino)
			t.FailNow()
		}
		if len(extend.dataMap) != 10 {
			t.Errorf("extend data map count mismatch, expect:10, actual:%v", len(extend.dataMap))
			t.FailNow()
		}
		expectExtend := tree.Get(extend)
		if !reflect.DeepEqual(extend, expectExtend) {
			t.Errorf("extend mismatch, expect:%v, actual:%v", expectExtend, extend)
			t.FailNow()
		}
	}
}

func TestMetaPartition_storeMultipart(t *testing.T) {
	filename := path.Join(rootdir, multipartFile)
	_, err := os.Stat(filename)
	if err == nil {
		os.Remove(filename)
	}
	defer os.Remove(filename)

	multiparts := make([]*Multipart, 0, 99)
	tree := NewBtree()
	timeNow := time.Now()
	for index := 1; index < 100; index++ {
		parts := make([]*Part, 0, 10)
		for partID := 1; partID <= 10; partID++ {
			part := &Part{
				ID: uint16(partID),
				UploadTime: timeNow.Add(time.Second * time.Duration(partID)).Local(),
				Inode: uint64(partID * 100),
				Size: uint64(partID * 1000),
			}
			parts = append(parts, part)
		}
		extend := NewMultipartExtend()
		for i := 1; i <= 10; i++ {
			key := fmt.Sprintf("test_store_multipart_key_%v_%v", index, i)
			value := fmt.Sprintf("test_store_multipart_value_%v_%v", index, i)
			extend[key] = value
		}
		p := fmt.Sprintf("/test_multipart_store/%v", index)
		multipart := &Multipart{
			key: p,
			id: util.CreateMultipartID(10).String(),
			parts: parts,
			extend: extend,
			initTime: timeNow.Local(),
		}
		tree.ReplaceOrInsert(multipart, false)
		multiparts = append(multiparts, multipart)
	}

	msg := new(storeMsg)
	msg.snap = &BTreeSnapShot{
		multipart: &MultipartBTree{tree},
	}

	mp := new(metaPartition)
	mp.config = new(MetaPartitionConfig)
	mp.config.PartitionId = 1
	mp.config.VolName = "ltptest"
	mp.config.StoreMode = proto.StoreModeMem
	mp.config.Cursor = 10000
	mp.config.Start = 0
	mp.initMemoryTree()
	_, err = mp.storeMultipart(rootdir, msg)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	mp.multipartTree = &MultipartBTree{NewBtree()}
	err = mp.loadMultipart(rootdir)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	if mp.multipartTree.Count() != uint64(tree.Len()) {
		t.FailNow()
	}

	for _, multipart := range multiparts {
		m, _ := mp.multipartTree.Get(multipart.key, multipart.id)
		if m == nil {
			t.Errorf("Not found multipart[%v]", multipart)
			t.FailNow()
		}

		if !compareMultipart(multipart, m) {
			t.Errorf("multipart info mismatch\nexpect:\n%v\n actual:\n%v\n", multipart, m)
			t.FailNow()
		}
	}
}

func compareMultipart(expect, actual *Multipart) bool {
	if expect.id != actual.id {
		return false
	}

	if expect.key != actual.key {
		return false
	}

	if expect.initTime != actual.initTime {
		return false
	}

	if !reflect.DeepEqual(expect.extend, actual.extend) {
		return false
	}

	if len(expect.parts) != len(actual.parts) {
		return false
	}

	for index, part := range expect.parts {
		if !reflect.DeepEqual(part, actual.parts[index]) {
			return false
		}
	}
	return true
}
package metanode

import (
	"encoding/json"
	"github.com/chubaofs/chubaofs/metanode/metamock"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/jacobsa/daemonize"
	"os"
	"testing"
	"time"
)

func init() {
	level := log.DebugLevel
	_, err := log.InitLog("./logs", "test", level, nil)
	if err != nil {
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
}

func mockInodeTree() InodeTree {
	tree := NewBtree()
	var id uint64
	for id = 10; id < 15; id++ {
		ino := NewInode(id, proto.Mode(os.ModeDir))
		tree.ReplaceOrInsert(ino, false)
	}
	for id = 20; id < 25; id++ {
		ino := NewInode(id, 1)
		tree.ReplaceOrInsert(ino, false)
	}
	return &InodeBTree{tree}
}

func mockInodeTreeByStoreMode(t *testing.T, storeMode proto.StoreMode, rocksTree *RocksTree) InodeTree {
	inodeTree := newInodeTree(t, storeMode, rocksTree)
	var id uint64
	for id = 10; id < 15; id++ {
		ino := NewInode(id, proto.Mode(os.ModeDir))
		_, _, _ = inodeCreate(inodeTree, ino, false)
	}
	for id = 20; id < 25; id++ {
		ino := NewInode(id, 1)
		_, _, _ = inodeCreate(inodeTree, ino, false)
	}
	return inodeTree
}

func mockDeletedInodeTree() DeletedInodeTree {
	tree := NewBtree()
	var id uint64
	for id = 10; id < 15; id++ {
		ino := NewInode(id, proto.Mode(os.ModeDir))
		dino := NewDeletedInode(ino, ts)
		tree.ReplaceOrInsert(dino, false)
	}
	for id = 20; id < 25; id++ {
		ino := NewInode(id, 1)
		dino := NewDeletedInode(ino, ts)
		tree.ReplaceOrInsert(dino, false)
	}
	return &DeletedInodeBTree{tree}
}

func TestMetaPartition_mvToDeletedInodeTree(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_delete_inode_01",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_delete_inode_01",
			applyFunc: ApplyMock,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp, err := mockMetaPartition(1, 1, test.storeMode, test.rootDir, test.applyFunc)
			if err != nil {
				t.Logf("mock mp failed:%v", err)
				return
			}
			defer releaseMetaPartition(mp)
			mp.config.Cursor = 0
			mp.config.Start = 1
			mp.config.End = 1000
			mp.config.TrashRemainingDays = 3

			var rocksTree *RocksTree
			if mp.HasRocksDBStore() {
				rocksTree, _ = DefaultRocksTree(mp.db)
			}
			mp.inodeTree = mockInodeTreeByStoreMode(t, test.storeMode, rocksTree)

			unlinkInodeReq := &UnlinkInoReq{
				PartitionID: 1,
				Inode:       10,
				TrashEnable: true,
			}
			var p = &Packet{}
			if err = mp.UnlinkInode(unlinkInodeReq, p); err != nil {
				t.Errorf("unlink inode failed:%v", err)
				t.FailNow()
			}

			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}
			ino := NewInode(10, proto.Mode(os.ModeDir))
			_, _, status, _ := mp.getDeletedInode(10)
			if status != proto.OpOk {
				t.Error(status)
				t.FailNow()
			}
			res, _ := mp.getInode(ino)
			if res.Status != proto.OpNotExistErr {
				t.Error(status)
				t.FailNow()
			}

			status, _ = mp.mvToDeletedInodeTree(nil, ino, ts)
			if status != proto.OpExistErr {
				t.Error(status)
				t.FailNow()
			}
		})
	}
}

func TestMetaPartition_RecoverDeletedInodeCase01(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_recover_inode_01",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_recover_inode_01",
			applyFunc: ApplyMock,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp, err := mockMetaPartition(1, 1, test.storeMode, test.rootDir, test.applyFunc)
			if err != nil {
				t.Logf("mock mp failed:%v", err)
				return
			}
			defer releaseMetaPartition(mp)
			mp.config.Cursor = 100000
			mp.config.Start = 1
			mp.config.End = 16000000
			mp.config.TrashRemainingDays = 3

			var rocksTree *RocksTree
			if mp.HasRocksDBStore() {
				rocksTree, _ = DefaultRocksTree(mp.db)
			}
			mp.inodeTree = mockInodeTreeByStoreMode(t, test.storeMode, rocksTree)
			mp.freeList = newFreeList()
			ino1 := NewInode(10, proto.Mode(os.ModeDir))
			t.Logf("ino1:%v", ino1)

			unlinkInodeReq := &UnlinkInoReq{
				PartitionID: 1,
				Inode:       10,
				TrashEnable: true,
			}
			var p = &Packet{}
			if err = mp.UnlinkInode(unlinkInodeReq, p); err != nil {
				t.Errorf("unlink inode failed:%v", err)
				t.FailNow()
			}

			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			ino2 := NewInode(20, 1)
			ino2.NLink = 0
			_, _, _ = inodeCreate(mp.inodeTree, ino2, true)
			evictInodeReq := &EvictInodeReq{
				PartitionID: 1,
				Inode:       20,
				TrashEnable: true,
			}
			p = &Packet{}
			if err = mp.EvictInode(evictInodeReq, p); err != nil {
				t.Errorf("evict inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			recoverInodeReq := &RecoverDeletedInodeReq{
				PartitionID: 1,
				Inode:       10,
			}
			if err = mp.RecoverDeletedInode(recoverInodeReq, p); err != nil {
				t.Errorf("recover inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}
			res, _ := mp.getInode(ino1)
			if res.Status != proto.OpOk {
				t.Errorf("status: %v", res.Status)
				t.FailNow()
			}
			if res.Msg.ShouldDelete() == true {
				t.Error(res.Msg)
				t.FailNow()
			}
			if res.Msg.NLink != 2 {
				t.Error(res.Msg)
				t.FailNow()
			}

			{
				if err = mp.RecoverDeletedInode(recoverInodeReq, p); err != nil {
					t.Errorf("recover inode failed:%v", err)
					t.FailNow()
				}
				if p.ResultCode != proto.OpOk {
					t.Error(p.ResultCode)
					t.FailNow()
				}
				res, _ = mp.getInode(ino1)
				if res.Status != proto.OpOk {
					t.Error(res.Status)
					t.FailNow()
				}
				if res.Msg.ShouldDelete() == true {
					t.Error(res.Msg)
					t.FailNow()
				}
				if res.Msg.NLink < 2 { // NLink should be 2, but 3
					t.Error(res.Msg)
					t.FailNow()
				}
			}

			recoverInodeReq = &RecoverDeletedInodeReq{
				PartitionID: 1,
				Inode:       20,
			}
			if err = mp.RecoverDeletedInode(recoverInodeReq, p); err != nil {
				t.Errorf("recover inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}
			res, _ = mp.getInode(ino2)
			if res.Status != proto.OpOk {
				t.Error(res.Status)
				t.FailNow()
			}
			if res.Msg.ShouldDelete() == true {
				t.Error(res.Msg)
				t.FailNow()
			}
			if res.Msg.NLink != 1 {
				t.Error(res.Msg)
				t.FailNow()
			}
			{
				if err = mp.RecoverDeletedInode(recoverInodeReq, p); err != nil {
					t.Errorf("recover inode failed:%v", err)
					t.FailNow()
				}
				if p.ResultCode != proto.OpOk {
					t.Error(p.ResultCode)
					t.FailNow()
				}
				res, _ = mp.getInode(ino2)
				if res.Status != proto.OpOk {
					t.Error(res.Status)
					t.FailNow()
				}
				if res.Msg.ShouldDelete() == true {
					t.Error(res.Msg)
					t.FailNow()
				}
			}
		})
	}
}

/*
case1: the original inode is not exist, which is file
case2: the original inode is not exist, which is dir
*/
func TestMetaPartition_RecoverDeletedInodeCase02(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_recover_inode_02",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_recover_inode_02",
			applyFunc: ApplyMock,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp, err := mockMetaPartition(1, 1, test.storeMode, test.rootDir, test.applyFunc)
			if err != nil {
				t.Logf("mock mp failed:%v", err)
				return
			}
			defer releaseMetaPartition(mp)
			mp.config.Cursor = 100000
			mp.config.Start = 1
			mp.config.End = 16000000
			mp.config.TrashRemainingDays = 3

			var rocksTree *RocksTree
			if mp.HasRocksDBStore() {
				rocksTree, _ = DefaultRocksTree(mp.db)
			}
			mp.inodeTree = mockInodeTreeByStoreMode(t, test.storeMode, rocksTree)
			mp.freeList = newFreeList()
			ino1 := NewInode(10, proto.Mode(os.ModeDir))
			t.Logf("ino1:%v", ino1)

			unlinkInodeReq := &UnlinkInoReq{
				PartitionID: 1,
				Inode:       10,
				TrashEnable: true,
			}
			var p = &Packet{}
			if err = mp.UnlinkInode(unlinkInodeReq, p); err != nil {
				t.Errorf("unlink inode failed:%v", err)
				t.FailNow()
			}

			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			ino2 := NewInode(20, 1)
			ino2.NLink = 0
			_, _, _ = inodeCreate(mp.inodeTree, ino2, true)
			evictInodeReq := &EvictInodeReq{
				PartitionID: 1,
				Inode:       20,
				TrashEnable: true,
			}
			p = &Packet{}
			if err = mp.EvictInode(evictInodeReq, p); err != nil {
				t.Errorf("evict inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}
			mp.inodeTree = mockInodeTreeByStoreMode(t, test.storeMode, rocksTree)

			recoverInodeReq := &RecoverDeletedInodeReq{
				PartitionID: 1,
				Inode:       10,
			}
			if err = mp.RecoverDeletedInode(recoverInodeReq, p); err != nil {
				t.Errorf("recover inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}
			res, _ := mp.getInode(ino1)
			if res.Status != proto.OpOk {
				t.Errorf("status: %v", res.Status)
				t.FailNow()
			}
			if res.Msg.ShouldDelete() == true {
				t.Error(res.Msg)
				t.FailNow()
			}
			if res.Msg.NLink != 2 {
				t.Error(res.Msg)
				t.FailNow()
			}

			{
				if err = mp.RecoverDeletedInode(recoverInodeReq, p); err != nil {
					t.Errorf("recover inode failed:%v", err)
					t.FailNow()
				}
				if p.ResultCode != proto.OpOk {
					t.Error(p.ResultCode)
					t.FailNow()
				}
				res, _ = mp.getInode(ino1)
				if res.Status != proto.OpOk {
					t.Error(res.Status)
					t.FailNow()
				}
				if res.Msg.ShouldDelete() == true {
					t.Error(res.Msg)
					t.FailNow()
				}
				if res.Msg.NLink < 2 { // NLink should be 2, but 3
					t.Error(res.Msg)
					t.FailNow()
				}
			}

			recoverInodeReq = &RecoverDeletedInodeReq{
				PartitionID: 1,
				Inode:       20,
			}
			if err = mp.RecoverDeletedInode(recoverInodeReq, p); err != nil {
				t.Errorf("recover inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}
			res, _ = mp.getInode(ino2)
			if res.Status != proto.OpOk {
				t.Error(res.Status)
				t.FailNow()
			}
			if res.Msg.ShouldDelete() == true {
				t.Error(res.Msg)
				t.FailNow()
			}
			if res.Msg.NLink != 1 {
				t.Error(res.Msg)
				t.FailNow()
			}
			{
				if err = mp.RecoverDeletedInode(recoverInodeReq, p); err != nil {
					t.Errorf("recover inode failed:%v", err)
					t.FailNow()
				}
				if p.ResultCode != proto.OpOk {
					t.Error(p.ResultCode)
					t.FailNow()
				}
				res, _ = mp.getInode(ino2)
				if res.Status != proto.OpOk {
					t.Error(res.Status)
					t.FailNow()
				}
				if res.Msg.ShouldDelete() == true {
					t.Error(res.Msg)
					t.FailNow()
				}
			}
		})
	}
}

func mockTestDeletedInodeTree() *BTree {
	tree := NewBtree()
	date := "2021-01-01"
	loc, _ := time.LoadLocation("Local")
	ts, _ := time.ParseInLocation("2006-01-02", date, loc)

	var id uint64
	for id = 10; id < 15; id++ {
		ino := NewInode(id, proto.Mode(os.ModeDir))
		ino.DecNLink()
		curr := ts.AddDate(0, 0, int(id))
		dino := NewDeletedInode(ino, curr.UnixNano()/1000)
		tree.ReplaceOrInsert(dino, false)
	}
	for id = 20; id < 25; id++ {
		ino := NewInode(id, 1)
		ino.DecNLink()
		curr := ts.AddDate(0, 0, int(id))
		dino := NewDeletedInode(ino, curr.UnixNano()/1000)
		tree.ReplaceOrInsert(dino, false)
	}
	return tree
}

func TestMetaPartition_CleanDeletedInodeCase01(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_clean_deleted_inode_02",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_clean_deleted_inode_02",
			applyFunc: ApplyMock,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp, err := mockMetaPartition(1, 1, test.storeMode, test.rootDir, test.applyFunc)
			if err != nil {
				t.Logf("mock mp failed:%v", err)
				return
			}
			defer releaseMetaPartition(mp)
			mp.config.Cursor = 100000
			mp.config.Start = 1
			mp.config.End = 16000000
			mp.config.TrashRemainingDays = 3

			var rocksTree *RocksTree
			if mp.HasRocksDBStore() {
				rocksTree, _ = DefaultRocksTree(mp.db)
			}
			mp.inodeTree = mockInodeTreeByStoreMode(t, test.storeMode, rocksTree)
			mp.freeList = newFreeList()

			ino1 := NewInode(10, proto.Mode(os.ModeDir))
			t.Logf("ino1:%v", ino1)

			unlinkInodeReq := &UnlinkInoReq{
				PartitionID: 1,
				Inode:       10,
				TrashEnable: true,
			}
			var p = &Packet{}
			if err = mp.UnlinkInode(unlinkInodeReq, p); err != nil {
				t.Errorf("unlink inode failed:%v", err)
				t.FailNow()
			}

			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			status, _ := mp.mvToDeletedInodeTree(nil, ino1, ts)
			if status != proto.OpExistErr {
				t.Error(status)
				t.FailNow()
			}

			ino2 := NewInode(20, 1)
			ino2.NLink = 0
			_, _, _ = inodeCreate(mp.inodeTree, ino2, true)
			evictInodeReq := &EvictInodeReq{
				PartitionID: 1,
				Inode:       20,
				TrashEnable: true,
			}
			p = &Packet{}
			if err = mp.EvictInode(evictInodeReq, p); err != nil {
				t.Errorf("evict inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}
			status, _ = mp.mvToDeletedInodeTree(nil, ino2, ts)
			if status != proto.OpExistErr {
				t.Error(status)
				t.FailNow()
			}

			ino3 := NewInode(21, 1)
			ino3.NLink = 0
			inodeCreate(mp.inodeTree, ino3, true)
			evictInodeReq = &EvictInodeReq{
				PartitionID: 1,
				Inode:       21,
				TrashEnable: true,
			}
			p = &Packet{}
			if err = mp.EvictInode(evictInodeReq, p); err != nil {
				t.Errorf("evict inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			cleanDeletedInodeReq := &CleanDeletedInodeReq{
				PartitionID: 1,
				Inode:       10,
			}
			p = &Packet{}
			if err = mp.CleanDeletedInode(cleanDeletedInodeReq, p); err != nil {
				t.Errorf("clean deleted inode failed:%v", err)
				t.FailNow()
			}

			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			_, _, status, _ = mp.getDeletedInode(ino1.Inode)
			if status != proto.OpNotExistErr {
				t.Error(status)
				t.FailNow()
			}

			if mp.freeList.Len() > 0 {
				t.Errorf("freelist: %v", mp.freeList.Len())
				t.FailNow()
			}

			_, delInode, _, _ := mp.getDeletedInode(ino2.Inode)
			delInode.NLink = 2
			deletedInodeCreate(mp.inodeDeletedTree, delInode, true)
			cleanDeletedInodeReq = &CleanDeletedInodeReq{
				PartitionID: 1,
				Inode:       20,
			}
			p = &Packet{}
			if err = mp.CleanDeletedInode(cleanDeletedInodeReq, p); err != nil {
				t.Errorf("clean deleted inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpErr {
				t.Error(p.ResultCode)
				t.FailNow()
			}
			_, _, status, _ = mp.getDeletedInode(ino2.Inode)
			if status != proto.OpOk {
				t.Error(status)
				t.FailNow()
			}
			if mp.freeList.Len() > 0 {
				t.Errorf("freelist: %v", mp.freeList.Len())
				t.FailNow()
			}

			cleanDeletedInodeReq = &CleanDeletedInodeReq{
				PartitionID: 1,
				Inode:       21,
			}
			p = &Packet{}
			if err = mp.CleanDeletedInode(cleanDeletedInodeReq, p); err != nil {
				t.Errorf("clean deleted inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}
			_, _, status, _ = mp.getDeletedInode(ino3.Inode)
			if status != proto.OpOk {
				t.Error(status)
				t.FailNow()
			}

			if mp.freeList.Len() != 1 {
				t.Errorf("freelist: %v", mp.freeList.Len())
				t.FailNow()
			}

		})
	}
}

func TestMetaPartition_CleanDeletedInodeCase02(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_clean_deleted_inode_02",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_clean_deleted_inode_02",
			applyFunc: ApplyMock,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp, err := mockMetaPartition(1, 1, test.storeMode, test.rootDir, test.applyFunc)
			if err != nil {
				t.Logf("mock mp failed:%v", err)
				return
			}
			defer releaseMetaPartition(mp)
			mp.config.Cursor = 100000
			mp.config.Start = 1
			mp.config.End = 16000000
			mp.config.TrashRemainingDays = 3

			var rocksTree *RocksTree
			if mp.HasRocksDBStore() {
				rocksTree, _ = DefaultRocksTree(mp.db)
			}
			mp.inodeTree = mockInodeTreeByStoreMode(t, test.storeMode, rocksTree)
			mp.freeList = newFreeList()

			ino1 := NewInode(10, proto.Mode(os.ModeDir))
			t.Logf("ino1:%v", ino1)

			unlinkInodeReq := &UnlinkInoReq{
				PartitionID: 1,
				Inode:       10,
				TrashEnable: true,
			}
			var p = &Packet{}
			if err = mp.UnlinkInode(unlinkInodeReq, p); err != nil {
				t.Errorf("unlink inode failed:%v", err)
				t.FailNow()
			}

			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			den := newPrimaryDeletedDentry(10, "f1", ts, 100)
			_, _, _ = deletedDentryCreate(mp.dentryDeletedTree, den, false)

			cleanDeletedInodeReq := &CleanDeletedInodeReq{
				PartitionID: 1,
				Inode:       10,
			}
			p = &Packet{}
			if err = mp.CleanDeletedInode(cleanDeletedInodeReq, p); err != nil {
				t.Errorf("clean deleted inode failed:%v", err)
				t.FailNow()
			}

			if p.ResultCode != proto.OpExistErr {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			_, _ = deletedDentryDelete(mp.dentryDeletedTree, den)
			cleanDeletedInodeReq = &CleanDeletedInodeReq{
				PartitionID: 1,
				Inode:       10,
			}
			p = &Packet{}
			if err = mp.CleanDeletedInode(cleanDeletedInodeReq, p); err != nil {
				t.Errorf("clean deleted inode failed:%v", err)
				t.FailNow()
			}

			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}
		})
	}
}

func TestMetaPartition_BatchCleanDeletedInode(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_batch_clean_deleted_inode_01",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_batch_clean_deleted_inode_01",
			applyFunc: ApplyMock,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp, err := mockMetaPartition(1, 1, test.storeMode, test.rootDir, test.applyFunc)
			if err != nil {
				t.Logf("mock mp failed:%v", err)
				return
			}
			defer releaseMetaPartition(mp)
			mp.config.Cursor = 100000
			mp.config.Start = 1
			mp.config.End = 16000000
			mp.config.TrashRemainingDays = 3

			var rocksTree *RocksTree
			if mp.HasRocksDBStore() {
				rocksTree, _ = DefaultRocksTree(mp.db)
			}
			mp.inodeTree = mockInodeTreeByStoreMode(t, test.storeMode, rocksTree)
			mp.freeList = newFreeList()

			unlinkInodeReq := &UnlinkInoReq{
				PartitionID: 1,
				Inode:       10,
				TrashEnable: true,
			}
			var p = &Packet{}
			if err = mp.UnlinkInode(unlinkInodeReq, p); err != nil {
				t.Errorf("unlink inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			ino2 := NewInode(20, 1)
			ino2.NLink = 0
			_, _, _ = inodeCreate(mp.inodeTree, ino2, true)
			evictInodeReq := &EvictInodeReq{
				PartitionID: 1,
				Inode:       20,
				TrashEnable: true,
			}
			p = &Packet{}
			if err = mp.EvictInode(evictInodeReq, p); err != nil {
				t.Errorf("evict inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			ino3 := NewInode(21, 1)
			ino3.NLink = 0
			_, _, _ = inodeCreate(mp.inodeTree, ino3, true)
			evictInodeReq = &EvictInodeReq{
				PartitionID: 1,
				Inode:       21,
				TrashEnable: true,
			}
			p = &Packet{}
			if err = mp.EvictInode(evictInodeReq, p); err != nil {
				t.Errorf("evict inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			batchCleanInodeReq := &BatchCleanDeletedInodeReq{
				PartitionID: 1,
				Inodes:      []uint64{10, 20 ,21},
			}
			p = &Packet{}
			if err = mp.BatchCleanDeletedInode(batchCleanInodeReq, p);err != nil {
				t.Errorf("batch clean deleted inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}
			resp := new(proto.BatchOpDeletedINodeRsp)
			if err = json.Unmarshal(p.Data, resp); err != nil {
				t.Errorf("unmarshal batch op deleted inode response failed:%v", err)
				t.FailNow()
			}
			if len(resp.Inos) > 0 {
				t.Errorf("len: %v", len(resp.Inos))
				for _, item := range resp.Inos {
					t.Errorf("ino: %v, st: %v", item.Inode, item.Status)
				}
			}
		})
	}
}

func TestMetaPartition_BatchCleanExpiredDeletedInodeCase01(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_clean_expired_deleted_inode_01",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_clean_expired_deleted_inode_01",
			applyFunc: ApplyMock,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp, err := mockMetaPartition(1, 1, test.storeMode, test.rootDir, test.applyFunc)
			if err != nil {
				t.Logf("mock mp failed:%v", err)
				return
			}
			defer releaseMetaPartition(mp)
			mp.config.Cursor = 100000
			mp.config.Start = 1
			mp.config.End = 16000000
			mp.config.TrashRemainingDays = 3

			var rocksTree *RocksTree
			if mp.HasRocksDBStore() {
				rocksTree, _ = DefaultRocksTree(mp.db)
			}
			mp.inodeTree = mockInodeTreeByStoreMode(t, test.storeMode, rocksTree)
			mp.freeList = newFreeList()

			unlinkInodeReq := &UnlinkInoReq{
				PartitionID: 1,
				Inode:       10,
				TrashEnable: true,
			}
			var p = &Packet{}
			if err = mp.UnlinkInode(unlinkInodeReq, p); err != nil {
				t.Errorf("unlink inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			ino2 := NewInode(20, 1)
			ino2.NLink = 0
			_, _, _ = inodeCreate(mp.inodeTree, ino2, true)
			evictInodeReq := &EvictInodeReq{
				PartitionID: 1,
				Inode:       20,
				TrashEnable: true,
			}
			p = &Packet{}
			if err = mp.EvictInode(evictInodeReq, p); err != nil {
				t.Errorf("evict inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			resp, _ := mp.cleanExpiredInode(nil, 11)
			if resp == nil {
				t.Errorf("resp expect not nil")
				t.FailNow()
			}
			if resp.Status != proto.OpOk {
				t.Log(resp.Status)
				t.FailNow()
			}
			if mp.freeList.Len() != 0 {
				t.Logf("len: %v", mp.freeList.Len())
				t.FailNow()
			}

			mp.config.TrashRemainingDays = 0
			if err = mp.CleanExpiredDeletedINode(); err != nil {
				t.Errorf("clean expired deleted inode failed:%v", err)
				t.FailNow()
			}
			if mp.freeList.Len() != 1 {
				t.Logf("len: %v", mp.freeList.Len())
				t.FailNow()
			}

			ino1 := NewInode(10, proto.Mode(os.ModeDir))
			srcIno, di, st, _ := mp.getDeletedInode(ino1.Inode)
			if st != proto.OpNotExistErr{
				t.Log(st)
				t.FailNow()
			}
			if di != nil {
				t.Errorf("di expect is nil, but not nil")
				t.FailNow()
			}
			if srcIno != nil {
				t.Errorf("srcIno expect is nil, but not nil")
				t.FailNow()
			}

			srcIno, di, st, _ = mp.getDeletedInode(ino2.Inode)
			if st != proto.OpOk {
				t.Log(st)
				t.FailNow()
			}
			if di == nil {
				t.Errorf("di expect is not nil, but is nil")
				t.FailNow()
			}
			if di.IsExpired != true {
				t.Errorf("di expect is expired, but not expired")
				t.FailNow()
			}
			if srcIno != nil {
				t.Errorf("srcIno expect is nil, but not nil")
				t.FailNow()
			}
		})
	}
}

func TestMetaPartition_BatchCleanExpiredDeletedInodeCase02(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_clean_expired_deleted_inode_02",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_clean_expired_deleted_inode_02",
			applyFunc: ApplyMock,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp, err := mockMetaPartition(1, 1, test.storeMode, test.rootDir, test.applyFunc)
			if err != nil {
				t.Logf("mock mp failed:%v", err)
				return
			}
			defer releaseMetaPartition(mp)
			mp.config.Cursor = 100000
			mp.config.Start = 1
			mp.config.End = 16000000
			mp.config.TrashRemainingDays = 3

			var rocksTree *RocksTree
			if mp.HasRocksDBStore() {
				rocksTree, _ = DefaultRocksTree(mp.db)
			}
			mp.inodeTree = mockInodeTreeByStoreMode(t, test.storeMode, rocksTree)
			mp.freeList = newFreeList()

			unlinkInodeReq := &UnlinkInoReq{
				PartitionID: 1,
				Inode:       10,
				TrashEnable: true,
			}
			var p = &Packet{}
			if err = mp.UnlinkInode(unlinkInodeReq, p); err != nil {
				t.Errorf("unlink inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			ino2 := NewInode(20, 1)
			ino2.NLink = 0
			_, _, _ = inodeCreate(mp.inodeTree, ino2, true)
			evictInodeReq := &EvictInodeReq{
				PartitionID: 1,
				Inode:       20,
				TrashEnable: true,
			}
			p = &Packet{}
			if err = mp.EvictInode(evictInodeReq, p); err != nil {
				t.Errorf("evict inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			ino3 := NewInode(21, 1)
			ino3.NLink = 0
			_, _, _ = inodeCreate(mp.inodeTree, ino3, true)
			evictInodeReq = &EvictInodeReq{
				PartitionID: 1,
				Inode:       21,
				TrashEnable: true,
			}
			p = &Packet{}
			if err = mp.EvictInode(evictInodeReq, p); err != nil {
				t.Errorf("evict inode failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			mp.config.TrashRemainingDays = 0
			if err = mp.CleanExpiredDeletedINode(); err != nil {
				t.Errorf("clean expired deleted inode failed:%v", err)
				t.FailNow()
			}
			if mp.freeList.Len() != 2 {
				t.Errorf("free list len mismatch, expect:2, actual:%v", mp.freeList.Len())
				t.FailNow()
			}
		})
	}
}

package metanode

import (
	"encoding/json"
	"github.com/chubaofs/chubaofs/metanode/metamock"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"os"
	"testing"
	"time"
)

var (
	ts             = time.Now().UnixNano() / 1000
	msFactor int64 = 1000
	from           = "localhost"
)

func mockDentryTree() DentryTree {
	tree := NewBtree()

	d1 := new(Dentry)
	d1.ParentId = 1
	d1.Inode = 10
	d1.Type = proto.Mode(os.ModeDir)
	d1.Name = "d1"
	tree.ReplaceOrInsert(d1, false)

	d2 := new(Dentry)
	d2.ParentId = 1
	d2.Inode = 11
	d2.Type = proto.Mode(os.ModeDir)
	d2.Name = "d2"
	tree.ReplaceOrInsert(d2, false)

	f1 := new(Dentry)
	f1.ParentId = 10
	f1.Inode = 100
	f1.Type = 1
	f1.Name = "f1"
	tree.ReplaceOrInsert(f1, false)

	f2 := new(Dentry)
	f2.ParentId = 10
	f2.Inode = 101
	f2.Type = 1
	f2.Name = "f2"
	tree.ReplaceOrInsert(f2, false)
	return &DentryBTree{tree}
}

func mockDentryTreeByStoreMode(t *testing.T, storeMode proto.StoreMode, rocksTree *RocksTree) DentryTree {
	dentryTree := newDentryTree(t, storeMode, rocksTree)
	d1 := new(Dentry)
	d1.ParentId = 1
	d1.Inode = 10
	d1.Type = proto.Mode(os.ModeDir)
	d1.Name = "d1"
	_, _, _ = dentryCreate(dentryTree, d1, false)

	d2 := new(Dentry)
	d2.ParentId = 1
	d2.Inode = 11
	d2.Type = proto.Mode(os.ModeDir)
	d2.Name = "d2"
	_, _, _ = dentryCreate(dentryTree, d2, false)

	f1 := new(Dentry)
	f1.ParentId = 10
	f1.Inode = 100
	f1.Type = 1
	f1.Name = "f1"
	_, _, _ = dentryCreate(dentryTree, f1, false)

	f2 := new(Dentry)
	f2.ParentId = 10
	f2.Inode = 101
	f2.Type = 1
	f2.Name = "f2"
	_, _, _ = dentryCreate(dentryTree, f2, false)

	f3 := new(Dentry)
	f3.ParentId = 10
	f3.Inode = 103
	f3.Type = 1
	f3.Name = "f3"
	_, _, _ = dentryCreate(dentryTree, f3, false)

	f4 := new(Dentry)
	f4.ParentId = 10
	f4.Inode = 104
	f4.Type = 1
	f4.Name = "f4"
	_, _, _ = dentryCreate(dentryTree, f4, false)
	return dentryTree
}

func mockDeletedDentryTree() DeletedDentryTree {
	tree := NewBtree()

	d1 := new(Dentry)
	d1.ParentId = 1
	d1.Inode = 10
	d1.Type = proto.Mode(os.ModeDir)
	d1.Name = "d1"
	dd := newDeletedDentry(d1, ts, "")
	tree.ReplaceOrInsert(dd, false)

	d2 := new(Dentry)
	d2.ParentId = 1
	d2.Inode = 11
	d2.Type = proto.Mode(os.ModeDir)
	d2.Name = "d2"
	dd = new(DeletedDentry)
	dd = newDeletedDentry(d2, ts, "")
	tree.ReplaceOrInsert(dd, false)

	f1 := new(Dentry)
	f1.ParentId = 10
	f1.Inode = 100
	f1.Type = 1
	f1.Name = "f1"
	dd = new(DeletedDentry)
	dd = newDeletedDentry(f1, ts, "")
	tree.ReplaceOrInsert(dd, false)

	f2 := new(Dentry)
	f2.ParentId = 10
	f2.Inode = 101
	f2.Type = 1
	f2.Name = "f2"
	dd = new(DeletedDentry)
	dd = newDeletedDentry(f2, ts, "")
	tree.ReplaceOrInsert(dd, false)

	f2 = new(Dentry)
	f2.ParentId = 10
	f2.Inode = 102
	f2.Type = 1
	f2.Name = "f2"
	dd = new(DeletedDentry)
	dd = newDeletedDentry(f2, ts+msFactor, from)
	tree.ReplaceOrInsert(dd, false)
	return &DeletedDentryBTree{tree}
}

func mockDeletedDentryTreeBak(deletedDentryTree DeletedDentryTree) {
	d1 := new(Dentry)
	d1.ParentId = 1
	d1.Inode = 10
	d1.Type = proto.Mode(os.ModeDir)
	d1.Name = "d1"
	dd := newDeletedDentry(d1, ts, "")
	_, _ , _ = deletedDentryCreate(deletedDentryTree, dd, false)

	d2 := new(Dentry)
	d2.ParentId = 1
	d2.Inode = 11
	d2.Type = proto.Mode(os.ModeDir)
	d2.Name = "d2"
	dd = new(DeletedDentry)
	dd = newDeletedDentry(d2, ts, "")
	_, _ , _ = deletedDentryCreate(deletedDentryTree, dd, false)

	f1 := new(Dentry)
	f1.ParentId = 10
	f1.Inode = 100
	f1.Type = 1
	f1.Name = "f1"
	dd = new(DeletedDentry)
	dd = newDeletedDentry(f1, ts, "")
	_, _ , _ = deletedDentryCreate(deletedDentryTree, dd, false)

	f2 := new(Dentry)
	f2.ParentId = 10
	f2.Inode = 101
	f2.Type = 1
	f2.Name = "f2"
	dd = new(DeletedDentry)
	dd = newDeletedDentry(f2, ts, "")
	_, _ , _ = deletedDentryCreate(deletedDentryTree, dd, false)

	f2 = new(Dentry)
	f2.ParentId = 10
	f2.Inode = 102
	f2.Type = 1
	f2.Name = "f2"
	dd = new(DeletedDentry)
	dd = newDeletedDentry(f2, ts+msFactor, from)
	_, _ , _ = deletedDentryCreate(deletedDentryTree, dd, false)
	return
}

func TestMetaPartition_mvToDeletedDentryTree(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_delete_dentry_01",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_delete_dentry_01",
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
			mp.dentryTree = mockDentryTreeByStoreMode(t, test.storeMode, rocksTree)

			var timestamp int64 = time.Now().UnixNano() / 1000
			f1 := new(Dentry)
			f1.ParentId = 10
			f1.Inode = 100
			f1.Type = 1
			f1.Name = "f1"
			df1 := newDeletedDentry(f1, timestamp, from)

			req := &DeleteDentryReq{
				PartitionID: 1,
				ParentID:    10,
				Name:        "f1",
				NoTrash:     false,
			}
			var p = &Packet{}
			if err = mp.DeleteDentry(req, p); err != nil {
				t.Errorf("delete dentry failed, error:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			df2 := newDeletedDentry(f1, time.Now().UnixNano() / 1000, from)
			ds, status, _ := mp.getDeletedDentry(df1, df2)
			if status != proto.OpOk {
				t.Errorf("0X%X", status)
				t.FailNow()
			}

			if len(ds) != 1 {
				t.FailNow()
			}

			if err = mp.DeleteDentry(req, p); err != nil {
				t.Errorf("delete dentry failed, error:%v", err)
				t.FailNow()
			}

			if p.ResultCode != proto.OpNotExistErr {
				t.Errorf("result code mismatch, expect:Not Exist, actual:%v", p.ResultCode)
			}
		})
	}
}

func TestMetaPartition_fsmCleanDeletedDentry(t *testing.T) {
	tests := []struct{
		name       string
		storeMode  proto.StoreMode
		rootDir string
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_clean_delete_dentry",
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_clean_delete_dentry",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp, err := mockMetaPartition(1,1, test.storeMode, test.rootDir, ApplyMock)
			if err != nil {
				return
			}
			defer releaseMetaPartition(mp)
			mp.config.Start = 1
			mp.config.End = 1000
			mockDeletedDentryTreeBak(mp.dentryDeletedTree)

			d1 := new(DeletedDentry)
			d1.ParentId = 1
			d1.Inode = 10
			d1.Type = proto.Mode(os.ModeDir)
			d1.Name = "d1"
			d1.Timestamp = ts
			d1.From = from
			req := &CleanDeletedDentryReq{
				PartitionID: 1,
				ParentID:    1,
				Name:        "d1",
				Timestamp:   ts,
				Inode:       10,
			}
			p := &Packet{
				remote: from,
			}
			if err = mp.CleanDeletedDentry(req, p); err != nil {
				t.Errorf("clean deleted dentry failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Errorf("DeletedDentry: %v, Status: %v", d1, p.ResultCode)
				t.FailNow()
			}

			_, st, _ := mp.getDeletedDentry(d1, d1)
			if st != proto.OpNotExistErr {
				t.Errorf("den: %v, status: %v", d1, st)
			}

			if err = mp.CleanDeletedDentry(req, p); err != nil {
				t.Errorf("clean deleted dentry failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpNotExistErr {
				t.Errorf("DeletedDentry: %v, Status: %v", d1, p.ResultCode)
				t.FailNow()
			}

			req.Timestamp = ts + msFactor
			if err = mp.CleanDeletedDentry(req, p); err != nil {
				t.Errorf("clean deleted dentry failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpNotExistErr {
				t.Errorf("DeletedDentry: %v, Status: %v", d1, p.ResultCode)
				t.FailNow()
			}

			req = &CleanDeletedDentryReq{
				PartitionID: 1,
				ParentID:    10,
				Name:        "f2",
				Timestamp:   ts,
				Inode:       100,
			}
			if err = mp.CleanDeletedDentry(req, p); err != nil {
				t.Errorf("clean deleted dentry failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpNotExistErr {
				t.Errorf("DeletedDentry: %v, Status: %v", req, p.ResultCode)
				t.FailNow()
			}

			req.Name = "f1"
			if err = mp.CleanDeletedDentry(req, p); err != nil {
				t.Errorf("clean deleted dentry failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Errorf("DeletedDentry: %v, Status: %v", req, p.ResultCode)
				t.FailNow()
			}

			req = &CleanDeletedDentryReq{
				PartitionID: 1,
				ParentID:    10,
				Name:        "f2",
				Timestamp:   ts + msFactor,
				Inode:       102,
			}
			if err = mp.CleanDeletedDentry(req, p); err != nil {
				t.Errorf("clean deleted dentry failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Errorf("DeletedDentry: %v, Status: %v", req, p.ResultCode)
				t.FailNow()
			}

			f1 := new(Dentry)
			f1.ParentId = 10
			f1.Inode = 102
			f1.Type = 1
			f1.Name = "f2"
			dd := newDeletedDentry(f1, ts + msFactor, from)
			var status uint8
			_, status, _ = mp.getDeletedDentry(dd, dd)
			if status != proto.OpNotExistErr {
				t.Error(status)
				t.FailNow()
			}

			req = &CleanDeletedDentryReq{
				PartitionID: 1,
				ParentID:    10,
				Name:        "d1",
				Timestamp:   ts + msFactor,
				Inode:       10,
			}
			if err = mp.CleanDeletedDentry(req, p); err != nil {
				t.Errorf("clean deleted dentry failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpNotExistErr {
				t.Errorf("DeletedDentry: %v, Status: %v", req, p.ResultCode)
				t.FailNow()
			}
			d1.Timestamp = ts + msFactor
			_, status, _ = mp.getDeletedDentry(d1, d1)
			if status != proto.OpNotExistErr {
				t.Error(status)
				t.FailNow()
			}

			req = &CleanDeletedDentryReq{
				PartitionID: 1,
				ParentID:    1,
				Name:        "d2",
				Timestamp:   ts,
				Inode:       11,
			}
			if err = mp.CleanDeletedDentry(req, p); err != nil {
				t.Errorf("clean deleted dentry failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Errorf("DeletedDentry: %v, Status: %v", req, p.ResultCode)
				t.FailNow()
			}
			d1.Timestamp = ts
			d1.Inode = 11
			d1.Name = "d2"
			_, status, _ = mp.getDeletedDentry(d1, d1)
			if status != proto.OpNotExistErr {
				t.Error(status)
				t.FailNow()
			}
		})
	}
}

//mem/rocksdb
func TestMetaPartition_RecoverDeletedDentry(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_recover_dentry_01",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_recover_dentry_01",
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
			mp.config.Start = 1
			mp.config.End = 1000
			mp.config.Cursor = 0
			mp.config.TrashRemainingDays = 3

			var rocksTree *RocksTree
			if mp.HasRocksDBStore() {
				rocksTree, _ = DefaultRocksTree(mp.db)
			}
			mp.dentryTree = mockDentryTreeByStoreMode(t, test.storeMode, rocksTree)

			ino := NewInode(10, proto.Mode(os.ModeDir))
			ino.NLink = 4
			_, _, _ = inodeCreate(mp.inodeTree, ino, false)

			timestamp1 := time.Now().UnixNano() / 1000
			req := &DeleteDentryReq{
				PartitionID: 1,
				ParentID:    10,
				Name:        "f3",
				NoTrash:     false,
			}
			var p = &Packet{}
			if err = mp.DeleteDentry(req, p); err != nil {
				t.Errorf("delete dentry failed, error:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}
			timestamp2 := time.Now().UnixNano() / 1000

			dds1, status, _ := mp.getDeletedDentry(newPrimaryDeletedDentry(10, "f3", timestamp1, 103),
				newPrimaryDeletedDentry(10, "f3", timestamp2, 103))
			if len(dds1) != 1 {
				t.Error(status)
				t.FailNow()
			}

			timestamp1 = time.Now().UnixNano() / 1000
			req = &DeleteDentryReq{
				PartitionID: 1,
				ParentID:    10,
				Name:        "f1",
				NoTrash:     false,
			}
			p = &Packet{}
			if err = mp.DeleteDentry(req, p); err != nil {
				t.Errorf("delete dentry failed, error:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			inoResp, _ := mp.getInode(ino)
			if inoResp.Status != proto.OpOk {
				t.Error(inoResp.Status)
				t.FailNow()
			}
			if inoResp.Msg.NLink != 2 {
				t.Errorf("Ino: %v", inoResp.Msg)
				t.FailNow()
			}

			timestamp2 = time.Now().UnixNano() / 1000

			dds2, status, _ := mp.getDeletedDentry(newPrimaryDeletedDentry(10, "f1", timestamp1, 100),
				newPrimaryDeletedDentry(10, "f1", timestamp2, 100))
			if len(dds2) != 1 {
				t.Error(status)
				t.FailNow()
			}

			p = &Packet{}
			createDReq := &CreateDentryReq{
				PartitionID: 1,
				ParentID:    10,
				Inode:       100,
				Name:        "f1",
				Mode:        1,
			}
			if err = mp.CreateDentry(createDReq, p); err != nil {
				t.Errorf("create dentry failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			inoResp, _ = mp.getInode(ino)
			if inoResp.Status != proto.OpOk {
				t.Error(inoResp.Status)
				t.FailNow()
			}
			if inoResp.Msg.NLink != 3 {
				t.Errorf("Ino: %v", inoResp.Msg)
				t.FailNow()
			}

			dd := newPrimaryDeletedDentry(10, "f33", ts, 100)
			recoverDentryReq := &RecoverDeletedDentryReq{
				ParentID:    10,
				Name:        "f33",
				TimeStamp:   ts,
				Inode:       100,
			}
			p = &Packet{}
			if err = mp.RecoverDeletedDentry(recoverDentryReq, p); err != nil {
				t.Errorf("recover deleted dentry failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpNotExistErr {
				t.Error(p.ResultCode)
				t.FailNow()
			}

			_, status, _ = mp.getDentry(dd.buildDentry())
			if status != proto.OpNotExistErr {
				t.Error(status)
				t.FailNow()
			}

			inoResp, _ = mp.getInode(ino)
			if inoResp.Status != proto.OpOk {
				t.Error(inoResp.Status)
				t.FailNow()
			}
			if inoResp.Msg.NLink != 3 {
				t.Errorf("Ino: %v", inoResp.Msg)
				t.FailNow()
			}

			dd = newPrimaryDeletedDentry(10, "f1", dds2[0].Timestamp, 100)
			dd1 := *dd
			recoverDentryReq = &RecoverDeletedDentryReq{
				ParentID:    10,
				Name:        "f1",
				TimeStamp:   dds2[0].Timestamp,
				Inode:       100,
			}
			p = &Packet{}
			if err = mp.RecoverDeletedDentry(recoverDentryReq, p); err != nil {
				t.Errorf("recover deleted dentry failed:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Errorf("dd:%v, status: %v", dd, p.ResultCode)
				t.FailNow()
			}
			resp := new(proto.RecoverDeletedDentryResponse)
			err = json.Unmarshal(p.Data, resp)
			if err != nil {
				t.Errorf("unmarshal resp failed:%v", err)
				t.FailNow()
			}
			_, status, _ = mp.getDeletedDentry(dd, dd)
			if status != proto.OpNotExistErr {
				t.Error(status)
				t.FailNow()
			}

			inoResp, _ = mp.getInode(ino)
			if inoResp.Status != proto.OpOk {
				t.Error(inoResp.Status)
				t.FailNow()
			}
			if inoResp.Msg.NLink != 4 {
				t.Errorf("Ino: %v", inoResp.Msg)
				t.FailNow()
			}

			dd.appendTimestampToName()
			var dentry *Dentry
			dentry, status, _ = mp.getDentry(dd.buildDentry())
			if status != proto.OpOk {
				t.Error(status)
				t.FailNow()
			}
			if dd.Name != dentry.Name {
				t.Errorf("resp: %v, dd: %v", dentry, dd)
				t.FailNow()
			}
			// re entrant
			{
				p = &Packet{}
				if err = mp.RecoverDeletedDentry(recoverDentryReq, p); err != nil {
					t.Errorf("recover deleted dentry failed:%v", err)
					t.FailNow()
				}
				if p.ResultCode != proto.OpOk {
					t.Errorf("dd:%v, status: %v", dd, p.ResultCode)
					t.FailNow()
				}
				resp = new(proto.RecoverDeletedDentryResponse)
				err = json.Unmarshal(p.Data, resp)
				if err != nil {
					t.Errorf("unmarshal resp failed:%v", err)
					t.FailNow()
				}
				_, status, _ = mp.getDeletedDentry(dd, dd)
				if status != proto.OpNotExistErr {
					t.Error(status)
					t.FailNow()
				}

				dentry, status, _ = mp.getDentry(dd1.buildDentry())
				if status != proto.OpOk {
					t.Error(status)
					t.FailNow()
				}
				if dd1.Name != dentry.Name {
					t.Errorf("resp: %v, dd: %v", dentry, dd1)
					t.FailNow()
				}
			}

			inoResp, _ = mp.getInode(ino)
			if inoResp.Status != proto.OpOk {
				t.Error(inoResp.Status)
				t.FailNow()
			}
			if inoResp.Msg.NLink != 4 {
				t.Errorf("Ino: %v", inoResp.Msg)
				t.FailNow()
			}

			for i:=0; i<2; i++ {
				recoverDentryReq = &RecoverDeletedDentryReq{
					PartitionID: 1,
					ParentID:    10,
					Name:        "f3",
					TimeStamp:   dds1[0].Timestamp,
					Inode:       103,
				}
				dd = newPrimaryDeletedDentry(10, "f3", dds1[0].Timestamp, 103)
				dd.Type = 1
				p = &Packet{}
				if err = mp.RecoverDeletedDentry(recoverDentryReq, p); err != nil {
					t.Errorf("recover deleted dentry failed:%v", err)
					t.FailNow()
				}
				if p.ResultCode != proto.OpOk {
					t.Errorf("dd: [%v], status: [%v]", dd, p.ResultCode)
					t.FailNow()
				}
				_, status, _ = mp.getDeletedDentry(dd, dd)
				if status != proto.OpNotExistErr {
					t.Error(status)
					t.FailNow()
				}

				inoResp, _ = mp.getInode(ino)
				if inoResp.Status != proto.OpOk {
					t.Error(inoResp.Status)
					t.FailNow()
				}
				if inoResp.Msg.NLink != 5 {
					t.Errorf("Ino: %v", inoResp.Msg)
					t.FailNow()
				}

				dentry, status, _ = mp.getDentry(dd.buildDentry())
				if status != proto.OpOk {
					t.Error(status)
					t.FailNow()
				}
				if dentry.ParentId != dd.ParentId {
					t.Errorf("[%v], [%v]", dentry.ParentId, dd)
					t.FailNow()
				}
				if dentry.Name != dd.Name {
					t.Errorf("[%v], [%v]", dentry.Name, dd)
					t.FailNow()
				}
				if dentry.Type != dd.Type {
					t.Errorf("[%v], [%v]", dentry.Type, dd)
					t.FailNow()
				}
			}
		})
	}
}

func TestMetaPartition_CopyGet(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_dentry_copy_get_01",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_dentry_copy_get_01",
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
			mp.dentryTree = mockDentryTreeByStoreMode(t, test.storeMode, rocksTree)
			ino := NewInode(10, proto.Mode(os.ModeDir))
			ino.NLink = 3
			_, _, _ = inodeCreate(mp.inodeTree, ino, false)

			req := &DeleteDentryReq{
				PartitionID: 1,
				ParentID:    10,
				Name:        "f4",
				NoTrash:     false,
			}
			var p = &Packet{}
			if err = mp.DeleteDentry(req, p); err != nil {
				t.Errorf("delete dentry failed, error:%v", err)
				t.FailNow()
			}
			if p.ResultCode != proto.OpOk {
				t.Error(p.ResultCode)
			}

			var timestamp int64 = 0

			mp.dentryDeletedTree.Range(nil, nil, func(data []byte) (bool, error) {
				dden := new(DeletedDentry)
				_ = dden.Unmarshal(data)
				if dden.ParentId == 10 && dden.Name == "f4" {
					timestamp = dden.Timestamp
				}
				t.Logf("fsmRecoverDeletedDentry: ascend: %v", dden)
				return true, nil
			})

			dd := newPrimaryDeletedDentry(10, "f4", timestamp, 104)
			item, _ := mp.dentryDeletedTree.Get(dd.ParentId, dd.Name, dd.Timestamp)
			if item == nil {
				t.Errorf("not found dentry: %v", dd)
				return
			}
			item.Timestamp++
			mp.dentryDeletedTree.Range(nil, nil, func(data []byte) (bool, error) {
				dden := new(DeletedDentry)
				_ = dden.Unmarshal(data)
				t.Logf("fsmRecoverDeletedDentry: ascend2: %v", dden)
				return true, nil
			})

			var str string
			str = "1234"
			str1 := str
			t.Logf("1str: %v, str1: %v", str, str1)
			str1 = "abc"
			t.Logf("2str: %v, str1: %v", str, str1)

			f3 := new(Dentry)
			f3.ParentId = 10
			f3.Inode = 103
			f3.Type = 1
			f3.Name = "f3"
			f1 := *f3
			f2 := *f3
			f3.Name = "f333"
			t.Logf("f1: %v, f2: %v", f1.Name, f2.Name)
		})
	}
}

func mockDeletedDentryTree2() DeletedDentryTree {
	tree := NewBtree()

	d1 := new(Dentry)
	d1.ParentId = 1
	d1.Inode = 10
	d1.Type = proto.Mode(os.ModeDir)
	d1.Name = "d1"
	dd := newDeletedDentry(d1, ts, from)
	tree.ReplaceOrInsert(dd, false)

	d2 := new(Dentry)
	d2.ParentId = 1
	d2.Inode = 11
	d2.Type = proto.Mode(os.ModeDir)
	d2.Name = "d2"
	dd = new(DeletedDentry)
	dd = newDeletedDentry(d2, ts, from)
	tree.ReplaceOrInsert(dd, false)

	d2 = new(Dentry)
	d2.ParentId = 1
	d2.Inode = 12
	d2.Type = proto.Mode(os.ModeDir)
	d2.Name = "d3"
	dd = newDeletedDentry(d2, ts, from)
	dd.Timestamp = ts + 1001
	tree.ReplaceOrInsert(dd, false)

	d2 = new(Dentry)
	d2.ParentId = 1
	d2.Inode = 13
	d2.Type = proto.Mode(os.ModeDir)
	d2.Name = "d4"
	dd = newDeletedDentry(d2, ts, from)
	dd.Timestamp = ts + 10001
	tree.ReplaceOrInsert(dd, false)

	f1 := new(Dentry)
	f1.ParentId = 10
	f1.Inode = 100
	f1.Type = 1
	f1.Name = "f1"
	dd = newDeletedDentry(f1, ts, from)
	dd.Timestamp = ts + 2001
	tree.ReplaceOrInsert(dd, false)

	f2 := new(Dentry)
	f2.ParentId = 10
	f2.Inode = 101
	f2.Type = 1
	f2.Name = "f2"
	dd = newDeletedDentry(f2, ts, from)
	dd.Timestamp = ts + 3001
	tree.ReplaceOrInsert(dd, false)

	f2 = new(Dentry)
	f2.ParentId = 10
	f2.Inode = 102
	f2.Type = 1
	f2.Name = "f2"
	dd = newDeletedDentry(f2, ts, from)
	dd.Timestamp = ts + 4001
	tree.ReplaceOrInsert(dd, false)
	return &DeletedDentryBTree{tree}
}
//
//func TestMetaPartition_fsmCleanExpiredDentry(t *testing.T) {
//	mp := new(metaPartition)
//	mp.dentryDeletedTree = mockDeletedDentryTree2()
//	originalSize := mp.dentryDeletedTree.Count()
//
//	var batch DeletedDentryBatch
//	d1 := newPrimaryDeletedDentry(10, "f2", ts+3001, 101)
//	batch = append(batch, d1)
//	d2 := newPrimaryDeletedDentry(10, "f2", ts+4001, 102)
//	batch = append(batch, d2)
//
//	data, err := batch.Marshal()
//	if err != nil {
//		t.Errorf(err.Error())
//	}
//
//	dens, err := DeletedDentryBatchUnmarshal(data)
//	if err != nil {
//		t.Errorf(err.Error())
//	}
//
//	if len(dens) != 2 {
//		t.Errorf("len: %v", len(dens))
//		t.FailNow()
//	}
//
//	if dens[0].Name != d1.Name ||
//		dens[0].ParentId != d1.ParentId ||
//		dens[0].Timestamp != d1.Timestamp ||
//		dens[0].Inode != d1.Inode {
//		t.Errorf("dens[0]: %v, d1: %v", dens[0], d1)
//		t.FailNow()
//	}
//
//	if dens[1].Name != d2.Name ||
//		dens[1].ParentId != d2.ParentId ||
//		dens[1].Timestamp != d2.Timestamp ||
//		dens[1].Inode != d2.Inode {
//		t.Errorf("dens[1]: %v, d2: %v", dens[1], d2)
//		t.FailNow()
//	}
//
//	res, _ := mp.fsmCleanExpiredDentry(dens)
//	if len(res) > 0 {
//		t.Errorf("len: %v", len(res))
//		t.FailNow()
//	}
//
//	if originalSize - 2 != mp.dentryDeletedTree.Count() {
//		t.Errorf("len: %v", mp.dentryDeletedTree.Count())
//	}
//
//}

func TestMain(m *testing.M) {
	m.Run()
	log.LogFlush()
}

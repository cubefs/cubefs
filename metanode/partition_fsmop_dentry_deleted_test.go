package metanode

import (
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

func TestMetaPartition_mvToDeletedDentryTree(t *testing.T) {
	mp := new(metaPartition)
	mp.dentryTree = mockDentryTree()
	mp.dentryDeletedTree = &DeletedDentryBTree{NewBtree()}

	var timestamp int64 = time.Now().UnixNano()
	f1 := new(Dentry)
	f1.ParentId = 10
	f1.Inode = 100
	f1.Type = 1
	f1.Name = "f1"
	df1 := newDeletedDentry(f1, timestamp, from)

	var  status uint8
	_, status, _ = mp.getDeletedDentry(df1, df1)
	if status != proto.OpNotExistErr {
		t.Error(status)
		t.FailNow()
	}

	status, _ = mp.mvToDeletedDentryTree(f1, timestamp, from)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	ds, status, _ := mp.getDeletedDentry(df1, df1)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	if ds[0].Timestamp != timestamp {
		t.FailNow()
	}

	status, _ = mp.mvToDeletedDentryTree(f1, timestamp, from)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}
}

func TestMetaPartition_fsmCleanDeletedDentry(t *testing.T) {
	mp := new(metaPartition)
	mp.dentryDeletedTree = mockDeletedDentryTree()

	d1 := new(DeletedDentry)
	d1.ParentId = 1
	d1.Inode = 10
	d1.Type = proto.Mode(os.ModeDir)
	d1.Name = "d1"
	d1.Timestamp = ts
	d1.From = from
	resp, _ := mp.fsmCleanDeletedDentry(d1)
	if resp.Status != proto.OpOk {
		t.Errorf("DeletedDentry: %v, Status: %v", d1, resp.Status)
		t.FailNow()
	}

	_, st, _ := mp.getDeletedDentry(d1, d1)
	if st != proto.OpNotExistErr {
		t.Errorf("den: %v, status: %v", d1, st)
	}

	resp, _ = mp.fsmCleanDeletedDentry(d1)
	if resp.Status != proto.OpNotExistErr {
		t.Errorf("DeletedDentry: %v, Status: %v", d1, resp.Status)
		t.FailNow()
	}

	d1.Timestamp = ts + msFactor
	resp, _ = mp.fsmCleanDeletedDentry(d1)
	if resp.Status != proto.OpNotExistErr {
		t.Errorf("DeletedDentry: %v, Status: %v", d1, resp.Status)
		t.FailNow()
	}

	f1 := new(Dentry)
	f1.ParentId = 10
	f1.Inode = 100
	f1.Type = 1
	f1.Name = "f2"
	dd := newDeletedDentry(f1, ts, from)

	resp, _ = mp.fsmCleanDeletedDentry(dd)
	if resp.Status != proto.OpNotExistErr {
		t.Errorf("DeletedDentry: %v, Status: %v", dd, resp.Status)
		t.FailNow()
	}

	dd.Name = "f1"
	resp, _ = mp.fsmCleanDeletedDentry(dd)
	if resp.Status != proto.OpOk {
		t.Errorf("DeletedDentry: %v, Status: %v", dd, resp.Status)
		t.FailNow()
	}

	_, status, _ := mp.getDeletedDentry(dd, dd)
	if status != proto.OpNotExistErr {
		t.Error(status)
		t.FailNow()
	}

	dd.Name = "f2"
	dd.Timestamp = ts + msFactor
	dd.Inode = 102
	resp, _ = mp.fsmCleanDeletedDentry(dd)
	if resp.Status != proto.OpOk {
		t.Errorf("DeletedDentry: %v, Status: %v", dd, resp.Status)
		t.FailNow()
	}
	_, status, _ = mp.getDeletedDentry(dd, dd)
	if status != proto.OpNotExistErr {
		t.Error(status)
		t.FailNow()
	}

	_, status, _ = mp.getDeletedDentry(dd, dd)
	if status != proto.OpNotExistErr {
		t.Error(status)
		t.FailNow()
	}

	resp, _ = mp.fsmCleanDeletedDentry(d1)
	if resp.Status != proto.OpNotExistErr {
		t.Errorf("DeletedDentry: %v, Status: %v", d1, resp.Status)
		t.FailNow()
	}
	_, status, _ = mp.getDeletedDentry(d1, d1)
	if status != proto.OpNotExistErr {
		t.Error(status)
		t.FailNow()
	}

	d1.Timestamp = ts
	d1.Inode = 11
	d1.Name = "d2"
	resp, _ = mp.fsmCleanDeletedDentry(d1)
	if resp.Status != proto.OpOk {
		t.Errorf("DeletedDentry: %v, Status: %v", d1, resp.Status)
		t.FailNow()
	}
	_, status, _ = mp.getDeletedDentry(d1, d1)
	if status != proto.OpNotExistErr {
		t.Error(status)
		t.FailNow()
	}
}

func TestMetaPartition_fsmRecoverDeletedDentry(t *testing.T) {
	mp := new(metaPartition)
	mp.config = new(MetaPartitionConfig)
	mp.config.Start = 1
	mp.config.Cursor = 100000
	mp.dentryTree = mockDentryTree()
	mp.inodeTree = &InodeBTree{NewBtree()}
	mp.dentryDeletedTree = &DeletedDentryBTree{NewBtree()}

	f3 := new(Dentry)
	f3.ParentId = 10
	f3.Inode = 103
	f3.Type = 1
	f3.Name = "f3"
	status, _ := mp.mvToDeletedDentryTree(f3, ts, from)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	f1 := new(Dentry)
	f1.ParentId = 10
	f1.Inode = 100
	f1.Type = 1
	f1.Name = "f1"
	status, _ = mp.mvToDeletedDentryTree(f1, ts, from)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	// case: the target dentry is not exist
	dd := newPrimaryDeletedDentry(10, "f33", ts, 100)
	resp, _ := mp.fsmRecoverDeletedDentry(dd)
	if resp.Status != proto.OpNotExistErr {
		t.Error(status)
		t.FailNow()
	}
	_, status, _ = mp.getDentry(dd.buildDentry())
	if status != proto.OpNotExistErr {
		t.Error(status)
		t.FailNow()
	}

	// case: the  the original dentry is exist
	mp.inodeTree = &InodeBTree{NewBtree()}
	ino := NewInode(10, proto.Mode(os.ModeDir))
	mp.fsmCreateInode(ino)
	dd = newPrimaryDeletedDentry(10, "f1", ts, 100)
	dd1 := *dd
	resp, _ = mp.fsmRecoverDeletedDentry(dd)
	if resp.Status != proto.OpOk {
		t.Errorf("dd:%v, status: %v", dd, resp.Status)
		t.FailNow()
	}
	_, status, _ = mp.getDeletedDentry(resp.Msg, resp.Msg)
	if status != proto.OpNotExistErr {
		t.Error(status)
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
		t.Errorf("resp: %v, dd: %v", resp.Msg, dd)
		t.FailNow()
	}
	// re entrant
	{
		resp, _ = mp.fsmRecoverDeletedDentry(&dd1)
		if resp.Status != proto.OpOk {
			t.Errorf("dd:%v, status: %v", dd, resp.Status)
			t.FailNow()
		}
		_, status, _ = mp.getDeletedDentry(resp.Msg, resp.Msg)
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
			t.Errorf("resp: %v, dd: %v", resp.Msg, dd)
			t.FailNow()
		}
	}

	// case: the source dentry is not exist
	for i:=0; i<2; i++ {
		dd = newPrimaryDeletedDentry(10, "f3", ts, 103)
		dd.Type = 1
		resp, _ = mp.fsmRecoverDeletedDentry(dd)
		if resp.Status != proto.OpOk {
			t.Errorf("dd: [%v], status: [%v]", dd, resp.Status)
			t.FailNow()
		}
		_, status, _ = mp.getDeletedDentry(dd, dd)
		if status != proto.OpNotExistErr {
			t.Error(status)
			t.FailNow()
		}

		inoResp, _ := mp.getInode(ino)
		if inoResp.Status != proto.OpOk {
			t.Error(inoResp.Status)
			t.FailNow()
		}
		if inoResp.Msg.NLink != 4 {
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
}

func TestMetaPartition_CopyGet(t *testing.T) {
	mp := new(metaPartition)
	mp.dentryTree = mockDentryTree()
	mp.inodeTree = &InodeBTree{NewBtree()}
	mp.dentryDeletedTree = &DeletedDentryBTree{NewBtree()}
	ino := NewInode(10, proto.Mode(os.ModeDir))
	mp.fsmCreateInode(ino)

	f4 := new(Dentry)
	f4.ParentId = 10
	f4.Inode = 104
	f4.Type = 1
	f4.Name = "f4"
	mp.mvToDeletedDentryTree(f4, ts, from)
	mp.dentryDeletedTree.Range(nil, nil, func(data []byte) (bool, error) {
		dden := new(DeletedDentry)
		_ = dden.Unmarshal(data)
		t.Logf("fsmRecoverDeletedDentry: ascend: %v", dden)
		return true, nil
	})

	dd := newPrimaryDeletedDentry(10, "f4", ts, 104)
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

func TestMetaPartition_fsmCleanExpiredDentry(t *testing.T) {
	mp := new(metaPartition)
	mp.dentryDeletedTree = mockDeletedDentryTree2()
	originalSize := mp.dentryDeletedTree.Count()

	var batch DeletedDentryBatch
	d1 := newPrimaryDeletedDentry(10, "f2", ts+3001, 101)
	batch = append(batch, d1)
	d2 := newPrimaryDeletedDentry(10, "f2", ts+4001, 102)
	batch = append(batch, d2)

	data, err := batch.Marshal()
	if err != nil {
		t.Errorf(err.Error())
	}

	dens, err := DeletedDentryBatchUnmarshal(data)
	if err != nil {
		t.Errorf(err.Error())
	}

	if len(dens) != 2 {
		t.Errorf("len: %v", len(dens))
		t.FailNow()
	}

	if dens[0].Name != d1.Name ||
		dens[0].ParentId != d1.ParentId ||
		dens[0].Timestamp != d1.Timestamp ||
		dens[0].Inode != d1.Inode {
		t.Errorf("dens[0]: %v, d1: %v", dens[0], d1)
		t.FailNow()
	}

	if dens[1].Name != d2.Name ||
		dens[1].ParentId != d2.ParentId ||
		dens[1].Timestamp != d2.Timestamp ||
		dens[1].Inode != d2.Inode {
		t.Errorf("dens[1]: %v, d2: %v", dens[1], d2)
		t.FailNow()
	}

	res, _ := mp.fsmCleanExpiredDentry(dens)
	if len(res) > 0 {
		t.Errorf("len: %v", len(res))
		t.FailNow()
	}

	if originalSize - 2 != mp.dentryDeletedTree.Count() {
		t.Errorf("len: %v", mp.dentryDeletedTree.Count())
	}

}

func TestMain(m *testing.M) {
	m.Run()
	log.LogFlush()
}

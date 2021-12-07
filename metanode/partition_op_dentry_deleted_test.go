package metanode

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"testing"
)

func TestMetaPartition_ReadDeletedDir(t *testing.T) {
	mp := new(metaPartition)
	mp.dentryDeletedTree = mockDeletedDentryTree()

	var req ReadDeletedDirReq
	req.ParentID = 10
	resp := mp.readDeletedDir(&req)
	if len(resp.Children) != 3 {
		t.Errorf("childs: %v", len(resp.Children))
		t.FailNow()
	}

	req.ParentID = 1
	resp = mp.readDeletedDir(&req)
	if len(resp.Children) != 2 {
		t.Errorf("childs: %v", len(resp.Children))
		for index, d := range resp.Children {
			t.Errorf("index: %v, d: %v", index, d)
		}
		t.FailNow()
	}

	req.ParentID = 101
	resp = mp.readDeletedDir(&req)
	if len(resp.Children) != 0 {
		t.Errorf("childs: %v", len(resp.Children))
		t.FailNow()
	}

	req.ParentID = 5
	resp = mp.readDeletedDir(&req)
	if len(resp.Children) != 0 {
		t.Errorf("childs: %v", len(resp.Children))
		t.FailNow()
	}
}

func TestMetaPartition_ReadDeletedDir2(t *testing.T) {
	tree := NewBtree()
	var ino uint64
	for ino = 2; ino < 2102; ino++ {
		di := mockDeletedDentry(1, ino, fmt.Sprintf("f_%v", ino), 1, ts)
		tree.ReplaceOrInsert(di, false)
	}
	mp := new(metaPartition)
	mp.dentryDeletedTree = tree

	var req ReadDeletedDirReq
	req.VolName = "ltp"
	req.ParentID = 1
	resp := mp.readDeletedDir(&req)
	if len(resp.Children) != proto.ReadDeletedDirBatchNum {
		t.Errorf("childs: %v", len(resp.Children))
		t.FailNow()
	}
	req.Name = resp.Children[proto.ReadDeletedDirBatchNum-1].Name
	req.Timestamp = resp.Children[proto.ReadDeletedDirBatchNum-1].Timestamp
	resp = mp.readDeletedDir(&req)
	if len(resp.Children) != proto.ReadDeletedDirBatchNum {
		t.Errorf("childs: %v", len(resp.Children))
		t.FailNow()
	}
	req.Name = resp.Children[proto.ReadDeletedDirBatchNum-1].Name
	req.Timestamp = resp.Children[proto.ReadDeletedDirBatchNum-1].Timestamp
	resp = mp.readDeletedDir(&req)
	if len(resp.Children) != 100 {
		t.Errorf("childs: %v", len(resp.Children))
		t.FailNow()
	}
}

func TestMetaPartition_getExactlyDeletedDentry(t *testing.T) {
	mp := new(metaPartition)
	mp.dentryDeletedTree = mockDeletedDentryTree()

	dd := newPrimaryDeletedDentry(1, "d1", ts, 0)
	_, status := mp.getDeletedDentry(dd, dd)
	if status != proto.OpOk {
		t.Errorf("dd: %v, status: %v", dd, status)
		t.FailNow()
	}

	dd = newPrimaryDeletedDentry(1, "d1", ts+1000, 0)
	_, status = mp.getDeletedDentry(dd, dd)
	if status != proto.OpNotExistErr {
		t.Errorf("dd: %v, status: %v", dd, status)
		t.FailNow()
	}
}

func TestMetaPartition_getDeletedDentry(t *testing.T) {
	mp := new(metaPartition)
	mp.dentryDeletedTree = mockDeletedDentryTree()

	d1 := newPrimaryDeletedDentry(1, "d1", ts, 0)
	d2 := newPrimaryDeletedDentry(1, "d1", ts, 0)
	ds, status := mp.getDeletedDentry(d1, d2)
	if status != proto.OpOk {
		t.Errorf("d1: %v, d2: %v, status: %v", d1, d2, status)
		t.FailNow()
	}
	if len(ds) != 1 {
		t.Errorf("d1: %v, d2: %v, len: %v", d1, d2, len(ds))
		t.FailNow()
	}
}

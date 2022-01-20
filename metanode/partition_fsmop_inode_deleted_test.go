package metanode

import (
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
	mp := new(metaPartition)
	mp.config = new(MetaPartitionConfig)
	mp.config.Start = 1
	mp.config.Cursor = 100000
	mp.inodeTree = mockInodeTree()
	mp.inodeDeletedTree = &DeletedInodeBTree{NewBtree()}

	ino := NewInode(10, proto.Mode(os.ModeDir))
	status, _ := mp.mvToDeletedInodeTree(ino, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}
	_, _, status, _ = mp.getDeletedInode(ino.Inode)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}
	res, _ := mp.getInode(ino)
	if res.Status != proto.OpNotExistErr {
		t.Error(status)
		t.FailNow()
	}

	status, _ = mp.mvToDeletedInodeTree(ino, ts)
	if status != proto.OpExistErr {
		t.Error(status)
		t.FailNow()
	}
}

/*
case1: the original inode is not exist, which is file
case2: the original inode is not exist, which is dir
*/
func TestMetaPartition_fsmRecoverDeletedInode(t *testing.T) {
	mp := new(metaPartition)
	mp.inodeTree = mockInodeTree()
	mp.inodeDeletedTree = &DeletedInodeBTree{NewBtree()}
	mp.dentryDeletedTree = &DeletedDentryBTree{NewBtree()}
	mp.freeList = newFreeList()
	mp.config = new(MetaPartitionConfig)
	mp.config.PartitionId = 1
	mp.config.Start = 1
	mp.config.Cursor = 100000
	ino1 := NewInode(10, proto.Mode(os.ModeDir))
	ino1.SetDeleteMark()
	ino1.DecNLink()
	t.Logf("ino1:%v", ino1)
	status, _ := mp.mvToDeletedInodeTree(ino1, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}
	ino2 := NewInode(20, 1)
	ino2.SetDeleteMark()
	ino2.DecNLink()
	status, _ = mp.mvToDeletedInodeTree(ino2, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	// recover a dir
	var req FSMDeletedINode
	req.inode = 10
	resp, _ := mp.fsmRecoverDeletedInode(&req)
	if resp.Status != proto.OpOk {
		t.Error(resp.Status)
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
		resp, _ = mp.fsmRecoverDeletedInode(&req)
		if resp.Status != proto.OpOk {
			t.Error(resp.Status)
			t.FailNow()
		}
		res, _ = mp.getInode(ino1)
		if res.Status != proto.OpOk {
			t.Error(resp.Status)
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

	// recover a regular file
	req.inode = 20
	resp, _ = mp.fsmRecoverDeletedInode(&req)
	if resp.Status != proto.OpOk {
		t.Error(resp.Status)
		t.FailNow()
	}
	res, _ = mp.getInode(ino2)
	if res.Status != proto.OpOk {
		t.Error(resp.Status)
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
		resp, _ = mp.fsmRecoverDeletedInode(&req)
		if resp.Status != proto.OpOk {
			t.Error(resp.Status)
			t.FailNow()
		}
		res, _ = mp.getInode(ino2)
		if res.Status != proto.OpOk {
			t.Error(resp.Status)
			t.FailNow()
		}
		if res.Msg.ShouldDelete() == true {
			t.Error(res.Msg)
			t.FailNow()
		}
	}
}

/*
case: the original inode is exist
*/
func TestMetaPartition_fsmRecoverDeletedInode2(t *testing.T) {
	mp := new(metaPartition)
	mp.inodeTree = mockInodeTree()
	mp.inodeDeletedTree = &DeletedInodeBTree{NewBtree()}
	mp.dentryDeletedTree = &DeletedDentryBTree{NewBtree()}
	mp.freeList = newFreeList()
	mp.config = new(MetaPartitionConfig)
	mp.config.PartitionId = 1
	ino1 := NewInode(10, proto.Mode(os.ModeDir))
	ino1.SetDeleteMark()
	status, _ := mp.mvToDeletedInodeTree(ino1, ts)
	if status != proto.OpOk {
		t.FailNow()
	}
	ino2 := NewInode(20, 1)
	ino2.SetDeleteMark()
	status, _ = mp.mvToDeletedInodeTree(ino2, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}
	mp.inodeTree = mockInodeTree()

	// recover a dir
	var req FSMDeletedINode
	req.inode = 10
	resp, _ := mp.fsmRecoverDeletedInode(&req)
	if resp.Status != proto.OpOk {
		t.Error(resp.Status)
		t.FailNow()
	}

	// recover a regular file
	req.inode = 20
	resp, _ = mp.fsmRecoverDeletedInode(&req)
	if resp.Status != proto.OpOk {
		t.Error(resp.Status)
		t.FailNow()
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

func TestMetaPartition_fsmCleanDeletedInode(t *testing.T) {
	mp := new(metaPartition)
	mp.inodeTree = mockInodeTree()
	mp.inodeDeletedTree = &DeletedInodeBTree{NewBtree()}
	mp.dentryDeletedTree = &DeletedDentryBTree{NewBtree()}
	mp.freeList = newFreeList()
	ino1 := NewInode(10, proto.Mode(os.ModeDir))
	ino1.SetDeleteMark()
	status, _ := mp.mvToDeletedInodeTree(ino1, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}
	status, _ = mp.mvToDeletedInodeTree(ino1, ts)
	if status != proto.OpExistErr {
		t.Error(status)
		t.FailNow()
	}

	ino2 := NewInode(20, 1)
	status, _ = mp.mvToDeletedInodeTree(ino2, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}
	status, _ = mp.mvToDeletedInodeTree(ino2, ts)
	if status != proto.OpExistErr {
		t.Error(status)
		t.FailNow()
	}

	ino3 := NewInode(21, 1)
	ino3.SetDeleteMark()
	ino3.DecNLink()
	status, _ = mp.mvToDeletedInodeTree(ino3, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	di := NewFSMDeletedINode(10)
	resp, _ := mp.fsmCleanDeletedInode(di)
	if resp.Status != proto.OpOk {
		t.Error(resp.Status)
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

	di.inode = 20
	resp, _ = mp.fsmCleanDeletedInode(di)
	if resp.Status != proto.OpErr {
		t.Error(resp.Status)
		t.FailNow()
	}
	_, _, status, _ = mp.getDeletedInode(ino2.Inode)
	if status != proto.OpOk {
		t.Error(resp.Status)
		t.FailNow()
	}
	if mp.freeList.Len() > 0 {
		t.Errorf("freelist: %v", mp.freeList.Len())
		t.FailNow()
	}

	di.inode = 21
	resp, _ = mp.fsmCleanDeletedInode(di)
	if resp.Status != proto.OpOk {
		t.Error(resp.Status)
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
}

func TestMetaPartition_fsmCleanDeletedInode2(t *testing.T) {
	mp := new(metaPartition)
	mp.inodeTree = mockInodeTree()
	mp.inodeDeletedTree = &DeletedInodeBTree{NewBtree()}
	mp.dentryDeletedTree = &DeletedDentryBTree{NewBtree()}
	mp.freeList = newFreeList()
	ino1 := NewInode(10, proto.Mode(os.ModeDir))
	ino1.SetDeleteMark()
	status, _ := mp.mvToDeletedInodeTree(ino1, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	den := newPrimaryDeletedDentry(10, "f1", ts, 100)
	mp.dentryDeletedTree.Create(den, false)

	di := NewFSMDeletedINode(10)
	resp, _ := mp.fsmCleanDeletedInode(di)
	if resp.Status != proto.OpExistErr{
		t.Error(resp.Status)
		t.FailNow()
	}

	mp.dentryDeletedTree.Delete(den.ParentId, den.Name, den.Timestamp)

	di = NewFSMDeletedINode(10)
	resp, _ = mp.fsmCleanDeletedInode(di)
	if resp.Status != proto.OpOk {
		t.Error(resp.Status)
		t.FailNow()
	}
}

func TestMetaPartition_fsmBatchCleanDeletedInode(t *testing.T) {
	mp := new(metaPartition)
	mp.inodeTree = mockInodeTree()
	mp.inodeDeletedTree = &DeletedInodeBTree{NewBtree()}
	mp.dentryDeletedTree = &DeletedDentryBTree{NewBtree()}
	mp.freeList = newFreeList()
	ino1 := NewInode(10, proto.Mode(os.ModeDir))
	ino1.SetDeleteMark()
	status, _ := mp.mvToDeletedInodeTree(ino1, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	ino2 := NewInode(20, 1)
	ino2.SetDeleteMark()
	ino2.DecNLink()
	status, _ = mp.mvToDeletedInodeTree(ino2, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	ino3 := NewInode(21, 1)
	ino3.SetDeleteMark()
	ino3.DecNLink()
	status, _ = mp.mvToDeletedInodeTree(ino3, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	var inos FSMDeletedINodeBatch
	inoArr := []uint64{10, 20 ,21}
	for _, ino := range inoArr {
		inos = append(inos, NewFSMDeletedINode(ino))
	}
	data, err := inos.Marshal()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	batch, err := FSMDeletedINodeBatchUnmarshal(data)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	if len(batch) != 3 {
		t.Errorf("len: %v", len(batch))
	}

	for i:=0; i<3; i++ {
		if batch[i].inode != inos[i].inode {
			t.Errorf("ino: %v, %v", batch[i].inode, inos[i].inode)
		}
	}

	res, _ := mp.fsmBatchCleanDeletedInode(batch)
	if len(res) > 0 {
		t.Errorf("len: %v", len(res))
		for _, item := range res {
			t.Errorf("ino: %v, st: %v", item.Inode, item.Status)
		}
	}
}

func TestMetaPartition_fsmCleanExpiredDeletedINode(t *testing.T) {
	mp := new(metaPartition)
	mp.inodeTree = mockInodeTree()
	mp.inodeDeletedTree = &DeletedInodeBTree{NewBtree()}
	mp.dentryDeletedTree = &DeletedDentryBTree{NewBtree()}
	mp.freeList = newFreeList()
	ino1 := NewInode(10, proto.Mode(os.ModeDir))
	ino1.SetDeleteMark()
	status, _ := mp.mvToDeletedInodeTree(ino1, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}
	resp, _ := mp.cleanExpiredInode(ino1.Inode+1)
	if resp == nil {
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

	resp, _ = mp.cleanExpiredInode(ino1.Inode)
	if resp == nil {
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
	srcIno, di, st, _ := mp.getDeletedInode(ino1.Inode)
	if st != proto.OpNotExistErr{
		t.Log(st)
		t.FailNow()
	}
	if di != nil {
		t.FailNow()
	}
	if srcIno != nil {
		t.FailNow()
	}

	ino2 := NewInode(20, 1)
	ino2.SetDeleteMark()
	ino2.DecNLink()
	status, _ = mp.mvToDeletedInodeTree(ino2, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	resp, _ = mp.cleanExpiredInode(ino2.Inode)
	if resp == nil {
		t.FailNow()
	}
	if resp.Status != proto.OpOk {
		t.Log(resp.Status)
		t.FailNow()
	}
	if mp.freeList.Len() != 1 {
		t.Logf("len: %v", mp.freeList.Len())
		t.FailNow()
	}
	srcIno, di, st, _ = mp.getDeletedInode(ino2.Inode)
	if st != proto.OpOk {
		t.Log(st)
		t.FailNow()
	}
	if di == nil {
		t.FailNow()
	}
	if di.IsExpired != true {
		t.FailNow()
	}
	if srcIno != nil {
		t.FailNow()
	}
}

func TestMetaPartition_fsmBatchCleanExpiredDeletedInode(t *testing.T) {
	mp := new(metaPartition)
	mp.inodeTree = mockInodeTree()
	mp.inodeDeletedTree = &DeletedInodeBTree{NewBtree()}
	mp.dentryDeletedTree = &DeletedDentryBTree{NewBtree()}
	mp.freeList = newFreeList()
	ino1 := NewInode(10, proto.Mode(os.ModeDir))
	ino1.SetDeleteMark()
	status, _ := mp.mvToDeletedInodeTree(ino1, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	ino2 := NewInode(20, 1)
	ino2.SetDeleteMark()
	ino2.DecNLink()
	status, _ = mp.mvToDeletedInodeTree(ino2, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	ino3 := NewInode(21, 1)
	ino3.SetDeleteMark()
	ino3.DecNLink()
	status, _ = mp.mvToDeletedInodeTree(ino3, ts)
	if status != proto.OpOk {
		t.Error(status)
		t.FailNow()
	}

	var inos FSMDeletedINodeBatch
	inoArr := []uint64{9, 10, 20 ,21}
	for _, ino := range inoArr {
		inos = append(inos, NewFSMDeletedINode(ino))
	}
	data, err := inos.Marshal()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	batch, err := FSMDeletedINodeBatchUnmarshal(data)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	if len(batch) != 4 {
		t.Errorf("len: %v", len(batch))
	}

	for i:=0; i<4; i++ {
		if batch[i].inode != inos[i].inode {
			t.Errorf("ino: %v, %v", batch[i].inode, inos[i].inode)
		}
	}

	res, _ := mp.fsmCleanExpiredInode(batch)
	if len(res) != 0 {
		t.Errorf("len: %v", len(res))
		for _, item := range res {
			t.Errorf("ino: %v, st: %v", item.Inode, item.Status)
		}
	}
}

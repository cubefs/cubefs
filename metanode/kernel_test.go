package metanode

import (
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/btree"
	"reflect"
	"testing"
)

func Test_Dentry(t *testing.T) {
	dentry := &Dentry{
		ParentId: 1000,
		Name:     "test",
		Inode:    56564,
		Type:     0,
	}
	t.Log("source dentry:", dentry)
	data, err := dentry.Marshal()
	if err != nil || len(data) == 0 {
		t.Fatalf("dentry marshal fail: %s", err.Error())
	}
	t.Log("marshaled:", data)
	denTmp := &Dentry{}
	if err = denTmp.Unmarshal(data); err != nil {
		t.Fatalf("dentry unmarshal fail: %s", err.Error())
	}
	t.Log("result:", denTmp)
	if !reflect.DeepEqual(denTmp, dentry) {
		t.Fatalf("dentry test failed!")
	}
}

func Test_Inode(t *testing.T) {
	ino := NewInode(1, 0)
	ino.Extents.Put(proto.ExtentKey{
		PartitionId: 1000,
		ExtentId:    1222,
		Size:        10234,
	})
	ino.Extents.Put(proto.ExtentKey{
		PartitionId: 1020,
		ExtentId:    28,
		Size:        150,
	})
	t.Log("source inode:", ino)
	data, err := ino.Marshal()
	if err != nil {
		t.Fatalf("inode marshal fail: %v", err)
	}
	t.Log("marshaled:", data)
	inoTmp := NewInode(0, 0)
	if err = inoTmp.Unmarshal(data); err != nil {
		t.Fatalf("inode unmarshal fail: %v.", err)
	}
	t.Log("result:", inoTmp)
	if !reflect.DeepEqual(inoTmp, ino) {
		t.Fatalf("inode test failed.")
	}
}

func TestDentryBtree(t *testing.T) {
	dTree := btree.New(32)
	dentry := &Dentry{
		ParentId: 1,
		Name:     "star",
		Inode:    10,
		Type:     proto.ModeDir,
	}
	dTree.ReplaceOrInsert(dentry)
	newDen := &Dentry{
		ParentId: 1,
		Name:     "star",
	}
	item := dTree.Get(newDen)
	if item == nil {
		t.Fatalf("get dentry empty failed")
	}
	newDen = item.(*Dentry)
	t.Logf("%v", newDen)
}

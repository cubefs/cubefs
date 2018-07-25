package meta

import (
	"fmt"
	"syscall"
	"testing"

	"github.com/google/uuid"

	"github.com/tiglabs/baudstorage/proto"
)

const (
	SimVolName    = "simserver"
	SimMasterAddr = "localhost"
	SimMasterPort = "8900"
)

const (
	TestFileCount = 110
)

var gMetaWrapper *MetaWrapper

func init() {
	mw, err := NewMetaWrapper(SimVolName, SimMasterAddr+":"+SimMasterPort)
	if err != nil {
		fmt.Println(err)
	}
	gMetaWrapper = mw
}

func TestGetVol(t *testing.T) {
	for _, mp := range gMetaWrapper.partitions {
		t.Logf("%v", *mp)
	}
}

func TestCreate(t *testing.T) {
	uuid := uuid.New()
	parent, err := gMetaWrapper.Create_ll(proto.RootIno, uuid.String(), proto.ModeDir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < TestFileCount; i++ {
		name := fmt.Sprintf("abc%v", i)
		info, err := gMetaWrapper.Create_ll(parent.Inode, name, proto.ModeRegular)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("inode: %v", *info)
	}
}

func TestInodeGet(t *testing.T) {
	doInodeGet(t, proto.RootIno)
	doInodeGet(t, 2)
	doInodeGet(t, 100)
	doInodeGet(t, 101)
}

func doInodeGet(t *testing.T, ino uint64) {
	info, err := gMetaWrapper.InodeGet_ll(ino)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Getting inode (%v), %v", ino, *info)
}

func TestLookup(t *testing.T) {
	id := uuid.New()
	filename := id.String()
	file, err := gMetaWrapper.Create_ll(proto.RootIno, filename, proto.ModeRegular)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Generate file: parent(%v) name(%v) ino(%v)", proto.RootIno, filename, file.Inode)

	ino, mode, err := gMetaWrapper.Lookup_ll(proto.RootIno, filename)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Lookup name(%v) : ino(%v) mode(%v)", filename, ino, mode)

	info, err := gMetaWrapper.InodeGet_ll(ino)
	if err != nil {
		t.Fatal(err)
	}

	if mode != info.Mode {
		t.Fatalf("dentry mode(%v), inode mode(%v)", mode, info.Mode)
	}
}

func TestDelete(t *testing.T) {
	id := uuid.New()
	filename := id.String()
	file, err := gMetaWrapper.Create_ll(proto.RootIno, filename, proto.ModeRegular)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Generate file: parent(%v) name(%v) ino(%v)", proto.RootIno, filename, file.Inode)

	err = gMetaWrapper.Delete_ll(proto.RootIno, filename)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Delete name(%v)", filename)

	_, _, err = gMetaWrapper.Lookup_ll(proto.RootIno, filename)
	if err != syscall.ENOENT {
		t.Fatal(err)
	}
}

func TestRename(t *testing.T) {
	id := uuid.New()
	filename := id.String()
	file, err := gMetaWrapper.Create_ll(proto.RootIno, filename, proto.ModeRegular)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Generate file: parent(%v) name(%v) ino(%v)", proto.RootIno, filename, file.Inode)

	id = uuid.New()
	parent, err := gMetaWrapper.Create_ll(proto.RootIno, id.String(), proto.ModeDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Generate dir: parent(%v) name(%v) ino(%v)", proto.RootIno, id.String(), parent.Inode)

	t.Logf("Rename [%v %v] --> [%v %v]", proto.RootIno, filename, parent.Inode, "abc10")
	err = gMetaWrapper.Rename_ll(proto.RootIno, filename, parent.Inode, "abc10")
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = gMetaWrapper.Lookup_ll(proto.RootIno, filename)
	if err != syscall.ENOENT {
		t.Fatal(err)
	}

	_, _, err = gMetaWrapper.Lookup_ll(parent.Inode, "abc10")
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadDir(t *testing.T) {
	doReadDir(t, proto.RootIno)
	doReadDir(t, 2)
}

func doReadDir(t *testing.T, ino uint64) {
	t.Logf("ReadDir ino(%v)", ino)
	children, err := gMetaWrapper.ReadDir_ll(ino)
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range children {
		t.Log(child)
	}
}

func TestExtents(t *testing.T) {
	uuid := uuid.New()
	info, err := gMetaWrapper.Create_ll(proto.RootIno, uuid.String(), proto.ModeRegular)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Create a file ino(%v) name(%v)", info.Inode, uuid.String())

	ek := proto.ExtentKey{PartitionId: 1, ExtentId: 2, Size: 3, Crc: 4}
	t.Logf("ExtentKey append: %v", ek)
	err = gMetaWrapper.AppendExtentKey(info.Inode, ek)
	if err != nil {
		t.Fatal(err)
	}

	ek = proto.ExtentKey{PartitionId: 5, ExtentId: 6, Size: 7, Crc: 8}
	t.Logf("ExtentKey append: %v", ek)
	err = gMetaWrapper.AppendExtentKey(info.Inode, ek)
	if err != nil {
		t.Fatal(err)
	}

	extents, err := gMetaWrapper.GetExtents(info.Inode)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Extents received: %v", extents)
}

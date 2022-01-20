package metanode

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"os"
	"reflect"
	"sort"
	"testing"
)

func createInode(t, uid, gid uint32, mp *metaPartition) (ino uint64, err error) {
	reqCreateInode := &proto.CreateInodeRequest{
		Mode: t,
		Uid: uid,
		Gid: gid,
	}
	packet := &Packet{}
	if err = mp.CreateInode(reqCreateInode, packet); err != nil || packet.ResultCode != proto.OpOk {
		err = fmt.Errorf("create inode failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}
	resp := &proto.CreateInodeResponse{}
	if err = packet.UnmarshalData(resp); err != nil {
		err = fmt.Errorf("unmarshal create inode response failed:%v", err)
		return
	}
	ino = resp.Info.Inode
	return
}

func extentAppend(ino uint64, extent proto.ExtentKey, leader *metaPartition) (err error) {
	reqAppendExtent := &proto.AppendExtentKeyRequest{
		Inode:  ino,
		Extent: extent,
	}
	packet := &Packet{}
	if err = leader.ExtentAppend(reqAppendExtent, packet); err != nil || packet.ResultCode != proto.OpOk {
		err = fmt.Errorf("extent append failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}
	return
}

func extentResultVerify(t *testing.T, leader, follower *metaPartition, ino uint64, expectExtents []proto.ExtentKey) {
	inodeInMem, _ := leader.inodeTree.Get(ino)
	inodeInRocks, _ := follower.inodeTree.Get(ino)
	if !reflect.DeepEqual(inodeInMem, inodeInRocks) {
		t.Errorf("inode info in mem is not equal to rocks, mem:%s, rocks:%s", inodeInMem.String(), inodeInRocks.String())
		return
	}
	if len(inodeInMem.Extents.eks) != len(expectExtents) {
		t.Fatalf("extent count mismatch, expect:%v, actual:%v", len(expectExtents), len(inodeInMem.Extents.eks))
	}
	for index, ext := range inodeInMem.Extents.eks {
		if !reflect.DeepEqual(ext, expectExtents[index]) {
			t.Fatalf("extent mismatch, expect:%v, actual:%v", expectExtents[index], ext)
		}
	}
}

func ExtentAppendInterTest(t *testing.T, leader, follower *metaPartition) {
	//create inode
	ino, err := createInode(470, 0, 0, leader)
	if err != nil {
		t.Fatal(err)
	}

	//append extent
	extent := proto.ExtentKey{
		PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100,
	}
	if err = extentAppend(ino, extent, leader); err != nil {
		t.Error(err)
		return
	}
	expectExtents := []proto.ExtentKey{
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
	}
	extentResultVerify(t, leader, follower, ino, expectExtents)

	//append extent
	extent = proto.ExtentKey{
		PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100,
	}
	if err = extentAppend(ino, extent, leader); err != nil {
		t.Error(err)
		return
	}
	expectExtents = []proto.ExtentKey{
		{PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100},
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
	}
	extentResultVerify(t, leader, follower, ino, expectExtents)

	//append extent
	extent = proto.ExtentKey{
		PartitionId: 3, ExtentId: 1027, FileOffset: 2500, ExtentOffset: 0, Size: 200,
	}
	if err = extentAppend(ino, extent, leader); err != nil {
		t.Error(err)
		return
	}
	expectExtents = []proto.ExtentKey{
		{PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100},
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
		{PartitionId: 3, ExtentId: 1027, FileOffset: 2500, ExtentOffset: 0, Size: 200},
	}
	extentResultVerify(t, leader, follower, ino, expectExtents)

	//append extent
	extent = proto.ExtentKey{
		PartitionId: 4, ExtentId: 1028, FileOffset: 1500, ExtentOffset: 0, Size: 100,
	}
	if err = extentAppend(ino, extent, leader); err != nil {
		t.Error(err)
		return
	}
	expectExtents = []proto.ExtentKey{
		{PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100},
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
		{PartitionId: 4, ExtentId: 1028, FileOffset: 1500, ExtentOffset: 0, Size: 100},
		{PartitionId: 3, ExtentId: 1027, FileOffset: 2500, ExtentOffset: 0, Size: 200},
	}
	extentResultVerify(t, leader, follower, ino, expectExtents)

	//append extent
	extent = proto.ExtentKey{
		PartitionId: 4, ExtentId: 1029, FileOffset: 1600, ExtentOffset: 0, Size: 100,
	}
	if err = extentAppend(ino, extent, leader); err != nil {
		t.Error(err)
		return
	}
	expectExtents = []proto.ExtentKey{
		{PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100},
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
		{PartitionId: 4, ExtentId: 1028, FileOffset: 1500, ExtentOffset: 0, Size: 100},
		{PartitionId: 4, ExtentId: 1029, FileOffset: 1600, ExtentOffset: 0, Size: 100},
		{PartitionId: 3, ExtentId: 1027, FileOffset: 2500, ExtentOffset: 0, Size: 200},
	}
	extentResultVerify(t, leader, follower, ino, expectExtents)

	//append extent
	extent = proto.ExtentKey{
		PartitionId: 4, ExtentId: 1030, FileOffset: 1400, ExtentOffset: 0, Size: 600,
	}
	if err = extentAppend(ino, extent, leader); err != nil {
		t.Error(err)
		return
	}
	expectExtents = []proto.ExtentKey{
		{PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100},
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
		{PartitionId: 4, ExtentId: 1030, FileOffset: 1400, ExtentOffset: 0, Size: 600},
		{PartitionId: 3, ExtentId: 1027, FileOffset: 2500, ExtentOffset: 0, Size: 200},
	}
	extentResultVerify(t, leader, follower, ino, expectExtents)
}

func TestExtentAppend(t *testing.T) {
	testFunc := []TestFunc{
		ExtentAppendInterTest,
	}
	doTest(t, testFunc)
}

func batchExtentAppend(ino uint64, eks []proto.ExtentKey, leader *metaPartition) (err error) {
	reqBatchExtentAppend := &proto.AppendExtentKeysRequest{
		Inode: ino,
		Extents: eks,
	}
	packet := &Packet{}
	if err = leader.BatchExtentAppend(reqBatchExtentAppend, packet); err != nil || packet.ResultCode != proto.OpOk {
		return fmt.Errorf("batch extent append failed, err:%v, resultCode:%v", err, packet.ResultCode)
	}
	return
}

func BatchExtentAppendInterTest(t *testing.T, leader, follower *metaPartition) {
	var err error
	//create inode
	ino, err := createInode(470, 0, 0, leader)
	if err != nil {
		t.Error(err)
		return
	}
	//batch extent append
	eks := []proto.ExtentKey{
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
		{PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100},
		{PartitionId: 3, ExtentId: 1027, FileOffset: 2500, ExtentOffset: 0, Size: 200},
		{PartitionId: 4, ExtentId: 1028, FileOffset: 1500, ExtentOffset: 0, Size: 100},
		{PartitionId: 4, ExtentId: 1029, FileOffset: 1600, ExtentOffset: 0, Size: 100},
	}
	if err = batchExtentAppend(ino, eks, leader); err != nil {
		t.Error(err)
		return
	}

	expectExtentKeys := []proto.ExtentKey{
		{PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100},
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
		{PartitionId: 4, ExtentId: 1028, FileOffset: 1500, ExtentOffset: 0, Size: 100},
		{PartitionId: 4, ExtentId: 1029, FileOffset: 1600, ExtentOffset: 0, Size: 100},
		{PartitionId: 3, ExtentId: 1027, FileOffset: 2500, ExtentOffset: 0, Size: 200},
	}
	extentResultVerify(t, leader, follower, ino, expectExtentKeys)
	return
}

func TestBatchExtentAppend(t *testing.T) {
	testFunc := []TestFunc{
		BatchExtentAppendInterTest,
	}
	doTest(t, testFunc)
}

func ListExtentInterTest(t *testing.T, leader, follower *metaPartition) {
	ino, err := createInode(470, 0, 0, leader)
	if err != nil {
		t.Fatal(err)
	}

	eks := []proto.ExtentKey{
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
		{PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100},
		{PartitionId: 3, ExtentId: 1027, FileOffset: 2500, ExtentOffset: 0, Size: 200},
		{PartitionId: 4, ExtentId: 1028, FileOffset: 1500, ExtentOffset: 0, Size: 100},
		{PartitionId: 4, ExtentId: 1029, FileOffset: 1600, ExtentOffset: 0, Size: 100},
	}
	if err = batchExtentAppend(ino, eks, leader); err != nil {
		t.Error(err)
		return
	}
	//list extent
	reqListExtent := &proto.GetExtentsRequest{
		Inode: ino,
	}
	packet := &Packet{}
	if err = leader.ExtentsList(reqListExtent, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("list extents failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}
	respListExtent := &proto.GetExtentsResponse{}
	if err = packet.UnmarshalData(respListExtent); err != nil {
		t.Errorf("unmarshal list extent response failed:%v, resultCode:%v", err, packet.ResultCode)
		return
	}
	//verify
	if len(respListExtent.Extents) != len(eks) {
		t.Errorf("test error, extent count expect is %v, but actual is %v", len(eks), len(respListExtent.Extents))
		return
	}
	sort.Slice(eks, func(i, j int) bool {
		return eks[i].FileOffset < eks[j].FileOffset
	})
	for index, extent := range respListExtent.Extents {
		if !reflect.DeepEqual(extent, eks[index]) {
			t.Errorf("ek[%v] mismatch: expect %s, actual %s", extent, eks[index].String(), extent.String())
			return
		}
	}
	return
}

func TestListExtent(t *testing.T) {
	testFunc := []TestFunc{
		ListExtentInterTest,
	}
	doTest(t, testFunc)
}

func extentInsert(ino uint64, extent proto.ExtentKey, mp *metaPartition) (err error) {
	req := &proto.InsertExtentKeyRequest{
		Inode: ino,
		Extent: extent,
	}
	packet := &Packet{}
	if err = mp.ExtentInsert(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		err = fmt.Errorf("extent insert failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}
	return
}

func ExtentInsertInterTest(t *testing.T, leader, follower *metaPartition) {
	//create inode
	ino, err := createInode(470, 0, 0, leader)
	if err != nil {
		t.Fatal(err)
	}
	eks := []proto.ExtentKey{
		{FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
		{FileOffset: 300, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 20},
		{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
		{FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 60},
		{FileOffset: 400, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 20},
	}

	for _, ek := range eks {
		if err = extentInsert(ino, ek, leader); err != nil {
			t.Error(err)
			return
		}
	}

	expectedEks := []proto.ExtentKey{
		{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
		{FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
		{FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 60},
		{FileOffset: 300, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 20},
		{FileOffset: 400, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 20},
	}
	extentResultVerify(t, leader, follower, ino, expectedEks)
}

func TestInsertAppend(t *testing.T) {
	testFunc := []TestFunc{
		ExtentInsertInterTest,
	}
	doTest(t, testFunc)
}

func ExtentsTruncateInterTest01(t *testing.T, leader, follower *metaPartition) {
	ino, err := createInode(470, 0, 0, leader)
	if err != nil {
		t.Fatal(err)
	}
	eks := []proto.ExtentKey{
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
		{PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100},
		{PartitionId: 3, ExtentId: 1027, FileOffset: 2500, ExtentOffset: 0, Size: 200},
		{PartitionId: 4, ExtentId: 1028, FileOffset: 1500, ExtentOffset: 0, Size: 100},
		{PartitionId: 4, ExtentId: 1029, FileOffset: 1600, ExtentOffset: 0, Size: 100},
	}
	if err = batchExtentAppend(ino, eks, leader); err != nil {
		t.Error(err)
		return
	}

	req := &proto.TruncateRequest{
		Inode: ino,
		Size: 1550,
		OldSize: 2700,
		Version: 1,
	}
	packet := &Packet{}
	if err = leader.ExtentsTruncate(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("truncate extents failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}

	//verify inode size, extents
	expectEks := []proto.ExtentKey{
		{PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100},
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
		{PartitionId: 4, ExtentId: 1028, FileOffset: 1500, ExtentOffset: 0, Size: 50},
	}
	extentResultVerify(t, leader, follower, ino, expectEks)
}

func ExtentsTruncateInterTest02(t *testing.T, leader, follower *metaPartition) {
	ino, err := createInode(uint32(os.ModeDir), 0, 0, leader)
	if err != nil {
		t.Fatal(err)
	}

	req := &proto.TruncateRequest{
		Inode:   ino,
		Size:    1550,
		OldSize: 2700,
		Version: 1,
	}
	packet := &Packet{}
	if err = leader.ExtentsTruncate(req, packet); err != nil || packet.ResultCode != proto.OpArgMismatchErr {
		t.Fatalf("truncate extent mismatch, expect resultCode is OpArgMismatchErr, but actual is" +
			" [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}
}

func ExtentsTruncateInterTest03(t *testing.T, leader, follower *metaPartition) {
	ino, err := createInode(470, 0, 0, leader)
	if err != nil {
		t.Fatal(err)
	}
	inode, _ := leader.inodeTree.Get(ino)
	inode.SetDeleteMark()
	leader.inodeTree.Put(inode)

	inode, _ = follower.inodeTree.Get(ino)
	inode.SetDeleteMark()
	follower.inodeTree.Put(inode)

	req := &proto.TruncateRequest{
		Inode:   ino,
		Size:    1550,
		OldSize: 2700,
		Version: 1,
	}
	packet := &Packet{}
	if err = leader.ExtentsTruncate(req, packet); err != nil || packet.ResultCode != proto.OpNotExistErr {
		t.Fatalf("truncate extent error[%v] or resultCode mismatch, expect resultCode is OpNotExistErr(0xF5), but actual is" +
			" [resultCode:Ox%X]", err, packet.ResultCode)
		return
	}
}

func ExtentsTruncateInterTest04(t *testing.T, leader, follower *metaPartition) {
	var err error
	req := &proto.TruncateRequest{
		Inode: 1000,
		Size: 1550,
		OldSize: 2700,
		Version: 1,
	}
	packet := &Packet{}
	if err = leader.ExtentsTruncate(req, packet); err != nil || packet.ResultCode != proto.OpNotExistErr {
		t.Fatalf("truncate extent error or resultCode mismatch, expect resultCode is OpNotExistErr(0xF5), but actual is" +
			" [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}
}

func ExtentsTruncateInterTest05(t *testing.T, leader, follower *metaPartition) {
	ino, err := createInode(470, 0, 0, leader)
	if err != nil {
		t.Fatal(err)
	}
	eks := []proto.ExtentKey{
		{PartitionId: 1, ExtentId: 1, FileOffset: 1000, ExtentOffset: 4096, Size: 100},
		{PartitionId: 2, ExtentId: 1026, FileOffset: 300, ExtentOffset: 0, Size: 100},
		{PartitionId: 3, ExtentId: 1027, FileOffset: 2500, ExtentOffset: 0, Size: 200},
		{PartitionId: 4, ExtentId: 1028, FileOffset: 1500, ExtentOffset: 0, Size: 100},
		{PartitionId: 4, ExtentId: 1029, FileOffset: 1600, ExtentOffset: 0, Size: 100},
	}
	if err = batchExtentAppend(ino, eks, leader); err != nil {
		t.Error(err)
		return
	}

	req := &proto.TruncateRequest{
		Inode: ino,
		Size: 1550,
		OldSize: 2600,
		Version: 1,
	}
	packet := &Packet{}
	if err = leader.ExtentsTruncate(req, packet); err != nil || packet.ResultCode != proto.OpArgMismatchErr {
		t.Fatalf("truncate extent error[%v] or mismatch, expect resultCode[OpArgMismatchErr(0xF4), actual:0x%X", err, packet.ResultCode)
		return
	}
}

func TestExtentsTruncate(t *testing.T) {
	testFunc := []TestFunc{
		//test extent truncate
		ExtentsTruncateInterTest01,
		//test dir mode type inode
		ExtentsTruncateInterTest02,
		//test set delete mark inode
		ExtentsTruncateInterTest03,
		//test not exist inode
		ExtentsTruncateInterTest04,
		//test error file size
		ExtentsTruncateInterTest05,
	}
	doTest(t, testFunc)
}
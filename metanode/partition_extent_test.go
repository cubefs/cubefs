package metanode

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/chubaofs/chubaofs/proto"
)

func createInode(t, uid, gid uint32, mp *metaPartition) (ino uint64, err error) {
	reqCreateInode := &proto.CreateInodeRequest{
		PartitionID: mp.config.PartitionId,
		Mode:        t,
		Uid:         uid,
		Gid:         gid,
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
	if inodeInMem.Extents.Len() != len(expectExtents) {
		t.Fatalf("extent count mismatch, expect:%v, actual:%v", len(expectExtents), inodeInMem.Extents.Len())
	}
	inodeInMem.Extents.Range2(func(index int, ext proto.ExtentKey) bool {
		if !reflect.DeepEqual(ext, expectExtents[index]) {
			t.Fatalf("extent mismatch, expect:%v, actual:%v", expectExtents[index], ext)
		}
		return true
	})
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

func TestMetaPartition_ExtentAppendCase01(t *testing.T) {
	//leader is mem mode
	dir := "extent_append_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ExtentAppendInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ExtentAppendInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func batchExtentAppend(ino uint64, eks []proto.ExtentKey, leader *metaPartition) (err error) {
	reqBatchExtentAppend := &proto.AppendExtentKeysRequest{
		Inode:   ino,
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

func TestMetaPartition_BatchExtentAppendCase01(t *testing.T) {
	//leader is mem mode
	dir := "batch_extent_append_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	BatchExtentAppendInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	BatchExtentAppendInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
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

func TestMetaPartition_ListExtentCase01(t *testing.T) {
	//leader is mem mode
	dir := "list_extent_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ListExtentInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ListExtentInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func extentInsert(ino uint64, extent proto.ExtentKey, mp *metaPartition) (err error) {
	req := &proto.InsertExtentKeyRequest{
		Inode:  ino,
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

func TestMetaPartition_InsertExtentCase01(t *testing.T) {
	//leader is mem mode
	dir := "insert_extent_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ExtentInsertInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ExtentInsertInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
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
		Inode:   ino,
		Size:    1550,
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
		t.Fatalf("truncate extent mismatch, expect resultCode is OpArgMismatchErr, but actual is"+
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
	_ = inodePut(leader.inodeTree, inode)

	inode, _ = follower.inodeTree.Get(ino)
	inode.SetDeleteMark()
	_ = inodePut(follower.inodeTree, inode)

	req := &proto.TruncateRequest{
		Inode:   ino,
		Size:    1550,
		OldSize: 2700,
		Version: 1,
	}
	packet := &Packet{}
	if err = leader.ExtentsTruncate(req, packet); err != nil || packet.ResultCode != proto.OpNotExistErr {
		t.Fatalf("truncate extent error[%v] or resultCode mismatch, expect resultCode is OpNotExistErr(0xF5), but actual is"+
			" [resultCode:Ox%X]", err, packet.ResultCode)
		return
	}
}

func ExtentsTruncateInterTest04(t *testing.T, leader, follower *metaPartition) {
	var err error
	req := &proto.TruncateRequest{
		Inode:   1000,
		Size:    1550,
		OldSize: 2700,
		Version: 1,
	}
	packet := &Packet{}
	if err = leader.ExtentsTruncate(req, packet); err != nil || packet.ResultCode != proto.OpNotExistErr {
		t.Fatalf("truncate extent error or resultCode mismatch, expect resultCode is OpNotExistErr(0xF5), but actual is"+
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
		Inode:   ino,
		Size:    1550,
		OldSize: 2600,
		Version: 1,
	}
	packet := &Packet{}
	if err = leader.ExtentsTruncate(req, packet); err != nil || packet.ResultCode != proto.OpArgMismatchErr {
		t.Fatalf("truncate extent error[%v] or mismatch, expect resultCode[OpArgMismatchErr(0xF4), actual:0x%X", err, packet.ResultCode)
		return
	}
}

func TestMetaPartition_ExtentsTruncateCase01(t *testing.T) {
	//leader is mem mode
	dir := "extents_truncate_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ExtentsTruncateInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ExtentsTruncateInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_ExtentsTruncateCase02(t *testing.T) {
	//leader is mem mode
	dir := "extents_truncate_test_02"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ExtentsTruncateInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ExtentsTruncateInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_ExtentsTruncateCase03(t *testing.T) {
	//leader is mem mode
	dir := "extents_truncate_test_03"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ExtentsTruncateInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ExtentsTruncateInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_ExtentsTruncateCase04(t *testing.T) {
	//leader is mem mode
	dir := "extents_truncate_test_04"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ExtentsTruncateInterTest04(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ExtentsTruncateInterTest04(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_ExtentsTruncateCase05(t *testing.T) {
	//leader is mem mode
	dir := "extents_truncate_test_05"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ExtentsTruncateInterTest05(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ExtentsTruncateInterTest05(t, leader, follower)
	releaseMp(leader, follower, dir)
}

package metanode

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/metanode/metamock"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"math"
	"os"
	"reflect"
	"strconv"
	"testing"
)

type FileNameGenFunc func(int) string

func createDentries(mp *metaPartition, parentId uint64, dentryCnt int, dentryMode uint32, inodeStart uint64, fn FileNameGenFunc) (err error) {
	for index := 0; index < dentryCnt; index++ {
		req := &proto.CreateDentryRequest{
			ParentID: parentId,
			Name:     fn(index),
			Inode:    inodeStart + uint64(index),
			Mode:     dentryMode,
		}
		packet := &Packet{}
		err = mp.CreateDentry(req, packet)
		if err != nil || packet.ResultCode != proto.OpOk {
			return fmt.Errorf("create dentry failed:%v, req:%v", err, req)
		}
	}
	return
}

//create dentries, and validate dentry info
func CreateDentryInterTest01(t *testing.T, leader, follower *metaPartition) {

	t.Logf("run create dentry test 01")
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Logf("dentry parent id:%v\n", parentID)
	//create dentries
	dirNameGen := func(i int) string{
		return fmt.Sprintf("test_dir_0%v", i)
	}
	if err = createDentries(leader, parentID, 100, uint32(os.ModeDir), 1000, dirNameGen); err != nil {
		t.Fatal(err)
	}

	fileNameGen := func(i int) string{
		return fmt.Sprintf("test_0%v", i)
	}
	if err = createDentries(leader, parentID, 100, 470, 2000, fileNameGen); err != nil {
		t.Fatal(err)
	}

	t.Logf("start validate create dentry result\n")
	//validate
	startDentry := &Dentry{
		ParentId: parentID,
	}
	endDentry := &Dentry{
		ParentId: parentID + 1,
	}
	dentriesInleader := make([]*Dentry, 0, 200)
	if err = leader.dentryTree.Range(startDentry, endDentry, func(d *Dentry) (bool, error) {
		dentriesInleader = append(dentriesInleader, d)
		return true, nil
	}); err != nil {
		t.Fatalf("range dentry tree failed:%v", err)
	}

	dentriesInFollower := make([]*Dentry, 0, 100)
	if err = follower.dentryTree.Range(startDentry, endDentry, func(d *Dentry) (bool, error) {
		dentriesInFollower = append(dentriesInFollower, d)
		return true, nil
	}); err != nil {
		t.Fatalf("range dentry tree failed:%v", err)
	}

	if len(dentriesInleader) != len(dentriesInFollower) || len(dentriesInFollower) != 200 {
		t.Fatalf("error dentry count [expect:200, leader actual:%v, follower actual:%v]", len(dentriesInleader), len(dentriesInFollower))
	}

	for index, dentryInLeader := range dentriesInleader {
		if !reflect.DeepEqual(dentryInLeader, dentriesInFollower[index]) {
			t.Fatalf("validate failed:%v, name[leader:%s, follower:%s], parentid[leader:%v, follower:%v]" +
				" inode[leader:%v, follower:%v] mode[leader:%v, follower:%v]", err,
				dentryInLeader.Name, dentriesInFollower[index].Name,
				dentryInLeader.ParentId, dentriesInFollower[index].ParentId,
				dentryInLeader.Inode, dentriesInFollower[index].Inode,
				dentryInLeader.Type, dentriesInFollower[index].Type)
		}
	}
	t.Logf("create dentry test 01 finished\n")
	return
}

func CreateDentryInterTest02(t *testing.T, leader, follower *metaPartition) {
	// create dentrys with parent inode not exist
	req := &proto.CreateDentryRequest{
		ParentID: 1000,
		Name:     "test",
		Inode:    100,
		Mode:     470,
	}
	packet := &Packet{}
	err := leader.CreateDentry(req, packet)
	if err == nil || packet.ResultCode != proto.OpNotExistErr {
		t.Fatalf("error[%v] or resultCode mismatch, expect:OpNotExistErr, actual:%v", err, packet.ResultCode)
	}
	t.Logf("create dentry test 02 finished\n")
	return
}

func CreateDentryInterTest03(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}
	inode, _ := leader.inodeTree.Get(parentID)
	inode.SetDeleteMark()
	_ = inodePut(leader.inodeTree, inode)

	inode, _ = follower.inodeTree.Get(parentID)
	inode.SetDeleteMark()
	_ = inodePut(follower.inodeTree, inode)

	//create dentry with parent inode be marked delete
	req := &proto.CreateDentryRequest{
		ParentID: parentID,
		Name:     "test",
		Inode:    100,
		Mode:     470,
	}
	packet := &Packet{}
	err = leader.CreateDentry(req, packet)
	if err == nil || packet.ResultCode != proto.OpNotExistErr {
		t.Fatalf("error[%v] or resultCode mismatch, expect:OpNotExistErr, actual:%v", err, packet.ResultCode)
	}
	t.Logf("create dentry test 03 finished\n")
	return
}

func CreateDentryInterTest04(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(470, 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry with parent inode file mode
	req := &proto.CreateDentryRequest{
		ParentID: parentID,
		Name:     "test",
		Inode:    100,
		Mode:     470,
	}
	packet := &Packet{}
	err = leader.CreateDentry(req, packet)
	if err != nil || packet.ResultCode != proto.OpArgMismatchErr {
		t.Fatalf("error[%v] or resultCode mismatch, expect:OpNotExistErr, actual:%v", err, packet.ResultCode)
	}
	t.Logf("create dentry test 04 finished\n")
	return
}

func CreateDentryInterTest05(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry
	req := &proto.CreateDentryRequest{
		ParentID: parentID,
		Name:     "test",
		Inode:    100,
		Mode:     470,
	}
	packet := &Packet{}
	err = leader.CreateDentry(req, packet)
	if err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("create dentry failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}
	inode, _ := leader.inodeTree.Get(parentID)
	if inode.NLink != 3 {
		t.Fatalf("parent inode nlink mistmatch, expect:3, actual:%v", inode.NLink)
	}
	inode, _ = follower.inodeTree.Get(parentID)
	if inode.NLink != 3 {
		t.Fatalf("parent inode nlink mistmatch, expect:3, actual:%v", inode.NLink)
	}

	//create repeat dentry with same dentry info
	packet = &Packet{}
	err = leader.CreateDentry(req, packet)
	if err != nil || packet.ResultCode != proto.OpOk {
		t.Fatalf("error[%v] or resultCode mismatch, expect:OpOk, actual:%v", err, packet.ResultCode)
	}

	//create repeat dentry, and dentry with different mode
	req = &proto.CreateDentryRequest{
		ParentID: parentID,
		Name:     "test",
		Inode:    100,
		Mode:     uint32(os.ModeDir),
	}
	packet = &Packet{}
	err = leader.CreateDentry(req, packet)
	if err != nil || packet.ResultCode != proto.OpArgMismatchErr {
		t.Fatalf("error[%v] or resultCode mismatch, expect:OpArgMismatchErr, actual:%v", err, packet.ResultCode)
	}

	//create repeat dentry, and dentry with different inode
	req = &proto.CreateDentryRequest{
		ParentID: parentID,
		Name:     "test",
		Inode:    102,
		Mode:     470,
	}
	packet = &Packet{}
	err = leader.CreateDentry(req, packet)
	if err != nil || packet.ResultCode != proto.OpExistErr {
		t.Fatalf("error[%v] or resultCode mismatch, expect:OpExistErr, actual:%v", err, packet.ResultCode)
	}
	t.Logf("create dentry test 05 finished\n")
	return
}


func CreateDentryInterTest06(t *testing.T, leader, follower *metaPartition) {
	//create parent inode
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentrys with parent id = inode
	req := &proto.CreateDentryRequest{
		ParentID: parentID,
		Name:     "test",
		Inode:    parentID,
		Mode:     470,
	}
	packet := &Packet{}
	err = leader.CreateDentry(req, packet)
	if err == nil {
		t.Fatalf("mistmatch, err expect:parentId is equal inodeId, actual:nil")
	}
	t.Logf("create dentry test 06 finished\n")
	return
}

func CreateDentryInterTest07(t *testing.T, leader, follower *metaPartition) {
	//create parent inode and change nlink number
	parentIno := NewInode(1000, uint32(os.ModeDir))
	parentIno.NLink = 1001

	if _, _, err := inodeCreate(leader.inodeTree, parentIno, true); err != nil {
		t.Errorf("create parent inode failed:%v", err)
		return
	}

	//create dentrys with parent id = inode
	req := &proto.CreateDentryRequest{
		ParentID: parentIno.Inode,
		Name:     "test",
		Inode:    1549830,
		Mode:     470,
	}
	packet := &Packet{}
	err := leader.CreateDentry(req, packet)
	if err == nil || packet.ResultCode != proto.OpNotPerm {
		t.Fatalf("mistmatch, err expect:child file count reach max count, actual:%v; resultCode expect:" +
			"OpNotPerm, actual:%v", err, packet.ResultCode)
		return
	}
	t.Logf("create dentry test 07 finished\n")
	return
}

func TestMetaPartition_CreateDentryCase01(t *testing.T) {
	//leader is mem mode
	dir := "create_dentry_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	CreateDentryInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	CreateDentryInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_CreateDentryCase02(t *testing.T) {
	//leader is mem mode
	dir := "create_dentry_test_02"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	CreateDentryInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	CreateDentryInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)
}


func TestMetaPartition_CreateDentryCase03(t *testing.T) {
	//leader is mem mode
	dir := "create_dentry_test_03"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	CreateDentryInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	CreateDentryInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)
}


func TestMetaPartition_CreateDentryCase04(t *testing.T) {
	//leader is mem mode
	dir := "create_dentry_test_04"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	CreateDentryInterTest04(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	CreateDentryInterTest04(t, leader, follower)
	releaseMp(leader, follower, dir)
}


func TestMetaPartition_CreateDentryCase05(t *testing.T) {
	//leader is mem mode
	dir := "create_dentry_test_05"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	CreateDentryInterTest05(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	CreateDentryInterTest05(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_CreateDentryCase06(t *testing.T) {
	//leader is mem mode
	dir := "create_dentry_test_06"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	CreateDentryInterTest06(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	CreateDentryInterTest06(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_CreateDentryCase07(t *testing.T) {
	//leader is mem mode
	dir := "create_dentry_test_07"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	leader.config.ChildFileMaxCount = 1000
	CreateDentryInterTest07(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	leader.config.ChildFileMaxCount = 1000
	CreateDentryInterTest07(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func DeleteDentryInterTest01(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry
	fileNameGen := func(i int) string{
		return fmt.Sprintf("test_0%v", i)
	}
	if err = createDentries(leader, parentID, 10, 470, 1000, fileNameGen); err != nil {
		t.Error(err)
		return
	}
	//delete dentry
	reqDeleteDentry := &proto.DeleteDentryRequest{
		ParentID: parentID,
		Name: "test_05",
	}
	resp := &Packet{}
	if err = leader.DeleteDentry(reqDeleteDentry, resp); err != nil || resp.ResultCode != proto.OpOk {
		t.Fatalf("delete file[%s] dentry failed:%v", reqDeleteDentry.Name, err)
	}

	//validate
	if _, status, _ := leader.getDentry(&Dentry{ParentId: reqDeleteDentry.ParentID, Name: reqDeleteDentry.Name}); status != proto.OpNotExistErr {
		t.Errorf("delete dentry[parentID:1, name:test_05] exist in memModeMp")
		return
	}

	if _, status, _ := follower.getDentry(&Dentry{ParentId: reqDeleteDentry.ParentID, Name: reqDeleteDentry.Name}); status != proto.OpNotExistErr {
		t.Errorf("delete dentry[parentID:1, name:test_05] exist in memModeMp")
		return
	}

	inode, _ := leader.inodeTree.Get(parentID)
	if inode == nil {
		t.Fatalf("parent inode not exist")
	}
	if inode.NLink != 11 {
		t.Fatalf("parent inode nlink mismatch, expect:11, actual:%v", inode.NLink)
	}

	inode, _ = follower.inodeTree.Get(parentID)
	if inode == nil {
		t.Fatalf("parent inode not exist")
	}
	if inode.NLink != 11 {
		t.Fatalf("parent inode nlink mismatch, expect:11, actual:%v", inode.NLink)
	}
}

func DeleteDentryInterTest02(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry dir mode
	fileNameGen := func(i int) string{
		return fmt.Sprintf("test_dir_0%v", i)
	}
	if err = createDentries(leader, parentID, 100, uint32(os.ModeDir), 1000, fileNameGen); err != nil {
		t.Error(err)
		return
	}
	//delete dentry
	reqDeleteDentry := &proto.DeleteDentryRequest{
		ParentID: parentID,
		Name: "test_dir_050",
	}
	resp := &Packet{}
	if err = leader.DeleteDentry(reqDeleteDentry, resp); err != nil || resp.ResultCode != proto.OpOk {
		t.Fatalf("delete file[%s] dentry failed:%v", reqDeleteDentry.Name, err)
	}

	//validate
	if _, status, _ := leader.getDentry(&Dentry{ParentId: reqDeleteDentry.ParentID, Name: reqDeleteDentry.Name}); status != proto.OpNotExistErr {
		t.Errorf("delete dentry[parentID:1, name:test_05] exist in memModeMp")
		return
	}

	if _, status, _ := follower.getDentry(&Dentry{ParentId: reqDeleteDentry.ParentID, Name: reqDeleteDentry.Name}); status != proto.OpNotExistErr {
		t.Errorf("delete dentry[parentID:1, name:test_05] exist in memModeMp")
		return
	}

	inode, _ := leader.inodeTree.Get(parentID)
	if inode == nil {
		t.Fatalf("parent inode not exist")
	}
	if inode.NLink != 101 {
		t.Fatalf("parent inode nlink mismatch, expect:101, actual:%v", inode.NLink)
	}

	inode, _ = follower.inodeTree.Get(parentID)
	if inode == nil {
		t.Fatalf("parent inode not exist")
	}
	if inode.NLink != 101 {
		t.Fatalf("parent inode nlink mismatch, expect:101, actual:%v", inode.NLink)
	}
}

func DeleteDentryInterTest03(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//delete not exist dentry
	reqDeleteDentry := &proto.DeleteDentryRequest{
		ParentID: parentID,
		Name: "test_dir_050",
	}
	resp := &Packet{}
	if err = leader.DeleteDentry(reqDeleteDentry, resp); err != nil || resp.ResultCode != proto.OpNotExistErr {
		t.Fatalf("error[%s] or resultCode mistmatch, resultCode expect:proto.OpNotExistErr(0xF5), actual:0x%X", err, resp.ResultCode)
	}
}

func TestMetaPartition_DeleteDentryCase01(t *testing.T) {
	//leader is mem mode
	dir := "delete_dentry_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	DeleteDentryInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	DeleteDentryInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_DeleteDentryCase02(t *testing.T) {
	//leader is mem mode
	dir := "delete_dentry_test_02"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	DeleteDentryInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	DeleteDentryInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_DeleteDentryCase03(t *testing.T) {
	//leader is mem mode
	dir := "delete_dentry_test_03"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	DeleteDentryInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	DeleteDentryInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func BatchDeleteDentryInterTest01(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry
	fileNameGen := func(i int) string{
		return fmt.Sprintf("test_0%v", i)
	}
	if err = createDentries(leader, parentID, 10, 470, 1000, fileNameGen); err != nil {
		t.Error(err)
		return
	}
	//batch delete
	batchFileDentry := make([]proto.Dentry, 0, 5)
	for index := 3; index < 8; index++ {
		batchFileDentry = append(batchFileDentry, proto.Dentry{Inode: uint64(1000 + index), Name: fileNameGen(index)})
	}
	deleteBatchDentryReq := &BatchDeleteDentryReq{
		ParentID: parentID,
		Dens:     batchFileDentry,
	}
	packet := &Packet{}
	if err = leader.DeleteDentryBatch(deleteBatchDentryReq, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Fatalf("error[%v] or resultCode mismatch, expect:proto.OpOk(), actual:ox%x", err, packet.ResultCode)
		return
	}

	//validate
	for _, dentry := range batchFileDentry {
		if _, status, _ := leader.getDentry(&Dentry{ParentId: parentID, Name: dentry.Name}); status != proto.OpNotExistErr {
			t.Errorf("mistmatch, dentry[parentID:%v, name:%s] be deleted",parentID, dentry.Name)
			return
		}

		if _, status, _ := follower.getDentry(&Dentry{ParentId: parentID, Name: dentry.Name}); status != proto.OpNotExistErr {
			t.Errorf("mismatch, dentry[parentID:%v, name:%s] be deleted",parentID, dentry.Name)
			return
		}
	}

	inode, _ := leader.inodeTree.Get(parentID)
	if inode == nil {
		t.Fatalf("parent inode not exist")
	}
	if inode.NLink != 7 {
		t.Fatalf("parent inode nlink mismatch, expect:7, actual:%v", inode.NLink)
	}

	inode, _ = follower.inodeTree.Get(parentID)
	if inode == nil {
		t.Fatalf("parent inode not exist")
	}
	if inode.NLink != 7 {
		t.Fatalf("parent inode nlink mismatch, expect:7, actual:%v", inode.NLink)
	}
}

func BatchDeleteDentryInterTest02(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry
	fileNameGen := func(i int) string{
		return fmt.Sprintf("test_dir_0%v", i)
	}
	if err = createDentries(leader, parentID, 10, uint32(os.ModeDir), 2000, fileNameGen); err != nil {
		t.Error(err)
		return
	}
	//batch delete
	batchFileDentry := make([]proto.Dentry, 0, 5)
	for index := 2; index < 8; index++ {
		batchFileDentry = append(batchFileDentry, proto.Dentry{Inode: uint64(2000 + index), Name: fileNameGen(index)})
	}
	deleteBatchDentryReq := &BatchDeleteDentryReq{
		ParentID: parentID,
		Dens:     batchFileDentry,
	}
	packet := &Packet{}
	if err = leader.DeleteDentryBatch(deleteBatchDentryReq, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Fatalf("error[%v] or resultCode mismatch, expect:proto.OpOk(0xF0), actual:ox%x", err, packet.ResultCode)
		return
	}

	//validate
	for _, dentry := range batchFileDentry {
		if _, status, _ := leader.getDentry(&Dentry{ParentId: parentID, Name: dentry.Name}); status != proto.OpNotExistErr {
			t.Errorf("mistmatch, dentry[parentID:1, name:%s] be deleted", dentry.Name)
			return
		}

		if _, status, _ := follower.getDentry(&Dentry{ParentId: parentID, Name: dentry.Name}); status != proto.OpNotExistErr {
			t.Errorf("mismatch, dentry[parentID:1, name:%s] be deleted", dentry.Name)
			return
		}
	}

	inode, _ := leader.inodeTree.Get(parentID)
	if inode == nil {
		t.Fatalf("parent inode not exist")
	}
	if inode.NLink != 6 {
		t.Fatalf("parent inode nlink mismatch, expect:6, actual:%v", inode.NLink)
	}

	inode, _ = follower.inodeTree.Get(parentID)
	if inode == nil {
		t.Fatalf("parent inode not exist")
	}
	if inode.NLink != 6 {
		t.Fatalf("parent inode nlink mismatch, expect:6, actual:%v", inode.NLink)
	}
}

func BatchDeleteDentryInterTest03(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry
	fileNameGen := func(i int) string{
		return fmt.Sprintf("test_0%v", i)
	}
	if err = createDentries(leader, parentID, 10, 470, 1000, fileNameGen); err != nil {
		t.Error(err)
		return
	}
	//batch delete not exist dentry
	batchFileDentry := make([]proto.Dentry, 0, 5)
	for index := 3; index < 8; index++ {
		batchFileDentry = append(batchFileDentry, proto.Dentry{Inode: uint64(3000 + index), Name: fileNameGen(index)})
	}
	deleteBatchDentryReq := &BatchDeleteDentryReq{
		ParentID: parentID,
		Dens:     batchFileDentry,
	}
	packet := &Packet{}
	err = leader.DeleteDentryBatch(deleteBatchDentryReq, packet)
	if err != nil || packet.ResultCode != proto.OpErr {
		t.Fatalf("error[%v] or resultCode mismatch, expect:proto.OpErr(0xF0), actual:ox%X", err, packet.ResultCode)
		return
	}
}

func TestMetaPartition_BatchDeleteDentryCase01(t *testing.T) {
	//leader is mem mode
	dir := "batch_delete_dentry_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	BatchDeleteDentryInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	BatchDeleteDentryInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_BatchDeleteDentryCase02(t *testing.T) {
	//leader is mem mode
	dir := "batch_delete_dentry_test_02"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	BatchDeleteDentryInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	BatchDeleteDentryInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_BatchDeleteDentryCase03(t *testing.T) {
	//leader is mem mode
	dir := "batch_delete_dentry_test_03"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	BatchDeleteDentryInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	BatchDeleteDentryInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func UpdateDentryInterTest01(t *testing.T, leader, follower *metaPartition) {
	//create inode
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry
	fileNameGen := func(i int) string{
		return fmt.Sprintf("test_0%v", i)
	}
	if err = createDentries(leader, parentID, 10, 470, 1000, fileNameGen); err != nil {
		t.Error(err)
		return
	}
	//update dentry
	req := &UpdateDentryReq{
		ParentID: parentID,
		Name:     "test_05",
		Inode:    10001,
	}
	packet := &Packet{}
	if err = leader.UpdateDentry(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Fatalf("error[%v] or resultCode mismatch, resultCode expect:OpOk(0xF0), actual:0x%X", err, packet.ResultCode)
	}

	//validate
	resp := &proto.UpdateDentryResponse{}
	if err = packet.UnmarshalData(resp); err != nil {
		t.Errorf("unmarshal update dentry response failed:%v", err)
		return
	}
	if resp.Inode != 1005 {
		t.Fatalf("inode mistmatch, inode expect:1005, actual:%v", resp.Inode)
	}

	//get dentry and check inode
	dentry, status, _ := leader.getDentry(&Dentry{ParentId: parentID, Name: "test_05"})
	if dentry == nil || status == proto.OpNotExistErr {
		t.Fatalf("dentry not exist, error")
	}

	if dentry.Inode != 10001 {
		t.Fatalf("dentry inode mistmatch, expect:10001, actual:%v", dentry.Inode)
	}

	dentry, status, _ = follower.getDentry(&Dentry{ParentId: parentID, Name: "test_05"})
	if dentry == nil || status == proto.OpNotExistErr {
		t.Fatalf("dentry not exist, error")
	}

	if dentry.Inode != 10001 {
		t.Fatalf("dentry inode mistmatch, expect:10001, actual:%v", dentry.Inode)
	}
	return
}

func UpdateDentryInterTest02(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry
	fileNameGen := func(i int) string{
		return fmt.Sprintf("test_0%v", i)
	}
	if err = createDentries(leader, parentID, 10, 470, 1000, fileNameGen); err != nil {
		t.Error(err)
		return
	}
	//update not exist dentry
	req := &UpdateDentryReq{
		ParentID: parentID,
		Name:     "test",
		Inode:    10000,
	}
	packet := &Packet{}
	if err = leader.UpdateDentry(req, packet); err != nil || packet.ResultCode != proto.OpNotExistErr {
		t.Fatalf("error[%v] or resultCode mismatch, resultCode expect:OpNotExistErr(0xF5), actual:0x%X", err, packet.ResultCode)
	}
}

func UpdateDentryInterTest03(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry
	fileNameGen := func(i int) string{
		return fmt.Sprintf("test_0%v", i)
	}
	if err = createDentries(leader, parentID, 10, 470, 1000, fileNameGen); err != nil {
		t.Error(err)
		return
	}
	//update dentry with parentId is equal to inodeId
	req := &UpdateDentryReq{
		ParentID: parentID,
		Name:     "test",
		Inode:    parentID,
	}
	packet := &Packet{}
	if err = leader.UpdateDentry(req, packet); err == nil || packet.ResultCode != proto.OpExistErr {
		t.Fatalf("error is null or resultCode mismatch, resultCode expect:OpExistErr(0xFA), actual:0x%X", packet.ResultCode)
	}
}

func TestMetaPartition_UpdateDentryCase01(t *testing.T) {
	//leader is mem mode
	dir := "update_dentry_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	UpdateDentryInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	UpdateDentryInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_UpdateDentryCase02(t *testing.T) {
	//leader is mem mode
	dir := "update_dentry_test_02"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	UpdateDentryInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	UpdateDentryInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_UpdateDentryCase03(t *testing.T) {
	//leader is mem mode
	dir := "update_dentry_test_03"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	UpdateDentryInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	UpdateDentryInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func ReadDirInterTest(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry
	fileNameGen := func(i int) string{
		return fmt.Sprintf("test_0%v", i)
	}
	if err = createDentries(leader, parentID, 2000, 470, 1000, fileNameGen); err != nil {
		t.Error(err)
		return
	}

	dirNameGen := func(i int) string{
		return fmt.Sprintf("test_dir_0%v", i)
	}
	if err = createDentries(leader, parentID, 2000, uint32(os.ModeDir), 2000, dirNameGen); err != nil {
		t.Error(err)
		return
	}

	//read dir
	marker := ""
	children := make([]proto.Dentry, 0)
	for {
		req := &proto.ReadDirRequest{
			ParentID:    parentID,
			Marker:      marker,
			IsBatch:     true,
		}
		packet := &Packet{}
		if err = leader.ReadDir(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			t.Fatalf("err[%v] or resultCode mismatch, resultCode expect:OpOk(0xF0), actual:0x%X", err, packet.ResultCode)
		}
		resp := &ReadDirResp{}
		if err = packet.UnmarshalData(resp); err != nil {
			t.Errorf("unmarshal read dir resp failed:%v", err)
			return
		}

		children = append(children, resp.Children...)
		if resp.NextMarker == "" {
			break
		}
		marker = resp.NextMarker
	}

	//validate
	if len(children) != 4000 {
		t.Errorf("read dir children number not equal, expect[4000] actual[%v]", len(children))
		return
	}
	for _, dentry := range children {
		if proto.IsDir(dentry.Type) {
			if dentry.Name != dirNameGen(int(dentry.Inode - 2000)) {
				t.Fatalf("read dir error, dentry info not equal, except[inode:%v, name:%s, type:%v], "+
					"actualp[inode:%v, name:%s, type:%v]", dentry.Inode, dirNameGen(int(dentry.Inode - 2000)), dentry.Type, dentry.Inode, dentry.Name, dentry.Type)
			}
		} else {
			if dentry.Name != fileNameGen(int(dentry.Inode - 1000)) {
				t.Fatalf("read dir error, dentry info not equal, except[inode:%v, name:%s, type:%v], "+
					"actualp[inode:%v, name:%s, type:%v]", dentry.Inode, fileNameGen(int(dentry.Inode - 1000)), dentry.Type, dentry.Inode, dentry.Name, dentry.Type)
			}
		}
	}
	return

}

func TestMetaPartition_ReadDirCase01(t *testing.T) {
	//leader is mem mode
	dir := "read_dir_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ReadDirInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ReadDirInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func LookupInterTest(t *testing.T, leader, follower *metaPartition) {
	parentID, err := createInode(uint32(os.ModeDir), 1000, 1000, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//create dentry
	fileNameGen := func(i int) string{
		return fmt.Sprintf("test_0%v", i)
	}
	if err = createDentries(leader, parentID, 100, 470, 1000, fileNameGen); err != nil {
		t.Error(err)
		return
	}

	dirNameGen := func(i int) string{
		return fmt.Sprintf("test_dir_0%v", i)
	}
	if err = createDentries(leader, parentID, 100, uint32(os.ModeDir), 2000, dirNameGen); err != nil {
		t.Error(err)
		return
	}

	for index := 0; index < 100; index++ {
		inode := uint64(1000 + index)
		req := &LookupReq{
			ParentID: parentID,
			Name:     fileNameGen(index),
		}
		packet := &Packet{}
		if err = leader.Lookup(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			t.Fatalf("look up (parentId:%v, inode:%v, name:%s) failed, error[%v] or resultCode mismatch," +
				" resultCode expect:OpOk(0xF0), actual:0x%X", parentID, inode, req.Name, err, packet.ResultCode)
		}
		resp := &LookupResp{}
		if err = packet.UnmarshalData(resp); err != nil {
			t.Errorf("unmarshal look up response failed, dentry info[parent id:%v, inode:%v, name:%s], error[%v]",
				parentID, inode, req.Name, err)
			return
		}
		if resp.Inode != inode {
			t.Fatalf("dentry[parentID:%v, name:%s] inode mimatch, except[%v] actual[%v]", parentID, req.Name, inode, resp.Inode)
		}
		if resp.Mode != 470 {
			t.Fatalf("dentry[parentID:%v, name:%s] mode mimatch, except[470] actual[%v]", parentID, req.Name, resp.Mode)
		}
	}

	for index := 0; index < 100; index++ {
		inode := uint64(index + 2000)
		req := &LookupReq{
			ParentID: parentID,
			Name:     dirNameGen(index),
		}
		packet := &Packet{}
		if err = leader.Lookup(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			t.Fatalf("look up (parentId:%v, inode:%v, name:%s) failed, error[%v] or resultCode mismatch," +
				" resultCode expect:OpOk(0xF0), actual:0x%X", parentID, inode, req.Name, err, packet.ResultCode)
		}
		resp := &LookupResp{}
		if err = packet.UnmarshalData(resp); err != nil {
			t.Errorf("unmarshal look up response failed, dentry info[parent id:%v, inode:%v, name:%s], error[%v]",
				parentID, inode, req.Name, err)
			return
		}
		if resp.Inode != inode {
			t.Fatalf("dentry[parentID:%v, name:%s] inode mimatch, except[%v] actual[%v]", parentID, req.Name, inode, resp.Inode)
		}
		if resp.Mode != uint32(os.ModeDir) {
			t.Fatalf("dentry[parentID:%v, name:%s] mode mimatch, except[%v] actual[%v]", parentID, req.Name, uint32(os.ModeDir), resp.Mode)
		}
	}
}

func TestMetaPartition_LookupCase01(t *testing.T) {
	//leader is mem mode
	dir := "look_up_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	LookupInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	LookupInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func newMetapartitionForTest(raft raftstore.Partition, config *MetaPartitionConfig, mangaer *metadataManager)(mp *metaPartition, err error) {
	tmp, err := CreateMetaPartition(config, mangaer)
	if  err != nil {
		fmt.Printf("create meta partition failed:%s", err.Error())
		return nil, err
	}
	mp = tmp.(*metaPartition)
	mp.raftPartition = raft
	return
}

func newDefaultMpConfig(pid, nodeId, start, end uint64, storeMode proto.StoreMode)(conf *MetaPartitionConfig) {
	conf = &MetaPartitionConfig{ PartitionId: pid,
		NodeId: nodeId,
		Start: start,
		End: math.MaxUint64 - 100,
		Peers: []proto.Peer{proto.Peer{ID: 1, Addr: "127.0.0.1"}, {ID: 2, Addr: "127.0.0.2"} },
		RootDir: "./partition_" +strconv.Itoa(int(pid)),
		StoreMode: storeMode,
		Cursor: math.MaxUint64 - 100000,
	}
	return
}

func createTestMetaPartitionWithApplyError() (*metaPartition, *metaPartition, error) {
	node := &MetaNode{nodeId: 1}
	manager := &metadataManager{nodeId: 1, metaNode: node, rocksDBDirs: []string{"./"}}
	memMpConf := newDefaultMpConfig(1, 1, 1, 1000, proto.StoreModeMem)
	rockMpConf := newDefaultMpConfig(2, 2, 1, 1000, proto.StoreModeRocksDb)
	raft := metamock.NewMockPartition(1)
	memMp, err := newMetapartitionForTest(raft, memMpConf, manager)
	if err != nil {
		return nil, nil, err
	}

	rockMp, err := newMetapartitionForTest(raft, rockMpConf, manager)

	raft.Mp = append(raft.Mp, memMp)
	raft.Mp = append(raft.Mp, rockMp)
	raft.Apply = ApplyMockError
	return memMp, rockMp, nil
}

func ApplyMockError(elem interface{},command []byte, index uint64) (resp interface{}, err error) {
	err = fmt.Errorf("apply mock error")
	return
}

func TestMetaPartition_CreateDentryWithSubmitErrorTest(t *testing.T) {
	memModeTestMp, rocksModeTestMp, err := createTestMetaPartitionWithApplyError()
	if err != nil {
		t.Logf("create mp failed:%s", err.Error())
		return
	}
	defer func() {
		if memModeTestMp != nil {
			releaseMetaPartition(memModeTestMp)
		}
		if rocksModeTestMp != nil {
			releaseMetaPartition(rocksModeTestMp)
		}
	}()
	_ = memModeTestMp.CursorReset(context.Background(), &proto.CursorResetRequest{
		PartitionId: 1,
		NewCursor:   10000,
		Force:       true,
	})
	defer func() {
		_ = memModeTestMp.CursorReset(context.Background(), &proto.CursorResetRequest{
			PartitionId: 1,
			NewCursor:   1,
			Cursor:      1,
			Force:       true,
		})
	}()
	t.Logf("cursor:%d\n" ,memModeTestMp.config.Cursor)

	if _, _, err = inodeCreate(memModeTestMp.inodeTree, NewInode(1, uint32(os.ModeDir)), true); err != nil {
		t.Errorf("inode create failed:%v", err)
		return
	}

	//create dentrys with parentId is equal to inode
	req := &proto.CreateDentryRequest{
		ParentID: 1,
		Name:     "test",
		Inode:    1000,
		Mode:     470,
	}
	packet := &Packet{}
	err = memModeTestMp.CreateDentry(req, packet)
	if packet.ResultCode != proto.OpAgain {
		t.Errorf("error:%v\n", err)
		t.Errorf("create dentry expect result code is again, but actual is %v", packet.ResultCode)
		return
	}
	return
}
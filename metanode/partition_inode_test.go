package metanode

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/metanode/metamock"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"math"
	"os"
	"path"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func mockMetaPartitionReplica(nodeID, partitionID uint64, storeMode proto.StoreMode, rootDir string) *metaPartition {
	partitionDir := path.Join(rootDir, partitionPrefix + strconv.Itoa(int(partitionID)))
	os.MkdirAll(partitionDir, 0666)
	node := &MetaNode{
		nodeId: nodeID,
	}
	manager := &metadataManager{
		nodeId: 1,
		metaNode: node,
		rocksDBDirs: []string{rootDir},
	}

	config := &MetaPartitionConfig{
		PartitionId: partitionID,
		NodeId:      nodeID,
		Start:       0,
		End:         math.MaxUint64 - 100,
		Peers:       []proto.Peer{proto.Peer{ID: 1, Addr: "127.0.0.1"}, {ID: 2, Addr: "127.0.0.2"}},
		RootDir:     partitionDir,
		StoreMode:   storeMode,
		Cursor:      math.MaxUint64 - 100000,
		RocksDBDir:  partitionDir,
	}

	mp, err := CreateMetaPartition(config, manager)
	if  err != nil {
		fmt.Printf("create meta partition failed:%s", err.Error())
		return nil
	}
	return mp.(*metaPartition)
}

func mockMp(t *testing.T, dir string, leaderStoreMode proto.StoreMode) (leader, follower *metaPartition){
	leaderRootDir := path.Join("./leader", dir)
	os.RemoveAll(leaderRootDir)
	if leader = mockMetaPartitionReplica(1, 1,  leaderStoreMode, leaderRootDir); leader == nil {
		t.Errorf("mock metapartition failed")
		return
	}

	followerRootDir := path.Join("./follower", dir)
	os.RemoveAll(followerRootDir)
	if follower = mockMetaPartitionReplica(1, 1,
		(proto.StoreModeMem | proto.StoreModeRocksDb) - leaderStoreMode, followerRootDir); follower == nil {
		t.Errorf("mock metapartition failed")
		return
	}

	raftPartition := metamock.NewMockPartition(1)
	raftPartition.Apply = ApplyMock
	raftPartition.Mp = append(raftPartition.Mp, leader)
	raftPartition.Mp = append(raftPartition.Mp, follower)

	leader.raftPartition = raftPartition
	follower.raftPartition = raftPartition
	return
}

func releaseMp(leader, follower *metaPartition, dir string) {
	leader.db.CloseDb()
	follower.db.CloseDb()
	leader.db.ReleaseRocksDb()
	follower.db.ReleaseRocksDb()
	os.RemoveAll("./leader")
	os.RemoveAll("./follower")
}

func CreateInodeInterTest(t *testing.T, leader, follower *metaPartition, start uint64) {
	reqCreateInode := &proto.CreateInodeRequest{
		Gid: 0,
		Uid: 0,
		Mode: 470,
	}
	resp := &Packet{}
	var err error
	cursor := leader.config.Cursor
	defer func() {
		leader.config.Cursor = cursor
	}()
	leader.config.Cursor = start

	for i := 0; i < 100; i++ {
		err = leader.CreateInode(reqCreateInode, resp)
		if err != nil {
			t.Errorf("create inode failed:%s", err.Error())
			return
		}
	}
	if leader.inodeTree.Count() != follower.inodeTree.Count() {
		t.Errorf("create inode failed, rocks mem not same, mem:%d, rocks:%d", leader.inodeTree.Count(), follower.inodeTree.Count())
		return
	}
	t.Logf("create 100 inodes success")

	cursor = leader.config.Cursor
	leader.config.Cursor = leader.config.End
	err = leader.CreateInode(reqCreateInode, resp)
	if err == nil {
		t.Errorf("cursor reach end failed")
		return
	}
	t.Logf("cursor reach end test  success:%s, result:%d, %s", err.Error(), resp.ResultCode, resp.GetResultMsg())

	leader.config.Cursor = 10 + start

	inode, _ := leader.inodeTree.Get(10)
	if inode == nil {
		t.Errorf("get inode 10 failed, err:%s", err.Error())
		return
	}
	err = leader.CreateInode(reqCreateInode, resp)
	if resp.ResultCode ==  proto.OpOk {
		t.Errorf("same inode create failed")
		return
	}
	t.Logf("same inode create success:%v, resuclt code:%d, %s", err, resp.ResultCode, resp.GetResultMsg())
}

func TestMetaPartition_CreateInodeCase01(t *testing.T) {
	//leader is mem mode
	dir := "create_inode_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	CreateInodeInterTest(t, leader, follower, 0)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	CreateInodeInterTest(t, leader, follower, 0)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_CreateInodeNewCase01(t *testing.T) {
	tests := []struct{
		name      string
		storeMode proto.StoreMode
		rootDir   string
		applyFunc metamock.ApplyFunc
	}{
		{
			name:      "MemMode",
			storeMode: proto.StoreModeMem,
			rootDir:   "./test_mem_inode_create_01",
			applyFunc: ApplyMock,
		},
		{
			name:      "RocksDBMode",
			storeMode: proto.StoreModeRocksDb,
			rootDir:   "./test_rocksdb_inode_create_01",
			applyFunc: ApplyMock,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp, err := mockMetaPartition(1, 1, test.storeMode, test.rootDir, test.applyFunc)
			if err != nil {
				return
			}
			defer releaseMetaPartition(mp)
			reqCreateInode := &proto.CreateInodeRequest{
				Gid: 0,
				Uid: 0,
				Mode: 470,
			}
			resp := &Packet{}
			mp.config.Cursor = 0

			for i := 0; i < 100; i++ {
				err = mp.CreateInode(reqCreateInode, resp)
				if err != nil {
					t.Errorf("create inode failed:%s", err.Error())
					return
				}
			}
			if mp.inodeTree.Count() != 100 {
				t.Errorf("create inode failed, rocks mem not same, expect:100, actual:%d", mp.inodeTree.Count())
				return
			}
			t.Logf("create 100 inodes success")

			mp.config.Cursor = mp.config.End
			err = mp.CreateInode(reqCreateInode, resp)
			if err == nil {
				t.Errorf("cursor reach end failed")
				return
			}
			t.Logf("cursor reach end test  success:%s, result:%d, %s", err.Error(), resp.ResultCode, resp.GetResultMsg())

			mp.config.Cursor = 10
			inode, _ := mp.inodeTree.Get(10)
			if inode == nil {
				t.Errorf("get inode 10 failed, err:%s", err.Error())
				return
			}
			err = mp.CreateInode(reqCreateInode, resp)
			if resp.ResultCode ==  proto.OpOk {
				t.Errorf("same inode create failed")
				return
			}
			t.Logf("same inode create success:%v, resuclt code:%d, %s", err, resp.ResultCode, resp.GetResultMsg())
		})
	}
}

func UnlinkInodeInterTest(t *testing.T, leader, follower *metaPartition, start uint64) {
	cursor := atomic.LoadUint64(&leader.config.Cursor)
	defer func() {
		atomic.StoreUint64(&leader.config.Cursor, cursor)
	}()

	reqCreateInode := &proto.CreateInodeRequest{
		Gid: 0,
		Uid: 0,
		Mode: 470,
	}

	reqUnlinkInode := &proto.UnlinkInodeRequest{
		Inode: 10 + cursor,
	}
	resp := &Packet{}
	var err error

	for i := 0; i < 100; i++ {
		err = leader.CreateInode(reqCreateInode, resp)
		if err != nil {
			t.Errorf("create inode failed:%s", err.Error())
			return
		}
	}

	if leader.inodeTree.Count() != follower.inodeTree.Count() {
		t.Errorf("create inode failed, rocks mem not same, mem:%d, rocks:%d", leader.inodeTree.Count(), follower.inodeTree.Count())
		return
	}
	t.Logf("create 100 inodes success")

	inode, _ := leader.inodeTree.Get(10 + cursor)
	if inode == nil {
		t.Errorf("get inode (%v) failed, inode is null", start + 10 + leader.config.Cursor)
		return
	}
	inode.Type = uint32(os.ModeDir)
	inode.NLink = 3
	if os.FileMode(inode.Type).IsDir() {
		t.Logf("inode is dir")
	}
	_ = inodePut(leader.inodeTree, inode)

	inode, _ = follower.inodeTree.Get(10 + cursor)
	if inode == nil {
		t.Errorf("get inode (%v) failed, inode is null", start + 10 + leader.config.Cursor)
		return
	}
	inode.Type = uint32(os.ModeDir)
	inode.NLink = 3
	_ = inodePut(follower.inodeTree, inode)

	err = leader.UnlinkInode(reqUnlinkInode, resp)
	if resp.ResultCode !=  proto.OpOk {
		t.Errorf("unlink inode test failed:%v, resuclt code:%d, %s", err, resp.ResultCode, resp.GetResultMsg())
		return
	}

	err = leader.UnlinkInode(reqUnlinkInode, resp)
	if resp.ResultCode !=  proto.OpOk {
		t.Errorf("unlink inode test failed:%v, resuclt code:%d, %s", err, resp.ResultCode, resp.GetResultMsg())
		return
	}

	err = leader.UnlinkInode(reqUnlinkInode, resp)
	if resp.ResultCode ==  proto.OpOk {
		t.Errorf("same inode create failed, inode link:%d", inode.NLink)
		return
	}
	t.Logf("unlink inode test success:%v, resuclt code:%d, %s", err, resp.ResultCode, resp.GetResultMsg())

	inode, _ = leader.inodeTree.Get(11 + cursor)
	if inode == nil {
		t.Errorf("get inode (%v) failed, inode is null", start + 11 + leader.config.Cursor)
		return
	}
	inode.SetDeleteMark()
	_ = inodePut(leader.inodeTree, inode)

	inode, _ = follower.inodeTree.Get(11 + cursor)
	if inode == nil {
		t.Errorf("get inode (%v) failed, inode is null", start + 11 + leader.config.Cursor)
		return
	}
	inode.SetDeleteMark()
	_ = inodePut(follower.inodeTree, inode)

	reqUnlinkInode.Inode = 11 + cursor
	err = leader.UnlinkInode(reqUnlinkInode, resp)
	if resp.ResultCode ==  proto.OpOk {
		t.Errorf("same inode create failed, inode link:%d", inode.NLink)
		return
	}
	t.Logf("unlink inode test success:%v, resuclt code:%d, %s", err, resp.ResultCode, resp.GetResultMsg())
}

func TestMetaPartition_UnlinkInodeCase01(t *testing.T) {
	//leader is mem mode
	dir := "unlink_inode_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	UnlinkInodeInterTest(t, leader, follower, 0)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	UnlinkInodeInterTest(t, leader, follower, 0)
	releaseMp(leader, follower, dir)
}

func createInodesForTest(leader, follower *metaPartition, inodeCnt int, mode, uid, gid uint32) (inos []uint64, err error) {
	reqCreateInode := &proto.CreateInodeRequest{
		Gid: gid,
		Uid: uid,
		Mode: mode,
	}

	inos = make([]uint64, 0, inodeCnt)
	for index := 0; index < inodeCnt; index++ {
		packet := &Packet{}
		err = leader.CreateInode(reqCreateInode, packet)
		if err != nil {
			err = fmt.Errorf("create inode failed:%s", err.Error())
			return
		}
		resp := &proto.CreateInodeResponse{}
		if err = packet.UnmarshalData(resp); err != nil {
			err = fmt.Errorf("unmarshal create inode response failed:%v", err)
			return
		}
		inos = append(inos, resp.Info.Inode)
	}

	//validate count and inode info
	if leader.inodeTree.Count() != follower.inodeTree.Count(){
		err = fmt.Errorf("create inode failed, leader and follower inode count not same, or mismatch expect," +
			" mem:%d, rocks:%d, expect:%v", leader.inodeTree.Count(), follower.inodeTree.Count(), inodeCnt)
		return
	}

	for _, ino := range inos {
		inodeFromLeader, _ := leader.inodeTree.Get(ino)
		inodeFromFollower, _ := follower.inodeTree.Get(ino)
		if inodeFromLeader == nil || inodeFromFollower == nil {
			err = fmt.Errorf("get inode result not same, leader:%s, follower:%s", inodeFromLeader.String(), inodeFromFollower.String())
			return
		}
		if !reflect.DeepEqual(inodeFromFollower, inodeFromFollower) {
			err = fmt.Errorf("inode info in leader is not equal to follower, leader:%s, follower:%s", inodeFromLeader.String(), inodeFromFollower.String())
			return
		}
	}
	return
}

func BatchInodeUnlinkInterTest(t *testing.T, leader, follower *metaPartition) {
	var (
		inos []uint64
		err  error
	)
	defer func() {
		for _, ino := range inos {
			req := &proto.DeleteInodeRequest{
				Inode: ino,
			}
			packet := &Packet{}
			leader.DeleteInode(req, packet)
		}
		if leader.inodeTree.Count() != follower.inodeTree.Count() || leader.inodeTree.Count() != 0 {
			t.Errorf("inode count must be zero after delete, but result is not expect, count[leader:%v, follower:%v]", leader.inodeTree.Count(), follower.inodeTree.Count())
			return
		}
	}()
	if inos, err = createInodesForTest(leader, follower, 100, uint32(os.ModeDir), 0, 0); err != nil || len(inos) != 100 {
		t.Fatal(err)
		return
	}
	testInos := inos[20:40]
	for _, ino := range testInos {
		//create nlink for dir
		req := &proto.LinkInodeRequest{
			Inode: ino,
		}
		packet := &Packet{}
		if err = leader.CreateInodeLink(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			t.Errorf("create inode link failed, err:%v, resultCode:%v", err, packet.ResultCode)
			return
		}
	}

	req := &proto.BatchUnlinkInodeRequest{
		Inodes: testInos,
	}
	packet := &Packet{}
	//unlink to empty dir
	if err = leader.UnlinkInodeBatch(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch unlink inode failed, [err:%v, packet result code:%v]", err, packet.ResultCode)
		return
	}

	//unlink empty dir, inode will be delete
	packet = &Packet{}
	if err = leader.UnlinkInodeBatch(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch unlink inode failed, [err:%v, packet result code:%v]", err, packet.ResultCode)
		return
	}

	for _, ino := range testInos {
		inodeInMem, _ := leader.inodeTree.Get(ino)
		if inodeInMem != nil {
			t.Errorf("test failed, inode get from mem mode mp error, [expect:inode is null, actual:%s]", inodeInMem.String())
			return
		}

		inodeInRocks, _ := follower.inodeTree.Get(ino)
		if inodeInRocks != nil {
			t.Errorf("test failed, inode get from rocks mode mp error, [expect:inode is null, actual:%s]", inodeInRocks.String())
			return
		}
	}
	return
}
//todo:test unlink batch when batchInodes include same inodeID
func TestMetaPartition_UnlinkInodeBatch01(t *testing.T) {
	//leader is mem mode
	dir := "unlink_inode_batch_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	BatchInodeUnlinkInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	BatchInodeUnlinkInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_UnlinkInodeBatchCase02(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_batch_unlink_inode", ApplyMock)
	if mp == nil {
		t.Logf("mock metapartition failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)

	_, _, err = inodeCreate(mp.inodeTree, NewInode(1, uint32(os.ModeDir)), true)
	if err != nil {
		t.Logf("create inode failed:%v", err)
		t.FailNow()
	}

	var inode *Inode
	if inode, err = mp.inodeTree.Get(1); err != nil {
		t.Logf("get exist inode:%v failed:%v", inode, err)
		t.FailNow()
	}
	//inc nlink
	for index := 0; index < 3; index++ {
		inode.IncNLink()
	}

	if err = inodePut(mp.inodeTree, inode); err != nil {
		t.Logf("update inode nlink failed:%v", err)
		t.FailNow()
	}

	testInos := []uint64{1, 1, 1}

	req := &proto.BatchUnlinkInodeRequest{
		Inodes: testInos,
	}
	packet := &Packet{}
	//unlink to empty dir
	if err = mp.UnlinkInodeBatch(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch unlink inode failed, [err:%v, packet result code:%v]", err, packet.ResultCode)
		return
	}

	if inode, err = mp.inodeTree.Get(1); err != nil {
		t.Logf("get exist inode:%v failed:%v", inode, err)
		t.FailNow()
	}

	if inode.NLink != 2 {
		t.Logf("test batch nlink inode failed, expect nlink:2, actual:%v", inode.NLink)
		t.FailNow()
	}

	req = &proto.BatchUnlinkInodeRequest{
		Inodes: []uint64{1, 1},
	}
	//unlink empty dir
	if err = mp.UnlinkInodeBatch(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch unlink inode failed, [err:%v, packet result code:%v]", err, packet.ResultCode)
		return
	}

	if inode, err = mp.inodeTree.Get(1); err != nil {
		t.Logf("get inode failed:%v", err)
		t.FailNow()
	}
	if inode != nil {
		t.Logf("batch uinlink inode test failed, unlink empty dir result expect:get nil, actual:%v", inode)
		t.FailNow()
	}
}

func TestMetaPartition_UnlinkInodeBatchCase03(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_batch_unlink_inode", ApplyMock)
	if mp == nil {
		t.Logf("mock metapartition failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)

	_, _, err = inodeCreate(mp.inodeTree, NewInode(100, 470), true)
	if err != nil {
		t.Logf("create inode failed:%v", err)
		t.FailNow()
	}

	var inode *Inode
	if inode, err = mp.inodeTree.Get(100); err != nil {
		t.Logf("get exist inode:%v failed:%v", inode, err)
		t.FailNow()
	}
	//inc nlink
	for index := 0; index < 3; index++ {
		inode.IncNLink()
	}

	if err = inodePut(mp.inodeTree, inode); err != nil {
		t.Logf("update inode nlink failed:%v", err)
		t.FailNow()
	}

	testInos := []uint64{100, 100, 100}

	req := &proto.BatchUnlinkInodeRequest{
		Inodes: testInos,
	}
	packet := &Packet{}

	if err = mp.UnlinkInodeBatch(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch unlink inode failed, [err:%v, packet result code:%v]", err, packet.ResultCode)
		return
	}

	if inode, err = mp.inodeTree.Get(100); err != nil {
		t.Logf("get exist inode:%v failed:%v", inode, err)
		t.FailNow()
	}

	if inode.NLink != 1 {
		t.Logf("test batch nlink inode failed, expect nlink:2, actual:%v", inode.NLink)
		t.FailNow()
	}

	req = &proto.BatchUnlinkInodeRequest{
		Inodes: []uint64{100, 100},
	}

	if err = mp.UnlinkInodeBatch(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch unlink inode failed, [err:%v, packet result code:%v]", err, packet.ResultCode)
		return
	}

	if inode, err = mp.inodeTree.Get(100); err != nil {
		t.Logf("get inode failed:%v", err)
		t.FailNow()
	}
	if inode == nil {
		t.Logf("batch uinlink inode test failed, unlink file result error, expect exist, actual not exist")
		t.FailNow()
	}

	if inode.NLink != 0 {
		t.Logf("nlink mismatch, expect:0, actual:%v", inode.NLink)
		t.FailNow()
	}
}

func InodeGetInterGet(t *testing.T, leader, follower *metaPartition) {
	//create inode
	ino, err := createInode(470, 0, 0, leader)
	req := &proto.InodeGetRequest{
		Inode: ino,
	}
	packet := &Packet{}
	if err = leader.InodeGet(req, packet, proto.OpInodeGetVersion1); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("get exist inode from leader failed, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}

	packet = &Packet{}
	if err = follower.InodeGet(req, packet, proto.OpInodeGetVersion1); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("get exist inode from follower failed, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}

	//get not exist inode
	req = &proto.InodeGetRequest{
		Inode: ino - 1,
	}
	packet = &Packet{}
	if err = leader.InodeGet(req, packet, proto.OpInodeGetVersion1); err == nil || packet.ResultCode != proto.OpNotExistErr {
		t.Errorf("get not exist inode from leader success, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}

	packet = &Packet{}
	if err = follower.InodeGet(req, packet, proto.OpInodeGetVersion1); err == nil || packet.ResultCode != proto.OpNotExistErr {
		t.Errorf("get not exist inode from follower success, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}
	return
}

func TestMetaPartition_InodeGetCase01(t *testing.T) {
	//leader is mem mode
	dir := "inode_get_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	InodeGetInterGet(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	InodeGetInterGet(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func BatchInodeGetInterTest(t *testing.T, leader, follower *metaPartition) {
	var (
		inos []uint64
		err  error
	)
	defer func() {
		for _, ino := range inos {
			req := &proto.DeleteInodeRequest{
				Inode: ino,
			}
			packet := &Packet{}
			leader.DeleteInode(req, packet)
		}
		if leader.inodeTree.Count() != follower.inodeTree.Count() || leader.inodeTree.Count() != 0 {
			t.Errorf("inode count must be zero after delete, but result is not expect, count[leader:%v, follower:%v]", leader.inodeTree.Count(), follower.inodeTree.Count())
			return
		}
	}()
	if inos, err = createInodesForTest(leader, follower, 100, 470, 0, 0); err != nil || len(inos) != 100{
		t.Fatal(err)
		return
	}

	testIno := inos[20:50]

	req := &proto.BatchInodeGetRequest{
		Inodes: testIno,
	}
	packet := &Packet{}
	if err = leader.InodeGetBatch(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch get inode from leader failed, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}
	resp := &proto.BatchInodeGetResponse{}
	if err = packet.UnmarshalData(resp); err != nil {
		t.Errorf("unmarshal batch inode get response failed:%v", err)
		return
	}
	if len(resp.Infos) != len(testIno) {
		t.Fatalf("get inode count not equla to expect, [expect:%v, actual:%v]", len(testIno), len(resp.Infos))
		return
	}

	packet = &Packet{}
	if err = follower.InodeGetBatch(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch get inode from follower failed, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}
	resp = &proto.BatchInodeGetResponse{}
	if err = packet.UnmarshalData(resp); err != nil {
		t.Errorf("unmarshal batch inode get response failed:%v", err)
		return
	}
	if len(resp.Infos) != len(testIno) {
		t.Errorf("get inode count not equla to expect, [expect:%v, actual:%v]", len(testIno), len(resp.Infos))
		return
	}
	return
}

func TestMetaPartition_BatchInodeGetCase01(t *testing.T) {
	//leader is mem mode
	dir := "batch_inode_get_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	BatchInodeGetInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	BatchInodeGetInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func CreateInodeLinkInterTest(t *testing.T, leader, follower *metaPartition) {
	ino, err := createInode(470, 0, 0, leader)
	if err != nil {
		t.Fatal(err)
		return
	}
	req := &proto.LinkInodeRequest{
		Inode: ino,
	}
	packet := &Packet{}
	if err = leader.CreateInodeLink(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("create inode link failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}

	//validate
	var inode *Inode
	if inode, _ = leader.inodeTree.Get(ino); inode == nil {
		t.Errorf("get exist inode failed, inode is nil")
		return
	}
	if inode.NLink != 2 {
		t.Errorf("inode nlink is error, [expect:2, actual:%v]", inode.NLink)
		return
	}

	if inode, _ = follower.inodeTree.Get(ino); inode == nil {
		t.Errorf("get exist inode failed, inode is nil")
		return
	}
	if inode.NLink != 2 {
		t.Errorf("inode nlink is error, [expect:2, actual:%v]", inode.NLink)
		return
	}

	//create inode link for not exist inode
	req = &proto.LinkInodeRequest{
		Inode: math.MaxUint64 - 10,
	}
	packet = &Packet{}
	if _ = leader.CreateInodeLink(req, packet); packet.ResultCode != proto.OpInodeOutOfRange {
		t.Errorf("create inode link for not exist inode failed, expect result code is OpInodeOutOfRange, " +
			"but actual result is:0x%X", packet.ResultCode)
		return
	}

	//create inode link for mark delete inode
	inode.SetDeleteMark()
	_ = inodePut(leader.inodeTree, inode)
	_ = inodePut(follower.inodeTree, inode)
	req = &proto.LinkInodeRequest{
		Inode: ino,
	}
	packet = &Packet{}
	if _ = leader.CreateInodeLink(req, packet); packet.ResultCode != proto.OpNotExistErr {
		t.Errorf("create inode link for mark delete inode failed, expect result code is OpNotExistErr, " +
			"but actual result is:0x%X", packet.ResultCode)
		return
	}
	return
}

func TestMetaPartition_CreateInodeLinkCase01(t *testing.T) {
	//leader is mem mode
	dir := "create_inode_link_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	CreateInodeLinkInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	CreateInodeLinkInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

//simulate remove file
func EvictFileInodeInterTest(t *testing.T, leader, follower *metaPartition) {
	//create inode
	ino, err := createInode(470, 0, 0, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	//unlink inode
	reqUnlinkInode := &proto.UnlinkInodeRequest{
		Inode: ino,
	}
	packet := &Packet{}
	if err = leader.UnlinkInode(reqUnlinkInode, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("unlink inode failed, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}
	//evict inode
	reqEvictInode := &proto.EvictInodeRequest{
		Inode: ino,
	}
	packet = &Packet{}
	if err = leader.EvictInode(reqEvictInode, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("evict inode failed, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}

	//validate
	var inode *Inode
	inode, _ = leader.inodeTree.Get(ino)
	if inode == nil {
		t.Fatalf("get exist inode failed")
		return
	}
	if inode.NLink != 0 {
		t.Fatalf("inode nlink mismatch, expect:0, actual:%v", inode.NLink)
		return
	}

	if !inode.ShouldDelete() {
		t.Fatalf("delete flag mismatch, inode should be marked delete, but it is not")
		return
	}

	inode, _ = follower.inodeTree.Get(ino)
	if inode == nil {
		t.Errorf("get exist inode failed")
		return
	}
	if inode.NLink != 0 {
		t.Errorf("test failed, error nlink, expect:0, actual:%v", inode.NLink)
		return
	}
	if !inode.ShouldDelete() {
		t.Errorf("test failed, inode should mark delete, but it is not")
		return
	}

	//evict mark delete inode, response is ok
	packet = &Packet{}
	if err = leader.EvictInode(reqEvictInode, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("evict inode failed, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}

	//evict not exist inode, response result code is not exist error
	reqEvictInode = &proto.EvictInodeRequest{
		Inode: math.MaxUint64 - 10,
	}
	packet = &Packet{}
	if err = leader.EvictInode(reqEvictInode, packet); packet.ResultCode != proto.OpInodeOutOfRange {
		t.Errorf("test failed, evict not exist inode, [err:%v, resultCode:0x%X]", err, packet.ResultCode)
		return
	}
	return
}

func EvictDirInodeInterTest(t *testing.T, leader, follower *metaPartition) {
	//create inode
	ino, err := createInode(uint32(os.ModeDir), 0, 0, leader)
	if err != nil {
		t.Fatal(err)
	}

	var inodeInLeaderMP, inodeInFollowerMP *Inode
	inodeInLeaderMP, _ = leader.inodeTree.Get(ino)
	inodeInFollowerMP, _ = follower.inodeTree.Get(ino)
	if !reflect.DeepEqual(inodeInLeaderMP, inodeInFollowerMP) {
		t.Errorf("inode info in mem is not equal to rocks, mem:%s, rocks:%s", inodeInLeaderMP.String(), inodeInFollowerMP.String())
		return
	}

	req := &proto.LinkInodeRequest{
		Inode: ino,
	}
	packet := &Packet{}
	if err = leader.CreateInodeLink(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("create inode link failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}

	//evict inode
	reqEvictInode := &proto.EvictInodeRequest{
		Inode: ino,
	}
	packet = &Packet{}
	if err = leader.EvictInode(reqEvictInode, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("evict inode failed, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}

	var inode *Inode
	inode, _ = leader.inodeTree.Get(ino)
	if inode == nil {
		t.Errorf("get exist inode failed")
		return
	}
	if inode.NLink != 3 {
		t.Errorf("test failed, error nlink, expect:0, actual:%v", inode.NLink)
		return
	}
	if inode.ShouldDelete() {
		t.Errorf("test failed, inode be set delete mark, error")
		return
	}

	inode, _ = follower.inodeTree.Get(ino)
	if inode == nil {
		t.Errorf("get exist inode failed")
		return
	}
	if inode.NLink != 3 {
		t.Errorf("test failed, error nlink, expect:0, actual:%v", inode.NLink)
		return
	}
	if inode.ShouldDelete() {
		t.Errorf("test failed, inode be set delete mark, error")
		return
	}

	//unlink to empty
	reqUnlinkInode := &proto.UnlinkInodeRequest{
		Inode: ino,
	}
	packet = &Packet{}
	if err = leader.UnlinkInode(reqUnlinkInode, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("unlink inode failed, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}

	packet = &Packet{}
	if err = leader.EvictInode(reqEvictInode, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("evict inode failed, [err:%v, resultCode:%v]", err, packet.ResultCode)
		return
	}

	inode, _ = leader.inodeTree.Get(ino)
	if inode == nil {
		t.Errorf("get exist inode failed")
		return
	}
	if inode.NLink != 2 {
		t.Errorf("test failed, error nlink, expect:0, actual:%v", inode.NLink)
		return
	}
	if !inode.ShouldDelete() {
		t.Errorf("test failed, inode should mark delete, but it is not")
		return
	}

	inode, _ = follower.inodeTree.Get(ino)
	if inode == nil {
		t.Errorf("get exist inode failed")
		return
	}
	if inode.NLink != 2 {
		t.Errorf("test failed, error nlink, expect:0, actual:%v", inode.NLink)
		return
	}
	if !inode.ShouldDelete() {
		t.Errorf("test failed, inode should mark delete, but it is not")
		return
	}
}

func TestMetaPartition_EvictInodeCase01(t *testing.T) {
	//leader is mem mode
	dir := "evict_inode_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	EvictFileInodeInterTest(t, leader, follower)
	EvictDirInodeInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	EvictFileInodeInterTest(t, leader, follower)
	EvictDirInodeInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func EvictBatchInodeInterTest(t *testing.T, leader, follower *metaPartition) {
	var (
		inos []uint64
		err  error
	)
	defer func() {
		for _, ino := range inos {
			req := &proto.DeleteInodeRequest{
				Inode: ino,
			}
			packet := &Packet{}
			leader.DeleteInode(req, packet)
		}
		if leader.inodeTree.Count() != follower.inodeTree.Count() || leader.inodeTree.Count() != 0 {
			t.Errorf("inode count must be zero after delete, but result is not expect, count[leader:%v, follower:%v]", leader.inodeTree.Count(), follower.inodeTree.Count())
			return
		}
	}()
	if inos, err = createInodesForTest(leader, follower, 100, 470, 0, 0); err != nil || len(inos) != 100{
		t.Fatal(err)
		return
	}

	testIno := inos[20:50]
	//batch unlink
	reqBatchUnlink := &proto.BatchUnlinkInodeRequest{
		Inodes: testIno,
	}
	packet := &Packet{}
	if err = leader.UnlinkInodeBatch(reqBatchUnlink, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch unlink inode failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}

	//batch evict
	reqBatchEvict := &proto.BatchEvictInodeRequest{
		Inodes: testIno,
	}
	packet = &Packet{}
	if err = leader.EvictInodeBatch(reqBatchEvict, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch evict inode failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}

	//validate
	for _, ino := range testIno {
		inodeInLeader, _ := leader.inodeTree.Get(ino)
		if inodeInLeader == nil {
			t.Errorf("get inode is null")
			return
		}
		if inodeInLeader.NLink != 0 {
			t.Errorf("error nlink number, expect:0, actual:%v", inodeInLeader.NLink)
			return
		}
		if !inodeInLeader.ShouldDelete() {
			t.Errorf("inode should mark delete, but it is not")
			return
		}

		inodeInFollower, _ := follower.inodeTree.Get(ino)
		if inodeInFollower == nil {
			t.Errorf("get inode is null")
			return
		}
		if inodeInFollower.NLink != 0 {
			t.Errorf("error nlink number, expect:0, actual:%v", inodeInFollower.NLink)
			return
		}
		if !inodeInLeader.ShouldDelete() {
			t.Errorf("inode should mark delete, but it is not")
			return
		}
	}
	return
}

//todo:test
func TestMetaPartition_BatchEvictInodeCase01(t *testing.T) {
	//leader is mem mode
	dir := "batch_evict_inode_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	EvictBatchInodeInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	EvictBatchInodeInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func SetAttrInterTest(t *testing.T, leader, follower *metaPartition) {
	var (
		err     error
		reqData []byte
	)
	ino, err := createInode(uint32(os.ModeDir), 0, 0, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	modifyTime := time.Now().Unix()
	accessTime := time.Now().Unix()
	req := &proto.SetAttrRequest{
		Inode: ino,
		Mode: uint32(os.ModeDir),
		Uid: 7,
		Gid: 8,
		ModifyTime: modifyTime,
		AccessTime: accessTime,
		Valid: 31,
	}
	reqData, err = json.Marshal(req)
	if err != nil {
		t.Errorf("marshal set attr request failed, err:%v", err)
		return
	}
	packet := &Packet{}
	if err = leader.SetAttr(reqData, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("set attr failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}

	var inodeInLeader, inodeInFollower *Inode
	inodeInLeader, _ = leader.inodeTree.Get(ino)
	inodeInFollower, _ = follower.inodeTree.Get(ino)
	if inodeInLeader == nil || inodeInFollower == nil {
		t.Errorf("get exist inode failed, leader:%v, follower:%v", inodeInLeader, inodeInFollower)
		return
	}
	if !reflect.DeepEqual(inodeInLeader, inodeInFollower) {
		t.Errorf("inode info in mem is not equal to rocks, mem:%s, rocks:%s", inodeInFollower.String(), inodeInFollower.String())
		return
	}
	if !proto.IsDir(inodeInLeader.Type) {
		t.Errorf("test failed, expect type is directory, but is %v", inodeInLeader.Type)
		return
	}
	if inodeInLeader.Uid != 7 {
		t.Errorf("test failed, inode uid expect type is 7, but actual is %v", inodeInLeader.Uid)
		return
	}
	if inodeInLeader.Gid != 8 {
		t.Errorf("test failed, inode gid expect type is 8, but actual is %v", inodeInLeader.Gid)
		return
	}
	if inodeInLeader.AccessTime != accessTime {
		t.Errorf("test failed, inode access time expect type is %v, but actual is %v", accessTime, inodeInLeader.AccessTime)
		return
	}
	if inodeInLeader.ModifyTime != modifyTime {
		t.Errorf("test failed, inode modify time expect type is %v, but actual is %v", modifyTime, inodeInLeader.ModifyTime)
		return
	}
	return
}

func TestMetaPartition_SetAttrCase01(t *testing.T) {
	//leader is mem mode
	dir := "set_attr_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	SetAttrInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	SetAttrInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func DeleteInodeInterTest(t *testing.T, leader, follower *metaPartition) {
	ino, err := createInode(uint32(os.ModeDir), 0, 0, leader)
	if err != nil {
		t.Fatal(err)
		return
	}

	req := &proto.DeleteInodeRequest{
		Inode: ino,
	}
	packet := &Packet{}
	if err = leader.DeleteInode(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("delete inode failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}

	inodeInLeader, _ := leader.inodeTree.Get(ino)
	inodeInFollower, _ := follower.inodeTree.Get(ino)
	if inodeInLeader != nil || inodeInFollower != nil {
		t.Errorf("inode get result expcet is nil, but actual is [leader:%v, follower:%v]", inodeInLeader, inodeInFollower)
		return
	}
}

func TestMetaPartition_DeleteInodeCase01(t *testing.T) {
	//leader is mem mode
	dir := "delete_inode_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	DeleteInodeInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	DeleteInodeInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func BatchDeleteInodeInterTest(t *testing.T, leader, follower *metaPartition) {
	var (
		inos []uint64
		err  error
	)
	defer func() {
		for _, ino := range inos {
			req := &proto.DeleteInodeRequest{
				Inode: ino,
			}
			packet := &Packet{}
			leader.DeleteInode(req, packet)
		}
		if leader.inodeTree.Count() != follower.inodeTree.Count() || leader.inodeTree.Count() != 0 {
			t.Errorf("inode count must be zero after delete, but result is not expect, count[leader:%v, follower:%v]", leader.inodeTree.Count(), follower.inodeTree.Count())
			return
		}
	}()
	if inos, err = createInodesForTest(leader, follower, 100, 470, 0, 0); err != nil || len(inos) != 100{
		t.Fatal(err)
		return
	}

	testIno := inos[20:50]
	reqBatchDeleteInode := &proto.DeleteInodeBatchRequest{
		Inodes: testIno,
	}
	packet := &Packet{}
	if err = leader.DeleteInodeBatch(reqBatchDeleteInode, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch delete inode failed, err:%v, resultCode:%v", err, packet.ResultCode)
		return
	}
	for _, ino := range testIno {
		inodeInLeader, _ := leader.inodeTree.Get(ino)
		inodeInFollower, _ := follower.inodeTree.Get(ino)
		if inodeInLeader != nil || inodeInFollower != nil {
			t.Errorf("inode get result expcet is nil, but actual is [leader:%v, follower:%v]", inodeInLeader, inodeInFollower)
			return
		}
	}
}

func TestMetaPartition_BatchDeleteInodeCase01(t *testing.T) {
	//leader is mem mode
	dir := "batch_delete_inode_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	BatchDeleteInodeInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	BatchDeleteInodeInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestResetCursor_OperationMismatch(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_cursor", ApplyMock)
	if mp == nil {
		t.Errorf("mock mp failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)
	mp.config.Cursor = 10000
	req := &proto.CursorResetRequest{
		PartitionId:     1,
		NewCursor:       15000,
		Force:           true,
		CursorResetType: int(SubCursor),
	}

	err = mp.CursorReset(context.Background(), req)
	if err == nil {
		t.Errorf("error mismatch, expect:operation mismatch, actual:nil")
		return
	}
	return
}

func TestResetCursor_OutOfMaxEnd(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_cursor", ApplyMock)
	if mp == nil {
		t.Errorf("mock mp failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)
	mp.config.Cursor = mp.config.End
	req := &proto.CursorResetRequest{
		PartitionId:     1,
		NewCursor:       15000,
		Force:           true,
		CursorResetType: int(SubCursor),
	}

	status, _ := mp.calcMPStatus()
	err = mp.CursorReset(context.Background(), req)
	if err != nil {
		t.Errorf("reset cursor test failed, err:%s", err.Error())
		return
	}
	t.Logf("reset cursor:%d, status:%d, err:%v", mp.config.Cursor, status, err)

	for i := 1; i <100; i++ {
		_, _, _ = inodeCreate(mp.inodeTree, NewInode(uint64(i), 0), false)
	}
	req.NewCursor = 90
	err = mp.CursorReset(context.Background(), req)
	if err == nil {
		t.Errorf("expect error is out of bound, but actual is nil")
	}
	if mp.config.Cursor != 15000 {
		t.Errorf("cursor mismatch, expect:10000, actual:%v", mp.config.Cursor)
		return
	}
	t.Logf("reset cursor:%d, status:%d, err:%v", mp.config.Cursor, status, err)
	return
}

func TestResetCursor_LimitedAndForce(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_cursor", ApplyMock)
	if mp == nil {
		t.Errorf("mock mp failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)
	mp.config.End = 10000
	mp.config.Cursor = 9999

	req := &proto.CursorResetRequest{
		PartitionId:     1,
		NewCursor:       9900,
		Force:           false,
		CursorResetType: int(SubCursor),
	}

	for i := 1; i <100; i++ {
		_, _, _ = inodeCreate(mp.inodeTree, NewInode(uint64(i), 0), false)
	}

	err = mp.CursorReset(context.Background(), req)
	if err == nil {
		t.Errorf("error mismatch, expect(no need reset), actual(nil)")
		return
	}
	if mp.config.Cursor != 9999 {
		t.Errorf("cursor mismatch, expect:9999, actual:%v", mp.config.Cursor)
		return
	}

	req.Force = true
	err = mp.CursorReset(context.Background(), req)
	if err != nil {
		t.Errorf("reset cursor:%d test failed, err:%v", mp.config.Cursor, err)
		return
	}
	if mp.config.Cursor != req.NewCursor {
		t.Errorf("reset cursor failed, expect:%v, actual:%v", req.NewCursor, mp.config.Cursor)
	}

	return
}

func TestResetCursor_CursorChange(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_cursor", ApplyMock)
	if mp == nil {
		t.Errorf("mock mp failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)

	req := &proto.CursorResetRequest{
		PartitionId: 1,
		NewCursor:   8000,
		Force:       false,
	}

	for i := 1; i <100; i++ {
		_, _, _ = inodeCreate(mp.inodeTree, NewInode(uint64(i), 0), false)
	}
	mp.config.Cursor = 99

	go func() {
		for i := 0; i < 100; i++{
			mp.nextInodeID()
			time.Sleep(time.Microsecond * 1)
		}
	}()
	time.Sleep(time.Microsecond * 5)
	err = mp.CursorReset(context.Background(), req)
	t.Logf("reset cursor:%d, err:%v", mp.config.Cursor,  err)

	return
}

func TestResetCursor_LeaderChange(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_cursor", ApplyMock)
	if mp == nil {
		t.Errorf("mock mp failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)

	mp.config.Cursor = 100

	req := &proto.CursorResetRequest{
		PartitionId:     1,
		NewCursor:       8000,
		Force:           false,
		CursorResetType: int(SubCursor),
	}

	mp.config.NodeId = 2
	err = mp.CursorReset(context.Background(), req)
	if err == nil {
		t.Errorf("expect error is leader change, but actual is nil")
	}
	if 100 != mp.config.Cursor {
		t.Errorf("cursor mismatch, expect:0, actual:%v", mp.config.Cursor)
		return
	}
	t.Logf("reset cursor:%d, err:%v", mp.config.Cursor,  err)

	return
}

func TestResetCursor_MPWriteStatus(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_cursor", ApplyMock)
	if mp == nil {
		t.Errorf("mock mp failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)

	configTotalMem = 100 * util.GB
	defer func() {
		configTotalMem = 0
	}()

	for i := 1; i <100; i++ {
		_, _, _ = inodeCreate(mp.inodeTree, NewInode(uint64(i), 0), false)
	}
	mp.config.Cursor = 10000

	req := &proto.CursorResetRequest{
		PartitionId:     1,
		NewCursor:       0,
		Force:           true,
		CursorResetType: int(SubCursor),
	}

	status, _ := mp.calcMPStatus()
	err = mp.CursorReset(context.Background(), req)
	if err == nil {
		t.Errorf("error mismatch, expect:(mp status not read only), but actual:(nil), mp status(%v)", status)
		return
	}
	if mp.config.Cursor != 10000 {
		t.Errorf("cursor mismatch, expect:99, actual:%v", mp.config.Cursor)
	}
}

func TestResetCursor_MPReadOnly(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_cursor", ApplyMock)
	if mp == nil {
		t.Errorf("mock mp failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)

	for i := 1; i <100; i++ {
		_, _, _ = inodeCreate(mp.inodeTree, NewInode(uint64(i), 0), false)
	}

	maxInode := mp.inodeTree.MaxItem()
	if maxInode == nil {
		t.Errorf("maxInode is nil")
		return
	}
	mp.config.Cursor = mp.config.End

	req := &proto.CursorResetRequest{
		PartitionId:     1,
		NewCursor:       0,
		Force:           true,
		CursorResetType: int(SubCursor),
	}

	status, _ := mp.calcMPStatus()
	err = mp.CursorReset(context.Background(), req)
	if err != nil {
		t.Errorf("error mismatch, expect:nil, actual:%v, mp status(%v)", err, status)
		return
	}
	if mp.config.Cursor != maxInode.Inode + mpResetInoStep {
		t.Errorf("cursor mismatch, expect:99, actual:%v", mp.config.Cursor)
	}
}

func TestResetCursor_SubCursorCase01(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_cursor", ApplyMock)
	if mp == nil {
		t.Errorf("mock mp failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)

	for i := 1; i <100; i++ {
		_, _, _ = inodeCreate(mp.inodeTree, NewInode(uint64(i), 0), false)
	}

	maxInode := mp.inodeTree.MaxItem()
	if maxInode == nil {
		t.Errorf("maxInode is nil")
		return
	}
	mp.config.Cursor = mp.config.End

	req := &proto.CursorResetRequest{
		PartitionId:     1,
		NewCursor:       maxInode.Inode + 2000,
		CursorResetType: int(SubCursor),
	}

	status, _ := mp.calcMPStatus()
	err = mp.CursorReset(context.Background(), req)
	if err != nil {
		t.Errorf("error mismatch, expect:nil, actual:%v, mp status(%v)", err, status)
		return
	}
	if mp.config.Cursor != maxInode.Inode + 2000 {
		t.Errorf("cursor mismatch, expect:99, actual:%v", mp.config.Cursor)
	}
}

func TestResetCursor_AddCursorCase01(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_cursor", ApplyMock)
	if mp == nil {
		t.Errorf("mock mp failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)
	configTotalMem = 100 * GB

	req := &proto.CursorResetRequest{
		PartitionId: 1,
		CursorResetType: int(AddCursor),
	}

	err = mp.CursorReset(context.Background(), req)
	if err != nil {
		t.Errorf("reset cursor failed, err:%v", err)
		return
	}
	status, _ := mp.calcMPStatus()
	if status != proto.ReadOnly {
		t.Errorf("mp status mismatch, expect:read only(%v), actual(%v)", proto.ReadOnly, status)
	}
	if mp.config.Cursor != mp.config.End {
		t.Errorf("mp cursor mismatch, expect:%v, actual:%v", mp.config.End, mp.config.Cursor)
	}
	return
}

func TestResetCursor_ResetMaxMP(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_cursor", ApplyMock)
	if mp == nil {
		t.Errorf("mock mp failed:%v", err)
		t.FailNow()
	}
	defer releaseMetaPartition(mp)
	mp.config.Cursor = 1000
	mp.config.End = defaultMaxMetaPartitionInodeID
	configTotalMem = 100 * GB

	req := &proto.CursorResetRequest{
		PartitionId:     1,
		CursorResetType: int(SubCursor),
	}

	err = mp.CursorReset(context.Background(), req)
	if err == nil {
		t.Errorf("error expect:not support reset cursor, but actual:nil")
		return
	}
	if mp.config.Cursor != 1000 {
		t.Errorf("cursor mismatch, expect:%v, actual:%v", defaultMaxMetaPartitionInodeID, mp.config.Cursor)
	}
	return
}
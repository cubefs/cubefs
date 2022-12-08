package metanode

import (
	"bytes"
	"fmt"
	"github.com/chubaofs/chubaofs/metanode/metamock"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/multipart"
	raftproto "github.com/tiglabs/raft/proto"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMetaPartition_ApplySnapshotNew(t *testing.T) {
	tests := []struct {
		name              string
		partitionID       uint64
		leaderStoreMode   proto.StoreMode
		leaderRootDir     string
		followerStoreMode proto.StoreMode
		followerRootDir   string
		applyFunc         metamock.ApplyFunc
		snapV             int
	}{
		{
			name:              "test1",
			partitionID:       1,
			leaderStoreMode:   proto.StoreModeMem,
			leaderRootDir:     "./test_apply_snapshot_leader_mp",
			followerStoreMode: proto.StoreModeMem,
			followerRootDir:   "./test_apply_snapshot_follower_mp",
			applyFunc:         ApplyMockWithNull,
			snapV:             BaseSnapshotV,
		},
		{
			name:              "test2",
			partitionID:       2,
			leaderStoreMode:   proto.StoreModeMem,
			leaderRootDir:     "./test_apply_snapshot_leader_mp",
			followerStoreMode: proto.StoreModeRocksDb,
			followerRootDir:   "./test_apply_snapshot_follower_mp",
			applyFunc:         ApplyMockWithNull,
			snapV:             BaseSnapshotV,
		},
		{
			name:              "test3",
			partitionID:       3,
			leaderStoreMode:   proto.StoreModeRocksDb,
			leaderRootDir:     "./test_apply_snapshot_leader_mp",
			followerStoreMode: proto.StoreModeMem,
			followerRootDir:   "./test_apply_snapshot_follower_mp",
			applyFunc:         ApplyMockWithNull,
			snapV:             BaseSnapshotV,
		},
		{
			name:              "test4",
			partitionID:       4,
			leaderStoreMode:   proto.StoreModeRocksDb,
			leaderRootDir:     "./test_apply_snapshot_leader_mp",
			followerStoreMode: proto.StoreModeRocksDb,
			followerRootDir:   "./test_apply_snapshot_follower_mp",
			applyFunc:         ApplyMockWithNull,
			snapV:             BaseSnapshotV,
		},
		{
			name:              "test5",
			partitionID:       5,
			leaderStoreMode:   proto.StoreModeMem,
			leaderRootDir:     "./test_apply_snapshot_leader_mp",
			followerStoreMode: proto.StoreModeMem,
			followerRootDir:   "./test_apply_snapshot_follower_mp",
			applyFunc:         ApplyMockWithNull,
			snapV:             BatchSnapshotV1,
		},
		{
			name:              "test6",
			partitionID:       6,
			leaderStoreMode:   proto.StoreModeMem,
			leaderRootDir:     "./test_apply_snapshot_leader_mp",
			followerStoreMode: proto.StoreModeRocksDb,
			followerRootDir:   "./test_apply_snapshot_follower_mp",
			applyFunc:         ApplyMockWithNull,
			snapV:             BatchSnapshotV1,
		},
		{
			name:              "test7",
			partitionID:       7,
			leaderStoreMode:   proto.StoreModeRocksDb,
			leaderRootDir:     "./test_apply_snapshot_leader_mp",
			followerStoreMode: proto.StoreModeMem,
			followerRootDir:   "./test_apply_snapshot_follower_mp",
			applyFunc:         ApplyMockWithNull,
			snapV:             BatchSnapshotV1,
		},
		{
			name:              "test8",
			partitionID:       8,
			leaderStoreMode:   proto.StoreModeRocksDb,
			leaderRootDir:     "./test_apply_snapshot_leader_mp",
			followerStoreMode: proto.StoreModeRocksDb,
			followerRootDir:   "./test_apply_snapshot_follower_mp",
			applyFunc:         ApplyMockWithNull,
			snapV:             BatchSnapshotV1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				leaderMp, followerMp *metaPartition
				err                  error
			)
			leaderMp, err = mockMetaPartition(test.partitionID, 1, test.leaderStoreMode, test.leaderRootDir, ApplyMockWithNull)
			if err != nil {
				t.Errorf("mock mem mode meta partition failed, error:%v", err)
				return
			}
			followerMp, err = mockMetaPartition(test.partitionID, 1, test.followerStoreMode, test.followerRootDir, ApplyMockWithNull)
			if err != nil {
				t.Errorf("mock rocksdb mode meta partition failed, error:%v", err)
				return
			}
			defer func() {
				releaseMetaPartition(leaderMp)
				releaseMetaPartition(followerMp)
			}()

			withTrashTest := false
			if test.snapV > 0 {
				withTrashTest = true
			}

			if err = mockForSnapshot(t, leaderMp, withTrashTest); err != nil {
				t.Errorf("mock for snapshot failed:%v", err)
				return
			}

			interTest(t, leaderMp, followerMp, test.snapV)
		})
	}
}

func TestMetaPartition_ApplySnapshotCase01(t *testing.T) {
	/*
		leader   follower
		mem      mem
	*/

	var (
		leaderMp, followerMp *metaPartition
		err                  error
		leaderRootDir        = "./test_apply_snapshot_leader_mp"
		followerRootDir      = "./test_apply_snapshot_follower_mp"
	)

	//new leader and follower mp
	leaderMp, err = mockMetaPartition(1, 1, proto.StoreModeMem, leaderRootDir, ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock mem mode meta partition failed, error:%v", err)
		return
	}
	followerMp, err = mockMetaPartition(1, 1, proto.StoreModeMem, followerRootDir, ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock rocksdb mode meta partition failed, error:%v", err)
		return
	}
	defer func() {
		releaseMetaPartition(leaderMp)
		releaseMetaPartition(followerMp)
	}()

	if err = mockForSnapshot(t, leaderMp, false); err != nil {
		t.Errorf("mock for snapshot failed:%v", err)
		return
	}

	interTest(t, leaderMp, followerMp, BaseSnapshotV)
}

func TestMetaPartition_ApplySnapshotCase02(t *testing.T) {
	/*
		leader   follower
		rocksdb  rocksdb
	*/

	var (
		leaderMp, followerMp *metaPartition
		err                  error
		leaderRootDir        = "./test_apply_snapshot_leader_mp"
		followerRootDir      = "./test_apply_snapshot_follower_mp"
	)

	//new leader and follower mp
	leaderMp, err = mockMetaPartition(2, 1, proto.StoreModeRocksDb, leaderRootDir, ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock mem mode meta partition failed, error:%v", err)
		return
	}
	followerMp, err = mockMetaPartition(2, 1, proto.StoreModeRocksDb, followerRootDir, ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock rocksdb mode meta partition failed, error:%v", err)
		return
	}
	defer func() {
		releaseMetaPartition(leaderMp)
		releaseMetaPartition(followerMp)
	}()

	if err = mockForSnapshot(t, leaderMp, false); err != nil {
		t.Errorf("mock for snapshot failed:%v", err)
		return
	}

	interTest(t, leaderMp, followerMp, BaseSnapshotV)
}

func TestMetaPartition_ApplySnapshotCase03(t *testing.T) {
	/*
		leader   follower
		mem      rocksdb
	*/
	var (
		leaderMp, followerMp *metaPartition
		err                  error
		leaderRootDir        = "./test_apply_snapshot_leader_mp"
		followerRootDir      = "./test_apply_snapshot_follower_mp"
	)

	//new leader and follower mp
	leaderMp, err = mockMetaPartition(3, 1, proto.StoreModeMem, leaderRootDir, ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock mem mode meta partition failed, error:%v", err)
		return
	}
	followerMp, err = mockMetaPartition(3, 1, proto.StoreModeRocksDb, followerRootDir, ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock rocksdb mode meta partition failed, error:%v", err)
		return
	}
	defer func() {
		releaseMetaPartition(leaderMp)
		releaseMetaPartition(followerMp)
	}()

	if err = mockForSnapshot(t, leaderMp, false); err != nil {
		t.Errorf("mock for snapshot failed:%v", err)
		return
	}

	interTest(t, leaderMp, followerMp, BaseSnapshotV)
}

func TestMetaPartition_ApplySnapshotCase04(t *testing.T) {
	/*
		leader   follower
		rocksdb  mem
	*/

	var (
		leaderMp, followerMp *metaPartition
		err                  error
		leaderRootDir        = "./test_apply_snapshot_leader_mp"
		followerRootDir      = "./test_apply_snapshot_follower_mp"
	)

	//new leader and follower mp
	leaderMp, err = mockMetaPartition(4, 1, proto.StoreModeRocksDb, leaderRootDir, ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock mem mode meta partition failed, error:%v", err)
		return
	}
	followerMp, err = mockMetaPartition(4, 1, proto.StoreModeMem, followerRootDir, ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock rocksdb mode meta partition failed, error:%v", err)
		return
	}
	defer func() {
		releaseMetaPartition(leaderMp)
		releaseMetaPartition(followerMp)
	}()

	if err = mockForSnapshot(t, leaderMp, false); err != nil {
		t.Errorf("mock for snapshot failed:%v", err)
		return
	}

	interTest(t, leaderMp, followerMp, BaseSnapshotV)
}

func mockForSnapshot(t *testing.T, mp *metaPartition, withTrashTest bool) (err error) {
	if err = mockMetaTree(mp, withTrashTest); err != nil {
		t.Errorf("mock trees failed, error:%v", err)
		return
	}

	//create extent del file
	if !withTrashTest {
		for index := 0; index < 5; index++ {
			fileName := path.Join(mp.config.RootDir, prefixDelExtent+"_"+strconv.Itoa(index))
			if _, err = os.Create(fileName); err != nil {
				t.Errorf("create file[%s] failed:%v", fileName, err)
				return
			}
			if err = os.WriteFile(fileName, []byte("test_apply_snapshot"), 0666); err != nil {
				t.Errorf("write data to file[%s] failed:%v", fileName, err)
				return
			}
		}
	}

	mp.applyID = 1000
	return
}

func mockMetaTree(mp *metaPartition, withTrashTest bool) (err error) {
	var dbHandle interface{}
	dbHandle, err = mp.inodeTree.CreateBatchWriteHandle()
	if err != nil {
		fmt.Printf("create batch write handle failed:%v\n", err)
		os.Exit(1)
	}
	defer mp.inodeTree.ReleaseBatchWriteHandle(dbHandle)

	defer func() {
		if err != nil {
			return
		}
		err = mp.inodeTree.CommitBatchWrite(dbHandle, true)
		if err != nil {
			fmt.Printf("commit batch write handle failed:%v\n", err)
			os.Exit(1)
		}
	}()
	//create root inode
	rootInode := NewInode(1, proto.Mode(os.ModeDir))
	_, _, err = mp.inodeTree.Create(dbHandle, rootInode, true)
	if err != nil {
		return
	}

	//mock inode tree, dentry tree
	mode := proto.Mode(os.ModeDir)
	t := "dir"
	ino := 2
	ino2 := 12
	for ; ino < 12; ino++ {
		name := fmt.Sprintf("second_level_%s_%v", t, ino)
		if _, _, err = mp.inodeTree.Create(dbHandle, NewInode(uint64(ino), mode), true); err != nil {
			fmt.Printf("create inode[%v] failed, error:%v", ino, err)
			return fmt.Errorf("create inode failed:%v", err)
		}
		dentry := &Dentry{
			ParentId: rootInode.Inode,
			Name:     name,
			Inode:    uint64(ino),
			Type:     mode,
		}
		if _, _, err = mp.dentryTree.Create(dbHandle, dentry, true); err != nil {
			fmt.Printf("create dentry[%v] failed, error:%v", dentry, err)
			return fmt.Errorf("create dentry failed:%v", err)
		}

		if ino > ino2/2 {
			continue
		}

		//create third level
		for i := 0; i < 5; i++ {
			if _, _, err = mp.inodeTree.Create(dbHandle, NewInode(uint64(ino2), 1), true); err != nil {
				fmt.Printf("create inode[%v] failed, error:%v", ino2, err)
				return fmt.Errorf("create inode failed:%v", err)
			}
			name = fmt.Sprintf("third_level_file_%v", ino2)
			dentry = &Dentry{
				ParentId: uint64(ino),
				Name:     name,
				Inode:    uint64(ino2),
				Type:     1,
			}
			if _, _, err = mp.dentryTree.Create(dbHandle, dentry, true); err != nil {
				fmt.Printf("create dentry[%v] failed, error:%v", dentry, err)
				return fmt.Errorf("create dentry failed:%v", err)
			}
			ino2++
		}

		if ino == ino2/2 {
			mode = 1
			t = "file"
		}
	}

	//mock multipart tree
	timeNow := time.Now()
	for index := 1; index < 100; index++ {
		parts := make([]*Part, 0, 10)
		for partID := 1; partID <= 10; partID++ {
			part := &Part{
				ID:         uint16(partID),
				UploadTime: timeNow.Add(time.Second * time.Duration(partID)).Local(),
				Inode:      uint64(partID * 100),
				Size:       uint64(partID * 1000),
			}
			parts = append(parts, part)
		}
		extend := NewMultipartExtend()
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("test_apply_snapshot_key_%v_%v", index, i)
			value := fmt.Sprintf("test_apply_snapshot_value_%v_%v", index, i)
			extend[key] = value
		}
		p := fmt.Sprintf("/test_apply_snapshot/%v", index)
		multipart := &Multipart{
			key:      p,
			id:       multipart.CreateMultipartID(10).String(),
			parts:    parts,
			extend:   extend,
			initTime: timeNow.Local(),
		}
		if _, _, err = mp.multipartTree.Create(dbHandle, multipart, true); err != nil {
			return fmt.Errorf("create multipart failed")
		}
	}

	//mock extend tree
	for ino = 2; ino < ino2; ino++ {
		extend := NewExtend(uint64(ino))
		for index := 1; index <= 10; index++ {
			key := []byte(fmt.Sprintf("test_apply_snapshot_key_%v_%v", ino, index))
			value := []byte(fmt.Sprintf("test_apply_snapshot_value_%v_%v", ino, index))
			extend.Put(key, value)
		}
		if _, _, err = mp.extendTree.Create(dbHandle, extend, true); err != nil {
			return fmt.Errorf("create extend[%v] failed:%v", extend, err)
		}
	}

	if !withTrashTest {
		return
	}

	//mock deleted inode, deleted dentry tree
	timeStamp := time.Now().UnixNano() / 1000
	for ino = 101; ino < 110; ino++ {
		dentry := &Dentry{
			ParentId: 1,
			Inode:    uint64(ino),
			Type:     1,
			Name:     fmt.Sprintf("test_apply_snapshot_%v", ino),
		}
		dd := newDeletedDentry(dentry, timeStamp, "")
		if _, _, err = mp.dentryDeletedTree.Create(dbHandle, dd, true); err != nil {
			return fmt.Errorf("create deleted dentry[%v] failed, error:%v", dd, err)
		}

		inode := NewInode(uint64(ino), 1)
		dino := NewDeletedInode(inode, timeStamp)
		if _, _, err = mp.inodeDeletedTree.Create(dbHandle, dino, false); err != nil {
			return fmt.Errorf("create deleted inode[%v] failed, error:%v", dino, err)
		}
	}

	key := make([]byte, dbExtentKeySize)
	updateKeyToNow(key)

	batchHandle, _ := mp.db.CreateBatchHandler()
	for index := 0; index < 100; index++ {
		ek := proto.ExtentKey{
			PartitionId: uint64(index),
			ExtentId:    uint64(index),
		}
		var valueBuff []byte
		defBuff := make([]byte, 1)
		valueBuff = defBuff
		if index%30 == 0 {
			valueBuff = make([]byte, 24)
			delEk := proto.MetaDelExtentKey{InodeId: uint64(index),
				TimeStamp: time.Now().Unix(),
				SrcType:   uint64(index / 30 % (proto.DelEkSrcTypeFromDelInode + 1))}
			delEk.MarshDelEkValue(valueBuff)
		}
		keyBuff, _ := ek.MarshalDbKey()
		copy(key[8:], keyBuff)
		mp.db.AddItemToBatch(batchHandle, key, valueBuff)
	}
	err = mp.db.CommitBatchAndRelease(batchHandle)
	if err != nil {
		fmt.Printf("del ek commit failed:%v\n", err.Error())
	}
	return
}

func interTest(t *testing.T, leaderMp, followerMp *metaPartition, snapV int) {
	var err error

	//new item iterator by leader mp
	var snap raftproto.Snapshot
	switch snapV {
	case BaseSnapshotV:
		snap, err = newMetaItemIterator(leaderMp)
	case BatchSnapshotV1:
		snap, err = newBatchMetaItemIterator(leaderMp, BatchSnapshotV1)
	default:
		t.Errorf("error snap version:%v", snapV)
		t.FailNow()
	}
	//snap, err = newMetaItemIteratorV2(leaderMp, NewMetaNodeVersion("2.7.0"))

	t.Logf("leader create snap success")
	//apply snapshot to follower mp
	if err = followerMp.ApplySnapshot(nil, snap, uint32(snapV)); err != nil {
		t.Errorf("follower mp apply snapshot failed:%v", err)
		return
	}
	snap.Close()
	t.Logf("follower apply snap success")
	//validate (compare leader with follower)
	if !validateApplySnapshotResult(t, leaderMp, followerMp) {
		t.Errorf("validate failed")
		return
	}
}

func validateApplySnapshotResult(t *testing.T, leaderMp, followerMp *metaPartition) bool {
	if leaderMp.applyID != followerMp.applyID || (followerMp.HasRocksDBStore() && leaderMp.applyID != followerMp.inodeTree.GetPersistentApplyID()) {
		t.Errorf("apply id mismatch, expect:%v, actual[mem:%v rocksdb:%v]", leaderMp.applyID, followerMp.applyID, followerMp.inodeTree.GetPersistentApplyID())
		return false
	}
	if leaderMp.inodeTree.Count() != followerMp.inodeTree.Count() {
		t.Errorf("inode tree count mismatch, leader:%v, follower:%v", leaderMp.inodeTree.Count(), followerMp.inodeTree.Count())
		return false
	}
	if err := leaderMp.inodeTree.Range(nil, nil, func(inode *Inode) (bool, error) {
		ino, _ := followerMp.inodeTree.Get(inode.Inode)
		if ino == nil {
			return false, fmt.Errorf("not found inode(%v) in follower meta partition", inode)
		}
		return true, nil
	}); err != nil {
		t.Errorf("validate failed:%v", err)
		return false
	}

	if leaderMp.dentryTree.Count() != followerMp.dentryTree.Count() {
		t.Errorf("dentry tree count mismatch, leader:%v, follower:%v", leaderMp.dentryTree.Count(), followerMp.dentryTree.Count())
		return false
	}
	if err := leaderMp.dentryTree.Range(nil, nil, func(dentry *Dentry) (bool, error) {
		if d, _ := followerMp.dentryTree.Get(dentry.ParentId, dentry.Name); d == nil {
			return false, fmt.Errorf("not found dentry(%v) in follower meta partition", dentry)
		}
		return true, nil
	}); err != nil {
		t.Errorf("validate failed:%v", err)
		return false
	}

	if leaderMp.multipartTree.Count() != followerMp.multipartTree.Count() {
		t.Errorf("multipart tree count mismatch, leader:%v, follower:%v", leaderMp.multipartTree.Count(), followerMp.multipartTree.Count())
		return false
	}
	if err := leaderMp.multipartTree.Range(nil, nil, func(multipart *Multipart) (bool, error) {
		if m, _ := followerMp.multipartTree.Get(multipart.key, multipart.id); m == nil {
			return false, fmt.Errorf("not found multipart(%v) in follower meta partition", multipart)
		}
		return true, nil
	}); err != nil {
		t.Errorf("validate failed:%v", err)
		return false
	}

	if leaderMp.extendTree.Count() != followerMp.extendTree.Count() {
		t.Errorf("extend tree count mismatch, leader:%v, follower:%v", leaderMp.extendTree.Count(), followerMp.extendTree.Count())
		return false
	}
	if err := leaderMp.extendTree.Range(nil, nil, func(extend *Extend) (bool, error) {
		if e, _ := followerMp.extendTree.Get(extend.inode); e == nil {
			return false, fmt.Errorf("not found extend(%v) in follower meta partition", extend)
		}
		return true, nil
	}); err != nil {
		t.Errorf("validate failed:%v", err)
		return false
	}

	if leaderMp.inodeDeletedTree.Count() != followerMp.inodeDeletedTree.Count() {
		t.Errorf("deleted inode tree count mismatch, leader:%v, follower:%v", leaderMp.inodeDeletedTree.Count(), followerMp.inodeDeletedTree.Count())
		return false
	}
	if err := leaderMp.inodeDeletedTree.Range(nil, nil, func(delInode *DeletedINode) (bool, error) {
		if di, _ := followerMp.inodeDeletedTree.Get(delInode.Inode.Inode); di == nil {
			return false, fmt.Errorf("not found deleted inode(%v) in follower meta partition", delInode)
		}
		return true, nil
	}); err != nil {
		t.Errorf("validate failed:%v", err)
		return false
	}

	if leaderMp.dentryDeletedTree.Count() != followerMp.dentryDeletedTree.Count() {
		t.Errorf("deleted dentry tree count mismatch, leader:%v, follower:%v", leaderMp.dentryDeletedTree.Count(), leaderMp.dentryDeletedTree.Count())
		return false
	}
	if err := leaderMp.dentryDeletedTree.Range(nil, nil, func(delDentry *DeletedDentry) (bool, error) {
		if dd, _ := followerMp.dentryDeletedTree.Get(delDentry.ParentId, delDentry.Name, delDentry.Timestamp); dd == nil {
			return false, fmt.Errorf("not found deleted dentry(%v) in follower meta partition", delDentry)
		}
		return true, nil
	}); err != nil {
		t.Errorf("validate failed:%v", err)
		return false
	}

	if _, err := compareExtentDeleteFile(leaderMp, followerMp); err != nil {
		t.Errorf("extent file validate failed:%v", err)
		return false
	}

	stKey := make([]byte, 1)
	endKey := make([]byte, 1)

	stKey[0] = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)
	if err := leaderMp.db.Range(stKey, endKey, func(k, v []byte) (bool, error) {
		value, tmpErr := followerMp.db.GetBytes(k)
		if tmpErr != nil {
			return false, tmpErr
		}

		delEkL := &proto.MetaDelExtentKey{}
		delEkF := &proto.MetaDelExtentKey{}
		if len(value) > 1 {
			delEkL.UnMarshDelEkValue(v)
			delEkF.UnMarshDelEkValue(value)
			t.Logf("leader:%d-%d-%d-%d, follower:%d-%d-%d-%d\n",
				delEkL.InodeId, delEkL.SrcType, delEkL.TimeStamp, len(v),
				delEkF.InodeId, delEkF.SrcType, delEkF.TimeStamp, len(value))
		}

		if bytes.Compare(v, value) == 0 && bytes.Compare(value, v) == 0 {
			return true, nil
		}

		return false, fmt.Errorf("del ek value failed ")
	}); err != nil {
		t.Errorf("validate failed:%v", err)
		return false
	}

	return true
}

func compareExtentDeleteFile(leaderMp, followerMp *metaPartition) (bool, error) {
	var fileNamesInLeader, fileNamesInFollower []string
	//leader
	fileInfos, err := ioutil.ReadDir(leaderMp.config.RootDir)
	if err != nil {
		return false, err
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), prefixDelExtent) {
			fileNamesInLeader = append(fileNamesInLeader, fileInfo.Name())
		}
	}

	//follower
	fileInfos, err = ioutil.ReadDir(followerMp.config.RootDir)
	if err != nil {
		return false, err
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), prefixDelExtent) {
			fileNamesInFollower = append(fileNamesInFollower, fileInfo.Name())
		}
	}

	if len(fileNamesInLeader) != len(fileNamesInFollower) {
		return false, fmt.Errorf("extend del file count mismatch, leader:%v, follower:%v", len(fileNamesInLeader), len(fileNamesInFollower))
	}

	sort.Slice(fileNamesInLeader, func(i, j int) bool {
		return fileNamesInLeader[i] < fileNamesInLeader[j]
	})
	sort.Slice(fileNamesInFollower, func(i, j int) bool {
		return fileNamesInFollower[i] < fileNamesInFollower[j]
	})

	for index, fileName := range fileNamesInLeader {
		if strings.Compare(fileName, fileNamesInFollower[index]) != 0 {
			return false, fmt.Errorf("extend del file name mismatch, leader:%s, follower:%v", fileName, fileNamesInFollower[index])
		}
		//todo: compare file content
	}
	return true, nil
}

func TestMetaPartition_GenSnap(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	mp, _ := mockMetaPartition(1, 1, proto.StoreModeMem, "./partition_1", ApplyMock)
	mp2, _ := mockMetaPartition(2, 1, proto.StoreModeMem, "./partition_2", ApplyMock)
	if mp == nil || mp2 == nil {
		fmt.Printf("new mock meta partition failed\n")
		t.FailNow()
	}
	defer func() {
		releaseMetaPartition(mp)
		releaseMetaPartition(mp2)
	}()
	maxInode := genInode(t, mp, count)
	if maxInode <= 0 {
		fmt.Printf("error max inode id:%v\n", maxInode)
		t.FailNow()
	}
	genDentry(t, mp, count, maxInode)
	mp.marshalVersion = MetaPartitionMarshVersion2
	mp2.marshalVersion = MetaPartitionMarshVersion1
	//mp.load(context.Background())

	start := time.Now()
	snap, _ := newMetaItemIterator(mp)
	//go dealChanel(mp2)
	err := mp2.ApplySnapshot(nil, snap, BaseSnapshotV)
	if err != nil {
		t.Errorf("applySnapshot failed, error:%v", err)
		t.FailNow()
	}
	cost := time.Since(start)

	checkMPInodeAndDentry(t, mp, mp2)
	t.Logf("%dW inodes %dW dentry, V2 gen snap, V1 aplly snnap success cost:%v", mp.inodeTree.Count()/10000, mp.dentryTree.Count()/10000, cost)
}

func TestMetaPartition_ApplySnap(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	mp, _ := mockMetaPartition(1, 1, proto.StoreModeMem, "./partition_1", ApplyMock)
	mp2, _ := mockMetaPartition(2, 1, proto.StoreModeMem, "./partition_2", ApplyMock)
	if mp == nil || mp2 == nil {
		fmt.Printf("new mock meta partition failed\n")
		t.FailNow()
	}
	defer func() {
		releaseMetaPartition(mp)
		releaseMetaPartition(mp2)
	}()
	mp.marshalVersion = MetaPartitionMarshVersion1
	mp2.marshalVersion = MetaPartitionMarshVersion2

	//mp.load(context.Background())
	maxInode := genInode(t, mp, count)
	if maxInode <= 0 {
		fmt.Printf("error max inode id:%v\n", maxInode)
		t.FailNow()
	}
	genDentry(t, mp, count, maxInode)

	start := time.Now()
	snap, _ := newMetaItemIterator(mp)
	//go dealChanel(mp2)
	mp2.ApplySnapshot(nil, snap, BaseSnapshotV)
	cost := time.Since(start)

	checkMPInodeAndDentry(t, mp, mp2)
	t.Logf("%dW inodes %dW dentry, V1 gen snap, V2 aplly snnap success cost:%v", mp.inodeTree.Count()/10000, mp.dentryTree.Count()/10000, cost)
}

func TestMetaPartition_ApplySnapV2(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	mp, _ := mockMetaPartition(1, 1, proto.StoreModeMem, "./partition_1", ApplyMock)
	mp2, _ := mockMetaPartition(2, 1, proto.StoreModeMem, "./partition_2", ApplyMock)
	if mp == nil || mp2 == nil {
		fmt.Printf("new mock meta partition failed\n")
		t.FailNow()
	}
	defer func() {
		releaseMetaPartition(mp)
		releaseMetaPartition(mp2)
	}()
	mp.marshalVersion = MetaPartitionMarshVersion2
	mp2.marshalVersion = MetaPartitionMarshVersion1

	maxInode := genInode(t, mp, count)
	if maxInode <= 0 {
		fmt.Printf("error max inode id:%v\n", maxInode)
		t.FailNow()
	}
	genDentry(t, mp, count, maxInode)

	start := time.Now()
	snap, _ := newMetaItemIterator(mp)
	//go dealChanel(mp2)
	mp2.ApplySnapshot(nil, snap, BaseSnapshotV)
	cost := time.Since(start)

	checkMPInodeAndDentry(t, mp, mp2)
	t.Logf("%dW inodes %dW dentry, V1 gen snap, V2 aplly snnap success cost:%v", mp.inodeTree.Count()/10000, mp.dentryTree.Count()/10000, cost)
}

func TestMetaPartition_Snap(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	mp, _ := mockMetaPartition(1, 1, proto.StoreModeMem, "./partition_1", ApplyMock)
	mp2, _ := mockMetaPartition(2, 1, proto.StoreModeMem, "./partition_2", ApplyMock)
	mp3, _ := mockMetaPartition(3, 1, proto.StoreModeMem, "./partition_3", ApplyMock)
	mp4, _ := mockMetaPartition(4, 1, proto.StoreModeMem, "./partition_4", ApplyMock)
	if mp == nil || mp2 == nil || mp3 == nil || mp4 == nil {
		fmt.Printf("new mock meta partition failed\n")
		t.FailNow()
	}
	defer func() {
		releaseMetaPartition(mp)
		releaseMetaPartition(mp2)
		releaseMetaPartition(mp3)
		releaseMetaPartition(mp4)
	}()
	mp.marshalVersion = MetaPartitionMarshVersion1
	mp3.marshalVersion = MetaPartitionMarshVersion1
	mp2.marshalVersion = MetaPartitionMarshVersion2
	mp4.marshalVersion = MetaPartitionMarshVersion2

	maxInode := genInode(t, mp, count)
	if maxInode <= 0 {
		fmt.Printf("error max inode id:%v\n", maxInode)
		t.FailNow()
	}
	genDentry(t, mp, count, maxInode)
	maxInode = genInode(t, mp2, count)
	if maxInode <= 0 {
		fmt.Printf("error max inode id:%v\n", maxInode)
		t.FailNow()
	}
	genDentry(t, mp2, count, maxInode)
	//mp.load(context.Background())
	//mp2.load(context.Background())

	start := time.Now()
	snap, _ := newMetaItemIterator(mp)
	//go dealChanel(mp3)
	mp3.ApplySnapshot(nil, snap, BaseSnapshotV)
	v1Cost := time.Since(start)

	start = time.Now()
	snap2, _ := newMetaItemIterator(mp2)
	//go dealChanel(mp4)
	mp4.ApplySnapshot(nil, snap2, BaseSnapshotV)
	v2Cost := time.Since(start)

	checkMPInodeAndDentry(t, mp, mp3)
	checkMPInodeAndDentry(t, mp2, mp4)
	t.Logf("V1 gen snap, V1 aplly snnap success cost:%v", v1Cost)
	t.Logf("V2 gen snap, V2 aplly snnap success cost:%v", v2Cost)
}

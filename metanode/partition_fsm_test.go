package metanode

import (
	"context"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	raftproto "github.com/tiglabs/raft/proto"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

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
	leaderMp, err = mockMetaPartition(proto.StoreModeMem, leaderRootDir, 1)
	if err != nil {
		t.Errorf("mock mem mode meta partition failed, error:%v", err)
		return
	}
	followerMp, err = mockMetaPartition(proto.StoreModeMem, followerRootDir, 1)
	if err != nil {
		t.Errorf("mock rocksdb mode meta partition failed, error:%v", err)
		return
	}
	defer func() {
		releaseMetaPartition(leaderMp)
		releaseMetaPartition(followerMp)
	}()

	if err = mockForSnapshot(t, leaderMp); err != nil {
		t.Errorf("mock for snapshot failed:%v", err)
		return
	}

	interTest(t, leaderMp, followerMp)
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
	leaderMp, err = mockMetaPartition(proto.StoreModeRocksDb, leaderRootDir, 2)
	if err != nil {
		t.Errorf("mock mem mode meta partition failed, error:%v", err)
		return
	}
	followerMp, err = mockMetaPartition(proto.StoreModeRocksDb, followerRootDir, 2)
	if err != nil {
		t.Errorf("mock rocksdb mode meta partition failed, error:%v", err)
		return
	}
	defer func() {
		releaseMetaPartition(leaderMp)
		releaseMetaPartition(followerMp)
	}()

	if err = mockForSnapshot(t, leaderMp); err != nil {
		t.Errorf("mock for snapshot failed:%v", err)
		return
	}

	interTest(t, leaderMp, followerMp)
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
	leaderMp, err = mockMetaPartition(proto.StoreModeMem, leaderRootDir, 3)
	if err != nil {
		t.Errorf("mock mem mode meta partition failed, error:%v", err)
		return
	}
	followerMp, err = mockMetaPartition(proto.StoreModeRocksDb, followerRootDir, 3)
	if err != nil {
		t.Errorf("mock rocksdb mode meta partition failed, error:%v", err)
		return
	}
	defer func() {
		releaseMetaPartition(leaderMp)
		releaseMetaPartition(followerMp)
	}()

	if err = mockForSnapshot(t, leaderMp); err != nil {
		t.Errorf("mock for snapshot failed:%v", err)
		return
	}

	interTest(t, leaderMp, followerMp)
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
	leaderMp, err = mockMetaPartition(proto.StoreModeRocksDb, leaderRootDir, 4)
	if err != nil {
		t.Errorf("mock mem mode meta partition failed, error:%v", err)
		return
	}
	followerMp, err = mockMetaPartition(proto.StoreModeMem, followerRootDir, 4)
	if err != nil {
		t.Errorf("mock rocksdb mode meta partition failed, error:%v", err)
		return
	}
	defer func() {
		releaseMetaPartition(leaderMp)
		releaseMetaPartition(followerMp)
	}()

	if err = mockForSnapshot(t, leaderMp); err != nil {
		t.Errorf("mock for snapshot failed:%v", err)
		return
	}

	interTest(t, leaderMp, followerMp)
}

func mockMetaPartition(storeMode proto.StoreMode, rootDir string, partitionID uint64) (*metaPartition, error) {
	_ = os.RemoveAll(rootDir)

	mpConfig := new(MetaPartitionConfig)
	mpConfig.PartitionId = partitionID
	mpConfig.RootDir = rootDir
	mpConfig.StoreMode = storeMode
	mpConfig.RocksDBDir = rootDir

	manager := new(metadataManager)
	manager.metaNode = new(MetaNode)

	mp := new(metaPartition)
	mp.config = mpConfig
	mp.manager = manager
	mp.storeChan = make(chan *storeMsg, 100)
	mp.extReset = make(chan struct{}, 1)
	mp.db = NewRocksDb()
	err := mp.db.OpenDb(mp.getRocksDbRootDir())
	if err != nil {
		fmt.Printf("open db failed, dir:%s, error:%v", mp.getRocksDbRootDir(), err)
		return nil, err
	}

	if mp.HasMemStore() {
		mp.initMemoryTree()
	}

	if mp.HasRocksDBStore() {
		err = mp.initRocksDBTree()
		if err != nil {
			return nil, err
		}
	}
	go func() {
		for{
			select{
			case sMsg := <- mp.storeChan:
				sMsg.snap.Close()
				return
			}
		}
	}()
	return mp, err
}

func releaseMetaPartition(mp *metaPartition) {
	_ = mp.db.CloseDb()
	_ = mp.db.ReleaseRocksDb()
	_ = os.RemoveAll(mp.config.RootDir)
}

func mockForSnapshot(t *testing.T, mp *metaPartition) (err error) {
	if err = mockMetaTree(mp); err != nil {
		t.Errorf("mock trees failed, error:%v", err)
		return
	}

	//create extent del file
	for index := 0; index < 5; index++ {
		fileName := path.Join(mp.config.RootDir, prefixDelExtent + "_" + strconv.Itoa(index))
		if _, err = os.Create(fileName); err != nil {
			t.Errorf("create file[%s] failed:%v", fileName, err)
			return
		}
		if err = os.WriteFile(fileName, []byte("test_apply_snapshot"), 0666); err != nil {
			t.Errorf("write data to file[%s] failed:%v", fileName, err)
			return
		}
	}
	mp.applyID = 1000
	return
}

func mockMetaTree(mp *metaPartition) (err error) {
	//create root inode
	rootInode := NewInode(1, proto.Mode(os.ModeDir))
	err = mp.inodeTree.Create(rootInode, false)
	if err != nil {
		return
	}

	//mock inode tree, dentry tree
	mode := proto.Mode(os.ModeDir)
	t := "dir"
	ino := 2
	ino2 := 12
	for ; ino < 12; ino ++ {
		name := fmt.Sprintf("second_level_%s_%v", t, ino)
		if err = mp.inodeTree.Create(NewInode(uint64(ino), mode), false); err != nil {
			fmt.Printf("create inode[%v] failed, error:%v", ino, err)
			return fmt.Errorf("create inode failed:%v", err)
		}
		dentry := &Dentry{
			ParentId: rootInode.Inode,
			Name:     name,
			Inode:    uint64(ino),
			Type:     mode,
		}
		if err = mp.dentryTree.Create(dentry, false); err != nil {
			fmt.Printf("create dentry[%v] failed, error:%v", dentry, err)
			return fmt.Errorf("create dentry failed:%v", err)
		}

		if ino > ino2/2 {
			continue
		}

		//create third level
		for i := 0; i < 5 ; i++ {
			if err = mp.inodeTree.Create(NewInode(uint64(ino2), 1), false); err != nil {
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
			if err = mp.dentryTree.Create(dentry, false); err != nil {
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
				ID: uint16(partID),
				UploadTime: timeNow.Add(time.Second * time.Duration(partID)).Local(),
				Inode: uint64(partID * 100),
				Size: uint64(partID * 1000),
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
			key: p,
			id: util.CreateMultipartID(10).String(),
			parts: parts,
			extend: extend,
			initTime: timeNow.Local(),
		}
		if err = mp.multipartTree.Create(multipart, false); err != nil {
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
		if err = mp.extendTree.Create(extend, false); err != nil {
			return fmt.Errorf("create extend[%v] failed:%v", extend, err)
		}
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
		if err = mp.dentryDeletedTree.Create(dd, false); err != nil {
			return fmt.Errorf("create deleted dentry[%v] failed, error:%v", dd, err)
		}

		inode := NewInode(uint64(ino), 1)
		dino := NewDeletedInode(inode, timeStamp)
		if err = mp.inodeDeletedTree.Create(dino, false); err != nil {
			return fmt.Errorf("create deleted inode[%v] failed, error:%v", dino, err)
		}
	}
	return
}

func interTest(t *testing.T, leaderMp, followerMp *metaPartition) {
	var err error

	//new item iterator by leader mp
	var snap raftproto.Snapshot
	if snap, err = leaderMp.Snapshot(); err != nil {
		t.Errorf("leader mp generate snapshot failed:%v", err)
		return
	}

	//apply snapshot to follower mp
	if err = followerMp.ApplySnapshot(nil, snap); err != nil {
		t.Errorf("follower mp apply snapshot failed:%v", err)
		return
	}

	//validate (compare leader with follower)
	if !validateApplySnapshotResult(t, leaderMp, followerMp) {
		t.Errorf("validate failed")
		return
	}
}

func validateApplySnapshotResult(t *testing.T, leaderMp, followerMp *metaPartition) bool {
	if leaderMp.inodeTree.Count() != followerMp.inodeTree.Count() {
		t.Errorf("inode tree count mismatch, leader:%v, follower:%v", leaderMp.inodeTree.Count(), followerMp.inodeTree.Count())
		return false
	}
	if err := leaderMp.inodeTree.Range(nil, nil, func(v []byte) (bool, error) {
		inode := NewInode(0, 0)
		if err := inode.Unmarshal(context.Background(), v); err != nil {
			return false, fmt.Errorf("unmarshal inode failed:%v", err)
		}
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
	if err := leaderMp.dentryTree.Range(nil, nil, func(v []byte) (bool, error) {
		dentry := new(Dentry)
		if err := dentry.Unmarshal(v); err != nil {
			return false, fmt.Errorf("unmarshal dentry failed:%v", err)
		}
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
	if err := leaderMp.multipartTree.Range(nil, nil, func(v []byte) (bool, error) {
		multipart := MultipartFromBytes(v)
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
	if err := leaderMp.extendTree.Range(nil, nil, func(v []byte) (bool, error) {
		extend, err := NewExtendFromBytes(v)
		if err != nil {
			return false, fmt.Errorf("unmarshal extend failed:%v", err)
		}
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
	if err := leaderMp.inodeDeletedTree.Range(nil, nil, func(v []byte) (bool, error) {
		delInode := new(DeletedINode)
		if err := delInode.Unmarshal(context.Background(), v); err != nil {
			return false, fmt.Errorf("unmarshal deleted inode failed:%v", err)
		}
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
	if err := leaderMp.dentryDeletedTree.Range(nil, nil, func(v []byte) (bool, error) {
		delDentry := new(DeletedDentry)
		if err := delDentry.Unmarshal(v); err != nil {
			 return false, fmt.Errorf("unmarshal deleted dentry failed:%v", err)
		}
		if dd, _ := followerMp.dentryDeletedTree.Get(delDentry.ParentId, delDentry.Name, delDentry.Timestamp); dd == nil {
			return false, fmt.Errorf("not found deleted dentry(%v) in follower meta partition", delDentry)
		}
		return true, nil
	}); err != nil {
		t.Errorf("validate failed:%v", err)
		return false
	}

	var fileNamesInLeader, fileNamesInFollower []string
	//leader
	fileInfos, err := ioutil.ReadDir(leaderMp.config.RootDir)
	if err != nil {
		return false
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), prefixDelExtent) {
			fileNamesInLeader = append(fileNamesInLeader, fileInfo.Name())
		}
	}

	//follower
	fileInfos, err = ioutil.ReadDir(followerMp.config.RootDir)
	if err != nil {
		return false
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), prefixDelExtent) {
			fileNamesInFollower = append(fileNamesInFollower, fileInfo.Name())
		}
	}

	if len(fileNamesInLeader) != len(fileNamesInFollower) {
		t.Errorf("extend del file count mismatch, leader:%v, follower:%v", len(fileNamesInLeader), len(fileNamesInFollower))
		return false
	}

	sort.Slice(fileNamesInLeader, func(i, j int) bool {
		return fileNamesInLeader[i] < fileNamesInLeader[j]
	})
	sort.Slice(fileNamesInFollower, func(i, j int) bool {
		return fileNamesInFollower[i] < fileNamesInFollower[j]
	})

	for index, fileName := range fileNamesInLeader {
		if strings.Compare(fileName, fileNamesInFollower[index]) != 0 {
			t.Errorf("extend del file name mismatch, leader:%s, follower:%v", fileName, fileNamesInFollower[index])
			return false
		}
		//todo: compare file content
	}
	return true
}
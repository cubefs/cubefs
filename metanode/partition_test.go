// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"

	"github.com/stretchr/testify/require"
)

func TestMetaPartition_LoadSnapshot(t *testing.T) {
	testPath := "/tmp/testMetaPartition/"
	os.RemoveAll(testPath)
	defer os.RemoveAll(testPath)
	mpC := &MetaPartitionConfig{
		PartitionId:   1,
		VolName:       "test_vol",
		Start:         0,
		End:           100,
		PartitionType: 1,
		Peers:         nil,
		RootDir:       testPath,
		StoreMode:     proto.StoreModeMem,
	}
	metaM := &metadataManager{
		nodeId:     1,
		zoneName:   "test",
		raftStore:  nil,
		partitions: make(map[uint64]MetaPartition),
		metaNode:   &MetaNode{},
	}

	partition := NewMetaPartition(mpC, metaM)
	require.NotNil(t, partition)

	// none data
	mp, ok := partition.(*metaPartition)
	err := mp.initObjects(true)
	require.NoError(t, err)
	require.True(t, ok)
	snap, err := mp.GetSnapShot()
	require.NoError(t, err)
	require.NotNil(t, snap)
	msg := &storeMsg{
		command:     1,
		snap:        snap,
		uniqId:      mp.GetUniqId(),
		uniqChecker: mp.uniqChecker,
	}
	mp.uidManager = NewUidMgr(mpC.VolName, mpC.PartitionId)
	mp.mqMgr = NewQuotaManager(mpC.VolName, mpC.PartitionId)
	mp.multiVersionList = &proto.VolVersionInfoList{}

	err = mp.store(msg)
	snap.Close()
	require.NoError(t, err)
	snapshotPath := path.Join(mp.config.RootDir, snapshotDir)
	err = partition.LoadSnapshot(snapshotPath)
	require.NoError(t, err)

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)

	// add data to mp
	ino := NewInode(0, 0)
	err = mp.inodeTree.Put(handle, ino)
	require.NoError(t, err)
	dentry := &Dentry{}
	err = mp.dentryTree.Put(handle, dentry)
	require.NoError(t, err)
	extend := &Extend{}
	err = mp.extendTree.Put(handle, extend)
	require.NoError(t, err)

	multipart := &Multipart{
		id:       "id",
		key:      "key",
		initTime: time.Unix(0, 0),
		parts:    Parts{},
		extend:   MultipartExtend{},
	}
	err = mp.multipartTree.Put(handle, multipart)
	require.NoError(t, err)

	dek := NewDeletedExtentKey(&proto.ExtentKey{
		PartitionId:  0,
		ExtentId:     0,
		ExtentOffset: 0,
		FileOffset:   0,
	}, 0, mp.AllocDeletedExtentId())
	err = mp.deletedExtentsTree.Put(handle, dek)
	require.NoError(t, err)

	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	snap, err = mp.GetSnapShot()
	require.NoError(t, err)
	require.NotNil(t, snap)
	msg = &storeMsg{
		command:     1,
		snap:        snap,
		uniqId:      mp.GetUniqId(),
		uniqChecker: mp.uniqChecker,
	}
	err = mp.store(msg)
	snap.Close()
	require.Nil(t, err)
	snapshotPath = path.Join(mp.config.RootDir, snapshotDir)
	err = partition.LoadSnapshot(snapshotPath)
	require.Nil(t, err)
	require.EqualValues(t, 1, mp.deletedExtentsTree.Count())

	// remove inode file
	os.Rename(path.Join(snapshotPath, inodeFile), path.Join(snapshotPath, inodeFile+"1"))
	err = partition.LoadSnapshot(snapshotPath)
	require.Error(t, err)
	os.Rename(path.Join(snapshotPath, inodeFile+"1"), path.Join(snapshotPath, inodeFile))

	// remove dentry file
	os.Rename(path.Join(snapshotPath, dentryFile), path.Join(snapshotPath, dentryFile+"1"))
	err = partition.LoadSnapshot(snapshotPath)
	require.Error(t, err)
	os.Rename(path.Join(snapshotPath, dentryFile+"1"), path.Join(snapshotPath, dentryFile))

	// modify crc file
	crcData, err := os.ReadFile(path.Join(snapshotPath, SnapshotSign))
	require.Nil(t, err)
	require.True(t, len(crcData) != 0)
	crcData[0] = '0'
	crcData[1] = '1'
	err = os.WriteFile(path.Join(snapshotPath, SnapshotSign), crcData, 0o644)
	require.Nil(t, err)
	err = partition.LoadSnapshot(snapshotPath)
	require.Equal(t, ErrSnapshotCrcMismatch, err)
}

func prepareDataForMpTest(t *testing.T, mp *metaPartition) {
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)

	ino := NewInode(0, DirModeType)
	err = mp.inodeTree.Put(handle, ino)
	require.NoError(t, err)

	den := &Dentry{
		ParentId: 0,
		Name:     "test",
		Inode:    1,
	}
	err = mp.dentryTree.Put(handle, den)
	require.NoError(t, err)

	err = mp.extendTree.Put(handle, &Extend{})
	require.NoError(t, err)

	err = mp.multipartTree.Put(handle, &Multipart{})
	require.NoError(t, err)

	err = mp.txProcessor.txManager.txTree.Put(handle, proto.NewTransactionInfo(0, 0))
	require.NoError(t, err)

	err = mp.txProcessor.txResource.txRbInodeTree.Put(handle, NewTxRollbackInode(ino, []uint32{}, proto.NewTxInodeInfo("", 0, 0), 0))
	require.NoError(t, err)

	err = mp.txProcessor.txResource.txRbDentryTree.Put(handle, NewTxRollbackDentry(den, proto.NewTxDentryInfo("", 0, "", 0), 0))
	require.NoError(t, err)

	err = mp.deletedExtentsTree.Put(handle, &DeletedExtentKey{})
	require.NoError(t, err)

	err = mp.deletedObjExtentsTree.Put(handle, &DeletedObjExtentKey{})
	require.NoError(t, err)

	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, true)
	require.NoError(t, err)
}

func checkTreeCntForMpTest(t *testing.T, mp *metaPartition) {
	cnt, err := mp.GetInodeRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt, err = mp.GetDentryRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt, err = mp.GetExtendRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt, err = mp.GetMultipartRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt, err = mp.GetTxRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt, err = mp.GetTxRbInodeRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt, err = mp.GetTxRbDentryRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt, err = mp.GetDeletedExtentsRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt, err = mp.GetDeletedObjExtentsRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)
}

func TestMultiPartitionOnDisk(t *testing.T) {
	dbManager := NewRocksdbManager()
	dbDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	t.Logf("db dir is %v", dbDir)
	err = dbManager.Register(dbDir)
	require.NoError(t, err)

	mp1C := MetaPartitionConfig{
		PartitionId:   1,
		VolName:       "test_vol",
		Start:         0,
		End:           100,
		PartitionType: 1,
		Peers:         nil,
		RootDir:       "",
		StoreMode:     proto.StoreModeRocksDb,
		RocksDBDir:    dbDir,
	}
	mp2C := mp1C
	mp2C.PartitionId = 2
	metaM := &metadataManager{
		nodeId:         1,
		zoneName:       "test",
		raftStore:      nil,
		partitions:     make(map[uint64]MetaPartition),
		metaNode:       &MetaNode{},
		rocksdbManager: dbManager,
	}
	partition := NewMetaPartition(&mp1C, metaM)
	require.NotNil(t, partition)
	mp1 := partition.(*metaPartition)
	partition = NewMetaPartition(&mp2C, metaM)
	require.NotNil(t, partition)
	mp2 := partition.(*metaPartition)

	err = mp1.initObjects(true)
	require.NoError(t, err)
	err = mp2.initObjects(true)
	require.NoError(t, err)

	prepareDataForMpTest(t, mp1)
	prepareDataForMpTest(t, mp2)

	checkTreeCntForMpTest(t, mp1)
	checkTreeCntForMpTest(t, mp2)

	mp2.Clear()

	checkTreeCntForMpTest(t, mp1)
}

func getSSTCountForPartitionTest(t *testing.T, dir string) (count int) {
	dentries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, dentry := range dentries {
		if strings.HasSuffix(dentry.Name(), ".sst") {
			count++
		}
	}
	return
}

func TestLoadAndStoreMetaPartition(t *testing.T) {
	dbManager := NewRocksdbManager()
	dbDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	t.Logf("db dir is %v", dbDir)
	err = dbManager.Register(dbDir)
	require.NoError(t, err)

	mpC := MetaPartitionConfig{
		PartitionId:   1,
		VolName:       "test_vol",
		Start:         0,
		End:           100,
		PartitionType: 1,
		Peers:         nil,
		RootDir:       "",
		StoreMode:     proto.StoreModeRocksDb,
		RocksDBDir:    dbDir,
	}

	metaM := &metadataManager{
		nodeId:         1,
		zoneName:       "test",
		raftStore:      nil,
		partitions:     make(map[uint64]MetaPartition),
		metaNode:       &MetaNode{},
		rocksdbManager: dbManager,
	}
	partition := NewMetaPartition(&mpC, metaM)
	require.NotNil(t, partition)
	mp := partition.(*metaPartition)

	err = mp.initObjects(true)
	require.NoError(t, err)

	prepareDataForMpTest(t, mp)

	checkTreeCntForMpTest(t, mp)

	count := getSSTCountForPartitionTest(t, dbDir)
	require.EqualValues(t, 0, count)

	snap, err := mp.GetSnapShot()
	require.NoError(t, err)
	require.NotNil(t, snap)
	msg := &storeMsg{
		command:     1,
		snap:        snap,
		uniqId:      mp.GetUniqId(),
		uniqChecker: mp.uniqChecker,
	}
	err = mp.store(msg)
	snap.Close()
	require.NoError(t, err)

	count = getSSTCountForPartitionTest(t, dbDir)
	require.NotEqualValues(t, 0, count)
}

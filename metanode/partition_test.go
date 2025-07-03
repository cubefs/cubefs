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
	"encoding/json"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/fileutil"
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
		nodeId:          1,
		zoneName:        "test",
		raftStore:       nil,
		partitions:      make(map[uint64]MetaPartition),
		metaNode:        &MetaNode{},
		fileStatsConfig: &fileStatsConfig{},
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
	require.Nil(t, err)

	// add data to mp
	ino := NewInode(0, 0)
	ino.StorageClass = proto.StorageClass_Replica_HDD
	mp.inodeTree.ReplaceOrInsert(ino, true)
	dentry := &Dentry{}
	mp.dentryTree.ReplaceOrInsert(dentry, true)
	extend := &Extend{}
	mp.extendTree.ReplaceOrInsert(extend, true)

	multipart := &Multipart{
		id:       "id",
		key:      "key",
		initTime: time.Unix(0, 0),
		parts:    Parts{},
		extend:   MultipartExtend{},
	}
	mp.multipartTree.ReplaceOrInsert(multipart, true)

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
	err = fileutil.WriteFileWithSync(path.Join(snapshotPath, SnapshotSign), crcData, 0o644)
	require.Nil(t, err)
	err = partition.LoadSnapshot(snapshotPath)
	require.Equal(t, ErrSnapshotCrcMismatch, err)
}

func TestMetaPartition_LoadHybridCloudMigrationSnapshot(t *testing.T) {
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
		nodeId:          1,
		zoneName:        "test",
		raftStore:       nil,
		partitions:      make(map[uint64]MetaPartition),
		metaNode:        &MetaNode{},
		fileStatsConfig: &fileStatsConfig{},
	}

	partition := NewMetaPartition(mpC, metaM)
	require.NotNil(t, partition)
	mp, ok := partition.(*metaPartition)
	err := mp.initObjects(true)
	if err != nil {
		panic(err)
	}
	require.True(t, ok)
	ino := NewInode(2, 0)
	ino.StorageClass = proto.StorageClass_BlobStore
	ino.HybridCloudExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{
			Size: uint64(1024), FileOffset: uint64(0), BlobSize: 4194304, BlobsLen: 1,
			Blobs: []proto.Blob{{Count: 1, MinBid: 30138734, Vid: 525}},
		}})
	ino.HybridCloudExtentsMigration.storageClass = proto.StorageClass_Replica_SSD
	ino.HybridCloudExtentsMigration.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 0, PartitionId: 164,
		ExtentId: 55, ExtentOffset: 0, Size: 1024, CRC: 0,
	}})
	mp.inodeTree.ReplaceOrInsert(ino, true)
	// dentry := &Dentry{}
	// mp.dentryTree.ReplaceOrInsert(dentry, true)
	// extend := &Extend{}
	// mp.extendTree.ReplaceOrInsert(extend, true)
	// multipart := &Multipart{}
	// mp.multipartTree.ReplaceOrInsert(multipart, true)
	snap, err := mp.GetSnapShot()
	require.NoError(t, err)
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
	require.Nil(t, err)
	snapshotPath := path.Join(mp.config.RootDir, snapshotDir)
	err = partition.LoadSnapshot(snapshotPath)
	require.Nil(t, err)
}

func prepareDataForMpTest(t *testing.T, mp *metaPartition) {
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)

	ino := NewInode(0, DirModeType)
	err = mp.inodeTree.BatchPut(handle, ino)
	require.NoError(t, err)

	den := &Dentry{
		ParentId: 0,
		Name:     "test",
		Inode:    1,
	}
	err = mp.dentryTree.BatchPut(handle, den)
	require.NoError(t, err)

	err = mp.extendTree.BatchPut(handle, &Extend{})
	require.NoError(t, err)

	err = mp.multipartTree.BatchPut(handle, &Multipart{})
	require.NoError(t, err)

	err = mp.txProcessor.txManager.txTree.BatchPut(handle, proto.NewTransactionInfo(0, 0))
	require.NoError(t, err)

	err = mp.txProcessor.txResource.txRbInodeTree.BatchPut(handle, NewTxRollbackInode(ino, []uint32{}, proto.NewTxInodeInfo("", 0, 0), 0))
	require.NoError(t, err)

	err = mp.txProcessor.txResource.txRbDentryTree.BatchPut(handle, NewTxRollbackDentry(den, proto.NewTxDentryInfo("", 0, "", 0), 0))
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
}

func TestMultiPartitionOnDisk(t *testing.T) {
	dbManager := NewPerDiskRocksdbManager(0, 0, 0, 0, 0)
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
	os.RemoveAll(mp1.config.RocksDBDir)
	os.RemoveAll(mp2.config.RocksDBDir)
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
	dbManager := NewPerDiskRocksdbManager(0, 0, 0, 0, 0)
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
	os.RemoveAll(mp.config.RocksDBDir)
}

func TestDoFileStats(t *testing.T) {
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
		nodeId:          1,
		zoneName:        "test",
		raftStore:       nil,
		partitions:      make(map[uint64]MetaPartition),
		metaNode:        &MetaNode{},
		fileStatsConfig: &fileStatsConfig{},
	}
	metaM.initFileStatsConfig()

	partition := NewMetaPartition(mpC, metaM)
	require.NotNil(t, partition)
	mp, ok := partition.(*metaPartition)
	require.True(t, ok)

	err := mp.initObjects(true)
	require.NoError(t, err)

	for i := 0; i < 10000000; i++ {
		ino := NewInode(uint64(i), 0)
		_, _, err = mp.inodeTree.ReplaceOrInsert(ino, true)
		require.NoError(t, err)
	}

	startTime := time.Now()
	mp.doFileStats(metaM.fileStatsConfig.thresholds)
	duration := time.Since(startTime)
	t.Logf("DoFileStats cost time %v", duration)
	require.Equal(t, 10000000, int(mp.fileRange[0]))
}

func TestLimitReadDir(t *testing.T) {
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
		nodeId:          1,
		zoneName:        "test",
		raftStore:       nil,
		partitions:      make(map[uint64]MetaPartition),
		metaNode:        &MetaNode{qosEnable: true},
		fileStatsConfig: &fileStatsConfig{},
		limitFactor:     make(map[uint32]*rate.Limiter),
	}
	metaM.limitFactor[readDirIops] = rate.NewLimiter(rate.Limit(2), 10)

	partition := NewMetaPartition(mpC, metaM)
	require.NotNil(t, partition)
	mp, ok := partition.(*metaPartition)
	require.True(t, ok)
	err := mp.initObjects(true)
	require.NoError(t, err)
	t.Logf("readDirIops:%v", mp.manager.limitFactor[readDirIops].Limit())

	req := &ReadDirLimitReq{
		PartitionID: partitionId,
		VolName:     mp.GetVolName(),
		ParentID:    1,
		Limit:       math.MaxUint64,
		VerSeq:      0,
	}

	const totalRequests = 20
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				err := mp.manager.allocCheckLimit(readDirIops)
				if err == TryAgainError {
					time.Sleep(time.Millisecond)
					continue
				}
				resp, _ := mp.readDirLimit(req)
				_, err = json.Marshal(resp)
				if err != nil {
					t.Errorf("readDir err: %v", err)
				}
				return
			}
		}()
	}

	wg.Wait()
	costTime1 := time.Since(start)

	t.Logf("costTime1: %v", costTime1)

	mp.manager.limitFactor[readDirIops].SetLimit(rate.Limit(10))
	t.Logf("readDirIops:%v", mp.manager.limitFactor[readDirIops].Limit())
	// mp.manager.metaNode.qosEnable = true
	start = time.Now()

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				err := mp.manager.allocCheckLimit(readDirIops)
				if err == TryAgainError {
					time.Sleep(time.Millisecond)
					continue
				}
				resp, _ := mp.readDirLimit(req)
				_, err = json.Marshal(resp)
				if err != nil {
					t.Errorf("readDir err: %v", err)
				}
				return
			}
		}()
	}
	wg.Wait()
	costTime2 := time.Since(start)
	t.Logf("costTime2: %v", costTime2)

	require.True(t, costTime1 > costTime2)
}

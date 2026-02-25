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
		applyIndex:  mp.GetAppliedID(),
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

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	// add data to mp
	ino := NewInode(0, 0)
	ino.PoolId = proto.DefaultHDDPoolId
	ino.StorageClass = proto.StorageClass_Replica_HDD
	mp.inodeTree.ReplaceOrInsert(handle, ino, true)
	dentry := &Dentry{}
	mp.dentryTree.ReplaceOrInsert(handle, dentry, true)
	extend := &Extend{}
	mp.extendTree.ReplaceOrInsert(handle, extend, true)

	multipart := &Multipart{
		id:       "id",
		key:      "key",
		initTime: time.Unix(0, 0),
		parts:    Parts{},
		extend:   MultipartExtend{},
	}
	mp.multipartTree.ReplaceOrInsert(handle, multipart, true)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	snap, err = mp.GetSnapShot()
	require.NoError(t, err)
	require.NotNil(t, snap)
	msg = &storeMsg{
		command:     1,
		snap:        snap,
		applyIndex:  mp.GetAppliedID(),
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
	ino.PoolId = proto.DefaultECPoolId
	ino.HybridCloudExtents.sortedEks = NewSortedObjExtentsFromObjEks(
		[]proto.ObjExtentKey{{
			Size: uint64(1024), FileOffset: uint64(0), BlobSize: 4194304, BlobsLen: 1,
			Blobs: []proto.Blob{{Count: 1, MinBid: 30138734, Vid: 525}},
		}})
	ino.HybridCloudExtentsMigration.storageClass = proto.StorageClass_Replica_SSD
	ino.HybridCloudExtentsMigration.poolId = proto.DefaultSSDPoolId
	ino.HybridCloudExtentsMigration.sortedEks = NewSortedExtentsFromEks([]proto.ExtentKey{{
		FileOffset: 0, PartitionId: 164,
		ExtentId: 55, ExtentOffset: 0, Size: 1024, CRC: 0,
	}})
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	mp.inodeTree.ReplaceOrInsert(handle, ino, true)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
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
	ino.PoolId = proto.DefaultHDDPoolId
	_, _, err = mp.inodeTree.ReplaceOrInsert(handle, ino, true)
	require.NoError(t, err)

	den := &Dentry{
		ParentId: 0,
		Name:     "test",
		Inode:    1,
	}
	_, _, err = mp.dentryTree.ReplaceOrInsert(handle, den, true)
	require.NoError(t, err)

	_, _, err = mp.extendTree.ReplaceOrInsert(handle, &Extend{}, true)
	require.NoError(t, err)

	_, _, err = mp.multipartTree.ReplaceOrInsert(handle, &Multipart{}, true)
	require.NoError(t, err)

	_, _, err = mp.txProcessor.txManager.txTree.ReplaceOrInsert(handle, proto.NewTransactionInfo(0, 0), true)
	require.NoError(t, err)

	_, _, err = mp.txProcessor.txResource.txRbInodeTree.ReplaceOrInsert(handle, NewTxRollbackInode(ino, []uint32{}, proto.NewTxInodeInfo("", 0, 0), 0), true)
	require.NoError(t, err)

	_, _, err = mp.txProcessor.txResource.txRbDentryTree.ReplaceOrInsert(handle, NewTxRollbackDentry(den, proto.NewTxDentryInfo("", 0, "", 0), 0), true)
	require.NoError(t, err)

	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, true)
	require.NoError(t, err)
}

func checkTreeCntForMpTest(t *testing.T, mp *metaPartition) {
	snap, err := mp.GetSnapShot()
	if err != nil {
		return
	}
	defer snap.Close()

	cnt := 0
	err = snap.Range(InodeType, func(item interface{}) bool {
		cnt++
		return true
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt = 0
	err = snap.Range(DentryType, func(item interface{}) bool {
		cnt++
		return true
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt = 0
	err = snap.Range(ExtendType, func(item interface{}) bool {
		cnt++
		return true
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt = 0
	err = snap.Range(MultipartType, func(item interface{}) bool {
		cnt++
		return true
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt = 0
	err = snap.Range(TransactionType, func(item interface{}) bool {
		cnt++
		return true
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt = 0
	err = snap.Range(TransactionRollbackInodeType, func(item interface{}) bool {
		cnt++
		return true
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)

	cnt = 0
	err = snap.Range(TransactionRollbackDentryType, func(item interface{}) bool {
		cnt++
		return true
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)
}

func TestMultiPartitionOnDisk(t *testing.T) {
	dbManager := NewPerDiskRocksdbManager(&RocksdbManagerConfig{})
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
	dbManager := NewPerDiskRocksdbManager(&RocksdbManagerConfig{})
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
		RootDir:       dbDir,
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
		fileStatsConfig: &fileStatsConfig{
			thresholds: []uint64{},
		},
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

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	for i := 0; i < 10000000; i++ {
		ino := NewInode(uint64(i), 0)
		ino.PoolId = proto.DefaultHDDPoolId
		_, _, err = mp.inodeTree.ReplaceOrInsert(handle, ino, true)
		require.NoError(t, err)
	}
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

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

func TestUpdateSizeLoopFunc(t *testing.T) {
	// prepare manager with file stats enabled and simple thresholds
	metaM := &metadataManager{
		fileStatsConfig: &fileStatsConfig{},
		metaNode:        &MetaNode{},
	}
	metaM.initFileStatsConfig()
	metaM.fileStatsConfig.thresholds = []uint64{50, 200}
	metaM.fileStatsConfig.fileStatsEnable = true

	mpC := &MetaPartitionConfig{
		PartitionId:   100,
		VolName:       "test_vol",
		Start:         0,
		End:           1000,
		PartitionType: 1,
		Peers:         nil,
		RootDir:       t.TempDir(),
		StoreMode:     proto.StoreModeMem,
	}

	partition := NewMetaPartition(mpC, metaM)
	require.NotNil(t, partition)
	mp := partition.(*metaPartition)
	require.NoError(t, mp.initObjects(true))
	// uidManager required by updateSizeLoopFunc
	mp.uidManager = NewUidMgr(mpC.VolName, mpC.PartitionId)

	// insert inodes
	h, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	// inode A: normal, size=100 -> fileRange bucket index 1
	inoA := NewInode(1, 0)
	inoA.Type = 0 // regular
	inoA.NLink = 1
	inoA.Size = 100
	inoA.PoolId = proto.DefaultSSDPoolId
	inoA.StorageClass = proto.StorageClass_Replica_SSD
	_, _, err = mp.inodeTree.ReplaceOrInsert(h, inoA, true)
	require.NoError(t, err)
	// inode B: normal + migration, size=300 -> fileRange bucket index 2
	inoB := NewInode(2, 0)
	inoB.Type = 0 // regular
	inoB.NLink = 1
	inoB.Size = 300
	inoB.StorageClass = proto.StorageClass_Replica_SSD
	inoB.HybridCloudExtentsMigration = &SortedHybridCloudExtentsMigration{}
	inoB.HybridCloudExtentsMigration.storageClass = proto.StorageClass_Replica_HDD
	inoB.HybridCloudExtentsMigration.poolId = proto.DefaultHDDPoolId
	inoB.HybridCloudExtentsMigration.sortedEks = NewSortedExtents()
	_, _, err = mp.inodeTree.ReplaceOrInsert(h, inoB, true)
	require.NoError(t, err)
	require.NoError(t, mp.inodeTree.CommitAndReleaseBatchWriteHandle(h, false))

	// run
	require.NoError(t, mp.updateSizeLoopFunc())

	// verify mp.size equals total migrate size (only inodeB has migration)
	require.EqualValues(t, uint64(300), mp.size)

	// verify statByStorageClass (Replica_SSD): 2 inodes, 100+300 bytes
	var norm *proto.StatOfStorageClass
	for _, st := range mp.statByStorageClass {
		if st.StorageClass == proto.StorageClass_Replica_SSD {
			norm = st
			break
		}
	}
	if norm == nil {
		t.Fatalf("missing normal storage class stats")
	}
	require.EqualValues(t, 2, norm.InodeCount)
	require.EqualValues(t, 400, norm.UsedSizeBytes)

	// verify statByMigrateStorageClass (Replica_HDD): 1 inode, 300 bytes
	var mig *proto.StatOfStorageClass
	for _, st := range mp.statByMigrateStorageClass {
		if st.StorageClass == proto.StorageClass_Replica_HDD {
			mig = st
			break
		}
	}
	if mig == nil {
		t.Fatalf("missing migrate storage class stats")
	}
	require.EqualValues(t, 1, mig.InodeCount)
	require.EqualValues(t, 300, mig.UsedSizeBytes)

	// verify fileRange buckets: thresholds [50,200] => 3 buckets
	require.EqualValues(t, 3, len(mp.fileRange))
	require.EqualValues(t, 0, mp.fileRange[0]) // <50
	require.EqualValues(t, 1, mp.fileRange[2]) // >=200
}

func TestScanRocksdb(t *testing.T) {
	// Setup temporary rocksdb root
	rootDir := t.TempDir()
	// Use per-partition rocksdb manager to create a db per metaPartition
	mgr := NewPerPartitionRocksdbManager(&RocksdbManagerConfig{})
	if err := mgr.Register(rootDir); err != nil {
		t.Fatalf("register rocksdb root error:%v", err)
	}

	mpC := &MetaPartitionConfig{
		PartitionId:   123,
		VolName:       "vol",
		Start:         0,
		End:           1000,
		PartitionType: 1,
		Peers:         nil,
		RootDir:       rootDir,
		StoreMode:     proto.StoreModeRocksDb,
		RocksDBDir:    rootDir,
	}
	metaM := &metadataManager{rocksdbManager: mgr, fileStatsConfig: &fileStatsConfig{}, metaNode: &MetaNode{}}
	metaM.initFileStatsConfig()
	// make file stats deterministic
	metaM.fileStatsConfig.thresholds = []uint64{10}
	metaM.fileStatsConfig.fileStatsEnable = true

	partition := NewMetaPartition(mpC, metaM)
	mp := partition.(*metaPartition)
	if err := mp.initObjects(true); err != nil {
		t.Fatalf("initObjects error:%v", err)
	}

	// seed a few inodes into rocksdb trees via mem API (rocks mode trees are set)
	h, err := mp.inodeTree.CreateBatchWriteHandle()
	if err != nil {
		t.Fatalf("CreateBatchWriteHandle error:%v", err)
	}
	// ino1: id=5, size=5 (<10), regular
	ino1 := NewInode(5, 0)
	ino1.PoolId = proto.DefaultSSDPoolId
	ino1.NLink = 1
	ino1.Size = 5
	ino1.StorageClass = proto.StorageClass_Replica_SSD
	ino1.PoolId = proto.DefaultSSDPoolId
	_, _, err = mp.inodeTree.ReplaceOrInsert(h, ino1, true)
	if err != nil {
		t.Fatalf("insert ino1 error:%v", err)
	}
	// ino2: id=20, size=20 (>=10), regular
	ino2 := NewInode(20, 0)
	ino2.NLink = 1
	ino2.Size = 20
	ino2.StorageClass = proto.StorageClass_Replica_SSD
	ino2.PoolId = proto.DefaultSSDPoolId
	_, _, err = mp.inodeTree.ReplaceOrInsert(h, ino2, true)
	if err != nil {
		t.Fatalf("insert ino2 error:%v", err)
	}
	if err := mp.inodeTree.CommitAndReleaseBatchWriteHandle(h, false); err != nil {
		t.Fatalf("commit error:%v", err)
	}

	// Run scan: it relies on loadRocksdbInode and loadRocksdbExtent
	if err := mp.ScanRocksdb(); err != nil {
		t.Fatalf("ScanRocksdb error:%v", err)
	}

	// cursor should be updated to max inode id (20)
	if got := mp.GetCursor(); got != 20 {
		t.Fatalf("cursor mismatch, expect:20 actual:%v", got)
	}
	// mp.size should accumulate sizes
	if mp.size == 0 {
		t.Fatalf("size should be accumulated, got 0")
	}
	// fileRange buckets [<10, >=10] should reflect two inodes
	if len(mp.fileRange) != 2 {
		t.Fatalf("fileRange length mismatch, expect:2 actual:%d", len(mp.fileRange))
	}
	if mp.fileRange[0] != 1 || mp.fileRange[1] != 1 {
		t.Fatalf("fileRange content mismatch, expect:[1,1] actual:%v", mp.fileRange)
	}
}

func TestLoadDataFromRocksDb(t *testing.T) {
	// Setup temporary rocksdb root
	rootDir := t.TempDir()
	// Use per-partition rocksdb manager to create a db per metaPartition
	mgr := NewPerPartitionRocksdbManager(&RocksdbManagerConfig{})
	if err := mgr.Register(rootDir); err != nil {
		t.Fatalf("register rocksdb root error:%v", err)
	}

	mpC := &MetaPartitionConfig{
		PartitionId:   124,
		VolName:       "vol",
		Start:         0,
		End:           1000,
		PartitionType: 1,
		Peers:         nil,
		RootDir:       rootDir,
		StoreMode:     proto.StoreModeRocksDb,
		RocksDBDir:    rootDir,
	}
	metaM := &metadataManager{rocksdbManager: mgr, metaNode: &MetaNode{}}
	wrapperPartition := NewMetaPartition(mpC, metaM)
	wrapperMp := wrapperPartition.(*metaPartition)
	if err := wrapperMp.initObjects(true); err != nil {
		t.Fatalf("initObjects error:%v", err)
	}

	if err := wrapperMp.LoadDataFromRocksDb(); err != nil {
		t.Fatalf("LoadDataFromRocksDb error:%v", err)
	}

	if got := wrapperMp.txProcessor.txManager.txIdAlloc.getTransactionID(); got != 0 {
		t.Fatalf("txIdAlloc mismatch, expect:0 actual:%v", got)
	}
	if got := wrapperMp.GetUniqId(); got != 0 {
		t.Fatalf("UniqId mismatch, expect:0 actual:%v", got)
	}
}

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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	require.True(t, ok)
	msg := &storeMsg{
		command:        1,
		applyIndex:     0,
		txId:           mp.txProcessor.txManager.txIdAlloc.getTransactionID(),
		inodeTree:      mp.inodeTree,
		dentryTree:     mp.dentryTree,
		extendTree:     mp.extendTree,
		multipartTree:  mp.multipartTree,
		txTree:         mp.txProcessor.txManager.txTree,
		txRbInodeTree:  mp.txProcessor.txResource.txRbInodeTree,
		txRbDentryTree: mp.txProcessor.txResource.txRbDentryTree,
		uniqId:         mp.GetUniqId(),
		uniqChecker:    mp.uniqChecker,
	}
	mp.uidManager = NewUidMgr(mpC.VolName, mpC.PartitionId)
	mp.mqMgr = NewQuotaManager(mpC.VolName, mpC.PartitionId)
	mp.multiVersionList = &proto.VolVersionInfoList{}

	err := mp.store(msg)
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

	msg = &storeMsg{
		command:        1,
		applyIndex:     0,
		txId:           mp.txProcessor.txManager.txIdAlloc.getTransactionID(),
		inodeTree:      mp.inodeTree,
		dentryTree:     mp.dentryTree,
		extendTree:     mp.extendTree,
		multipartTree:  mp.multipartTree,
		txTree:         mp.txProcessor.txManager.txTree,
		txRbInodeTree:  mp.txProcessor.txResource.txRbInodeTree,
		txRbDentryTree: mp.txProcessor.txResource.txRbDentryTree,
		uniqId:         mp.GetUniqId(),
		uniqChecker:    mp.uniqChecker,
	}
	err = mp.store(msg)
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
	mp, ok := partition.(*metaPartition)
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
	msg := &storeMsg{
		command:        1,
		applyIndex:     0,
		txId:           mp.txProcessor.txManager.txIdAlloc.getTransactionID(),
		inodeTree:      mp.inodeTree,
		dentryTree:     mp.dentryTree,
		extendTree:     mp.extendTree,
		multipartTree:  mp.multipartTree,
		txTree:         mp.txProcessor.txManager.txTree,
		txRbInodeTree:  mp.txProcessor.txResource.txRbInodeTree,
		txRbDentryTree: mp.txProcessor.txResource.txRbDentryTree,
		uniqId:         mp.GetUniqId(),
		uniqChecker:    mp.uniqChecker,
	}
	mp.uidManager = NewUidMgr(mpC.VolName, mpC.PartitionId)
	mp.mqMgr = NewQuotaManager(mpC.VolName, mpC.PartitionId)
	mp.multiVersionList = &proto.VolVersionInfoList{}
	err := mp.store(msg)
	require.Nil(t, err)
	snapshotPath := path.Join(mp.config.RootDir, snapshotDir)
	err = partition.LoadSnapshot(snapshotPath)
	require.Nil(t, err)
}

// Copyright 2023 The CubeFS Authors.
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
	"fmt"
	"math/rand"
	"net"
	"os"
	"path"
	"sort"
	"testing"
	"time"

	raftstoremock "github.com/cubefs/cubefs/metanode/mocktest/raftstore"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/synclist"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func getMpConfigForDelExtTest(storeMode proto.StoreMode) (config *MetaPartitionConfig) {
	config = &MetaPartitionConfig{
		PartitionId:   10001,
		VolName:       VolNameForTest,
		PartitionType: proto.VolumeTypeHot,
		StoreMode:     storeMode,
	}
	if config.StoreMode == proto.StoreModeRocksDb {
		config.RocksDBDir = fmt.Sprintf("%v/%v_%v", "/tmp/cfs/del_ext_test", partitionId, time.Now().UnixMilli())
		os.RemoveAll(config.RocksDBDir)
	}
	return
}

func newMpForDelExtTest(t *testing.T, storeMode proto.StoreMode) (mp *metaPartition) {
	config := getMpConfigForDelExtTest(storeMode)
	mp = newPartition(config, newManager())
	return
}

func mockPartitionRaftForDelExtTest(t *testing.T, ctrl *gomock.Controller, storeMode proto.StoreMode) *metaPartition {
	partition := newMpForDelExtTest(t, storeMode)
	raft := raftstoremock.NewMockPartition(ctrl)
	idx := uint64(0)
	raft.EXPECT().Submit(gomock.Any()).DoAndReturn(func(cmd []byte) (resp interface{}, err error) {
		idx++
		return partition.Apply(cmd, idx)
	}).AnyTimes()

	raft.EXPECT().IsRaftLeader().DoAndReturn(func() bool {
		return true
	}).AnyTimes()

	raft.EXPECT().LeaderTerm().Return(uint64(1), uint64(1)).AnyTimes()
	partition.raftPartition = raft
	return partition
}

func testLoadFromDelExtentFile(t *testing.T, mp *metaPartition) {
	const testEkCountPerFile = 10000
	const testFileCount = 5
	const testEkCount = testFileCount * testEkCountPerFile

	rootDir := fmt.Sprintf("/tmp/cfs/test_del_exts/%v_%v", mp.config.PartitionId, time.Now().UnixMilli())
	os.RemoveAll(rootDir)
	defer os.RemoveAll(rootDir)
	err := os.MkdirAll(rootDir, 0755)
	require.NoError(t, err)

	mp.config.RootDir = rootDir

	expectedEks := make([]*proto.ExtentKey, 0)
	randGen := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < testEkCount; i++ {
		ek := &proto.ExtentKey{
			PartitionId:  randGen.Uint64(),
			ExtentId:     randGen.Uint64(),
			ExtentOffset: randGen.Uint64(),
		}
		expectedEks = append(expectedEks, ek)
	}

	mockList := synclist.New()
	for i := 0; i < testFileCount; i++ {
		fp, _, _, err := mp.createExtentDeleteFile(prefixDelExtentV2, int64(i), mockList)
		require.NoError(t, err)
		for j := 0; j < testEkCountPerFile; j++ {
			ek := expectedEks[i*testEkCountPerFile+j]
			buf, err := ek.MarshalBinaryWithCheckSum(true)
			require.NoError(t, err)
			_, err = fp.Write(buf)
			require.NoError(t, err)
		}
	}
	expectedFiles := make([]string, 0)
	for mockList.Front() != nil {
		tmp := mockList.Front()
		expectedFiles = append(expectedFiles, tmp.Value.(string))
		mockList.Remove(tmp)
	}
	sort.Slice(expectedFiles, func(i, j int) bool {
		return getDelExtFileIdx(expectedFiles[i]) < getDelExtFileIdx(expectedFiles[j])
	})

	mp.sortExtentsForMove(expectedEks)
	handle, err := mp.deletedExtentsTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	files, err := mp.moveDelExtentToTree(handle)
	require.NoError(t, err)
	err = mp.deletedExtentsTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	eks := make([]*proto.ExtentKey, 0)
	snap, err := mp.GetSnapShot()
	require.NoError(t, err)
	defer snap.Close()
	snap.Range(DeletedExtentsType, func(item interface{}) (bool, error) {
		dek := item.(*DeletedExtentKey)
		eks = append(eks, &dek.ExtentKey)
		return true, nil
	})

	require.EqualValues(t, len(expectedFiles), len(files))
	require.ElementsMatch(t, expectedFiles, files)
	require.EqualValues(t, len(expectedEks), len(eks))
	require.ElementsMatch(t, expectedEks, eks)
}

func TestLoadFromDelExtentFile(t *testing.T) {
	mp := newMpForDelExtTest(t, proto.StoreModeMem)
	testLoadFromDelExtentFile(t, mp)
}

func TestLoadFromDelExtentFile_Rocksdb(t *testing.T) {
	mp := newMpForDelExtTest(t, proto.StoreModeRocksDb)
	testLoadFromDelExtentFile(t, mp)
}

func testDelExtUpgradeMp(t *testing.T, mp *metaPartition) {
	const testEkCountPerFile = 10000
	const testFileCount = 5
	const testEkCount = testFileCount * testEkCountPerFile

	rootDir := fmt.Sprintf("/tmp/cfs/test_del_exts/%v_%v", mp.config.PartitionId, time.Now().UnixMilli())
	os.RemoveAll(rootDir)
	defer os.RemoveAll(rootDir)
	err := os.MkdirAll(rootDir, 0755)
	require.NoError(t, err)

	mp.config.RootDir = rootDir

	expectedEks := make([]*proto.ExtentKey, 0)
	randGen := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < testEkCount; i++ {
		ek := &proto.ExtentKey{
			PartitionId:  randGen.Uint64(),
			ExtentId:     randGen.Uint64(),
			ExtentOffset: randGen.Uint64(),
		}
		expectedEks = append(expectedEks, ek)
	}

	mockList := synclist.New()
	for i := 0; i < testFileCount; i++ {
		fp, _, _, err := mp.createExtentDeleteFile(prefixDelExtentV2, int64(i), mockList)
		require.NoError(t, err)
		for j := 0; j < testEkCountPerFile; j++ {
			ek := expectedEks[i*testEkCountPerFile+j]
			buf, err := ek.MarshalBinaryWithCheckSum(true)
			require.NoError(t, err)
			_, err = fp.Write(buf)
			require.NoError(t, err)
		}
	}
	expectedFiles := make([]string, 0)
	for mockList.Front() != nil {
		tmp := mockList.Front()
		expectedFiles = append(expectedFiles, tmp.Value.(string))
		mockList.Remove(tmp)
	}
	sort.Slice(expectedFiles, func(i, j int) bool {
		return getDelExtFileIdx(expectedFiles[i]) < getDelExtFileIdx(expectedFiles[j])
	})

	// NOTE: change leader
	mp.HandleLeaderChange(mp.GetBaseConfig().NodeId)

	time.Sleep(3 * time.Second)
	require.NotEqualValues(t, 0, mp.GetDeletedExtentId())
	count, err := mp.GetDeletedExtentsRealCount()
	require.NoError(t, err)
	require.NotEqualValues(t, 0, count)

	for _, file := range expectedFiles {
		filePath := path.Join(mp.config.RootDir, file)
		_, err = os.Stat(filePath)
		require.True(t, err != nil)
		require.True(t, os.IsNotExist(err))
	}
}

func TestDelExtUpgradeMp(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForDelExtTest(t, mockCtrl, proto.StoreModeMem)
	serverPort = "12345"
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%v", serverPort))
	require.NoError(t, err)
	defer listener.Close()
	mp.manager = &metadataManager{
		metaNode: &MetaNode{
			localAddr: "",
		},
	}
	testDelExtUpgradeMp(t, mp)
}

func TestDelExtUpgradeMp_Rocksdb(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForDelExtTest(t, mockCtrl, proto.StoreModeRocksDb)
	serverPort = "12345"
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%v", serverPort))
	require.NoError(t, err)
	defer listener.Close()
	mp.manager = &metadataManager{
		metaNode: &MetaNode{
			localAddr: "",
		},
	}
	testDelExtUpgradeMp(t, mp)
}

func testDelExtUpgradeEmptyMp(t *testing.T, mp *metaPartition) {
	rootDir := fmt.Sprintf("/tmp/cfs/test_del_exts/%v_%v", mp.config.PartitionId, time.Now().UnixMilli())
	os.RemoveAll(rootDir)
	defer os.RemoveAll(rootDir)
	err := os.MkdirAll(rootDir, 0755)
	require.NoError(t, err)

	mp.config.RootDir = rootDir

	// NOTE: change leader
	mp.HandleLeaderChange(mp.GetBaseConfig().NodeId)

	time.Sleep(3 * time.Second)

	count, err := mp.GetDeletedExtentsRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, count)
}

func TestDelExtUpgradeEmptyMp(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForDelExtTest(t, mockCtrl, proto.StoreModeMem)
	serverPort = "12345"
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%v", serverPort))
	require.NoError(t, err)
	defer listener.Close()
	mp.manager = &metadataManager{
		metaNode: &MetaNode{
			localAddr: "",
		},
	}
	testDelExtUpgradeEmptyMp(t, mp)
}

func TestDelExtUpgradeEmptyMp_Rocksdb(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForDelExtTest(t, mockCtrl, proto.StoreModeRocksDb)
	serverPort = "12345"
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%v", serverPort))
	require.NoError(t, err)
	defer listener.Close()
	mp.manager = &metadataManager{
		metaNode: &MetaNode{
			localAddr: "",
		},
	}
	testDelExtUpgradeEmptyMp(t, mp)
}

func testMutliUpgradeDelExtMp(t *testing.T, mp *metaPartition) {
	rootDir := fmt.Sprintf("/tmp/cfs/test_del_exts/%v_%v", mp.config.PartitionId, time.Now().UnixMilli())
	os.RemoveAll(rootDir)
	defer os.RemoveAll(rootDir)
	err := os.MkdirAll(rootDir, 0755)
	require.NoError(t, err)

	mp.config.RootDir = rootDir

	const testCount = 3

	for i := 0; i < testCount; i++ {
		// NOTE: change leader
		mp.HandleLeaderChange(mp.GetBaseConfig().NodeId)

		time.Sleep(3 * time.Second)

		count, err := mp.GetDeletedExtentsRealCount()
		require.NoError(t, err)
		require.EqualValues(t, 0, count)
	}
}

func TestMutliUpgradeDelExtMp(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForDelExtTest(t, mockCtrl, proto.StoreModeMem)
	serverPort = "12345"
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%v", serverPort))
	require.NoError(t, err)
	defer listener.Close()
	mp.manager = &metadataManager{
		metaNode: &MetaNode{
			localAddr: "",
		},
	}
	testMutliUpgradeDelExtMp(t, mp)
}

func TestMutliUpgradeDelExtMp_Rocksdb(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForDelExtTest(t, mockCtrl, proto.StoreModeRocksDb)
	serverPort = "12345"
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%v", serverPort))
	require.NoError(t, err)
	defer listener.Close()
	mp.manager = &metadataManager{
		metaNode: &MetaNode{
			localAddr: "",
		},
	}
	testMutliUpgradeDelExtMp(t, mp)
}

func testDeleteExtentsFromTree(t *testing.T, mp *metaPartition) {
	handle, err := mp.deletedExtentsTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	const testEkCount = 10
	deks := make([]*DeletedExtentKey, 0)
	for i := 0; i < testEkCount; i++ {
		dek := NewDeletedExtentKey(&proto.ExtentKey{
			PartitionId:  0,
			ExtentId:     uint64(i),
			ExtentOffset: 0,
		}, 0, mp.AllocDeletedExtentId())
		deks = append(deks, dek)
		err = mp.deletedExtentsTree.Put(handle, dek)
		require.NoError(t, err)
	}
	err = mp.deletedExtentsTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	req := &DeleteExtentsFromTreeRequest{
		DeletedKeys: deks[1:],
	}

	v, err := req.Marshal()
	require.NoError(t, err)
	_, err = mp.submit(opFSMDeleteExtentFromTree, v)
	require.NoError(t, err)

	require.EqualValues(t, 1, mp.deletedExtentsTree.RealCount())
}

func TestDeleteExtentsFromTree(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForDelExtTest(t, mockCtrl, proto.StoreModeMem)
	mp.manager = &metadataManager{
		metaNode: &MetaNode{
			localAddr: "",
		},
	}
	testDeleteExtentsFromTree(t, mp)
}

func TestDeleteExtentsFromTree_Rocksdb(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForDelExtTest(t, mockCtrl, proto.StoreModeRocksDb)
	mp.manager = &metadataManager{
		metaNode: &MetaNode{
			localAddr: "",
		},
	}
	testDeleteExtentsFromTree(t, mp)
}

func testDeleteObjExtentsFromTree(t *testing.T, mp *metaPartition) {
	handle, err := mp.deletedObjExtentsTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	const testEkCount = 10
	doeks := make([]*DeletedObjExtentKey, 0)
	for i := 0; i < testEkCount; i++ {
		doek := NewDeletedObjExtentKey(&proto.ObjExtentKey{
			Cid: 0,
		}, 0, mp.AllocDeletedExtentId())
		doeks = append(doeks, doek)
		err = mp.deletedObjExtentsTree.Put(handle, doek)
		require.NoError(t, err)
	}
	err = mp.deletedObjExtentsTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	req := &DeleteObjExtentsFromTreeRequest{
		DeletedObjKeys: doeks[1:],
	}

	v, err := req.Marshal()
	require.NoError(t, err)
	_, err = mp.submit(opFSMDeleteObjExtentFromTree, v)
	require.NoError(t, err)

	require.EqualValues(t, 1, mp.deletedObjExtentsTree.RealCount())
}

func TestDeleteObjExtentsFromTree(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForDelExtTest(t, mockCtrl, proto.StoreModeMem)
	mp.manager = &metadataManager{
		metaNode: &MetaNode{
			localAddr: "",
		},
	}
	testDeleteObjExtentsFromTree(t, mp)
}

func TestDeleteObjExtentsFromTree_Rocksdb(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp := mockPartitionRaftForDelExtTest(t, mockCtrl, proto.StoreModeRocksDb)
	mp.manager = &metadataManager{
		metaNode: &MetaNode{
			localAddr: "",
		},
	}
	testDeleteObjExtentsFromTree(t, mp)
}

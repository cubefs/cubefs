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
	"os"
	"sort"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/synclist"
	"github.com/stretchr/testify/require"
)

func testFsmDeleteExtentsFromTree(t *testing.T, mp *metaPartition) {
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

	handle, err = mp.deletedExtentsTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	req := &DeleteExtentsFromTreeRequest{
		DeletedKeys: deks[1:],
	}
	err = mp.fsmDeleteExtentsFromTree(handle, req)
	require.NoError(t, err)
	err = mp.deletedExtentsTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	cnt, err := mp.GetDeletedExtentsRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)
}

func TestFsmDeleteExtentsFromTree(t *testing.T) {
	mp := newMpForDelExtTest(t, proto.StoreModeMem)
	testFsmDeleteExtentsFromTree(t, mp)
}

func TestFsmDeleteExtentsFromTree_Rocksdb(t *testing.T) {
	mp := newMpForDelExtTest(t, proto.StoreModeRocksDb)
	testFsmDeleteExtentsFromTree(t, mp)
}

func testFsmDeleteExtentsMoveToTree(t *testing.T, mp *metaPartition) {
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

	handle, err := mp.deletedExtentsTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	err = mp.fsmDeletedExtentMoveToTree(handle)
	require.NoError(t, err)
	err = mp.deletedExtentsTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	cnt, err := mp.GetDeletedExtentsRealCount()
	require.NoError(t, err)
	require.EqualValues(t, testEkCount, cnt)
}

func TestFsmDeleteExtentsMoveToTree(t *testing.T) {
	mp := newMpForDelExtTest(t, proto.StoreModeMem)
	testFsmDeleteExtentsMoveToTree(t, mp)
}

func TestFsmDeleteExtentsMoveToTree_Rocksdb(t *testing.T) {
	mp := newMpForDelExtTest(t, proto.StoreModeRocksDb)
	testFsmDeleteExtentsMoveToTree(t, mp)
}

func testFsmEmptyDeleteExtentsMoveToTree(t *testing.T, mp *metaPartition) {
	rootDir := fmt.Sprintf("/tmp/cfs/test_del_exts/%v_%v", mp.config.PartitionId, time.Now().UnixMilli())
	os.RemoveAll(rootDir)
	defer os.RemoveAll(rootDir)
	err := os.MkdirAll(rootDir, 0755)
	require.NoError(t, err)

	mp.config.RootDir = rootDir

	handle, err := mp.deletedExtentsTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	err = mp.fsmDeletedExtentMoveToTree(handle)
	require.NoError(t, err)
	err = mp.deletedExtentsTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	cnt, err := mp.GetDeletedExtentsRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 0, cnt)
}

func TestFsmEmptyDeleteExtentsMoveToTree(t *testing.T) {
	mp := newMpForDelExtTest(t, proto.StoreModeMem)
	testFsmEmptyDeleteExtentsMoveToTree(t, mp)
}

func TestFsmEmptyDeleteExtentsMoveToTree_Rocksdb(t *testing.T) {
	mp := newMpForDelExtTest(t, proto.StoreModeRocksDb)
	testFsmEmptyDeleteExtentsMoveToTree(t, mp)
}

func testFsmDeleteObjExtentsFromTree(t *testing.T, mp *metaPartition) {
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

	handle, err = mp.deletedObjExtentsTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	req := &DeleteObjExtentsFromTreeRequest{
		DeletedObjKeys: doeks[1:],
	}
	err = mp.fsmDeleteObjExtentsFromTree(handle, req)
	require.NoError(t, err)
	err = mp.deletedObjExtentsTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	cnt, err := mp.GetDeletedObjExtentsRealCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, cnt)
}

func TestFsmDeleteObjExtentsFromTree(t *testing.T) {
	mp := newMpForDelExtTest(t, proto.StoreModeMem)
	testFsmDeleteObjExtentsFromTree(t, mp)
}

func TestFsmDeleteObjExtentsFromTree_Rocksdb(t *testing.T) {
	mp := newMpForDelExtTest(t, proto.StoreModeRocksDb)
	testFsmDeleteObjExtentsFromTree(t, mp)
}

// Copyright 2026 The CubeFS Authors.
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

package datanode

import (
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

func newTestDataPartitionExtentStore(t *testing.T, partitionID uint64) (*DataPartition, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "cfs_partition_raft_test_")
	require.NoError(t, err)
	dataPath := filepath.Join(dir, "extents")
	s, err := storage.NewExtentStore(dataPath, partitionID, util.GB, proto.PartitionTypeNormal, 0, true)
	require.NoError(t, err)
	dp := &DataPartition{
		partitionID: partitionID,
		extentStore: s,
	}
	cleanup := func() {
		s.Close()
		_ = os.RemoveAll(dir)
	}
	return dp, cleanup
}

func createNormalExtentWithData(t *testing.T, s *storage.ExtentStore) uint64 {
	t.Helper()
	id, err := s.NextExtentID()
	require.NoError(t, err)
	require.NoError(t, s.Create(id))
	data := []byte("hello partition raft test")
	crc := crc32.ChecksumIEEE(data)
	param := &storage.WriteParam{
		ExtentID:      id,
		Offset:        0,
		Size:          int64(len(data)),
		Data:          data,
		Crc:           crc,
		WriteType:     storage.AppendWriteType,
		IsSync:        true,
		IsHole:        false,
		IsRepair:      false,
		IsBackupWrite: false,
	}
	_, err = s.Write(param)
	require.NoError(t, err)
	return id
}

func extentInfoFromStore(t *testing.T, s *storage.ExtentStore, fileID uint64) *storage.ExtentInfo {
	t.Helper()
	ei, err := s.Watermark(fileID)
	require.NoError(t, err)
	return ei
}

// ageNormalExtentsPastRepairWindow sets modify time so NormalExtentFilter includes these extents
// (see storage.NormalExtentFilter: now - GetModifyTime() > RepairInterval).
func ageNormalExtentsPastRepairWindow(t *testing.T, s *storage.ExtentStore, ids ...uint64) {
	t.Helper()
	past := time.Now().Unix() - int64(storage.RepairInterval) - 120
	for _, id := range ids {
		ei, ok := s.GetExtentInfo(id)
		require.True(t, ok, "extent %v missing from store map", id)
		ei.SetModifyTime(past)
	}
}

func TestCompareExtentsBySizeMatchesFileIDsAndSizes(t *testing.T) {
	dp, cleanup := newTestDataPartitionExtentStore(t, 100)
	defer cleanup()

	id1 := createNormalExtentWithData(t, dp.extentStore)
	id2 := createNormalExtentWithData(t, dp.extentStore)
	ageNormalExtentsPastRepairWindow(t, dp.extentStore, id1, id2)

	leader := []*storage.ExtentInfo{
		extentInfoFromStore(t, dp.extentStore, id2),
		extentInfoFromStore(t, dp.extentStore, id1),
	}
	require.True(t, dp.compareExtentsBySize(dp.partitionID, leader))
}

func TestCompareExtentsBySizeMissingFileID(t *testing.T) {
	dp, cleanup := newTestDataPartitionExtentStore(t, 101)
	defer cleanup()

	id := createNormalExtentWithData(t, dp.extentStore)
	ageNormalExtentsPastRepairWindow(t, dp.extentStore, id)

	ghost := &storage.ExtentInfo{FileID: 999999}
	ghost.SetSize(128)

	require.False(t, dp.compareExtentsBySize(dp.partitionID, []*storage.ExtentInfo{ghost}))
}

func TestCompareExtentsBySizeSizeMismatch(t *testing.T) {
	dp, cleanup := newTestDataPartitionExtentStore(t, 102)
	defer cleanup()

	id := createNormalExtentWithData(t, dp.extentStore)
	ageNormalExtentsPastRepairWindow(t, dp.extentStore, id)
	local := extentInfoFromStore(t, dp.extentStore, id)

	leader := &storage.ExtentInfo{FileID: local.FileID}
	leader.SetSize(local.GetSize() + 4096)

	require.False(t, dp.compareExtentsBySize(dp.partitionID, []*storage.ExtentInfo{leader}))
}

func TestCompareExtentsBySizeEmptyLeader(t *testing.T) {
	dp, cleanup := newTestDataPartitionExtentStore(t, 103)
	defer cleanup()
	_ = createNormalExtentWithData(t, dp.extentStore)
	require.True(t, dp.compareExtentsBySize(dp.partitionID, nil))
}

func TestCompareExtentsBySizeAfterStoreClosed(t *testing.T) {
	dp, cleanup := newTestDataPartitionExtentStore(t, 104)

	id := createNormalExtentWithData(t, dp.extentStore)
	ageNormalExtentsPastRepairWindow(t, dp.extentStore, id)
	leader := []*storage.ExtentInfo{extentInfoFromStore(t, dp.extentStore, id)}
	cleanup()

	require.False(t, dp.compareExtentsBySize(dp.partitionID, leader))
}

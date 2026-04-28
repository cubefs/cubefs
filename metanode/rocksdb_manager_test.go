// Copyright 2024 The CubeFS Authors.
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

package metanode_test

import (
	"os"
	"testing"

	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/util/diskmon"
	"github.com/stretchr/testify/require"
)

func skipIfRocksdbDiskSelectionWouldFail(t *testing.T, dbDir string, usableFactor float64) {
	t.Helper()

	stat, err := diskmon.NewDiskStat(dbDir)
	require.NoError(t, err)

	if usableFactor <= 0 || usableFactor > 1 {
		usableFactor = diskmon.DefaultMAXFsUsedFactor
	}

	spaceLimit := float64(stat.Total) * usableFactor
	if (stat.Total - stat.Available) > uint64(spaceLimit) {
		t.Skipf("skip rocksdb disk selection check: disk usage for %s exceeds usableFactor %.2f", dbDir, usableFactor)
	}
}

func testRocksdbManager(t *testing.T, manager metanode.RocksdbManager) {
	dbDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)
	err = manager.Register(dbDir)
	require.NoError(t, err)
	err = manager.Register(dbDir)
	require.ErrorIs(t, err, metanode.ErrRocksdbPathRegistered)
	_, err = manager.OpenRocksdb(dbDir+"_123", 0)
	require.ErrorIs(t, err, metanode.ErrUnregisteredRocksdbPath)
	db, err := manager.OpenRocksdb(dbDir, 0)
	require.NoError(t, err)
	count, err := manager.GetPartitionCount(dbDir)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	manager.CloseRocksdb(db)
	count, err = manager.GetPartitionCount(dbDir)
	require.NoError(t, err)
	require.EqualValues(t, 0, count)
	skipIfRocksdbDiskSelectionWouldFail(t, dbDir, 0)
	disk, err := manager.SelectRocksdbDisk(0)
	require.NoError(t, err)
	require.EqualValues(t, dbDir, disk)
}

func TestPerDiskRocksdbManager(t *testing.T) {
	manager := metanode.NewPerDiskRocksdbManager(&metanode.RocksdbManagerConfig{})
	testRocksdbManager(t, manager)
}

func testPartitionRocksdbManager(t *testing.T, manager metanode.RocksdbManager) {
	dbDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)
	err = manager.Register(dbDir)
	require.NoError(t, err)
	err = manager.Register(dbDir)
	require.ErrorIs(t, err, metanode.ErrRocksdbPathRegistered)
	_, err = manager.OpenRocksdb(dbDir+"_123", 0)
	require.ErrorIs(t, err, metanode.ErrUnregisteredRocksdbPath)
	db, err := manager.OpenRocksdb(dbDir, 0)
	require.NoError(t, err)
	manager.CloseRocksdb(db)
	err = manager.AttachPartition(dbDir)
	require.NoError(t, err)
	count, err := manager.GetPartitionCount(dbDir)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	err = manager.DetachPartition(dbDir)
	require.NoError(t, err)
	count, err = manager.GetPartitionCount(dbDir)
	require.NoError(t, err)
	require.EqualValues(t, 0, count)
	skipIfRocksdbDiskSelectionWouldFail(t, dbDir, 0)
	disk, err := manager.SelectRocksdbDisk(0)
	require.NoError(t, err)
	require.EqualValues(t, dbDir, disk)
}

func TestPerPartitionRocksdbManager(t *testing.T) {
	manager := metanode.NewPerPartitionRocksdbManager(&metanode.RocksdbManagerConfig{})
	testPartitionRocksdbManager(t, manager)
}

func TestParseRocksdbMode(t *testing.T) {
	mode := metanode.ParseRocksdbMode("disk")
	require.EqualValues(t, metanode.PerDiskRocksdbMode, mode)

	mode = metanode.ParseRocksdbMode("partition")
	require.EqualValues(t, metanode.PerPartitionRocksdbMode, mode)

	mode = metanode.ParseRocksdbMode("")
	require.EqualValues(t, metanode.DefaultRocksdbMode, mode)
}

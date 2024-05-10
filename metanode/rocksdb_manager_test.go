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
	"github.com/stretchr/testify/require"
)

func TestRocksdbManager(t *testing.T) {
	manager := metanode.NewRocksdbManager(0, 0, 0)
	dbDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)
	err = manager.Register(dbDir)
	require.NoError(t, err)
	err = manager.Register(dbDir)
	require.ErrorIs(t, err, metanode.ErrRocksdbPathRegistered)
	_, err = manager.OpenRocksdb(dbDir + "_123")
	require.ErrorIs(t, err, metanode.ErrUnregisteredRocksdbPath)
	db, err := manager.OpenRocksdb(dbDir)
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
	disk, err := manager.SelectRocksdbDisk(0)
	require.NoError(t, err)
	require.EqualValues(t, dbDir, disk)
}

// Copyright 2025 The CubeFS Authors.
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
	"os"
	"path"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

func TestCheckAvailable(t *testing.T) {
	tmpDir, err := os.MkdirTemp(".", "")
	defer os.RemoveAll(tmpDir)
	require.NoError(t, err)

	testDpPath := path.Join(tmpDir, "dp1")
	err = os.Mkdir(testDpPath, 0o755)
	require.NoError(t, err)

	dp := &DataPartition{
		disk:               &Disk{dataNode: &DataNode{}},
		partitionID:        1,
		partitionSize:      1 * util.TB,
		path:               testDpPath,
		partitionStatus:    proto.ReadWrite,
		diskErrCnt:         0,
		config:             &dataPartitionCfg{},
		volVersionInfoList: &proto.VolVersionInfoList{},
		extentStore:        &storage.ExtentStore{},
	}

	statusFilePath := filepath.Join(dp.path, DpStatusFile)

	t.Run("file not exist", func(t *testing.T) {
		err := dp.checkAvailable()
		require.Error(t, err)
		require.Equal(t, proto.ReadWrite, dp.partitionStatus)
		require.Equal(t, uint64(0), dp.diskErrCnt)
	})

	require.NoError(t, os.WriteFile(statusFilePath, []byte(DpStatusFile), 0o644))

	t.Run("normal readWrite", func(t *testing.T) {
		err := dp.checkAvailable()
		require.NoError(t, err)
		require.Equal(t, proto.ReadWrite, dp.partitionStatus)
		require.Equal(t, uint64(0), dp.diskErrCnt)
	})

	t.Run("disk err simulate", func(t *testing.T) {
		fp, err := os.OpenFile(statusFilePath, os.O_TRUNC|os.O_RDWR, 0o755)
		require.NoError(t, err)
		defer fp.Close()

		data := []byte(DpStatusFile)
		t.Run("read err", func(t *testing.T) {
			_, err = fp.WriteAt(data, 0)
			require.NoError(t, err)
			fp.Close()
			_, err = fp.ReadAt(data, 0)
			require.Error(t, err)
			err = syscall.EIO
			dp.checkIsDiskError(err, ReadFlag)
			require.Equal(t, proto.Unavailable, dp.partitionStatus)
			require.Equal(t, uint64(1), dp.diskErrCnt)
		})

		fp, err = os.OpenFile(statusFilePath, os.O_TRUNC|os.O_RDWR, 0o755)

		t.Run("sync err", func(t *testing.T) {
			_, err = fp.WriteAt(data, 0)
			require.NoError(t, err)
			fp.Close()
			err = fp.Sync()
			require.Error(t, err)
			err = syscall.EIO
			dp.checkIsDiskError(err, ReadFlag)
			require.Equal(t, proto.Unavailable, dp.partitionStatus)
			require.Equal(t, uint64(2), dp.diskErrCnt)
		})

		t.Run("write err", func(t *testing.T) {
			_, err = fp.WriteAt(data, 0)
			require.Error(t, err)
			err = syscall.EIO
			dp.checkIsDiskError(err, WriteFlag)
			require.Equal(t, proto.Unavailable, dp.partitionStatus)
			require.Equal(t, uint64(3), dp.diskErrCnt)
		})
	})
}

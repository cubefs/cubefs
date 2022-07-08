// Copyright 2022 The CubeFS Authors.
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

package disk

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"

	"github.com/stretchr/testify/require"
)

func TestCleanTrash(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "TestRunCompact")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")
	println(diskpath)

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

	// first time. create
	diskConfig := core.Config{
		BaseConfig: core.BaseConfig{
			Path:       diskpath,
			AutoFormat: true,
		},
		AllocDiskID:      getDiskIDFn,
		NotifyCompacting: setChunkCompactFn,
		HandleIOError:    handleIOErrorFn,
	}
	ds, err := NewDiskStorage(ctx, diskConfig)
	require.NoError(t, err)
	require.NotNil(t, ds)
	ds.Conf.CompactEmptyRateThreshold = 0

	cs, err := ds.CreateChunk(ctx, proto.Vuid(1), core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	trashdir := filepath.Join(diskpath, ".trash")
	trashFile := filepath.Join(trashdir, "trashfile1")
	f, err := os.Create(trashFile)
	require.NoError(t, err)
	defer f.Close()
	err = ds.cleanTrash(ctx)
	require.NoError(t, err)

	ds.Conf.DiskTrashProtectionM = 0
	err = ds.cleanTrash(ctx)
	require.NoError(t, err)

	ds.Conf.AllowCleanTrash = true
	err = ds.cleanTrash(ctx)
	require.Nil(t, err)

	ds.Conf.Path = "/temp"
	err = ds.cleanTrash(ctx)
	require.Error(t, err)

	err = ds.moveToTrash(ctx, "/temp")
	require.Error(t, err)

	err = ds.moveToTrash(ctx, trashdir)
	require.Error(t, err)
}

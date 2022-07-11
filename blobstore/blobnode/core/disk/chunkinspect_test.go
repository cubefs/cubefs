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
	"time"

	dnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"

	"github.com/stretchr/testify/require"
)

func TestMayChunkLost(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "TestMayChunkLost")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

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
	defer ds.ResetChunks(ctx)

	vuid := proto.Vuid(2001)

	cs, err := ds.CreateChunk(context.TODO(), vuid, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	vm := core.VuidMeta{
		Vuid:    vuid,
		DiskID:  ds.DiskID,
		ChunkId: cs.ID(),
		Mtime:   time.Now().UnixNano(),
		Status:  dnapi.ChunkStatusReadOnly,
	}

	lost, err := ds.maybeChunkLost(ctx, cs.ID(), vm)
	require.NoError(t, err)
	require.Equal(t, false, lost)

	vm1 := core.VuidMeta{
		Vuid:    vuid,
		DiskID:  ds.DiskID,
		ChunkId: cs.ID(),
		Mtime:   time.Now().UnixNano(),
		Status:  dnapi.ChunkStatusNormal,
	}

	lost, err = ds.maybeChunkLost(ctx, dnapi.ChunkId{12}, vm1)
	require.NoError(t, err)
	require.Equal(t, false, lost)

	var InvalidChunkId dnapi.ChunkId = [16]byte{}

	lost, err = ds.maybeChunkLost(ctx, InvalidChunkId, vm1)
	require.Error(t, err)
	require.Equal(t, false, lost)
}

func TestMaybeCleanRubbishChunk(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "TestMaybeCleanRubbishChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

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
	defer ds.ResetChunks(ctx)

	vuid := proto.Vuid(2001)

	cs, err := ds.CreateChunk(context.TODO(), vuid, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	err = ds.maybeCleanRubbishChunk(ctx, cs.ID())
	require.Nil(t, err)

	var InvalidChunkId dnapi.ChunkId = [16]byte{}
	err = ds.maybeCleanRubbishChunk(ctx, InvalidChunkId)
	require.Error(t, err)

	err = ds.maybeCleanRubbishChunk(ctx, dnapi.ChunkId{12})
	require.Nil(t, err)

	err = ds.SuperBlock.DeleteChunk(ctx, cs.ID())
	require.Nil(t, err)
	ds.Conf.ChunkGcCreateTimeProtectionM = 0
	err = ds.maybeCleanRubbishChunk(ctx, cs.ID())
	require.Nil(t, err)

	ds.Conf.ChunkGcModifyTimeProtectionM = 0
	err = ds.maybeCleanRubbishChunk(ctx, cs.ID())
	require.Nil(t, err)
}

func TestGcRubbishChunk(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "TestGcRubbishChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskpath := filepath.Join(testDir, "DiskPath")

	err = os.MkdirAll(diskpath, 0o755)
	require.NoError(t, err)

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
	defer ds.ResetChunks(ctx)

	vuid := proto.Vuid(2001)

	cs, err := ds.CreateChunk(context.TODO(), vuid, core.DefaultChunkSize)
	require.NoError(t, err)
	require.NotNil(t, cs)

	addDir := filepath.Join(ds.DataPath, "temp")
	err = os.MkdirAll(addDir, 0o644)
	require.NoError(t, err)

	addFile := filepath.Join(ds.DataPath, "temp1")
	f, err := os.Create(addFile)
	require.Nil(t, err)
	defer f.Close()

	_, err = ds.GcRubbishChunk(ctx)
	require.Nil(t, err)

	ds.DataPath = "/emptyDir"
	_, err = ds.GcRubbishChunk(ctx)
	require.Error(t, err)
}

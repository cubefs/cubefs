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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/chunk"
	db2 "github.com/cubefs/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func TestDiskStorage_StartCompact(t *testing.T) {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "NewBlobNodeService")

	testDir, err := os.MkdirTemp(os.TempDir(), "DoCompact2")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	conf := &core.Config{
		RuntimeConfig: core.RuntimeConfig{
			MetricReportIntervalS: 30,
		},
	}
	conf.CompactBatchSize = core.DefaultCompactBatchSize

	vuid := proto.Vuid(2001)
	chunkID := bnapi.NewChunkId(vuid)

	err = core.EnsureDiskArea(testDir, "")
	require.NoError(t, err)

	dataPath := core.GetDataPath(testDir)
	log.Info(dataPath)
	metaPath := core.GetMetaPath(testDir, "")
	log.Info(metaPath)

	mockDiskStorage := &DiskStorageWrapper{&DiskStorage{
		DiskID:   proto.DiskID(101),
		DataPath: dataPath,
		MetaPath: metaPath,
		Conf:     &core.Config{},
	}}

	db, err := db2.NewMetaHandler(metaPath, db2.MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, db)

	vm := core.VuidMeta{
		Vuid:    vuid,
		DiskID:  12,
		ChunkId: chunkID,
		Mtime:   time.Now().UnixNano(),
		Status:  bnapi.ChunkStatusNormal,
	}

	srcChunkStorage, err := chunk.NewChunkStorage(ctx, dataPath, vm, func(option *core.Option) {
		option.Conf = conf
		option.DB = db
		option.CreateDataIfMiss = true
		option.Disk = mockDiskStorage
	})
	require.NoError(t, err)

	_ = srcChunkStorage.NeedCompact(ctx)

	srcChunkStorage.Disk().(*DiskStorageWrapper).Conf = nil
	_, err = srcChunkStorage.StartCompact(ctx)
	require.Error(t, err)
}

func TestCompactChunkInternal(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "TestCompactChunkInternal")
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

	err = ds.CompactChunkInternal(ctx, 2002)
	require.Error(t, err)

	shardData := []byte("test")
	shard := &core.Shard{
		Bid:  1,
		Vuid: cs.Vuid(),
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(len(shardData)),
		Body: bytes.NewReader(shardData),
	}

	// write data
	err = cs.Write(ctx, shard)
	require.NoError(t, err)

	err = ds.CompactChunkInternal(ctx, vuid)
	require.Nil(t, err)

	// bad bytes
	badBytes := []byte{0x00, 0x00, 0x00, 0x00, 0xaa, 0xaa, 0xaa, 0xaa}
	srcFile := filepath.Join(ds.DataPath, cs.ID().String())
	log.Info(srcFile)
	f, err := os.OpenFile(srcFile, 2, 0o644)
	require.NoError(t, err)
	_, err = f.WriteAt(badBytes, 4128)
	require.NoError(t, err)
	err = ds.CompactChunkInternal(ctx, vuid)
	require.Error(t, err)
}

func TestExecCompactChunk(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "TestExecCompactChunk")
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

	err = ds.UpdateChunkCompactState(ctx, proto.Vuid(2011), false)
	require.Error(t, err)
}

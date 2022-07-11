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

package storage

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/db"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestNewChunkMeta(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"NewChunkMeta")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	diskmetapath := filepath.Join(testDir, "DiskPath")
	println(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	kvdb, err := db.NewMetaHandler(diskmetapath, db.MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, kvdb)

	// create chunk meta
	vuid := 1024
	diskid := proto.DiskID(10)
	chunkid := bnapi.NewChunkId(proto.Vuid(vuid))
	vm := core.VuidMeta{
		Vuid:    proto.Vuid(vuid),
		ChunkId: chunkid,
		DiskID:  diskid,
	}

	cm, err := NewChunkMeta(context.TODO(), &core.Config{}, vm, kvdb)
	require.NoError(t, err)
	require.NotNil(t, cm)

	cm.Close()
}

func TestChunkMeta_Write(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"ChunkMetaWrite")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	println(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	kvdb, err := db.NewMetaHandler(diskmetapath, db.MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, kvdb)
	// create chunk meta
	vuid := 1024
	diskid := proto.DiskID(10)
	chunkid := bnapi.NewChunkId(proto.Vuid(vuid))
	vm := core.VuidMeta{
		Vuid:    proto.Vuid(vuid),
		ChunkId: chunkid,
		DiskID:  diskid,
	}

	cm, err := NewChunkMeta(ctx, &core.Config{}, vm, kvdb)
	require.NoError(t, err)
	require.NotNil(t, cm)
	defer cm.Close()

	// write
	bid := 7
	meta := core.ShardMeta{
		Version: _shardVer[0],
		Size:    10,
		Crc:     10,
		Offset:  0,
		Flag:    bnapi.ShardStatusNormal,
	}
	err = cm.Write(ctx, proto.BlobID(bid), meta)
	require.NoError(t, err)

	// read
	rd_meta, err := cm.Read(ctx, proto.BlobID(bid))
	require.NoError(t, err)
	require.Equal(t, meta, rd_meta)

	// delete
	err = cm.Delete(ctx, proto.BlobID(bid))
	require.NoError(t, err)
	_, err = cm.Read(ctx, proto.BlobID(bid))
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}

func TestChunkMeta_Scan(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"ChunkMetaScan")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	println(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	kvdb, err := db.NewMetaHandler(diskmetapath, db.MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, kvdb)

	// create chunk meta
	vuid := 1024
	diskid := proto.DiskID(10)
	chunkid := bnapi.NewChunkId(proto.Vuid(vuid))
	vm := core.VuidMeta{
		Vuid:    proto.Vuid(vuid),
		ChunkId: chunkid,
		DiskID:  diskid,
	}

	cm, err := NewChunkMeta(ctx, &core.Config{}, vm, kvdb)
	require.NoError(t, err)
	require.NotNil(t, cm)
	defer cm.Close()

	// multi write
	for i := 0; i < 10; i++ {
		bid := 1 + i
		meta := core.ShardMeta{
			Version: _shardVer[0],
			Size:    uint32(1 + i),
			Crc:     10,
			Offset:  0,
			Flag:    bnapi.ShardStatusNormal,
		}
		err = cm.Write(ctx, proto.BlobID(bid), meta)
		require.NoError(t, err)
	}

	// scan
	count := 0
	err = cm.Scan(ctx, 0, 10, func(bid proto.BlobID, shard *core.ShardMeta) (err error) {
		fmt.Printf("----- scan => %d, shard: %v\n", bid, shard)
		count += 1
		return nil
	})
	require.Equal(t, count, 10)
	require.NoError(t, err)

	count = 0
	err = cm.Scan(ctx, 2, 10, func(bid proto.BlobID, shard *core.ShardMeta) (err error) {
		fmt.Printf("----- scan => %d, shard: %v\n", bid, shard)
		count += 1
		return nil
	})
	require.Equal(t, count, 8)
	require.Equal(t, core.ErrChunkScanEOF, err)

	// eof
	count = 0
	err = cm.Scan(ctx, 8, 1, func(bid proto.BlobID, shard *core.ShardMeta) (err error) {
		fmt.Printf("----- scan => %d, shard: %v\n", bid, shard)
		count += 1
		return nil
	})
	require.Equal(t, count, 1)
	require.NoError(t, err)

	count = 0
	err = cm.Scan(ctx, 8, 2, func(bid proto.BlobID, shard *core.ShardMeta) (err error) {
		fmt.Printf("----- scan => %d, shard: %v\n", bid, shard)
		count += 1
		return nil
	})
	require.Equal(t, count, 2)
	require.Equal(t, nil, err)

	count = 0
	err = cm.Scan(ctx, 9, 2, func(bid proto.BlobID, shard *core.ShardMeta) (err error) {
		fmt.Printf("----- scan => %d, shard: %v\n", bid, shard)
		count += 1
		return nil
	})
	require.Equal(t, count, 1)
	require.Equal(t, core.ErrChunkScanEOF, err)
}

func TestGenShardKey(t *testing.T) {
	chunkid := bnapi.ChunkId{12}
	bid := proto.BlobID(12)

	id := core.ShardKey{
		Chunk: chunkid,
		Bid:   bid,
	}

	buffer := make([]byte, shardKeyLen)
	writeShardKey(buffer, &id)

	nKey, err := parseShardKey(buffer)

	require.NoError(t, err)
	require.Equal(t, id, nKey)
}

func TestChunkMeta_Destroy(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), defaultDiskTestDir+"ChunkMetaDestroy")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	println(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	kvdb, err := db.NewMetaHandler(diskmetapath, db.MetaConfig{})
	require.NoError(t, err)
	require.NotNil(t, kvdb)

	// create chunk meta
	vuid := 1024
	diskid := proto.DiskID(10)
	chunkid := bnapi.NewChunkId(proto.Vuid(vuid))
	vm := core.VuidMeta{
		Vuid:    proto.Vuid(vuid),
		ChunkId: chunkid,
		DiskID:  diskid,
	}

	cm, err := NewChunkMeta(ctx, &core.Config{}, vm, kvdb)
	require.NoError(t, err)
	require.NotNil(t, cm)
	defer cm.Close()

	// write
	for bid := proto.BlobID(3001); bid <= proto.BlobID(3010); bid++ {
		meta := core.ShardMeta{
			Version: _shardVer[0],
			Size:    10,
			Crc:     10,
			Offset:  0,
			Flag:    bnapi.ShardStatusNormal,
		}
		err = cm.Write(ctx, bid, meta)
		require.NoError(t, err)
	}
	err = cm.Destroy(ctx)
	require.NoError(t, err)
}

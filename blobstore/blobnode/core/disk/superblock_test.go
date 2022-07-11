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

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/storage"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestNewSuperBlock(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "NewSuperBlock")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	println(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	_, err = NewSuperBlock("", &core.Config{})
	require.Error(t, err)

	// create
	s, err := NewSuperBlock(diskmetapath, &core.Config{})
	require.NoError(t, err)
	require.NotNil(t, s)

	// add chunk
	vuid := proto.Vuid(1024)
	diskid := proto.DiskID(10)
	chunkid := bnapi.NewChunkId(vuid)
	vm := core.VuidMeta{
		Vuid:    vuid,
		ChunkId: chunkid,
		DiskID:  diskid,
	}

	// create chunk
	err = s.UpsertChunk(ctx, chunkid, vm)
	require.NoError(t, err)

	//
	vm_read, err := s.ReadChunk(ctx, chunkid)
	require.NoError(t, err)
	require.NotNil(t, vm_read)

	require.Equal(t, vm, vm_read)

	_, err = s.ReadChunk(ctx, bnapi.InvalidChunkId)
	require.Error(t, err)

	err = s.UpsertDisk(ctx, proto.InvalidDiskID, core.DiskMeta{})
	require.Error(t, err)

	err = s.DeleteChunk(ctx, bnapi.InvalidChunkId)
	require.Error(t, err)

	_, _ = s.ReadVuidBind(ctx, vuid)

	_, err = s.ReadVuidBind(ctx, proto.Vuid(123456))
	require.Error(t, err)
	require.Equal(t, true, os.IsNotExist(err))
}

func TestSuperBlock_RegisterDisk(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "SBRegisterDisk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	diskmetapath := filepath.Join(testDir, "DiskPath")
	println(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	// create
	s, err := NewSuperBlock(diskmetapath, &core.Config{})
	require.NoError(t, err)
	require.NotNil(t, s)

	diskid := proto.DiskID(1024)
	now := int64(time.Now().UnixNano())

	format := &core.FormatInfo{
		FormatInfoProtectedField: core.FormatInfoProtectedField{
			DiskID:  diskid,
			Version: 0x1,
			Format:  core.FormatMetaTypeV1,
			Ctime:   now,
		},
	}

	dm := core.DiskMeta{
		FormatInfo: *format,
		Host:       "127.0.0.1:1024",
		Path:       "/Test_data0",
		Registered: true,
		Status:     0x1,
	}

	// register disk
	err = s.UpsertDisk(context.TODO(), diskid, dm)
	require.NoError(t, err)

	// read disk info
	dm_read, err := s.LoadDiskInfo(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, dm_read)

	require.Equal(t, dm, dm_read)
}

func TestSuperBlock_ListChunks(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "SBListChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	println(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	// create
	s, err := NewSuperBlock(diskmetapath, &core.Config{})
	require.NoError(t, err)
	require.NotNil(t, s)

	// create chunk 0
	for i := 0; i < 10; i++ {
		vuid := 1024 + i
		diskid := proto.DiskID(10)
		chunkid := bnapi.NewChunkId(proto.Vuid(vuid))
		vm := core.VuidMeta{
			Vuid:    proto.Vuid(vuid),
			ChunkId: chunkid,
			DiskID:  diskid,
		}
		err = s.UpsertChunk(ctx, chunkid, vm)
		require.NoError(t, err)
	}

	chunks, err := s.ListChunks(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, len(chunks))

	err = s.CleanChunkSpace(ctx, bnapi.NewChunkId(proto.Vuid(1024)))
	require.NoError(t, err)

	err = s.DeleteChunk(ctx, bnapi.NewChunkId(proto.Vuid(1025)))
	require.NoError(t, err)
}

func TestSuperBlock_ListVuids(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "SBListVuid")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	println(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	// create
	s, err := NewSuperBlock(diskmetapath, &core.Config{})
	require.NoError(t, err)
	require.NotNil(t, s)

	// create chunk 0
	for i := 0; i < 10; i++ {
		vuid := 1024 + i
		chunkid := bnapi.NewChunkId(proto.Vuid(vuid))
		err = s.BindVuidChunk(ctx, proto.Vuid(vuid), chunkid)
		require.NoError(t, err)
	}

	vuids, err := s.ListVuids(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, len(vuids))
}

func TestSuperBlock_genVuidSpaceKey(t *testing.T) {
	vuid := proto.Vuid(1001)
	key := GenVuidSpaceKey(vuid)

	nvuid, err := parseVuidSpacePrefix(key)
	require.Equal(t, vuid, nvuid)
	require.NoError(t, err)
}

func TestParseVuidSpacePrefix(t *testing.T) {
	k1 := "vuid/11"
	_, err := parseVuidSpacePrefix(k1)
	require.Error(t, err)

	k2 := "vuids/11a"
	_, err = parseVuidSpacePrefix(k2)
	require.Error(t, err)
}

func TestSuperblockErrorCondition(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "TestWriteData")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	s, err := NewSuperBlock(diskmetapath, &core.Config{})
	require.NoError(t, err)
	require.NotNil(t, s)

	err = s.writeData(ctx, []byte("test"), nil)
	require.Error(t, err)

	err = s.writeData(ctx, nil, []byte("test"))
	require.Error(t, err)

	_, err = s.readData(ctx, nil)
	require.Error(t, err)

	vuid := proto.Vuid(1023)
	diskid := proto.DiskID(1)
	chunkid := bnapi.NewChunkId(vuid)
	vm := core.VuidMeta{
		Vuid:    vuid,
		ChunkId: chunkid,
		DiskID:  diskid,
	}

	var InvalidChunkId bnapi.ChunkId = [16]byte{}
	// upsert invalid ChunkId
	err = s.UpsertChunk(ctx, InvalidChunkId, vm)
	require.Error(t, err)

	err = s.BindVuidChunk(ctx, vuid, InvalidChunkId)
	require.Error(t, err)
}

func TestCleanChunkSpace(t *testing.T) {
	testDir, err := ioutil.TempDir(os.TempDir(), "CleanChunkSpace")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	println(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	s, err := NewSuperBlock(diskmetapath, &core.Config{})
	require.NoError(t, err)
	require.NotNil(t, s)

	// create chunk meta
	vuid := 1024
	diskid := proto.DiskID(10)
	chunkid := bnapi.NewChunkId(proto.Vuid(vuid))
	vm := core.VuidMeta{
		Vuid:    proto.Vuid(vuid),
		ChunkId: chunkid,
		DiskID:  diskid,
	}

	cm, err := storage.NewChunkMeta(ctx, &core.Config{}, vm, s.db)
	require.NoError(t, err)
	require.NotNil(t, cm)
	defer cm.Close()

	// write
	_shardVer := []byte{0x1}
	bid := 8
	meta := core.ShardMeta{
		Version: _shardVer[0],
		Size:    10,
		Crc:     10,
		Offset:  0,
		Flag:    bnapi.ShardStatusNormal,
	}
	err = cm.Write(ctx, proto.BlobID(bid), meta)
	require.NoError(t, err)

	err = s.CleanChunkSpace(ctx, chunkid)
	require.Nil(t, err)
}

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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/storage"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func TestNewSuperBlock(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "NewSuperBlock")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	log.Info(diskmetapath)

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
	chunkid := clustermgr.NewChunkID(vuid)
	vm := core.VuidMeta{
		Vuid:    vuid,
		ChunkID: chunkid,
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

	_, err = s.ReadChunk(ctx, clustermgr.InvalidChunkID)
	require.Error(t, err)

	err = s.UpsertDisk(ctx, proto.InvalidDiskID, core.DiskMeta{})
	require.Error(t, err)

	err = s.DeleteChunk(ctx, clustermgr.InvalidChunkID)
	require.Error(t, err)

	_, _ = s.ReadVuidBind(ctx, vuid)

	_, err = s.ReadVuidBind(ctx, proto.Vuid(123456))
	require.Error(t, err)
	require.Equal(t, true, os.IsNotExist(err))
}

func TestSuperBlock_RegisterDisk(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "SBRegisterDisk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	diskmetapath := filepath.Join(testDir, "DiskPath")
	log.Info(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	// create
	s, err := NewSuperBlock(diskmetapath, &core.Config{})
	require.NoError(t, err)
	require.NotNil(t, s)

	diskid := proto.DiskID(1024)
	now := int64(time.Now().UnixNano())

	format := &core.FormatInfo{}
	format.FormatInfoProtectedField = core.FormatInfoProtectedField{
		DiskID:  diskid,
		Version: 1,
		Format:  core.FormatMetaTypeV1,
		Ctime:   now,
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
	testDir, err := os.MkdirTemp(os.TempDir(), "SBListChunk")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	log.Info(diskmetapath)

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
		chunkid := clustermgr.NewChunkID(proto.Vuid(vuid))
		vm := core.VuidMeta{
			Vuid:    proto.Vuid(vuid),
			ChunkID: chunkid,
			DiskID:  diskid,
		}
		err = s.UpsertChunk(ctx, chunkid, vm)
		require.NoError(t, err)
	}

	chunks, err := s.ListChunks(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, len(chunks))

	err = s.CleanChunkSpace(ctx, clustermgr.NewChunkID(proto.Vuid(1024)))
	require.NoError(t, err)

	err = s.DeleteChunk(ctx, clustermgr.NewChunkID(proto.Vuid(1025)))
	require.NoError(t, err)
}

func TestSuperBlock_ListVuids(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "SBListVuid")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	log.Info(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	// create
	s, err := NewSuperBlock(diskmetapath, &core.Config{})
	require.NoError(t, err)
	require.NotNil(t, s)

	// create chunk 0
	for i := 0; i < 10; i++ {
		vuid := 1024 + i
		chunkid := clustermgr.NewChunkID(proto.Vuid(vuid))
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
	testDir, err := os.MkdirTemp(os.TempDir(), "TestWriteData")
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
	chunkid := clustermgr.NewChunkID(vuid)
	vm := core.VuidMeta{
		Vuid:    vuid,
		ChunkID: chunkid,
		DiskID:  diskid,
	}

	var InvalidChunkID clustermgr.ChunkID = [16]byte{}
	// upsert invalid ChunkID
	err = s.UpsertChunk(ctx, InvalidChunkID, vm)
	require.Error(t, err)

	err = s.BindVuidChunk(ctx, vuid, InvalidChunkID)
	require.Error(t, err)
}

func TestCleanChunkSpace(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "CleanChunkSpace")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx := context.Background()

	diskmetapath := filepath.Join(testDir, "DiskPath")
	log.Info(diskmetapath)

	err = os.MkdirAll(diskmetapath, 0o755)
	require.NoError(t, err)

	s, err := NewSuperBlock(diskmetapath, &core.Config{})
	require.NoError(t, err)
	require.NotNil(t, s)

	// create chunk meta
	vuid := 1024
	diskid := proto.DiskID(10)
	chunkid := clustermgr.NewChunkID(proto.Vuid(vuid))
	vm := core.VuidMeta{
		Vuid:    proto.Vuid(vuid),
		ChunkID: chunkid,
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

// ---------------------------------------------------------------------------
// DataInspectMgr persisted inspect state (inspect_disk_state/, inspect_state/)
// ---------------------------------------------------------------------------

func TestSuperBlock_InspectState_InvalidArgs(t *testing.T) {
	sb := newTestSuperBlock(t)
	ctx := context.Background()
	var err error

	err = sb.UpsertInspectDiskState(ctx, core.InspectDiskState{DiskID: proto.InvalidDiskID})
	require.ErrorIs(t, err, bloberr.ErrInvalidParam)

	_, err = sb.ReadInspectChunkState(ctx, proto.InvalidVuid)
	require.ErrorIs(t, err, bloberr.ErrInvalidParam)

	err = sb.UpsertInspectChunkState(ctx, core.InspectChunkState{Vuid: proto.InvalidVuid})
	require.ErrorIs(t, err, bloberr.ErrInvalidParam)

	err = sb.DeleteInspectChunkState(ctx, proto.InvalidVuid)
	require.ErrorIs(t, err, bloberr.ErrInvalidParam)
}

// newTestSuperBlock builds a SuperBlock backed by a real (temp-dir) RocksDB meta handler,
// so inspect-state CRUD/Range semantics are exercised against the same storage engine used
// in production, not a fake.
func newTestSuperBlock(t *testing.T) *SuperBlock {
	t.Helper()
	testDir, err := os.MkdirTemp(os.TempDir(), "superblock_test")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(testDir) })

	dir := filepath.Join(testDir, "meta")
	require.NoError(t, os.MkdirAll(dir, 0o755))

	sb, err := NewSuperBlock(dir, &core.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { sb.Close(context.Background()) }) //nolint:errcheck

	return sb
}

func TestSuperBlock_InspectChunkStateCRUD(t *testing.T) {
	sb := newTestSuperBlock(t)
	ctx := context.Background()

	vuid := proto.Vuid(1001)

	// not found initially
	_, err := sb.ReadInspectChunkState(ctx, vuid)
	require.Error(t, err)

	st := core.InspectChunkState{
		Vuid:         vuid,
		Cursor:       proto.BlobID(100),
		CycleMaxBid:  proto.BlobID(1000),
		CycleCnt:     500,
		CycleScanned: 100,
		BadBids:      map[proto.BlobID]core.BadBidMeta{7: {}, 8: {}},
	}
	require.NoError(t, sb.UpsertInspectChunkState(ctx, st))

	got, err := sb.ReadInspectChunkState(ctx, vuid)
	require.NoError(t, err)
	require.Equal(t, st.Vuid, got.Vuid)
	require.Equal(t, st.Cursor, got.Cursor)
	require.Equal(t, st.CycleMaxBid, got.CycleMaxBid)
	require.Equal(t, st.CycleCnt, got.CycleCnt)
	require.Equal(t, st.CycleScanned, got.CycleScanned)
	require.Equal(t, st.BadBids, got.BadBids)

	// update
	got.Cursor = got.CycleMaxBid
	require.NoError(t, sb.UpsertInspectChunkState(ctx, got))
	got2, err := sb.ReadInspectChunkState(ctx, vuid)
	require.NoError(t, err)
	require.False(t, got2.NeedCount())
	require.Equal(t, got2.CycleMaxBid, got2.Cursor)

	// delete
	require.NoError(t, sb.DeleteInspectChunkState(ctx, vuid))
	_, err = sb.ReadInspectChunkState(ctx, vuid)
	require.Error(t, err)
}

func TestSuperBlock_InspectDiskStateCRUD(t *testing.T) {
	sb := newTestSuperBlock(t)
	ctx := context.Background()
	diskID := proto.DiskID(11)

	_, err := sb.ReadInspectDiskState(ctx)
	require.Error(t, err)

	st := core.InspectDiskState{DiskID: diskID, CycleStartAt: 999, CycleID: 1}
	require.NoError(t, sb.UpsertInspectDiskState(ctx, st))

	got, err := sb.ReadInspectDiskState(ctx)
	require.NoError(t, err)
	require.Equal(t, st.DiskID, got.DiskID)
	require.Equal(t, st.CycleStartAt, got.CycleStartAt)
	require.Equal(t, st.CycleID, got.CycleID)

	_, err = sb.db.Get(ctx, GenInspectDiskStateKey())
	require.NoError(t, err)
}

func TestSuperBlock_RangeInspectChunkState(t *testing.T) {
	sb := newTestSuperBlock(t)
	ctx := context.Background()

	vuids := []proto.Vuid{101, 102, 103}
	for _, v := range vuids {
		require.NoError(t, sb.UpsertInspectChunkState(ctx, core.InspectChunkState{Vuid: v, CycleCnt: int64(v)}))
	}
	// a disk-level key with a different prefix must not be picked up by chunk Range
	require.NoError(t, sb.UpsertInspectDiskState(ctx, core.InspectDiskState{DiskID: 1, CycleStartAt: 1}))

	seen := map[proto.Vuid]bool{}
	require.NoError(t, sb.RangeInspectChunkState(ctx, func(st *core.InspectChunkState) bool {
		seen[st.Vuid] = true
		return true
	}))
	require.Len(t, seen, len(vuids))
	for _, v := range vuids {
		require.True(t, seen[v])
	}

	// early stop
	count := 0
	err := sb.RangeInspectChunkState(ctx, func(st *core.InspectChunkState) bool {
		count++
		return false
	})
	require.ErrorIs(t, err, errInspectChunkStateRange)
	require.Equal(t, 1, count)

	// Value-only mutate during Range
	require.NoError(t, sb.RangeInspectChunkState(ctx, func(st *core.InspectChunkState) bool {
		st.CycleCnt = 0
		require.NoError(t, sb.UpsertInspectChunkState(ctx, *st))
		return true
	}))
	require.NoError(t, sb.RangeInspectChunkState(ctx, func(st *core.InspectChunkState) bool {
		require.Equal(t, int64(0), st.CycleCnt)
		return true
	}))
}

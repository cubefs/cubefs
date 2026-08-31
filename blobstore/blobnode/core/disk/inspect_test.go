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

package disk

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// testInspectConfig wires inspect engine config into a disk config so
// NewDiskStorage can be used in tests.
func testInspectConfig(cfg core.Config) core.Config {
	return cfg
}

func newTestInspectStateStore(t *testing.T) *inspectStateStore {
	t.Helper()
	st := &inspectStateStore{superBlock: newTestSuperBlock(t), diskID: proto.DiskID(11)}
	require.NoError(t, st.load(context.Background()))
	return st
}

// newTestInspectDisk builds a bare DiskStorage with a loaded in-memory inspect
// state store, mirroring the production initialization in newDiskStorage.
func newTestInspectDisk(t *testing.T) *DiskStorage {
	t.Helper()
	sb := newTestSuperBlock(t)
	ds := &DiskStorage{
		DiskID:       proto.DiskID(11),
		SuperBlock:   sb,
		closeCh:      make(chan struct{}),
		inspectStore: &inspectStateStore{superBlock: sb, diskID: proto.DiskID(11)},
	}
	require.NoError(t, ds.inspectStore.load(context.Background()))
	return ds
}

func TestInspectStateStore_LoadFlushRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := newTestInspectStateStore(t)

	require.NoError(t, st.load(ctx))

	diskID := proto.DiskID(11)
	diskSt := core.InspectDiskState{DiskID: diskID, CycleStartAt: 12345, CycleID: 7}
	require.NoError(t, st.StoreInspectDiskState(ctx, diskSt))

	chunkSt := core.InspectChunkState{
		Vuid:         proto.Vuid(1001),
		Cursor:       proto.BlobID(100),
		CycleMaxBid:  proto.BlobID(1000),
		CycleCnt:     500,
		CycleScanned: 100,
		BadBids:      map[proto.BlobID]core.BadBidMeta{7: {}, 8: {}},
	}
	require.NoError(t, st.StoreInspectChunkState(ctx, chunkSt))

	// before flush nothing is persisted
	_, err := st.superBlock.ReadInspectDiskState(ctx)
	require.Error(t, err)

	st.FlushInspectState(ctx)

	gotDisk, err := st.superBlock.ReadInspectDiskState(ctx)
	require.NoError(t, err)
	require.Equal(t, diskSt, gotDisk)

	gotChunk, err := st.superBlock.ReadInspectChunkState(ctx, chunkSt.Vuid)
	require.NoError(t, err)
	require.Equal(t, chunkSt, gotChunk)

	// flush is idempotent: no dirty entries left
	st.FlushInspectState(ctx)
	gotDisk, err = st.superBlock.ReadInspectDiskState(ctx)
	require.NoError(t, err)
	require.Equal(t, diskSt, gotDisk)
}

func TestInspectStateStore_DeletePersistsOnFlush(t *testing.T) {
	ctx := context.Background()
	st := newTestInspectStateStore(t)

	require.NoError(t, st.load(ctx))
	vuid := proto.Vuid(1001)
	require.NoError(t, st.StoreInspectChunkState(ctx, core.InspectChunkState{Vuid: vuid, CycleCnt: 3}))
	st.FlushInspectState(ctx)
	_, err := st.superBlock.ReadInspectChunkState(ctx, vuid)
	require.NoError(t, err)

	require.NoError(t, st.DeleteChunkState(ctx, vuid))
	st.FlushInspectState(ctx)

	_, err = st.superBlock.ReadInspectChunkState(ctx, vuid)
	require.Error(t, err)
}

func TestInspectStateStore_DeleteBadBid(t *testing.T) {
	ctx := context.Background()
	st := newTestInspectStateStore(t)

	vuid := proto.Vuid(1001)
	chunkSt := core.InspectChunkState{
		Vuid:    vuid,
		BadBids: map[proto.BlobID]core.BadBidMeta{7: {}, 8: {}},
	}
	require.NoError(t, st.StoreInspectChunkState(ctx, chunkSt))

	// tracked bid is deleted in place
	cleared, err := st.DeleteBadBid(ctx, vuid, proto.BlobID(7))
	require.NoError(t, err)
	require.True(t, cleared)

	got, err := st.LoadInspectChunkState(ctx, vuid)
	require.NoError(t, err)
	require.Equal(t, map[proto.BlobID]core.BadBidMeta{8: {}}, got.BadBids)

	// deleting the same bid again reports false
	cleared, err = st.DeleteBadBid(ctx, vuid, proto.BlobID(7))
	require.NoError(t, err)
	require.False(t, cleared)

	// unknown chunk reports false
	cleared, err = st.DeleteBadBid(ctx, proto.Vuid(9999), proto.BlobID(7))
	require.NoError(t, err)
	require.False(t, cleared)

	// invalid vuid is rejected
	_, err = st.DeleteBadBid(ctx, proto.InvalidVuid, proto.BlobID(7))
	require.Error(t, err)

	// deleted state is persisted on flush
	st.FlushInspectState(ctx)
	persisted, err := st.superBlock.ReadInspectChunkState(ctx, vuid)
	require.NoError(t, err)
	require.Equal(t, map[proto.BlobID]core.BadBidMeta{8: {}}, persisted.BadBids)
}

func TestInspectStateStore_AddBadBid(t *testing.T) {
	ctx := context.Background()
	st := newTestInspectStateStore(t)

	vuid := proto.Vuid(1001)
	require.NoError(t, st.StoreInspectChunkState(ctx, core.InspectChunkState{Vuid: vuid, CycleCnt: 3}))

	// first discovery is recorded
	added, err := st.AddBadBid(ctx, vuid, proto.BlobID(7), core.BadBidMeta{FoundAt: 100, Reason: "crc mismatch"})
	require.NoError(t, err)
	require.True(t, added)

	// first discovery wins: an existing record is never refreshed
	added, err = st.AddBadBid(ctx, vuid, proto.BlobID(7), core.BadBidMeta{FoundAt: 200, Reason: "io error"})
	require.NoError(t, err)
	require.False(t, added)

	got, err := st.LoadInspectChunkState(ctx, vuid)
	require.NoError(t, err)
	require.Equal(t, map[proto.BlobID]core.BadBidMeta{7: {FoundAt: 100, Reason: "crc mismatch"}}, got.BadBids)

	// unknown chunk initializes the zero state so the record survives
	added, err = st.AddBadBid(ctx, proto.Vuid(1002), proto.BlobID(9), core.BadBidMeta{FoundAt: 300})
	require.NoError(t, err)
	require.True(t, added)
	got, err = st.LoadInspectChunkState(ctx, proto.Vuid(1002))
	require.NoError(t, err)
	require.Equal(t, int64(-1), got.CycleCnt)
	require.Equal(t, map[proto.BlobID]core.BadBidMeta{9: {FoundAt: 300}}, got.BadBids)

	// invalid vuid is rejected
	_, err = st.AddBadBid(ctx, proto.InvalidVuid, proto.BlobID(7), core.BadBidMeta{})
	require.Error(t, err)

	// per-chunk cap is enforced
	cappedVuid := proto.Vuid(1003)
	require.NoError(t, st.StoreInspectChunkState(ctx, core.InspectChunkState{Vuid: cappedVuid, CycleCnt: 1}))
	for i := 1; i <= core.MaxInspectBadBids; i++ {
		added, err = st.AddBadBid(ctx, cappedVuid, proto.BlobID(i), core.BadBidMeta{FoundAt: int64(i)})
		require.NoError(t, err)
		require.True(t, added)
	}
	added, err = st.AddBadBid(ctx, cappedVuid, proto.BlobID(core.MaxInspectBadBids+1), core.BadBidMeta{FoundAt: 1})
	require.NoError(t, err)
	require.False(t, added)

	// added bids are persisted on flush
	st.FlushInspectState(ctx)
	persisted, err := st.superBlock.ReadInspectChunkState(ctx, vuid)
	require.NoError(t, err)
	require.Equal(t, map[proto.BlobID]core.BadBidMeta{7: {FoundAt: 100, Reason: "crc mismatch"}}, persisted.BadBids)
}

func TestInspectStateStore_ClosedSuperBlock(t *testing.T) {
	ctx := context.Background()

	testDir, err := os.MkdirTemp(os.TempDir(), "inspect_store_closed")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)
	dir := filepath.Join(testDir, "meta")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	sb, err := NewSuperBlock(dir, &core.Config{})
	require.NoError(t, err)

	st := &inspectStateStore{superBlock: sb, diskID: 11}

	require.NoError(t, st.load(ctx))
	require.NoError(t, st.StoreInspectDiskState(ctx, core.InspectDiskState{DiskID: 11}))
	st.FlushInspectState(ctx)
	_, err = sb.ReadInspectDiskState(ctx)
	require.NoError(t, err)

	// flush skips silently when the superblock is closed
	require.NoError(t, sb.Close(ctx))
	require.NotPanics(t, func() { st.FlushInspectState(ctx) })

	// already-loaded reads keep serving from memory after close
	diskSt, err := st.LoadInspectDiskState(ctx)
	require.NoError(t, err)
	require.Equal(t, core.InspectDiskState{DiskID: 11}, diskSt)

	// loading a fresh store after close errors out instead of touching the closed db
	fresh := &inspectStateStore{superBlock: sb}
	require.ErrorIs(t, fresh.load(ctx), errInspectStoreClosed)
}

func TestInspectStateStore_MissingStateReturnsZeroState(t *testing.T) {
	ctx := context.Background()
	st := newTestInspectStateStore(t)

	diskSt, err := st.LoadInspectDiskState(ctx)
	require.NoError(t, err)
	require.Equal(t, core.InspectDiskState{DiskID: 11}, diskSt)

	chunkSt, err := st.LoadInspectChunkState(ctx, proto.Vuid(1001))
	require.NoError(t, err)
	require.Equal(t, core.InspectChunkState{Vuid: proto.Vuid(1001), CycleCnt: -1}, chunkSt)
}

func TestInspectStateStore_InvalidArgs(t *testing.T) {
	ctx := context.Background()
	st := newTestInspectStateStore(t)

	st.diskID = proto.InvalidDiskID
	_, err := st.LoadInspectDiskState(ctx)
	require.ErrorIs(t, err, bloberr.ErrInvalidParam)
	require.Error(t, st.StoreInspectDiskState(ctx, core.InspectDiskState{DiskID: proto.InvalidDiskID}))
	_, err = st.LoadInspectChunkState(ctx, proto.InvalidVuid)
	require.Error(t, err)
	require.Error(t, st.StoreInspectChunkState(ctx, core.InspectChunkState{Vuid: proto.InvalidVuid}))
	require.Error(t, st.DeleteChunkState(ctx, proto.InvalidVuid))
}

func TestDiskStorage_FlushInspectState(t *testing.T) {
	ctx := context.Background()
	ds := newTestInspectDisk(t)
	store := ds.InspectState()
	require.NotNil(t, store)

	// Store* only touches memory; nothing reaches the superblock yet
	require.NoError(t, store.StoreInspectDiskState(ctx, core.InspectDiskState{DiskID: 11, CycleID: 1}))
	require.NoError(t, store.StoreInspectChunkState(ctx, core.InspectChunkState{Vuid: proto.Vuid(1001), CycleCnt: 5}))
	_, err := ds.SuperBlock.ReadInspectDiskState(ctx)
	require.Error(t, err)

	// explicit flush persists the dirty state
	store.FlushInspectState(ctx)
	_, err = ds.SuperBlock.ReadInspectDiskState(ctx)
	require.NoError(t, err)

	// disk closing (preClose) alone does not stop flush: the superblock is still open
	ds.preClose(ctx)
	require.NoError(t, store.StoreInspectChunkState(ctx, core.InspectChunkState{Vuid: proto.Vuid(1002), CycleCnt: 5}))
	store.FlushInspectState(ctx)

	_, err = ds.SuperBlock.ReadInspectChunkState(ctx, proto.Vuid(1002))
	require.NoError(t, err)

	// once the superblock is closed, flush skips silently and memory keeps serving
	require.NoError(t, ds.SuperBlock.Close(ctx))
	require.NoError(t, store.StoreInspectChunkState(ctx, core.InspectChunkState{Vuid: proto.Vuid(1003), CycleCnt: 5}))
	require.NotPanics(t, func() { store.FlushInspectState(ctx) })
	got, err := store.LoadInspectChunkState(ctx, proto.Vuid(1003))
	require.NoError(t, err)
	require.Equal(t, core.InspectChunkState{Vuid: proto.Vuid(1003), CycleCnt: 5}, got)
}

// TestDiskStorage_InspectStateReturnsStore verifies InspectState exposes the
// concrete inspectStateStore directly (no wrapper layer on DiskStorage).
func TestDiskStorage_InspectStateReturnsStore(t *testing.T) {
	ds := newTestInspectDisk(t)
	store := ds.InspectState()
	require.NotNil(t, store)
	require.Same(t, ds.inspectState(), store)
}

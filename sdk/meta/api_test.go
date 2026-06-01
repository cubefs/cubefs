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

package meta

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/stretchr/testify/require"
)

func newTrashDeleteTestMetaWrapper(t *testing.T, addr string) *MetaWrapper {
	t.Helper()

	mw := &MetaWrapper{
		volname:         "api-test-vol",
		metaSendTimeout: 30,
		conns:           util.NewConnectPool(),
		partitions:      make(map[uint64]*MetaPartition),
		ranges:          btree.New(32),
		dirtyInodes:     newTestDirtyInodeCacheNoBg(DirtyInodeTTL, MaxDirtyInodeCache),
	}
	t.Cleanup(func() { mw.conns.Close() })

	mp := &MetaPartition{
		PartitionID: 1,
		Start:       1,
		End:         1 << 20,
		LeaderAddr:  addr,
		Members:     []string{addr},
	}
	mw.addPartition(mp)
	mw.trashPolicy = &Trash{
		mw:          mw,
		subDirCache: NewDirInodeCache(DefaultDirInodeExpiration, DefaultMaxDirInode),
	}
	return mw
}

// Covers api.go shouldNotMoveToTrash non-empty directory branch (syscall.ENOTEMPTY).
func TestShouldNotMoveToTrashNonemptyDirReturnsENOTEMPTY(t *testing.T) {
	const (
		parentID   = uint64(100)
		childInode = uint64(200)
	)
	dirMode := uint32(os.ModeDir | 0o755)

	addr, cleanup := startMockMetaPacketListener(t, mockLookupThenIgetHandler(childInode, dirMode, 3))
	t.Cleanup(cleanup)

	mw := newTrashDeleteTestMetaWrapper(t, addr)
	mp := mw.getPartitionByInode(parentID)
	require.NotNil(t, mp)

	err, skipTrash := mw.shouldNotMoveToTrash(mp, parentID, "subdir", true, false)
	require.Equal(t, syscall.ENOTEMPTY, err)
	require.False(t, skipTrash)
}

// Covers api.go Delete_ll_EX ENOTEMPTY warn-and-return path when trash is enabled.
func TestDeleteLlEXReturnsENOTEMPTYWhenDirNotEmpty(t *testing.T) {
	const (
		parentID   = uint64(100)
		childInode = uint64(200)
	)
	dirMode := uint32(os.ModeDir | 0o755)

	addr, cleanup := startMockMetaPacketListener(t, mockLookupThenIgetHandler(childInode, dirMode, 3))
	t.Cleanup(cleanup)

	mw := newTrashDeleteTestMetaWrapper(t, addr)

	info, err := mw.Delete_ll_EX(parentID, "subdir", true, 0, "/parent/subdir", false)
	require.Nil(t, info)
	require.Equal(t, syscall.ENOTEMPTY, err)
}

// Covers api.go Delete_ll_EX dirty parent mark when near-read is enabled.
func TestDeleteLlEXMarksParentDirtyWhenNearReadEnabled(t *testing.T) {
	const (
		parentID   = uint64(100)
		childInode = uint64(200)
	)
	dirMode := uint32(os.ModeDir | 0o755)

	addr, cleanup := startMockMetaPacketListener(t, mockLookupThenIgetHandler(childInode, dirMode, 3))
	t.Cleanup(cleanup)

	mw := newTrashDeleteTestMetaWrapper(t, addr)
	mw.FollowerRead = true
	mw.NearRead = true

	_, err := mw.Delete_ll_EX(parentID, "subdir", true, 0, "/parent/subdir", false)
	require.Equal(t, syscall.ENOTEMPTY, err)
	require.True(t, mw.dirtyInodes.isDirty(parentID))
}

// TestRename_ll_TxDstExistsNoOverwriteReturnsEEXIST covers api.go txRename_ll change in commit 85c657c:
// when overwritten=false and dst dentry exists, return EEXIST (not EAGAIN).
func TestRename_ll_TxDstExistsNoOverwriteReturnsEEXIST(t *testing.T) {
	var lookupCalls int32
	addr, cleanup := startMockMetaPacketListener(t, mockLookupAlwaysOKHandler(t, &lookupCalls))
	t.Cleanup(cleanup)

	mw := newTrashDeleteTestMetaWrapper(t, addr)
	mw.EnableTransaction = proto.TxOpMaskRename

	err := mw.Rename_ll(150, "src", 180, "dst", "/data/src", "/.Trash/Current/bucket/dst", false, false)
	require.ErrorIs(t, err, syscall.EEXIST)
	require.Equal(t, int32(2), lookupCalls)
}

func TestBatchInodeGetExtentsUsesAsyncExtentsOpcode(t *testing.T) {
	const inode = uint64(100)
	extentsOpCh := make(chan uint8, 1)

	addr, cleanup := startMockMetaPacketListener(t, mockBatchIgetThenExtentsListHandler(inode, extentsOpCh))
	t.Cleanup(cleanup)

	mw := newTrashDeleteTestMetaWrapper(t, addr)
	infos := mw.BatchInodeGetExtents([]uint64{inode}, true)
	require.Len(t, infos, 1)
	require.NotNil(t, infos[0].Extents)
	require.Equal(t, uint64(3), infos[0].Extents.Generation)

	select {
	case op := <-extentsOpCh:
		require.Equal(t, proto.OpMetaAsyncExtentsList, op)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for async extents list request")
	}
}

func TestBatchInodeGetExtentsUsesSyncExtentsOpcode(t *testing.T) {
	const inode = uint64(100)
	extentsOpCh := make(chan uint8, 1)

	addr, cleanup := startMockMetaPacketListener(t, mockBatchIgetThenExtentsListHandler(inode, extentsOpCh))
	t.Cleanup(cleanup)

	mw := newTrashDeleteTestMetaWrapper(t, addr)
	_ = mw.BatchInodeGetExtents([]uint64{inode}, false)

	select {
	case op := <-extentsOpCh:
		require.Equal(t, proto.OpMetaExtentsList, op)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for sync extents list request")
	}
}

func TestInodeGetExt_llFetchesInodeAndExtents(t *testing.T) {
	const inode = uint64(88)
	extentsOpCh := make(chan uint8, 2)

	addr, cleanup := startMockMetaPacketListener(t, mockInodeGetAndExtentsHandler(inode, extentsOpCh))
	t.Cleanup(cleanup)

	mw := newTrashDeleteTestMetaWrapper(t, addr)
	mw.addPartition(&MetaPartition{
		PartitionID: 1,
		Start:       1,
		End:         1 << 20,
		LeaderAddr:  addr,
		Members:     []string{addr},
	})

	info, err := mw.InodeGetExt_ll(inode)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.NotNil(t, info.Extents)
	require.Equal(t, uint64(11), info.Extents.Generation)
	require.Equal(t, uint64(8192), info.Extents.Size)

	select {
	case op := <-extentsOpCh:
		require.Equal(t, proto.OpMetaExtentsList, op)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for InodeGetExt_ll extents request")
	}
}

func TestGetExtentsPublicAPIUsesSyncOpcode(t *testing.T) {
	ch := make(chan *proto.Packet, 1)
	addr, cleanup := startMockMetaPacketListener(t, mockExtentsListOKHandler(ch))
	t.Cleanup(cleanup)

	mw := newTrashDeleteTestMetaWrapper(t, addr)
	mw.addPartition(&MetaPartition{
		PartitionID: 1,
		Start:       1,
		End:         1 << 20,
		LeaderAddr:  addr,
		Members:     []string{addr},
	})

	const inode = uint64(50)
	gen, size, _, err := mw.GetExtents(inode, false, false, false)
	require.NoError(t, err)
	require.Equal(t, uint64(7), gen)
	require.Equal(t, uint64(4096), size)

	select {
	case got := <-ch:
		require.Equal(t, proto.OpMetaExtentsList, got.Opcode)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for GetExtents request")
	}
}

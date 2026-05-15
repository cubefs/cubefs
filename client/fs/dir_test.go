// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package fs

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
	"github.com/cubefs/cubefs/depends/bazil.org/fuse/fs"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
)

func TestDir_getCwd_nodeCacheMiss_immediate(t *testing.T) {
	t.Parallel()
	const rootIno uint64 = 1
	super := &Super{
		rootIno:   rootIno,
		nodeCache: make(map[uint64]fs.Node),
	}
	d := &Dir{
		super:     super,
		info:      &proto.InodeInfo{Inode: 999},
		parentIno: 2,
		name:      "leaf",
	}
	require.Equal(t, "unknown/", d.getCwd())
}

func TestDir_getCwd_nodeCacheMiss_afterParentSegment(t *testing.T) {
	t.Parallel()
	const rootIno uint64 = 1
	super := &Super{
		rootIno:   rootIno,
		nodeCache: make(map[uint64]fs.Node),
	}
	leaf := &Dir{
		super:     super,
		info:      &proto.InodeInfo{Inode: 100},
		parentIno: 50,
		name:      "leaf",
	}
	super.nodeCache[100] = leaf

	require.Equal(t, "unknown/leaf", leaf.getCwd())
}

func TestDir_getCwd_nodeInCacheButNotDir(t *testing.T) {
	t.Parallel()
	const rootIno uint64 = 1
	super := &Super{
		rootIno:   rootIno,
		nodeCache: make(map[uint64]fs.Node),
	}
	f := &File{
		super:     super,
		info:      &proto.InodeInfo{Inode: 200},
		parentIno: rootIno,
		name:      "notadir",
	}
	super.nodeCache[200] = f

	d := &Dir{
		super:     super,
		info:      &proto.InodeInfo{Inode: 200},
		parentIno: rootIno,
		name:      "x",
	}
	require.Equal(t, "unknown/", d.getCwd())
}

func TestDir_Lookup_metaCacheMissReadDirGate(t *testing.T) {
	t.Parallel()
	now := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	cooldownOk := now.Add(-6 * time.Minute).Unix()

	t.Run("triggers_when_idle", func(t *testing.T) {
		t.Parallel()
		require.True(t, dirLookupMetaCacheAccelerationGate(6, 0, now, 0))
	})

	t.Run("no_trigger_miss_count_not_above_5", func(t *testing.T) {
		t.Parallel()
		require.False(t, dirLookupMetaCacheAccelerationGate(5, 0, now, 0))
	})

	t.Run("no_trigger_while_lastDoing_set", func(t *testing.T) {
		t.Parallel()
		require.False(t, dirLookupMetaCacheAccelerationGate(6, 0, now, 1))
	})

	t.Run("no_trigger_within_5min_since_last", func(t *testing.T) {
		t.Parallel()
		recent := now.Add(-2 * time.Minute).Unix()
		require.False(t, dirLookupMetaCacheAccelerationGate(6, recent, now, 0))
	})

	t.Run("triggers_after_5min_cooldown", func(t *testing.T) {
		t.Parallel()
		require.True(t, dirLookupMetaCacheAccelerationGate(6, cooldownOk, now, 0))
	})

	t.Run("exactly_5min_since_last_triggers", func(t *testing.T) {
		t.Parallel()
		last := now.Add(-5 * time.Minute).Unix()
		require.True(t, dirLookupMetaCacheAccelerationGate(6, last, now, 0))
	})

	t.Run("just_under_5min_no_trigger", func(t *testing.T) {
		t.Parallel()
		last := now.Add(-5*time.Minute + time.Second).Unix()
		require.False(t, dirLookupMetaCacheAccelerationGate(6, last, now, 0))
	})
}

// superForDirMutationTest builds a minimal Super so Dir.* paths can run with inode
// metadata only from ic (InodeGet cache hit), without MetaWrapper / ExtentClient.
func superForDirMutationTest(t *testing.T) *Super {
	t.Helper()
	rm := NewRunningMonitor(0)
	return &Super{
		metaCacheAcceleration: true,
		volname:               "ut-vol",
		volType:               proto.VolumeTypeHot,
		rootIno:               1,
		ic:                    NewInodeCache(time.Hour, 10000, true),
		runningMonitor:        rm,
		nodeCache:             make(map[uint64]fs.Node),
		dirDirtyCache:         make(map[uint64]bool),
		dirDirtyCount:         make(map[uint64]int),
		// File paths resolve storage class via poolCache; test inode helpers use PoolId 0.
		poolCache: map[uint8]*proto.StoragePoolInfo{
			0: {Id: 0, StorageClass: uint8(proto.StorageClass_Replica_HDD)},
		},
	}
}

func dirInodeInfoForMutationTest(ino uint64) *proto.InodeInfo {
	now := time.Now()
	return &proto.InodeInfo{
		Inode:        ino,
		Mode:         uint32(os.ModeDir | 0o755),
		Nlink:        2,
		Uid:          1000,
		Gid:          1000,
		Size:         4096,
		AccessTime:   now,
		ModifyTime:   now,
		CreateTime:   now,
		Extents:      &proto.GetExtentsResponse{},
		StorageClass: proto.StorageClass_Replica_HDD,
	}
}

func fileInodeInfoForMutationTest(ino uint64) *proto.InodeInfo {
	now := time.Now()
	return &proto.InodeInfo{
		Inode:        ino,
		Mode:         uint32(0o644),
		Nlink:        1,
		Uid:          1000,
		Gid:          1000,
		Size:         0,
		AccessTime:   now,
		ModifyTime:   now,
		CreateTime:   now,
		Extents:      &proto.GetExtentsResponse{},
		StorageClass: proto.StorageClass_Replica_HDD,
	}
}

func TestDir_Setattr_metaAccel_beginEndPaired_inodeFromIcache(t *testing.T) {
	t.Parallel()
	const dirIno uint64 = 88001
	s := superForDirMutationTest(t)
	info := dirInodeInfoForMutationTest(dirIno)
	s.ic.Put(info)

	d := NewDir(s, info, 1, "utdir").(*Dir)

	req := &fuse.SetattrRequest{Header: fuse.Header{Pid: 4242}}
	resp := &fuse.SetattrResponse{}

	err := d.Setattr(context.Background(), req, resp)
	require.NoError(t, err)
	require.NotZero(t, resp.Attr.Inode)

	_, inCount := s.dirDirtyCount[dirIno]
	require.False(t, inCount, "EndDirMutation must clear count after Setattr returns")
}

func TestDir_Link_nonFileOld_returnsEPermBeforeBegin(t *testing.T) {
	t.Parallel()
	s := superForDirMutationTest(t)
	const parentIno = uint64(88010)
	srcDir := NewDir(s, dirInodeInfoForMutationTest(parentIno), 1, "p").(*Dir)
	dstDir := NewDir(s, dirInodeInfoForMutationTest(parentIno+1), 1, "q").(*Dir)

	_, err := srcDir.Link(context.Background(), &fuse.LinkRequest{
		Header:  fuse.Header{Pid: 1},
		NewName: "hard",
	}, dstDir)
	require.ErrorIs(t, err, fuse.EPERM)
	require.Empty(t, s.dirDirtyCount, "Link must reject non-*File before BeginDirMutation")
}

func TestDir_Link_nonRegularFile_returnsEPermBeforeBegin(t *testing.T) {
	t.Parallel()
	s := superForDirMutationTest(t)
	const parentIno = uint64(88020)
	srcDir := NewDir(s, dirInodeInfoForMutationTest(parentIno), 1, "p").(*Dir)

	old := NewFile(s, &proto.InodeInfo{
		Inode:        88021,
		Mode:         uint32(os.ModeSymlink | 0o777),
		Nlink:        1,
		StorageClass: proto.StorageClass_Replica_HDD,
	}, syscall.O_RDONLY, parentIno, "sym").(*File)

	_, err := srcDir.Link(context.Background(), &fuse.LinkRequest{
		Header:  fuse.Header{Pid: 1},
		NewName: "l",
	}, old)
	require.ErrorIs(t, err, fuse.EPERM)
	require.Empty(t, s.dirDirtyCount)
}

func TestDir_Mknod_rdevNonZero_returnsENOSYSBeforeBegin(t *testing.T) {
	t.Parallel()
	s := superForDirMutationTest(t)
	d := NewDir(s, dirInodeInfoForMutationTest(88030), 1, "d").(*Dir)

	_, err := d.Mknod(context.Background(), &fuse.MknodRequest{
		Header: fuse.Header{Pid: 1},
		Name:   "dev",
		Rdev:   1,
	})
	require.ErrorIs(t, err, fuse.ENOSYS)
	require.Empty(t, s.dirDirtyCount)
}

func TestDir_Rename_nonDirDst_returnsENOTSUPBeforeBegin(t *testing.T) {
	t.Parallel()
	s := superForDirMutationTest(t)
	src := NewDir(s, dirInodeInfoForMutationTest(88040), 1, "src").(*Dir)
	dstFile := NewFile(s, fileInodeInfoForMutationTest(88041), syscall.O_RDONLY, 88040, "notadir").(*File)

	err := src.Rename(context.Background(), &fuse.RenameRequest{
		Header:  fuse.Header{Pid: 1},
		OldName: "a",
		NewName: "b",
	}, dstFile)
	require.ErrorIs(t, err, fuse.ENOTSUP)
	require.Empty(t, s.dirDirtyCount)
}

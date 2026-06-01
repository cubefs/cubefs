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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
	"github.com/cubefs/cubefs/depends/bazil.org/fuse/fs"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
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

func TestDir_ReadDir_metaCacheAccelerationUsesBatchInodeGetExtentsAsync(t *testing.T) {
	const parentIno = uint64(300)
	extentsAsync := make([]bool, 0)

	addr, cleanup := startUTMetaPacketListener(t, func(conn net.Conn) error {
		for {
			pkt := proto.NewPacket()
			if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
				return err
			}
			resp := proto.NewPacketReqID()
			resp.ReqID = pkt.ReqID
			resp.Opcode = pkt.Opcode
			resp.PartitionID = pkt.PartitionID
			resp.ResultCode = proto.OpOk

			var body []byte
			var err error
			switch pkt.Opcode {
			case proto.OpMetaReadDirLimit:
				body, err = json.Marshal(&proto.ReadDirLimitResponse{
					Children: []proto.Dentry{
						{Name: "a", Inode: 301, Type: uint32(syscall.S_IFREG | 0o644)},
						{Name: "b", Inode: 302, Type: uint32(syscall.S_IFREG | 0o644)},
					},
				})
			case proto.OpMetaBatchInodeGet:
				body, err = json.Marshal(&proto.BatchInodeGetResponse{
					Infos: []*proto.InodeInfo{
						{Inode: 301, Mode: 0o100644},
						{Inode: 302, Mode: 0o100644},
					},
				})
			case proto.OpMetaExtentsList, proto.OpMetaAsyncExtentsList:
				extentsAsync = append(extentsAsync, pkt.Opcode == proto.OpMetaAsyncExtentsList)
				body, err = json.Marshal(&proto.GetExtentsResponse{Generation: 1, Size: 0})
			default:
				return nil
			}
			if err != nil {
				return err
			}
			resp.Data = body
			resp.Size = uint32(len(body))
			if err = resp.WriteToConn(conn); err != nil {
				return err
			}
		}
	})
	t.Cleanup(cleanup)

	mw := meta.NewTestMetaWrapperWithLeader(t, addr)
	rm := NewRunningMonitor(30)
	rm.Start()
	t.Cleanup(rm.Stop)

	super := &Super{
		volname:               "ut-vol",
		mw:                    mw,
		metaCacheAcceleration: true,
		ic:                    NewInodeCache(time.Hour, 4096, true),
		runningMonitor:        rm,
		disableDcache:         false,
	}
	d := &Dir{
		super: super,
		info:  &proto.InodeInfo{Inode: parentIno},
		name:  "dir",
		dctx:  NewDirContexts(),
	}

	dirents, err := d.ReadDir(context.Background(), &fuse.ReadRequest{
		Handle: 1,
		Offset: 0,
	}, &fuse.ReadResponse{})
	if err != nil {
		require.ErrorIs(t, err, io.EOF)
	}
	require.Len(t, dirents, 4) // ".", "..", "a", "b"
	require.NotEmpty(t, extentsAsync)
	for i, async := range extentsAsync {
		require.True(t, async, "ReadDir extents request %d should use OpMetaAsyncExtentsList", i)
	}
	require.NotNil(t, super.ic.Get(301))
	require.NotNil(t, super.ic.Get(302))
}

func TestDir_ReadDir_withoutMetaCacheAccelerationUsesBatchInodeGet(t *testing.T) {
	const parentIno = uint64(400)
	var batchInodeGetCalls int
	extentsOpcodeSeen := false

	addr, cleanup := startUTMetaPacketListener(t, func(conn net.Conn) error {
		for {
			pkt := proto.NewPacket()
			if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
				return err
			}
			resp := proto.NewPacketReqID()
			resp.ReqID = pkt.ReqID
			resp.Opcode = pkt.Opcode
			resp.PartitionID = pkt.PartitionID
			resp.ResultCode = proto.OpOk

			var body []byte
			var err error
			switch pkt.Opcode {
			case proto.OpMetaReadDirLimit:
				body, err = json.Marshal(&proto.ReadDirLimitResponse{
					Children: []proto.Dentry{
						{Name: "c", Inode: 401, Type: uint32(syscall.S_IFREG | 0o644)},
					},
				})
			case proto.OpMetaBatchInodeGet:
				batchInodeGetCalls++
				body, err = json.Marshal(&proto.BatchInodeGetResponse{
					Infos: []*proto.InodeInfo{{Inode: 401, Mode: 0o100644}},
				})
			case proto.OpMetaExtentsList, proto.OpMetaAsyncExtentsList:
				extentsOpcodeSeen = true
				body, err = json.Marshal(&proto.GetExtentsResponse{Generation: 1, Size: 0})
			default:
				return nil
			}
			if err != nil {
				return err
			}
			resp.Data = body
			resp.Size = uint32(len(body))
			if err = resp.WriteToConn(conn); err != nil {
				return err
			}
		}
	})
	t.Cleanup(cleanup)

	mw := meta.NewTestMetaWrapperWithLeader(t, addr)
	rm := NewRunningMonitor(30)
	rm.Start()
	t.Cleanup(rm.Stop)

	super := &Super{
		volname:               "ut-vol",
		mw:                    mw,
		metaCacheAcceleration: false,
		ic:                    NewInodeCache(time.Hour, 4096, true),
		runningMonitor:        rm,
		disableDcache:         false,
	}
	d := &Dir{
		super: super,
		info:  &proto.InodeInfo{Inode: parentIno},
		name:  "dir",
		dctx:  NewDirContexts(),
	}

	dirents, err := d.ReadDir(context.Background(), &fuse.ReadRequest{
		Handle: 1,
		Offset: 0,
	}, &fuse.ReadResponse{})
	if err != nil {
		require.ErrorIs(t, err, io.EOF)
	}
	require.Len(t, dirents, 3) // ".", "..", "c"
	require.Equal(t, 1, batchInodeGetCalls)
	require.False(t, extentsOpcodeSeen, "ReadDir without metaCacheAcceleration must not fetch extents")
	require.NotNil(t, super.ic.Get(401))
}

func TestDir_readDirAllBatchesInodeGetsPerReadDirLimit(t *testing.T) {
	const parentIno uint64 = 100

	mw := &readDirAllMetaClientMock{
		pages: readDirAllPagedDentries(),
	}
	super := &Super{
		rootIno:       parentIno,
		ic:            NewInodeCache(time.Hour, 4096, true),
		nodeCache:     make(map[uint64]fs.Node),
		inodeLruLimit: 4096,
	}
	d := &Dir{
		super: super,
		info:  &proto.InodeInfo{Inode: parentIno},
		name:  "dir",
	}

	dirents, err := d.readDirAll(mw)
	require.NoError(t, err)
	require.Len(t, dirents, DefaultReaddirLimit+2)
	require.Equal(t, "file-0000", dirents[0].Name)
	require.Equal(t, "file-1025", dirents[len(dirents)-1].Name)

	require.Equal(t, []string{"", "file-1023"}, mw.readDirMarkers)
	require.Len(t, mw.batchInodeCalls, 2)
	require.Len(t, mw.batchInodeCalls[0], DefaultReaddirLimit)
	require.Equal(t, uint64(1), mw.batchInodeCalls[0][0])
	require.Equal(t, uint64(1024), mw.batchInodeCalls[0][len(mw.batchInodeCalls[0])-1])
	require.Equal(t, []uint64{1025, 1026}, mw.batchInodeCalls[1])
	require.NotNil(t, super.ic.Get(1))
	require.NotNil(t, super.ic.Get(1026))
}

func TestDir_readDirAllBatchesInodeGetExtentsWhenMetaCacheAcceleration(t *testing.T) {
	const parentIno uint64 = 200

	mw := &readDirAllMetaClientMock{
		pages: readDirAllPagedDentries(),
	}
	super := &Super{
		rootIno:               parentIno,
		ic:                    NewInodeCache(time.Hour, 4096, true),
		nodeCache:             make(map[uint64]fs.Node),
		metaCacheAcceleration: true,
		inodeLruLimit:         4096,
		dirDirtyCache:         map[uint64]bool{parentIno: false},
	}
	d := &Dir{
		super: super,
		info:  &proto.InodeInfo{Inode: parentIno},
		name:  "dir",
	}

	dirents, err := d.readDirAll(mw)
	require.NoError(t, err)
	require.Len(t, dirents, DefaultReaddirLimit+2)

	require.Empty(t, mw.batchInodeCalls)
	require.Len(t, mw.batchInodeExtentsCalls, 2)
	require.Equal(t, []bool{true, true}, mw.batchInodeExtentsAsync)
	require.Len(t, mw.batchInodeExtentsCalls[0], DefaultReaddirLimit)
	require.Equal(t, uint64(1), mw.batchInodeExtentsCalls[0][0])
	require.Equal(t, uint64(1024), mw.batchInodeExtentsCalls[0][len(mw.batchInodeExtentsCalls[0])-1])
	require.Equal(t, []uint64{1025, 1026}, mw.batchInodeExtentsCalls[1])
	require.NotNil(t, super.ic.Get(1))
	require.NotNil(t, super.ic.Get(1026))
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

type readDirAllMetaClientMock struct {
	pages                  map[string][]proto.Dentry
	readDirMarkers         []string
	batchInodeCalls        [][]uint64
	batchInodeExtentsCalls [][]uint64
	batchInodeExtentsAsync []bool
}

func (m *readDirAllMetaClientMock) ReadDirLimit_ll(parentID uint64, from string, limit uint64, isAsync bool) ([]proto.Dentry, error) {
	m.readDirMarkers = append(m.readDirMarkers, from)
	children := m.pages[from]
	return append([]proto.Dentry(nil), children...), nil
}

func (m *readDirAllMetaClientMock) BatchInodeGet(inodes []uint64) []*proto.InodeInfo {
	m.batchInodeCalls = append(m.batchInodeCalls, append([]uint64(nil), inodes...))
	return inodeInfosFor(inodes)
}

func (m *readDirAllMetaClientMock) BatchInodeGetExtents(inodes []uint64, async bool) []*proto.InodeInfo {
	m.batchInodeExtentsCalls = append(m.batchInodeExtentsCalls, append([]uint64(nil), inodes...))
	m.batchInodeExtentsAsync = append(m.batchInodeExtentsAsync, async)
	return inodeInfosFor(inodes)
}

func readDirAllPagedDentries() map[string][]proto.Dentry {
	firstPage := make([]proto.Dentry, 0, DefaultReaddirLimit)
	for i := 0; i < DefaultReaddirLimit; i++ {
		firstPage = append(firstPage, proto.Dentry{
			Name:  fmt.Sprintf("file-%04d", i),
			Inode: uint64(i + 1),
		})
	}
	secondPage := []proto.Dentry{
		firstPage[len(firstPage)-1],
		{Name: "file-1024", Inode: 1025},
		{Name: "file-1025", Inode: 1026},
	}
	return map[string][]proto.Dentry{
		"":          firstPage,
		"file-1023": secondPage,
	}
}

func inodeInfosFor(inodes []uint64) []*proto.InodeInfo {
	infos := make([]*proto.InodeInfo, 0, len(inodes))
	for _, ino := range inodes {
		infos = append(infos, &proto.InodeInfo{Inode: ino})
	}
	return infos
}

var utMetaProtoOnce sync.Once

func utMetaInitProto() {
	utMetaProtoOnce.Do(func() {
		proto.InitBufferPool(int64(32768))
	})
}

func startUTMetaPacketListener(t *testing.T, handler func(conn net.Conn) error) (addr string, cleanup func()) {
	t.Helper()
	utMetaInitProto()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr = ln.Addr().String()
	ready := make(chan struct{})
	done := make(chan struct{})

	go func() {
		close(ready)
		for {
			conn, err := ln.Accept()
			if err != nil {
				break
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = handler(c)
			}(conn)
		}
		close(done)
	}()

	<-ready

	cleanup = func() {
		_ = ln.Close()
		<-done
	}
	return addr, cleanup
}

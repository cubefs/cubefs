// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package fs

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/stretchr/testify/require"
)

var inodeTestProtoOnce sync.Once

func inodeTestInitProto() {
	inodeTestProtoOnce.Do(func() {
		proto.InitBufferPool(int64(32768))
	})
}

func startInodeTestMetaListener(t *testing.T, handler func(net.Conn) error) (addr string, cleanup func()) {
	t.Helper()
	inodeTestInitProto()
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

func mockInodeGetHandler(info *proto.InodeInfo, resultCode uint8) func(net.Conn) error {
	return func(conn net.Conn) error {
		pkt := proto.NewPacket()
		if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
			return err
		}
		resp := proto.NewPacketReqID()
		resp.ReqID = pkt.ReqID
		resp.Opcode = pkt.Opcode
		resp.PartitionID = pkt.PartitionID
		resp.ResultCode = resultCode
		if resultCode == proto.OpOk {
			body, err := json.Marshal(&proto.InodeGetResponse{Info: info})
			if err != nil {
				return err
			}
			resp.Data = body
			resp.Size = uint32(len(body))
		}
		return resp.WriteToConn(conn)
	}
}

func superForLoadInodeTest(t *testing.T, mw *meta.MetaWrapper, ec *stream.ExtentClient) *Super {
	t.Helper()
	s := superForDirMutationTest(t)
	s.mw = mw
	s.ec = ec
	return s
}

func TestInodeGet_cacheHit(t *testing.T) {
	t.Parallel()
	const ino = uint64(50001)
	s := superForDirMutationTest(t)
	cached := fileInodeInfoForMutationTest(ino)
	s.ic.Put(cached)

	info, err := s.InodeGet(ino)
	require.NoError(t, err)
	require.Equal(t, cached.Inode, info.Inode)
}

func TestInodeGet_cacheMissDelegatesToLoadInodeInfo(t *testing.T) {
	t.Parallel()
	const ino = uint64(60001)
	want := fileInodeInfoForMutationTest(ino)

	addr, cleanup := startInodeTestMetaListener(t, mockInodeGetHandler(want, proto.OpOk))
	t.Cleanup(cleanup)

	mw := meta.NewTestMetaWrapperWithLeader(t, addr)
	s := superForLoadInodeTest(t, mw, stream.NewTestExtentClient(nil))

	info, err := s.InodeGet(ino)
	require.NoError(t, err)
	require.Equal(t, ino, info.Inode)
	require.NotNil(t, s.ic.Get(ino))
}

func TestLoadInodeInfo_metaError(t *testing.T) {
	t.Parallel()
	const ino = uint64(60002)
	addr, cleanup := startInodeTestMetaListener(t, mockInodeGetHandler(nil, proto.OpErr))
	t.Cleanup(cleanup)

	mw := meta.NewTestMetaWrapperWithLeader(t, addr)
	s := superForLoadInodeTest(t, mw, stream.NewTestExtentClient(nil))

	_, err := s.LoadInodeInfo(ino)
	require.Error(t, err)
}

func TestLoadInodeInfo_metaNilInfo(t *testing.T) {
	t.Parallel()
	const ino = uint64(60003)
	addr, cleanup := startInodeTestMetaListener(t, mockInodeGetHandler(nil, proto.OpOk))
	t.Cleanup(cleanup)

	mw := meta.NewTestMetaWrapperWithLeader(t, addr)
	s := superForLoadInodeTest(t, mw, stream.NewTestExtentClient(nil))

	_, err := s.LoadInodeInfo(ino)
	require.ErrorIs(t, err, fuse.ENOENT)
}

func TestLoadInodeInfo_migratedForceRefreshError(t *testing.T) {
	t.Parallel()
	const ino = uint64(60004)
	oldInfo := fileInodeInfoForMutationTest(ino)
	oldInfo.PoolId = 0
	newInfo := fileInodeInfoForMutationTest(ino)
	newInfo.PoolId = 1

	addr, cleanup := startInodeTestMetaListener(t, mockInodeGetHandler(newInfo, proto.OpOk))
	t.Cleanup(cleanup)

	mw := meta.NewTestMetaWrapperWithLeader(t, addr)
	ec := stream.NewTestExtentClient(func(uint64, bool, bool, bool) (uint64, uint64, []proto.ExtentKey, error) {
		return 0, 0, nil, errors.New("force refresh failed")
	})
	ec.RegisterTestStreamer(stream.NewTestStreamer(ec, ino))

	s := superForLoadInodeTest(t, mw, ec)
	s.nodeCache[ino] = newTestFile(s, oldInfo, 1, "mig.txt")

	info, err := s.LoadInodeInfo(ino)
	require.Error(t, err)
	require.Contains(t, err.Error(), "force refresh failed")
	require.NotNil(t, info)
}

func TestLoadInodeInfo_refreshExtentsSuccess(t *testing.T) {
	t.Parallel()
	const ino = uint64(60005)
	want := fileInodeInfoForMutationTest(ino)

	addr, cleanup := startInodeTestMetaListener(t, mockInodeGetHandler(want, proto.OpOk))
	t.Cleanup(cleanup)

	mw := meta.NewTestMetaWrapperWithLeader(t, addr)
	var refreshCalls int32
	ec := stream.NewTestExtentClient(func(uint64, bool, bool, bool) (uint64, uint64, []proto.ExtentKey, error) {
		atomic.AddInt32(&refreshCalls, 1)
		return 1, 0, nil, nil
	})
	ec.RegisterTestStreamer(stream.NewTestStreamer(ec, ino))

	s := superForLoadInodeTest(t, mw, ec)
	info, err := s.LoadInodeInfo(ino)
	require.NoError(t, err)
	require.Equal(t, ino, info.Inode)
	require.Equal(t, int32(1), atomic.LoadInt32(&refreshCalls))
}

func TestLoadInodeInfo_blobStoreSkipsExtentRefresh(t *testing.T) {
	t.Parallel()
	const ino = uint64(60006)
	want := fileInodeInfoForMutationTest(ino)
	want.StorageClass = proto.StorageClass_BlobStore

	addr, cleanup := startInodeTestMetaListener(t, mockInodeGetHandler(want, proto.OpOk))
	t.Cleanup(cleanup)

	mw := meta.NewTestMetaWrapperWithLeader(t, addr)
	var refreshCalls int32
	ec := stream.NewTestExtentClient(func(uint64, bool, bool, bool) (uint64, uint64, []proto.ExtentKey, error) {
		atomic.AddInt32(&refreshCalls, 1)
		return 0, 0, nil, nil
	})
	s := superForLoadInodeTest(t, mw, ec)

	info, err := s.LoadInodeInfo(ino)
	require.NoError(t, err)
	require.Equal(t, ino, info.Inode)
	require.Equal(t, int32(0), atomic.LoadInt32(&refreshCalls))
}

func TestLoadInodeInfo_dirInNodeCacheUpdatesInfo(t *testing.T) {
	t.Parallel()
	const ino = uint64(60007)
	old := dirInodeInfoForMutationTest(ino)
	old.Size = 100
	newInfo := dirInodeInfoForMutationTest(ino)
	newInfo.Size = 200

	addr, cleanup := startInodeTestMetaListener(t, mockInodeGetHandler(newInfo, proto.OpOk))
	t.Cleanup(cleanup)

	mw := meta.NewTestMetaWrapperWithLeader(t, addr)
	s := superForLoadInodeTest(t, mw, stream.NewTestExtentClient(nil))
	s.nodeCache[ino] = NewDir(s, old, 1, "subdir").(*Dir)

	info, err := s.LoadInodeInfo(ino)
	require.NoError(t, err)
	require.Equal(t, uint64(200), info.Size)
	dir := s.nodeCache[ino].(*Dir)
	require.Equal(t, uint64(200), dir.info.Size)
}

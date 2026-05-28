// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package stream

import (
	"bytes"
	"errors"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

// Mirrors loadInodeInfoTimer branch in Streamer.server() without starting the goroutine.
func applyLoadInodeInfoTick(s *Streamer) {
	if s.client == nil || s.client.loadInodeInfo == nil {
		return
	}
	_, err := s.client.loadInodeInfo(s.inode)
	if err != nil {
		s.markNeedReloadInode()
	} else {
		s.clearNeedReloadInode()
	}
}

func TestLoadInodeInfoTickSuccessClearsReloadFlag(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		inode: 2001,
		client: &ExtentClient{
			loadInodeInfo: func(uint64) (*proto.InodeInfo, error) {
				return &proto.InodeInfo{Inode: 2001}, nil
			},
		},
	}
	s.markNeedReloadInode()
	applyLoadInodeInfoTick(s)
	require.Equal(t, int32(0), atomic.LoadInt32(&s.needReloadInode))
}

func TestLoadInodeInfoTickFailureMarksReloadFlag(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		inode: 2002,
		client: &ExtentClient{
			loadInodeInfo: func(uint64) (*proto.InodeInfo, error) {
				return nil, errors.New("tick load failed")
			},
		},
	}
	applyLoadInodeInfoTick(s)
	require.Equal(t, int32(1), atomic.LoadInt32(&s.needReloadInode))
}

// Mirrors renewalForbiddenMigration branch when openForWrite is true.
func applyRenewalForbiddenMigrationTick(s *Streamer) error {
	if !s.openForWrite {
		return nil
	}
	err := s.client.renewalForbiddenMigration(s.inode)
	if err != nil {
		s.setError()
	}
	return err
}

func TestRenewalForbiddenMigrationTickReadOnlySkips(t *testing.T) {
	t.Parallel()
	var calls int32
	s := &Streamer{
		inode:        2003,
		openForWrite: false,
		client: &ExtentClient{
			renewalForbiddenMigration: func(uint64) error {
				atomic.AddInt32(&calls, 1)
				return nil
			},
		},
	}
	require.NoError(t, applyRenewalForbiddenMigrationTick(s))
	require.Equal(t, int32(0), atomic.LoadInt32(&calls))
}

func TestRenewalForbiddenMigrationTickErrorSetsStreamerError(t *testing.T) {
	t.Parallel()
	s := &Streamer{
		inode:        2004,
		openForWrite: true,
		client: &ExtentClient{
			renewalForbiddenMigration: func(uint64) error {
				return errors.New("renewal failed")
			},
		},
	}
	err := applyRenewalForbiddenMigrationTick(s)
	require.Error(t, err)
	require.Equal(t, int32(StreamerError), atomic.LoadInt32(&s.status))
}

func TestDoOverwriteIntraGroupNetErrRetriesOtherHost(t *testing.T) {
	if proto.Buffers == nil {
		proto.InitBufferPool(32768)
	}

	var firstHostHits int32
	firstHostAddr, closeFirst := startOverwriteReplyServer(t, proto.OpIntraGroupNetErr, &firstHostHits, 2)
	defer closeFirst()

	var secondHostHits int32
	secondHostAddr, closeSecond := startOverwriteReplyServer(t, proto.OpOk, &secondHostHits, 1)
	defer closeSecond()

	const (
		inode       = uint64(22305304)
		partitionID = uint64(38951)
	)

	dp := &wrapper.DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: partitionID,
			LeaderAddr:  firstHostAddr,
			Hosts:       []string{firstHostAddr, secondHostAddr},
			Status:      proto.ReadWrite,
		},
		Metrics: wrapper.NewDataPartitionMetrics(),
	}

	w := &wrapper.Wrapper{
		HostsStatus: map[string]bool{
			firstHostAddr:  true,
			secondHostAddr: true,
		},
	}
	setUnexportedField(t, w, "partitions", map[uint64]*wrapper.DataPartition{partitionID: dp})
	setUnexportedField(t, w, "volType", int(proto.VolumeTypeHot))
	dp.ClientWrapper = w

	s := &Streamer{
		inode:     inode,
		client:    &ExtentClient{dataWrapper: w, streamRetryTimeout: 3 * time.Second},
		extents:   NewExtentCache(inode),
		dirtylist: NewDirtyExtentList(),
	}
	ek := &proto.ExtentKey{
		FileOffset:   0,
		PartitionId:  partitionID,
		ExtentId:     59,
		ExtentOffset: 25337856,
		Size:         uint32(util.BlockSize),
	}
	s.extents.Append(ek, true)

	req := &ExtentRequest{
		FileOffset: 82973,
		Size:       1024,
		Data:       bytes.Repeat([]byte("a"), 1024),
		ExtentKey:  ek,
	}

	total, err := s.doOverwrite(req, false, 0)
	require.NoError(t, err)
	require.Equal(t, req.Size, total)
	require.GreaterOrEqual(t, atomic.LoadInt32(&firstHostHits), int32(1))
	require.Equal(t, int32(1), atomic.LoadInt32(&secondHostHits))
}

func startOverwriteReplyServer(t *testing.T, resultCode uint8, hitCounter *int32, maxRequests int) (string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < maxRequests; i++ {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}

			req := &Packet{}
			readErr := req.ReadFromConnWithVer(conn, proto.ReadDeadlineTime)
			if readErr == nil {
				atomic.AddInt32(hitCounter, 1)
				reply := NewReply(req.ReqID, req.PartitionID, req.ExtentID)
				reply.ExtentOffset = req.ExtentOffset
				reply.CRC = req.CRC
				reply.ResultCode = resultCode
				_ = reply.WriteToConn(conn)
			}
			_ = conn.Close()
		}
	}()

	closeFn := func() {
		_ = ln.Close()
		<-done
	}
	return ln.Addr().String(), closeFn
}

func setUnexportedField(t *testing.T, target interface{}, fieldName string, value interface{}) {
	t.Helper()

	rv := reflect.ValueOf(target).Elem()
	fv := rv.FieldByName(fieldName)
	require.True(t, fv.IsValid(), "field %s should exist", fieldName)

	ptr := unsafe.Pointer(fv.UnsafeAddr())
	reflect.NewAt(fv.Type(), ptr).Elem().Set(reflect.ValueOf(value))
}

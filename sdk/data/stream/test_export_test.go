// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package stream

import (
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/manager"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestNewTestExtentClientForceRefresh(t *testing.T) {
	t.Parallel()
	const ino = uint64(80001)
	var calls int32
	ec := NewTestExtentClient(func(uint64, bool, bool, bool) (uint64, uint64, []proto.ExtentKey, error) {
		atomic.AddInt32(&calls, 1)
		return 0, 0, nil, errors.New("refresh err")
	})
	ec.RegisterTestStreamer(NewTestStreamer(ec, ino))
	err := ec.ForceRefreshExtentsCache(ino)
	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestReadReloadInodeAtReadEntry(t *testing.T) {
	t.Parallel()
	var loads int32
	ec := NewTestExtentClient(nil)
	ec.readLimiter = rate.NewLimiter(rate.Inf, 128)
	ec.LimitManager = manager.NewLimitManager(ec)
	ec.loadInodeInfo = func(uint64) (*proto.InodeInfo, error) {
		atomic.AddInt32(&loads, 1)
		return &proto.InodeInfo{Inode: 80002, Size: 0}, nil
	}
	s := NewTestStreamer(ec, 80002)
	s.extents.update(1, 4096, false, nil)
	s.markNeedReloadInode()
	buf := make([]byte, 4)
	n, err := s.read(buf, 0, 4, 0)
	require.Equal(t, int32(1), atomic.LoadInt32(&loads))
	require.Equal(t, int32(0), atomic.LoadInt32(&s.needReloadInode))
	require.Equal(t, 4, n)
	if err != nil {
		require.Equal(t, io.EOF, err)
	}
}

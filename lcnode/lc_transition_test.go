// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package lcnode

import (
	"errors"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

type md5ClassifyMW struct {
	*MockMetaWrapper
	inodeGet func(uint64, bool) (*proto.InodeInfo, error)
}

func (m *md5ClassifyMW) InodeGet_ll(inode uint64, isAsync bool) (*proto.InodeInfo, error) {
	if m.inodeGet != nil {
		return m.inodeGet(inode, isAsync)
	}
	return m.MockMetaWrapper.InodeGet_ll(inode, isAsync)
}

func TestClassifyMd5Mismatch(t *testing.T) {
	t.Parallel()
	baseTime := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	e := &proto.ScanDentry{
		Inode: 9001,
		InodeInfo: &proto.InodeInfo{
			Inode:      9001,
			ModifyTime: baseTime,
		},
	}
	mgr := &TransitionMgr{meta: &md5ClassifyMW{MockMetaWrapper: NewMockMetaWrapper()}}

	t.Run("inode get error", func(t *testing.T) {
		t.Parallel()
		m := mgr
		m.meta = &md5ClassifyMW{
			MockMetaWrapper: NewMockMetaWrapper(),
			inodeGet: func(uint64, bool) (*proto.InodeInfo, error) {
				return nil, errors.New("inode gone")
			},
		}
		err := m.classifyMd5Mismatch(e, "dst", "aaa", "bbb")
		require.Error(t, err)
		require.Contains(t, err.Error(), "get inode failed after check md5")
	})

	t.Run("modify time advanced", func(t *testing.T) {
		t.Parallel()
		m := &TransitionMgr{meta: &md5ClassifyMW{
			MockMetaWrapper: NewMockMetaWrapper(),
			inodeGet: func(uint64, bool) (*proto.InodeInfo, error) {
				return &proto.InodeInfo{
					Inode:      e.Inode,
					ModifyTime: baseTime.Add(time.Second),
				}, nil
			},
		}}
		err := m.classifyMd5Mismatch(e, "src", "deadbeef", "expected")
		require.Error(t, err)
		require.Contains(t, err.Error(), "file modified when migrating")
	})

	t.Run("pure md5 mismatch", func(t *testing.T) {
		t.Parallel()
		stale := baseTime.Add(-time.Second)
		m := &TransitionMgr{meta: &md5ClassifyMW{
			MockMetaWrapper: NewMockMetaWrapper(),
			inodeGet: func(uint64, bool) (*proto.InodeInfo, error) {
				return &proto.InodeInfo{Inode: e.Inode, ModifyTime: stale}, nil
			},
		}}
		err := m.classifyMd5Mismatch(e, "dst", "got", "want")
		require.Error(t, err)
		require.Contains(t, err.Error(), "check dst md5 inconsistent")
	})

	t.Run("nil inode info falls through to md5 mismatch", func(t *testing.T) {
		t.Parallel()
		m := &TransitionMgr{meta: &md5ClassifyMW{
			MockMetaWrapper: NewMockMetaWrapper(),
			inodeGet: func(uint64, bool) (*proto.InodeInfo, error) {
				return nil, nil
			},
		}}
		err := m.classifyMd5Mismatch(e, "src", "a", "b")
		require.Error(t, err)
		require.Contains(t, err.Error(), "check src md5 inconsistent")
	})
}

// zeroReadExtent always returns zero bytes (src path).
type zeroReadExtent struct{ *MockExtentClient }

func (z *zeroReadExtent) Read(_ uint64, data []byte, _ int, size int, _ uint8, _ bool) (int, error) {
	if size <= 0 {
		return 0, nil
	}
	n := size
	if n > len(data) {
		n = len(data)
	}
	return n, nil
}

// ffReadExtent returns 0xff bytes (dst migration extent path).
type ffReadExtent struct{ *MockExtentClient }

func (f *ffReadExtent) Read(_ uint64, data []byte, _ int, size int, _ uint8, _ bool) (int, error) {
	if size <= 0 {
		return 0, nil
	}
	n := size
	if n > len(data) {
		n = len(data)
	}
	for i := 0; i < n; i++ {
		data[i] = 0xff
	}
	return n, nil
}

func TestMigrate_dstMd5MismatchUsesClassify(t *testing.T) {
	t.Parallel()
	baseTime := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	e := &proto.ScanDentry{
		Inode: 9100,
		Size:  util.BlockSize,
		InodeInfo: &proto.InodeInfo{
			Inode:      9100,
			ModifyTime: baseTime,
		},
	}
	mgr := &TransitionMgr{
		ec:     &zeroReadExtent{NewMockExtentClient()},
		ecForW: &ffReadExtent{NewMockExtentClient()},
		meta: &md5ClassifyMW{
			MockMetaWrapper: NewMockMetaWrapper(),
			inodeGet: func(uint64, bool) (*proto.InodeInfo, error) {
				return &proto.InodeInfo{Inode: e.Inode, ModifyTime: baseTime}, nil
			},
		},
	}
	err := mgr.migrate(e)
	require.Error(t, err)
	require.Contains(t, err.Error(), "check dst md5 inconsistent")
}

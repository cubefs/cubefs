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
	"sync/atomic"
	"testing"

	"github.com/cubefs/cubefs/proto"
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

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

package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/mergetask"
)

func doBlobFileOp(t *testing.T, f *os.File, emptyIoPool bool) {
	// create
	syncWorker := mergetask.NewMergeTask(-1, func(interface{}) error { return nil })

	ctr := gomock.NewController(t)
	ioPool := mocks.NewMockIoPool(ctr)
	ioPool.EXPECT().Submit(gomock.Any()).Do(func(args base.IoPoolTaskArgs) {
		args.TaskFn()
	}).AnyTimes()
	ioPools := map[bnapi.IOType]base.IoPool{
		bnapi.ReadIO:       ioPool,
		bnapi.WriteIO:      ioPool,
		bnapi.DeleteIO:     ioPool,
		bnapi.BackgroundIO: ioPool,
	}

	ef := blobFile{f, 1, syncWorker, nil, ioPools}
	log.Info(ef.Name())
	fd := ef.Fd()
	require.NotNil(t, fd)

	info, err := ef.Stat()
	require.NoError(t, err)
	require.NotNil(t, info)

	data := []byte("test data")

	// WriteAtCtx
	ctx, cancel := context.WithCancel(context.Background())
	ctx = bnapi.SetIoType(ctx, bnapi.WriteIO)
	n, err := ef.WriteAtCtx(ctx, data, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	cancel()
	n, err = ef.WriteAtCtx(ctx, data, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, n)

	// ReadAtCtx
	ctx, cancel = context.WithCancel(context.Background())
	ctx = bnapi.SetIoType(ctx, bnapi.ReadIO)
	buf := make([]byte, len(data))
	n, err = ef.ReadAtCtx(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, buf)

	cancel()
	n, err = ef.ReadAtCtx(ctx, buf, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, n)

	// stat
	stat, err := ef.SysStat()
	require.NoError(t, err)
	log.Infof("stat: %v", stat)
	require.Equal(t, int32(stat.Size), int32(len(data)))

	log.Infof("blksize: %d", stat.Blocks)

	// pre allocate 1M
	err = ef.Allocate(0, 1*1024*1024)
	require.NoError(t, err)
	stat, err = ef.SysStat()
	require.NoError(t, err)
	log.Infof("blksize: %d", stat.Blocks)
	// scale size
	require.Equal(t, int32(stat.Size), int32(1*1024*1024))
	// phy allocate >= 1M
	require.True(t, int32(stat.Blocks) >= 1*1024*1024/512)

	// punch hole, release phy space
	err = ef.Discard(0, 1*1024*1024)
	require.NoError(t, err)
	stat, err = ef.SysStat()
	require.NoError(t, err)
	log.Infof("blksize: %d", stat.Blocks)
	// keep size
	require.Equal(t, int32(stat.Size), int32(1*1024*1024))
	// phy allocate == 0
	require.Equal(t, int(stat.Blocks), 0)
}

func TestBlobFile_Op(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "BlobFileOp")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	posixfilepath := filepath.Join(testDir, "PoxsixFile")
	log.Info(posixfilepath)

	temppath := filepath.Join(posixfilepath, "xxxtemp")
	f, err := OpenFile(temppath, false)
	require.Error(t, err)
	require.Nil(t, f)

	f, err = OpenFile(posixfilepath, true)
	require.NoError(t, err)
	require.NotNil(t, f)

	// enable io pools
	doBlobFileOp(t, f, false)

	// invalid/empty io pools
	posixfilepath2 := filepath.Join(testDir, "PoxsixFile2")
	f, err = OpenFile(posixfilepath2, true)
	require.NoError(t, err)
	require.NotNil(t, f)

	doBlobFileOp(t, f, true)
}

func TestBlobFile_doTaskFnCtxCancel(t *testing.T) {
	testDir, err := os.MkdirTemp(os.TempDir(), "BlobFileTaskCancel")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	posixfilepath := filepath.Join(testDir, "PoxsixFile")
	log.Info(posixfilepath)

	temppath := filepath.Join(posixfilepath, "xxxtemp")
	f, err := OpenFile(temppath, false)
	require.Error(t, err)
	require.Nil(t, f)

	f, err = OpenFile(posixfilepath, true)
	require.NoError(t, err)

	require.NotNil(t, f)

	// create
	syncWorker := mergetask.NewMergeTask(-1, func(interface{}) error { return nil })

	ctr := gomock.NewController(t)
	ioPool := mocks.NewMockIoPool(ctr)
	ioPools := map[bnapi.IOType]base.IoPool{
		bnapi.ReadIO:       ioPool,
		bnapi.WriteIO:      ioPool,
		bnapi.DeleteIO:     ioPool,
		bnapi.BackgroundIO: ioPool,
	}

	ef := blobFile{f, 1, syncWorker, nil, ioPools}
	fd := ef.Fd()
	require.NotNil(t, fd)

	info, err := ef.Stat()
	require.NoError(t, err)
	require.NotNil(t, info)

	data := []byte("test data")

	// WriteAtCtx
	ctx, cancel := context.WithCancel(context.Background())
	ctx = bnapi.SetIoType(ctx, bnapi.WriteIO)
	ioPool.EXPECT().Submit(gomock.Any()).Do(func(args base.IoPoolTaskArgs) {
		args.TaskFn()
	})
	n, err := ef.WriteAtCtx(ctx, data, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	ioPool.EXPECT().Submit(gomock.Any()).Do(func(args base.IoPoolTaskArgs) {
		cancel()
		args.TaskFn()
	})
	ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)
	n, err = ef.WriteAtCtx(ctx, data, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, n)

	// ReadAtCtx
	ctx, cancel = context.WithCancel(context.Background())
	ctx = bnapi.SetIoType(ctx, bnapi.ReadIO)
	buf := make([]byte, len(data))
	ioPool.EXPECT().Submit(gomock.Any()).Do(func(args base.IoPoolTaskArgs) {
		args.TaskFn()
	})
	n, err = ef.ReadAtCtx(ctx, buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, buf)

	ioPool.EXPECT().Submit(gomock.Any()).Do(func(args base.IoPoolTaskArgs) {
		cancel()
		args.TaskFn()
	})
	ctx = bnapi.SetIoType(ctx, bnapi.BackgroundIO)
	n, err = ef.ReadAtCtx(ctx, buf, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, n)

	require.Panics(t, func() {
		ctx = context.Background()
		ctx = bnapi.SetIoType(ctx, bnapi.IOTypeMax)
		ef.ReadAtCtx(ctx, buf, 0)
	})
}

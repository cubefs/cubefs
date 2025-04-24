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
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/mergetask"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

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

	// create
	syncWorker := mergetask.NewMergeTask(-1, func(interface{}) error { return nil })

	ctr := gomock.NewController(t)
	ioPool := mocks.NewMockIoPool(ctr)
	ioPool.EXPECT().Submit(gomock.Any()).Do(func(args taskpool.IoPoolTaskArgs) {
		args.TaskFn()
	}).AnyTimes()
	ioPools := map[qos.IOTypeRW]taskpool.IoPool{
		qos.IOTypeRead:  ioPool,
		qos.IOTypeWrite: ioPool,
		qos.IOTypeDel:   ioPool,
	}

	ef := blobFile{f, 1, syncWorker, nil, ioPools}
	log.Info(ef.Name())
	fd := ef.Fd()
	require.NotNil(t, fd)

	info, err := ef.Stat()
	require.NoError(t, err)
	require.NotNil(t, info)

	data := []byte("test data")

	// write
	n, err := ef.WriteAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	// read
	buf := make([]byte, len(data))
	n, err = ef.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	require.Equal(t, data, buf)

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

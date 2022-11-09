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

package cacher

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func TestProxyCacherDiskUpdate(t *testing.T) {
	c, cmCli, clean := newCacher(t, 2)
	defer clean()
	cmCli.EXPECT().DiskInfo(A, A).Return(&blobnode.DiskInfo{}, nil).Times(4)

	for range [100]struct{}{} {
		_, err := c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 1})
		require.NoError(t, err)
		_, err = c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 2})
		require.NoError(t, err)
	}

	time.Sleep(time.Second * 4) // expired
	for range [100]struct{}{} {
		_, err := c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 1})
		require.NoError(t, err)
		_, err = c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 2})
		require.NoError(t, err)
	}

	cmCli.EXPECT().DiskInfo(A, A).Return(&blobnode.DiskInfo{}, nil).Times(100)
	for range [100]struct{}{} {
		_, err := c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 1, Flush: true})
		require.NoError(t, err)
	}
}

func TestProxyCacherDiskError(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0)
	defer clean()
	cmCli.EXPECT().DiskInfo(A, A).Return(nil, errors.New("mock error")).Times(1)
	cmCli.EXPECT().DiskInfo(A, A).Return(nil, errcode.ErrCMDiskNotFound).Times(2)

	_, err := c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 1})
	require.Error(t, err)
	_, err = c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 2, Flush: true})
	require.ErrorIs(t, errcode.ErrCMDiskNotFound, err)
	_, err = c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 1, Flush: false})
	require.ErrorIs(t, errcode.ErrCMDiskNotFound, err)
}

func TestProxyCacherDiskCacheMiss(t *testing.T) {
	c, cmCli, clean := newCacher(t, 2)
	defer clean()

	cmCli.EXPECT().DiskInfo(A, A).Return(&blobnode.DiskInfo{}, nil).Times(3)
	_, err := c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 1})
	require.NoError(t, err)
	time.Sleep(time.Second) // waiting diskv write to disk

	basePath := c.(*cacher).config.DiskvBasePath
	{ // memory cache miss, load from diskv
		c, _ = New(1, ConfigCache{DiskvBasePath: basePath, DiskExpirationS: 2}, cmCli)
		_, err = c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 1})
		require.NoError(t, err)
	}
	{ // cannot decode diskv value
		file, err := os.OpenFile(c.(*cacher).DiskvFilename(diskvKeyDisk(1)), os.O_RDWR, 0o644)
		require.NoError(t, err)
		file.Write([]byte("}}}}}"))
		file.Close()

		c, _ = New(1, ConfigCache{DiskvBasePath: basePath, DiskExpirationS: 2}, cmCli)
		_, err = c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 1})
		require.NoError(t, err)
	}
	{ // load diskv expired
		c, _ = New(1, ConfigCache{DiskvBasePath: basePath, DiskExpirationS: 2}, cmCli)
		time.Sleep(time.Second * 3)
		_, err = c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 1})
		require.NoError(t, err)
	}
}

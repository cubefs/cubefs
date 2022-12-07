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
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

var (
	A = gomock.Any()
	C = gomock.NewController
)

func TestMain(m *testing.M) {
	basePath := path.Join(os.TempDir(), "proxy-cacher")
	os.RemoveAll(basePath)
	exitCode := m.Run()
	time.Sleep(500 * time.Millisecond)
	os.RemoveAll(basePath)
	os.Exit(exitCode)
}

func newCacher(t gomock.TestReporter, expiration int) (Cacher, *mocks.MockClientAPI, func()) {
	cmCli := mocks.NewMockClientAPI(C(t))
	var basePath string
	for {
		basePath = path.Join(os.TempDir(), "proxy-cacher", fmt.Sprintf("%d", rand.Intn(10000)+10000))
		if _, err := os.Stat(basePath); err != nil && errors.Is(err, os.ErrNotExist) {
			break
		}
	}
	cacher, _ := New(1, ConfigCache{
		DiskvBasePath:     basePath,
		VolumeExpirationS: expiration,
		DiskExpirationS:   expiration,
	}, cmCli)
	return cacher, cmCli, func() { os.RemoveAll(basePath) }
}

func TestProxyCacherConfigVolume(t *testing.T) {
	config := ConfigCache{}
	getCacher := func() *cacher {
		c, err := New(1, config, nil)
		require.NoError(t, err)
		return c.(*cacher)
	}

	for _, cs := range []struct {
		capacity, expCapacity     int
		expiration, expExpiration int
	}{
		{0, _defaultCapacity, 0, _defaultExpirationS},
		{-100, _defaultCapacity, 0, _defaultExpirationS},
		{-100, _defaultCapacity, -1, _defaultExpirationS},
		{1 << 11, 1 << 11, 600, 600},
	} {
		config.VolumeCapacity = cs.capacity
		config.VolumeExpirationS = cs.expiration
		c := getCacher()
		require.Equal(t, cs.expCapacity, c.config.VolumeCapacity)
		require.Equal(t, cs.expExpiration, c.config.VolumeExpirationS)
	}
}

func TestProxyCacherErase(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0)
	defer clean()
	cc := c.(*cacher)

	ctx, expErr := context.Background(), errcode.ErrIllegalArguments
	for _, key := range []string{
		"", "-", "abc", "namespace-", "-volume", "disk-not-a-number",
		"volume-", "volume-0", "disk-2147483647", "disk-2147483648",
	} {
		require.ErrorIs(t, c.Erase(ctx, key), expErr)
	}

	{
		cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil)
		_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
		<-cc.syncChan
		require.NoError(t, c.Erase(ctx, diskvKeyVolume(1)))
		require.Nil(t, cc.volumeCache.Get(proto.Vid(1)))
		require.False(t, cc.diskv.Has(diskvKeyVolume(1)))
	}
	{
		require.Error(t, c.Erase(ctx, diskvKeyDisk(1)))
		require.Nil(t, cc.diskCache.Get(proto.DiskID(1)))
		require.False(t, cc.diskv.Has(diskvKeyDisk(1)))
	}

	{
		cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil)
		cmCli.EXPECT().DiskInfo(A, A).Return(&blobnode.DiskInfo{}, nil)
		_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
		<-cc.syncChan
		_, err = c.GetDisk(context.Background(), &proxy.CacheDiskArgs{DiskID: 1})
		require.NoError(t, err)
		<-cc.syncChan

		require.NotNil(t, cc.volumeCache.Get(proto.Vid(1)))
		require.NotNil(t, cc.diskCache.Get(proto.DiskID(1)))
		require.True(t, cc.diskv.Has(diskvKeyVolume(1)))
		require.True(t, cc.diskv.Has(diskvKeyDisk(1)))

		require.NoError(t, c.Erase(ctx, "ALL"))
		require.Nil(t, cc.volumeCache.Get(proto.Vid(1)))
		require.Nil(t, cc.diskCache.Get(proto.DiskID(1)))
		require.False(t, cc.diskv.Has(diskvKeyVolume(1)))
		require.False(t, cc.diskv.Has(diskvKeyDisk(1)))
	}
}

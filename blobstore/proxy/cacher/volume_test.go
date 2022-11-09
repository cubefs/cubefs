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
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/peterbourgon/diskv/v3"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var (
	A = gomock.Any()
	C = gomock.NewController
)

func newCacher(t gomock.TestReporter, expiration int) (Cacher, *mocks.MockClientAPI, func()) {
	cmCli := mocks.NewMockClientAPI(C(t))
	basePath := path.Join(os.TempDir(), fmt.Sprintf("proxy-cacher-%d", rand.Intn(1000)+1000))
	cacher, _ := New(1, ConfigCache{
		DiskvBasePath:     basePath,
		VolumeExpirationS: expiration,
		DiskExpirationS:   expiration,
	}, cmCli)
	return cacher, cmCli, func() { os.RemoveAll(basePath) }
}

func TestProxyCacherVolumeUpdate(t *testing.T) {
	c, cmCli, clean := newCacher(t, 2)
	defer clean()
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).Times(4)

	for range [100]struct{}{} {
		_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
	}
	for range [100]struct{}{} {
		_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 2})
		require.NoError(t, err)
	}

	time.Sleep(time.Second * 4) // expired
	for range [100]struct{}{} {
		_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
	}
	for range [100]struct{}{} {
		_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 2})
		require.NoError(t, err)
	}
}

func TestProxyCacherVolumeFlush(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0)
	defer clean()

	volume := new(clustermgr.VolumeInfo)
	volume.Units = []clustermgr.Unit{{Vuid: 1234}, {Vuid: 5678}}
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(1)
	for range [100]struct{}{} {
		vol, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
		require.Equal(t, uint32(0x9d31f755), vol.Version)
	}

	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(100)
	for range [100]struct{}{} {
		vol, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1, Flush: true})
		require.NoError(t, err)
		require.Equal(t, uint32(0x9d31f755), vol.Version)
	}

	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(1)
	for range [100]struct{}{} {
		vol, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 3, Flush: true, Version: 0x01})
		require.NoError(t, err)
		require.Equal(t, uint32(0x9d31f755), vol.Version)
	}

	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(100)
	for range [100]struct{}{} {
		vol, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 4, Flush: true, Version: 0x9d31f755})
		require.NoError(t, err)
		require.Equal(t, uint32(0x9d31f755), vol.Version)
	}
}

func TestProxyCacherVolumeSingle(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0)
	defer clean()
	cmCli.EXPECT().GetVolumeInfo(A, A).DoAndReturn(
		func(_ context.Context, _ *clustermgr.GetVolumeArgs) (*clustermgr.VolumeInfo, error) {
			time.Sleep(time.Second)
			return &clustermgr.VolumeInfo{}, nil
		}).Times(3)

	var wg sync.WaitGroup
	wg.Add(_defaultClustermgrConcurrency)
	for range [_defaultClustermgrConcurrency]struct{}{} {
		go func() {
			c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
			wg.Done()
		}()
	}
	wg.Wait()

	wg.Add(_defaultClustermgrConcurrency + 1)
	for range [_defaultClustermgrConcurrency + 1]struct{}{} {
		go func() {
			c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 2})
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestProxyCacherVolumeError(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0)
	defer clean()
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(nil, errors.New("mock error")).Times(1)
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(nil, errcode.ErrVolumeNotExist).Times(2)

	_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
	require.Error(t, err)
	_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 2, Flush: true})
	require.ErrorIs(t, errcode.ErrVolumeNotExist, err)
	_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1, Flush: false})
	require.ErrorIs(t, errcode.ErrVolumeNotExist, err)
}

func TestProxyCacherVolumeCacheMiss(t *testing.T) {
	c, cmCli, clean := newCacher(t, 2)
	defer clean()

	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).Times(3)
	_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
	require.NoError(t, err)
	time.Sleep(time.Microsecond * 200) // waiting diskv write to disk

	basePath := c.(*cacher).config.DiskvBasePath
	{ // memory cache miss, load from diskv
		c, _ = New(1, ConfigCache{DiskvBasePath: basePath, VolumeExpirationS: 2}, cmCli)
		_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
	}
	{ // cannot decode diskv value
		file, err := os.OpenFile(path.Join(basePath, "a7", "13", "volume-1"), os.O_RDWR, 0o644)
		require.NoError(t, err)
		file.Write([]byte("}}}}}"))
		file.Close()

		c, _ = New(1, ConfigCache{DiskvBasePath: basePath, VolumeExpirationS: 2}, cmCli)
		_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
	}
	{ // load diskv expired
		c, _ = New(1, ConfigCache{DiskvBasePath: basePath, VolumeExpirationS: 2}, cmCli)
		time.Sleep(time.Second * 3)
		_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
	}
}

func BenchmarkProxyMemoryHit(b *testing.B) {
	c, cmCli, clean := newCacher(b, 0)
	defer clean()
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).AnyTimes()

	ctx := context.Background()
	args := &proxy.CacheVolumeArgs{Vid: 1}
	_, err := c.GetVolume(ctx, args)
	require.NoError(b, err)

	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		c.GetVolume(ctx, args)
	}
}

func BenchmarkProxyDiskvHit(b *testing.B) {
	c, cmCli, clean := newCacher(b, 0)
	defer clean()
	c.(*cacher).diskv.AdvancedTransform = func(s string) *diskv.PathKey {
		return &diskv.PathKey{Path: proxy.DiskvPathTransform(s), FileName: diskvKeyVolume(1)}
	}
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).AnyTimes()

	ctx := context.Background()
	args := &proxy.CacheVolumeArgs{}
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		args.Vid = proto.Vid(ii)
		c.GetVolume(ctx, args)
	}
}

func BenchmarkProxyDiskvMiss(b *testing.B) {
	c, cmCli, clean := newCacher(b, 0)
	defer clean()
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).AnyTimes()

	ctx := context.Background()
	args := &proxy.CacheVolumeArgs{}
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		args.Vid = proto.Vid(ii)
		c.GetVolume(ctx, args)
	}
}

func BenchmarkProxyClusterMiss(b *testing.B) {
	c, cmCli, clean := newCacher(b, 0)
	defer clean()
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(nil, errcode.ErrVolumeNotExist).AnyTimes()

	ctx := context.Background()
	args := &proxy.CacheVolumeArgs{Vid: 1}
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		args.Vid = proto.Vid(ii)
		c.GetVolume(ctx, args)
	}
}

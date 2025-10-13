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
	"sync"
	"testing"
	"time"

	"github.com/peterbourgon/diskv/v3"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

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

	done := make(chan struct{})
	cmCli.EXPECT().GetVolumeInfo(A, A).DoAndReturn(
		func(_ context.Context, _ *clustermgr.GetVolumeArgs) (*clustermgr.VolumeInfo, error) {
			<-done
			return &clustermgr.VolumeInfo{}, nil
		})

	var wg sync.WaitGroup
	const n = _defaultClustermgrConcurrency
	wg.Add(n)
	for range [n]struct{}{} {
		go func() {
			c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
			wg.Done()
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(done)
	wg.Wait()
	time.Sleep(200 * time.Millisecond)
}

func TestProxyCacherVolumeError(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0)
	defer clean()
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(nil, errors.New("mock error")).Times(1)
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(nil, errcode.ErrVolumeNotExist).Times(2)

	_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
	require.Error(t, err)
	_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 2, Flush: true})
	require.ErrorIs(t, err, errcode.ErrVolumeNotExist)
	_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1, Flush: false})
	require.ErrorIs(t, err, errcode.ErrVolumeNotExist)
}

func TestProxyCacherVolumeCacheMiss(t *testing.T) {
	c, cmCli, clean := newCacher(t, 2)
	defer clean()

	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).Times(3)
	_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
	require.NoError(t, err)
	<-c.(*cacher).syncChan

	basePath := c.(*cacher).config.DiskvBasePath
	{ // memory cache miss, load from diskv
		c, _ = New(1, ConfigCache{DiskvBasePath: basePath, VolumeExpirationS: 2}, cmCli)
		_, err = c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
	}
	{ // cannot decode diskv value
		file, err := os.OpenFile(c.(*cacher).DiskvFilename(diskvKeyVolume(1)), os.O_RDWR, 0o644)
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

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

	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/peterbourgon/diskv/v3"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func TestProxyCacherVolumeUpdate(t *testing.T) {
	c, cmCli, clean := newCacher(t, 2, nil)
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
	c, cmCli, clean := newCacher(t, 0, nil)
	defer clean()

	volume := new(clustermgr.VolumeInfo)
	volume.Units = []clustermgr.Unit{{Vuid: 1234}, {Vuid: 5678}}
	version := proto.RouteVersion(12345678)
	volume.RouteVersion = proto.RouteVersion(version)
	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(1)
	for range [100]struct{}{} {
		vol, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
		require.NoError(t, err)
		require.Equal(t, version, vol.RouteVersion)
	}

	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(100)
	for range [100]struct{}{} {
		vol, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1, Flush: true})
		require.NoError(t, err)
		require.Equal(t, version, vol.RouteVersion)
	}

	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(1)
	for range [100]struct{}{} {
		vol, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 3, Flush: true, Version: 0x01})
		require.NoError(t, err)
		require.Equal(t, version, vol.RouteVersion)
	}

	cmCli.EXPECT().GetVolumeInfo(A, A).Return(volume, nil).Times(1)
	for range [100]struct{}{} {
		vol, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 4, Flush: true, Version: 0x9d31f755})
		require.NoError(t, err)
		require.Equal(t, version, vol.RouteVersion)
	}
}

func TestProxyCacherVolumeSingle(t *testing.T) {
	c, cmCli, clean := newCacher(t, 0, nil)
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
	c, cmCli, clean := newCacher(t, 0, nil)
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
	c, cmCli, clean := newCacher(t, 2, nil)
	defer clean()

	cmCli.EXPECT().GetVolumeInfo(A, A).Return(&clustermgr.VolumeInfo{}, nil).Times(3)
	_, err := c.GetVolume(context.Background(), &proxy.CacheVolumeArgs{Vid: 1})
	require.NoError(t, err)
	cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{}, nil).AnyTimes()
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
	c, cmCli, clean := newCacher(b, 0, nil)
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
	c, cmCli, clean := newCacher(b, 0, nil)
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
	c, cmCli, clean := newCacher(b, 0, nil)
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
	c, cmCli, clean := newCacher(b, 0, nil)
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

func TestProxyCacheCloser(t *testing.T) {
	c, _, clean := newCacher(t, 0, nil)
	defer clean()
	closer.Close(c)
	require.True(t, c.(closer.Closer).IsClosed())
}

func TestVolumeRouteSyncAddVolume(t *testing.T) {
	cmCli := mocks.NewMockClientAPI(C(t))
	payload := &clustermgr.RouteItemAddVolume{
		Vid:          1,
		RouteVersion: proto.RouteVersion(10),
		Units: []clustermgr.VolumeUnitInfoBase{
			{Vuid: proto.Vuid(1), DiskID: proto.DiskID(1), Host: "host-1"},
		},
		VolumeInfoBase: clustermgr.VolumeInfoBasePB{
			Vid:          1,
			CodeMode:     codemode.EC6P6,
			RouteVersion: proto.RouteVersion(10),
		},
	}
	cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{
		RouteVersion: proto.RouteVersion(10),
		Items: []clustermgr.VolumeRouteItem{{
			RouteVersion: proto.RouteVersion(10),
			Type:         proto.RouteItemTypeAddVolume,
			Item:         mustMarshalAny(t, payload),
		}},
	}, nil).AnyTimes()

	c, _, clean := newCacher(t, 0, cmCli)
	defer clean()
	cc := c.(*cacher)

	require.NoError(t, cc.syncVolumeRoutes(context.Background()))
	require.Equal(t, proto.RouteVersion(10), cc.getVolRouteVersion())
	data, err := cc.diskv.Read(diskvKeyVolumeRouteVersion)
	require.NoError(t, err)
	require.Equal(t, "10", string(data))

	span := trace.SpanFromContextSafe(context.Background())
	vol := cc.getVolume(span, proto.Vid(1))
	require.NotNil(t, vol)
	require.Equal(t, proto.RouteVersion(10), vol.VolumeInfo.RouteVersion)
	require.Len(t, vol.VolumeInfo.Units, 1)
	require.Equal(t, proto.DiskID(1), vol.VolumeInfo.Units[0].DiskID)
}

func TestVolumeRouteSyncUpdateUnit(t *testing.T) {
	cmCli := mocks.NewMockClientAPI(C(t))
	addPayload := &clustermgr.RouteItemAddVolume{
		Vid:          1,
		RouteVersion: proto.RouteVersion(10),
		Units: []clustermgr.VolumeUnitInfoBase{
			{Vuid: proto.Vuid(1), DiskID: proto.DiskID(1), Host: "host-1"},
			{Vuid: proto.Vuid(2), DiskID: proto.DiskID(2), Host: "host-2"},
		},
		VolumeInfoBase: clustermgr.VolumeInfoBasePB{
			Vid:          1,
			CodeMode:     codemode.EC6P6,
			RouteVersion: proto.RouteVersion(10),
		},
	}
	updatePayload := &clustermgr.RouteItemUpdateVolume{
		Vid:          1,
		RouteVersion: proto.RouteVersion(11),
		Unit: clustermgr.VolumeUnitInfoBase{
			Vuid:   proto.EncodeVuid(proto.VuidPrefix(2), 1),
			DiskID: proto.DiskID(3),
			Host:   "host-3",
		},
		VolumeInfoBase: clustermgr.VolumeInfoBasePB{
			Vid:          1,
			CodeMode:     codemode.EC6P6,
			RouteVersion: proto.RouteVersion(11),
		},
	}

	gomock.InOrder(
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{}, nil),
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{
			RouteVersion: proto.RouteVersion(10),
			Items: []clustermgr.VolumeRouteItem{{
				RouteVersion: proto.RouteVersion(10),
				Type:         proto.RouteItemTypeAddVolume,
				Item:         mustMarshalAny(t, addPayload),
			}},
		}, nil),
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{
			RouteVersion: proto.RouteVersion(11),
			Items: []clustermgr.VolumeRouteItem{{
				RouteVersion: proto.RouteVersion(11),
				Type:         proto.RouteItemTypeUpdateVolume,
				Item:         mustMarshalAny(t, updatePayload),
			}},
		}, nil),
	)

	c, _, clean := newCacher(t, 0, cmCli)
	defer clean()
	cc := c.(*cacher)

	require.NoError(t, cc.syncVolumeRoutes(context.Background()))
	require.NoError(t, cc.syncVolumeRoutes(context.Background()))
	require.Equal(t, proto.RouteVersion(11), cc.getVolRouteVersion())

	span := trace.SpanFromContextSafe(context.Background())
	vol := cc.getVolume(span, proto.Vid(1))
	require.NotNil(t, vol)
	require.Equal(t, proto.RouteVersion(11), vol.VolumeInfo.RouteVersion)
	require.Len(t, vol.VolumeInfo.Units, 2)
	require.Equal(t, proto.DiskID(3), vol.VolumeInfo.Units[0].DiskID)
	require.Equal(t, "host-3", vol.VolumeInfo.Units[0].Host)
}

func TestVolumeRouteSyncNoChange(t *testing.T) {
	cmCli := mocks.NewMockClientAPI(C(t))
	cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{}, nil).AnyTimes()
	c, _, clean := newCacher(t, 0, cmCli)
	defer clean()
	require.NoError(t, c.(*cacher).syncVolumeRoutes(context.Background()))
	require.Equal(t, proto.RouteVersion(0), c.(*cacher).getVolRouteVersion())
}

func TestVolumeRouteSyncApplyFailed(t *testing.T) {
	cmCli := mocks.NewMockClientAPI(C(t))
	gomock.InOrder(
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{}, nil),
		cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{
			RouteVersion: proto.RouteVersion(11),
			Items: []clustermgr.VolumeRouteItem{{
				RouteVersion: proto.RouteVersion(11),
				Type:         proto.RouteItemTypeAddVolume,
				Item:         nil,
			}},
		}, nil),
	)

	c, _, clean := newCacher(t, 0, cmCli)
	defer clean()
	cc := c.(*cacher)

	err := cc.syncVolumeRoutes(context.Background())
	require.Error(t, err)
	require.Equal(t, proto.RouteVersion(0), cc.getVolRouteVersion())
}

func TestVolumeRouteVersionPersist(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cmCli := mocks.NewMockClientAPI(ctrl)
	cmCli.EXPECT().GetVolumeRoutes(A, A).Return(&clustermgr.GetVolumeRoutesRet{}, nil).AnyTimes()

	basePath := path.Join(os.TempDir(), "proxy-cacher", fmt.Sprintf("persist-%d", rand.Int()))
	defer os.RemoveAll(basePath)

	cfg := ConfigCache{
		DiskvBasePath: basePath,
	}
	c1, err := New(1, cfg, cmCli)
	require.NoError(t, err)
	cc1 := c1.(*cacher)
	require.NoError(t, cc1.persistVolRouteVersion(context.Background(), proto.RouteVersion(99)))
	cc1.setVolRouteVersion(proto.RouteVersion(99))

	c2, err := New(1, cfg, cmCli)
	require.NoError(t, err)
	cc2 := c2.(*cacher)
	require.Equal(t, proto.RouteVersion(99), cc2.getVolRouteVersion())
}

func mustMarshalAny(t *testing.T, msg gogoproto.Message) *types.Any {
	t.Helper()
	any, err := types.MarshalAny(msg)
	require.NoError(t, err)
	return any
}

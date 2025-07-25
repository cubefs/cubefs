// Copyright 2025 The CubeFS Authors.
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

package base

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	snproto "github.com/cubefs/cubefs/blobstore/shardnode/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

var (
	any     = gomock.Any()
	errMock = errors.New("mock err")
)

func TestVolumeCache(t *testing.T) {
	tp := mocks.NewMockVolumeTransport(gomock.NewController(t))
	tp.EXPECT().ListVolume(any, any, any).Times(2).DoAndReturn(
		func(_ context.Context, marker proto.Vid, _ int) ([]*snproto.VolumeInfoSimple, proto.Vid, error) {
			if marker == defaultMarker {
				return []*snproto.VolumeInfoSimple{{Vid: 4}}, proto.Vid(10), nil
			}
			return []*snproto.VolumeInfoSimple{{Vid: 9}}, defaultMarker, nil
		},
	)
	tp.EXPECT().GetVolumeInfo(any, any).DoAndReturn(
		func(_ context.Context, vid proto.Vid) (*snproto.VolumeInfoSimple, error) {
			return &snproto.VolumeInfoSimple{Vid: vid}, nil
		},
	)

	volCache := NewVolumeCache(tp, 10*time.Second)
	err := volCache.LoadVolumes()
	require.NoError(t, err)

	// no cache will update
	_, err = volCache.GetVolume(1)
	require.NoError(t, err)
	// return cache
	_, err = volCache.GetVolume(1)
	require.NoError(t, err)

	// update ErrFrequentlyUpdate
	_, err = volCache.UpdateVolume(1)
	require.ErrorIs(t, err, errcode.ErrUpdateVolCacheFreq)

	// list and get failed
	tp.EXPECT().ListVolume(any, any, any).AnyTimes().Return(nil, proto.Vid(0), errMock)
	tp.EXPECT().GetVolumeInfo(any, any).Return(&snproto.VolumeInfoSimple{}, errMock)
	volCache = NewVolumeCache(tp, -1)
	_, err = volCache.GetVolume(1)
	require.ErrorIs(t, err, errMock)
	err = volCache.LoadVolumes()
	require.ErrorIs(t, err, errMock)
}

func TestDoubleCheckedRun(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	volume := &snproto.VolumeInfoSimple{Vid: proto.Vid(1)}
	{
		// get volume failed and return
		tp := mocks.NewMockVolumeTransport(ctr)
		tp.EXPECT().GetVolumeInfo(any, any).Return(nil, errMock)

		c := NewVolumeCache(tp, 1)
		err := c.DoubleCheckedRun(ctx, 1, func(*snproto.VolumeInfoSimple) (*snproto.VolumeInfoSimple, error) { return volume, nil })
		require.ErrorIs(t, err, errMock)
	}
	{
		// do task failed and not check again
		tp := mocks.NewMockVolumeTransport(ctr)
		tp.EXPECT().GetVolumeInfo(any, any).Return(nil, errMock)

		c := NewVolumeCache(tp, 1)
		err := c.DoubleCheckedRun(ctx, 1, func(*snproto.VolumeInfoSimple) (*snproto.VolumeInfoSimple, error) { return volume, errMock })
		require.ErrorIs(t, err, errMock)
	}
	{
		// do task success and check task: get volume failed in check phase
		// do task failed and not check again
		tp := mocks.NewMockVolumeTransport(ctr)
		tp.EXPECT().GetVolumeInfo(any, any).Return(nil, errMock)

		c := NewVolumeCache(tp, 1)
		err := c.DoubleCheckedRun(ctx, 1, func(*snproto.VolumeInfoSimple) (*snproto.VolumeInfoSimple, error) { return volume, nil })
		require.ErrorIs(t, err, errMock)
	}
	{
		// do task success and check task: volume change after task done and do it again
		tp := mocks.NewMockVolumeTransport(ctr)
		tp.EXPECT().GetVolumeInfo(any, any).Return(&snproto.VolumeInfoSimple{}, nil)
		tp.EXPECT().GetVolumeInfo(any, any).Return(&snproto.VolumeInfoSimple{}, nil)
		tp.EXPECT().GetVolumeInfo(any, any).Return(volume, nil)

		c := NewVolumeCache(tp, 1)
		err := c.DoubleCheckedRun(ctx, 1, func(*snproto.VolumeInfoSimple) (*snproto.VolumeInfoSimple, error) { return volume, nil })
		require.NoError(t, err)
	}
	{
		// do task success and check task: volume change when doing task, and no need to do it
		tp := mocks.NewMockVolumeTransport(ctr)
		tp.EXPECT().GetVolumeInfo(any, any).Return(&snproto.VolumeInfoSimple{}, nil)
		tp.EXPECT().GetVolumeInfo(any, any).Return(volume, nil)

		c := NewVolumeCache(tp, 1)
		err := c.DoubleCheckedRun(ctx, 1, func(*snproto.VolumeInfoSimple) (*snproto.VolumeInfoSimple, error) { return volume, nil })
		require.NoError(t, err)
	}
	{
		// do task success and check task: max times retry
		tp := mocks.NewMockVolumeTransport(ctr)
		tp.EXPECT().GetVolumeInfo(any, any).Return(&snproto.VolumeInfoSimple{}, nil)
		tp.EXPECT().GetVolumeInfo(any, any).Return(&snproto.VolumeInfoSimple{}, nil)
		tp.EXPECT().GetVolumeInfo(any, any).Return(&snproto.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 2}}}, nil)

		c := NewVolumeCache(tp, 1)
		retry := 0
		err := c.DoubleCheckedRun(ctx, 1, func(*snproto.VolumeInfoSimple) (*snproto.VolumeInfoSimple, error) {
			retry++
			if retry == 2 {
				return &snproto.VolumeInfoSimple{Vid: 1, VunitLocations: []proto.VunitLocation{{Vuid: 2}}}, nil
			}
			return volume, nil
		})
		require.NoError(t, err)
	}
	{
		// do task success and check task: max times retry and do task failed
		tp := mocks.NewMockVolumeTransport(ctr)
		tp.EXPECT().GetVolumeInfo(any, any).Return(&snproto.VolumeInfoSimple{}, nil).Times(4)

		c := NewVolumeCache(tp, 1)
		err := c.DoubleCheckedRun(ctx, 1, func(*snproto.VolumeInfoSimple) (*snproto.VolumeInfoSimple, error) { return volume, nil })
		require.ErrorIs(t, err, errVolumeMissmatch)
	}
}

func BenchmarkVolumeCache(b *testing.B) {
	for _, cs := range []struct {
		Name  string
		Items int
	}{
		{"tiny", 1 << 5},
		{"norm", 1 << 16},
		{"huge", 1 << 20},
	} {
		items := cs.Items
		b.Run(cs.Name, func(b *testing.B) {
			vols := make([]*snproto.VolumeInfoSimple, items)
			for idx := range vols {
				vols[idx] = &snproto.VolumeInfoSimple{Vid: proto.Vid(idx)}
			}
			tp := mocks.NewMockVolumeTransport(gomock.NewController(b))
			tp.EXPECT().ListVolume(any, any, any).Return(vols, defaultMarker, nil)

			cacher := NewVolumeCache(tp, -1)
			require.NoError(b, cacher.LoadVolumes())

			b.ResetTimer()
			for ii := 0; ii < b.N; ii++ {
				cacher.GetVolume(proto.Vid(ii % items))
			}
		})
	}
}

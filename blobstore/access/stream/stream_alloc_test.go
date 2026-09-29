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

package stream

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestAccessStreamAllocBase(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamAllocBase")
	// 4M blobsize
	{
		loc, err := streamer.Alloc(ctx(), 1<<30, 0, 0, 0)
		require.NoError(t, err)
		require.Equal(t, clusterID, loc.ClusterID)
		require.Equal(t, codemode.EC6P6, loc.CodeMode)
		require.Equal(t, uint64(1<<30), loc.Size_)
		require.Equal(t, uint32(1<<22), loc.SliceSize)
		require.Equal(t, 2, len(loc.Slices))
		require.Equal(t, uint32(1), loc.Slices[0].Count)
		require.Equal(t, uint32((1<<8)-1), loc.Slices[1].Count)
	}
	{
		loc, err := streamer.Alloc(ctx(), (1<<30)+1, 0, 0, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(loc.Slices))
		require.Equal(t, uint32(1), loc.Slices[0].Count)
		require.Equal(t, uint32(1<<8), loc.Slices[1].Count)
	}
	// 1M blobsize
	{
		loc, err := streamer.Alloc(ctx(), 1<<30, 1<<20, 0, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(loc.Slices))
		require.Equal(t, uint32(1), loc.Slices[0].Count)
		require.Equal(t, uint32((1<<10)-1), loc.Slices[1].Count)
	}
	// max size + 1
	{
		_, err := streamer.Alloc(ctx(), uint64(defaultMaxObjectSize+1), 1<<20, 0, 0)
		require.EqualError(t, errcode.ErrAccessExceedSize, err.Error())
	}

	{
		// wait for service manager to reload
		defer func() {
			time.Sleep(time.Second)
		}()
		_, err := streamer.Alloc(ctx(), allocTimeoutSize+1, 0, 0, 0)
		require.Error(t, err)
	}
}

func TestAccessStreamAllocCanceled(t *testing.T) {
	ctxfunc := ctxWithName("TestAccessStreamAllocCanceled")
	ctx, cancel := context.WithCancel(ctxfunc())
	cancel()
	_, err := streamer.Alloc(ctx, 1, 0, 0, 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAccessStreamAllocWithClusterIDAndCodeMode(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamAllocWithClusterIDAndCodeMode")
	{
		size := uint64(1 << 22)
		_, err := streamer.Alloc(ctx(), size, 0, 0, codemode.EC16P20L2)
		require.EqualError(t, errcode.ErrIllegalArguments, err.Error())
	}
	{
		size := uint64(1 << 22)
		_, err := streamer.Alloc(ctx(), size, 0, proto.ClusterID(2), codemode.CodeModeNone)
		require.Error(t, err)
	}
	{
		size := uint64(1 << 22)
		_, err := streamer.Alloc(ctx(), size, 0, proto.ClusterID(2), codemode.EC15P12)
		require.Error(t, err)
	}
	{
		size := uint64(1 << 22)
		_, err := streamer.Alloc(ctx(), size, 0, clusterID, codemode.EC15P12)
		require.EqualError(t, errcode.ErrIllegalArguments, err.Error())
	}
	{
		size := uint64(1 << 22)
		_, err := streamer.Alloc(ctx(), size, 0, 0, codemode.EC15P12)
		require.EqualError(t, errcode.ErrIllegalArguments, err.Error())
	}
	{
		size := uint64(1 << 22)
		_, err := streamer.Alloc(ctx(), size, 0, 0, codemode.CodeMode(0xff))
		require.EqualError(t, errcode.ErrIllegalArguments, err.Error())
	}
	{
		size := uint64(1 << 20)
		loc, err := streamer.Alloc(ctx(), size, 0, 0, codemode.EC6P6)
		require.NoError(t, err)
		require.Equal(t, codemode.EC6P6, loc.CodeMode)
		require.Equal(t, clusterID, loc.ClusterID)
		require.Equal(t, 1, len(loc.Slices))
		require.Equal(t, uint32(1), loc.Slices[0].Count)
	}
	{
		size := uint64(1 << 22)
		loc, err := streamer.Alloc(ctx(), size, 0, clusterID, codemode.EC6P6)
		require.NoError(t, err)
		require.Equal(t, codemode.EC6P6, loc.CodeMode)
		require.Equal(t, clusterID, loc.ClusterID)
		require.Equal(t, 1, len(loc.Slices))
		require.Equal(t, uint32(1), loc.Slices[0].Count)
	}
	{
		size := uint64(1 << 22)
		loc, err := streamer.Alloc(ctx(), size, 0, clusterID, codemode.CodeModeNone)
		require.NoError(t, err)
		require.Equal(t, codemode.EC6P6, loc.CodeMode)
		require.Equal(t, clusterID, loc.ClusterID)
		require.Equal(t, 1, len(loc.Slices))
		require.Equal(t, uint32(1), loc.Slices[0].Count)
	}
	{
		loc, err := streamer.Alloc(ctx(), 1, 0, 0, codemode.EC6P6)
		require.NoError(t, err)
		require.Equal(t, codemode.EC6P6, loc.CodeMode)
	}
}

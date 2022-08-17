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

package access

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
)

func TestAccessStreamAllocBase(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamAllocBase")
	// 4M blobsize
	{
		loc, err := streamer.Alloc(ctx(), 1<<30, 0, 0, 0)
		require.NoError(t, err)
		require.Equal(t, clusterID, loc.ClusterID)
		require.Equal(t, codemode.EC6P6, loc.CodeMode)
		require.Equal(t, uint64(1<<30), loc.Size)
		require.Equal(t, uint32(1<<22), loc.BlobSize)
		require.Equal(t, 2, len(loc.Blobs))
		require.Equal(t, uint32(1), loc.Blobs[0].Count)
		require.Equal(t, uint32((1<<8)-1), loc.Blobs[1].Count)
	}
	{
		loc, err := streamer.Alloc(ctx(), (1<<30)+1, 0, 0, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(loc.Blobs))
		require.Equal(t, uint32(1), loc.Blobs[0].Count)
		require.Equal(t, uint32(1<<8), loc.Blobs[1].Count)
	}
	// 1M blobsize
	{
		loc, err := streamer.Alloc(ctx(), 1<<30, 1<<20, 0, 0)
		require.NoError(t, err)
		require.Equal(t, 2, len(loc.Blobs))
		require.Equal(t, uint32(1), loc.Blobs[0].Count)
		require.Equal(t, uint32((1<<10)-1), loc.Blobs[1].Count)
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

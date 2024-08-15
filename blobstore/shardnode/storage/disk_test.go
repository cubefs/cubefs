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

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
)

//go:generate mockgen -source=../base/transport.go -destination=../mock/mock_transport.go -package=mock -mock_names Transport=MockTransport

func TestServerDisk_Shard(t *testing.T) {
	d, diskClean := NewMockDisk(t)
	defer diskClean()

	rg := sharding.New(sharding.RangeType_RangeTypeHash, 1)
	require.Panics(t, func() {
		d.d.AddShard(ctx, 1, 1, *rg, []clustermgr.ShardUnit{{}})
	})

	shardID := proto.ShardID(1)
	suid := proto.EncodeSuid(shardID, 0, 0)
	_, err := d.d.GetShard(suid)
	require.Error(t, err)

	require.NoError(t, d.d.AddShard(ctx, suid, 1, *rg, []clustermgr.ShardUnit{{DiskID: 1}}))
	require.NoError(t, d.d.AddShard(ctx, suid, 1, *rg, []clustermgr.ShardUnit{{DiskID: 1}}))

	s, err := d.d.GetShard(suid)
	require.NoError(t, err)
	require.Equal(t, proto.RouteVersion(1), s.GetRouteVersion())

	shardID2 := proto.ShardID(2)
	suid2 := proto.EncodeSuid(shardID2, 0, 0)

	require.NoError(t, d.d.AddShard(ctx, suid2, 1, *rg, []clustermgr.ShardUnit{{DiskID: 1}}))
	_, err = d.d.GetShard(suid2)
	require.NoError(t, err)

	d.d.RangeShard(func(s ShardHandler) bool {
		require.NoError(t, s.Checkpoint(ctx))
		return true
	})

	err = d.d.UpdateShardRouteVersion(ctx, suid2, proto.RouteVersion(2))
	require.NoError(t, err)
	s2, err := d.d.GetShard(suid)
	require.NoError(t, err)
	require.Equal(t, proto.RouteVersion(1), s2.GetRouteVersion())

	require.NoError(t, d.d.DeleteShard(ctx, suid2, proto.RouteVersion(3)))
	require.NoError(t, d.d.DeleteShard(ctx, suid2, proto.RouteVersion(3)))

	_, err = d.d.GetShard(suid2)
	require.Equal(t, apierr.ErrShardDoesNotExist, err)

	require.NoError(t, d.d.Load(ctx))
}

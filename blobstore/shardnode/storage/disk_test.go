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
	disk := d.GetDisk()

	diskID := disk.DiskID()
	info := disk.GetDiskInfo()
	require.Equal(t, diskID, info.DiskID)
	require.True(t, disk.IsRegistered())

	empty, err := IsEmptyDisk(disk.diskInfo.Path)
	require.Nil(t, err)
	require.False(t, empty)

	shardID := proto.ShardID(1)
	suid := proto.EncodeSuid(shardID, 0, 0)
	version := proto.RouteVersion(1)

	rg := sharding.New(sharding.RangeType_RangeTypeHash, 1)
	// no shard uint as raft member, create raft group failed
	require.Panics(t, func() {
		disk.AddShard(ctx, suid, version, *rg, []clustermgr.ShardUnit{{}})
	})

	_, err = disk.GetShard(suid)
	require.NotNil(t, err)

	require.NoError(t, disk.AddShard(ctx, suid, version, *rg, []clustermgr.ShardUnit{{DiskID: diskID, Suid: suid}}))
	require.NoError(t, disk.AddShard(ctx, suid, version, *rg, []clustermgr.ShardUnit{{DiskID: diskID, Suid: suid}}))

	s, err := disk.GetShard(suid)
	require.Nil(t, err)
	require.Equal(t, version, s.GetRouteVersion())

	shardID2 := proto.ShardID(2)
	suid2 := proto.EncodeSuid(shardID2, 0, 0)

	require.NoError(t, disk.AddShard(ctx, suid2, 1, *rg, []clustermgr.ShardUnit{{DiskID: diskID, Suid: suid2}}))
	_, err = disk.GetShard(suid2)
	require.Nil(t, err)

	disk.RangeShard(func(s ShardHandler) bool {
		require.NoError(t, s.Checkpoint(ctx))
		return true
	})

	require.Equal(t, 2, disk.GetShardCnt())

	suid3 := proto.EncodeSuid(shardID, 1, 0)
	err = disk.UpdateShard(ctx, suid, proto.ShardUpdateTypeAddMember, clustermgr.ShardUnit{DiskID: diskID, Suid: suid3})
	require.Nil(t, err)

	version2 := proto.RouteVersion(2)
	err = disk.UpdateShardRouteVersion(ctx, suid2, version2)
	require.Nil(t, err)
	s2, err := disk.GetShard(suid2)
	require.Nil(t, err)
	require.Equal(t, version2, s2.GetRouteVersion())

	require.NoError(t, disk.DeleteShard(ctx, suid2, proto.RouteVersion(3)))
	require.NoError(t, disk.DeleteShard(ctx, suid2, proto.RouteVersion(3)))

	_, err = disk.GetShard(suid2)
	require.Equal(t, apierr.ErrShardDoesNotExist, err)

	require.Equal(t, 1, disk.GetShardCnt())

	require.NoError(t, disk.Load(ctx))
	require.NoError(t, disk.SaveDiskInfo(ctx))

	disk.SetBroken()
	disk.ResetShards()
	require.Equal(t, 0, disk.GetShardCnt())
}

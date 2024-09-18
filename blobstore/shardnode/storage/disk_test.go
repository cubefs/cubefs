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
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
)

func TestServerDisk_Shard(t *testing.T) {
	diskID1, diskID2, diskID3 := proto.DiskID(1), proto.DiskID(2), proto.DiskID(3)
	d1, diskClean1, err := NewMockDisk(t, diskID1, true)
	require.Nil(t, err)
	d2, diskClean2, err := NewMockDisk(t, diskID2, true)
	require.Nil(t, err)
	d3, diskClean3, err := NewMockDisk(t, diskID3, true)
	require.Nil(t, err)
	defer func() {
		diskClean1()
		diskClean2()
		diskClean3()
	}()
	disk1 := d1.GetDisk()
	disk2 := d2.GetDisk()
	disk3 := d3.GetDisk()
	disks := []*Disk{disk1, disk2, disk3}

	info := disk1.GetDiskInfo()
	require.Equal(t, disk1.DiskID(), info.DiskID)
	require.True(t, disk1.IsRegistered())

	empty, err := IsEmptyDisk(disk1.diskInfo.Path)
	require.Nil(t, err)
	require.False(t, empty)

	shardID := proto.ShardID(1)
	suid1 := proto.EncodeSuid(shardID, 0, 0)
	suid2 := proto.EncodeSuid(shardID, 1, 0)
	suid3 := proto.EncodeSuid(shardID, 2, 0)
	units := []clustermgr.ShardUnit{
		{DiskID: diskID1, Suid: suid1},
		{DiskID: diskID2, Suid: suid2},
		{DiskID: diskID3, Suid: suid3},
	}
	version := proto.RouteVersion(1)
	rg := sharding.New(sharding.RangeType_RangeTypeHash, 1)

	// add shards
	require.NoError(t, disk1.AddShard(ctx, suid1, version, *rg, units))
	require.NoError(t, disk2.AddShard(ctx, suid2, version, *rg, units))
	require.NoError(t, disk3.AddShard(ctx, suid3, version, *rg, units))

	leaderDiskID := proto.InvalidDiskID
	for {
		shard1, _err := disk1.GetShard(suid1)
		require.Nil(t, _err)
		stat, _ := shard1.Stats(ctx)
		if stat.LeaderDiskID != proto.InvalidDiskID {
			leaderDiskID = stat.LeaderDiskID
			break
		}
	}

	t.Log("leader selection finish")
	leaderIdx := 0
	for i, d := range disks {
		if d.diskInfo.DiskID == leaderDiskID {
			leaderIdx = i
			break
		}
	}
	t.Logf("leader diskID:%d", leaderDiskID)
	shardLeader, err := disks[leaderIdx].GetShard(units[leaderIdx].GetSuid())
	require.NoError(t, err)

	version += 1
	diskID4 := proto.DiskID(4)
	suid4 := proto.EncodeSuid(shardID, 0, 1)
	newUnit := clustermgr.ShardUnit{DiskID: diskID4, Suid: suid4, Learner: true}
	units = append(units, newUnit)
	d4, diskClean4, err := NewMockDisk(t, diskID4, true)
	require.Nil(t, err)
	defer diskClean4()

	disk4 := d4.GetDisk()
	disks = append(disks, disk4)

	require.NoError(t, disk4.AddShard(ctx, suid4, version, *rg, units))
	require.NoError(t, disks[leaderIdx].UpdateShard(ctx, units[leaderIdx].GetSuid(), proto.ShardUpdateTypeAddMember, newUnit))

	for {
		stat, err := shardLeader.Stats(ctx)
		require.Nil(t, err)
		if len(units) == len(stat.RaftStat.Peers) {
			break
		}
	}
	t.Logf("add shard[%d] success", suid4)

	version += 1
	delIdx := 0
	for i, d := range disks {
		if d.DiskID() != leaderDiskID {
			delIdx = i
			break
		}
	}

	t.Logf("delete diskID:%d", disks[delIdx].diskInfo.DiskID)
	require.NoError(t, disks[delIdx].DeleteShard(ctx, units[delIdx].GetSuid(), version))
}

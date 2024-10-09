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
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
)

func setUpRaftDisks(t *testing.T, ids []proto.DiskID) ([]*Disk, func(), error) {
	disks := make([]*Disk, len(ids))
	closers := make([]func(), len(ids))
	for i := range ids {
		d, closer, err := NewMockDisk(t, ids[i], true)
		if err != nil {
			return nil, nil, err
		}
		disks[i] = d.GetDisk()
		closers[i] = closer
	}
	clearFunc := func() {
		for i := range closers {
			closers[i]()
		}
	}
	return disks, clearFunc, nil
}

func TestServerDisk_Shard(t *testing.T) {
	diskID := proto.DiskID(1)
	disk, clearFunc, err := NewMockDisk(t, diskID, false)
	defer clearFunc()
	require.NoError(t, err)

	suid := proto.EncodeSuid(1, 0, 0)
	rg := sharding.New(sharding.RangeType_RangeTypeHash, 1)
	err = disk.GetDisk().AddShard(ctx, suid, 0, *rg, []clustermgr.ShardUnit{
		{DiskID: diskID, Suid: suid},
	})
	require.Nil(t, err)

	shard, err := disk.GetDisk().GetShard(suid)
	require.Nil(t, err)
	require.NotNil(t, shard)

	// test disk range shard func
	disk.GetDisk().RangeShard(func(s ShardHandler) bool {
		s.Checkpoint(ctx)
		return true
	})
	require.Equal(t, 1, disk.GetDisk().GetShardCnt())

	// route version
	routeVersion := proto.RouteVersion(1)
	err = disk.GetDisk().UpdateShardRouteVersion(ctx, suid, routeVersion)
	require.Nil(t, err)
	require.Equal(t, routeVersion, shard.GetRouteVersion())

	err = disk.GetDisk().UpdateShard(ctx, suid, proto.ShardUpdateTypeAddMember, clustermgr.ShardUnit{
		Suid:    proto.EncodeSuid(suid.ShardID(), suid.Index()+1, 0),
		DiskID:  diskID + 1,
		Learner: true,
	})
	require.Nil(t, err)

	err = disk.GetDisk().DeleteShard(ctx, suid, 0)
	require.Nil(t, err)
	require.Equal(t, 0, disk.GetDisk().GetShardCnt())
}

func TestServerDisk_Load(t *testing.T) {
	diskID := proto.DiskID(1)
	disk, _, err := NewMockDisk(t, diskID, false)
	defer func() {
		os.Remove(disk.d.cfg.DiskPath)
	}()
	require.NoError(t, err)

	suid := proto.EncodeSuid(1, 0, 0)
	rg := sharding.New(sharding.RangeType_RangeTypeHash, 1)
	err = disk.GetDisk().AddShard(ctx, suid, 0, *rg, []clustermgr.ShardUnit{
		{DiskID: diskID, Suid: suid},
	})
	require.Nil(t, err)
	disk.GetDisk().Close()

	newDisk, err := OpenDisk(ctx, disk.d.cfg)
	require.Nil(t, err)
	newDisk.diskInfo.DiskID = diskID
	err = newDisk.Load(ctx)
	require.Nil(t, err)
	require.Equal(t, 1, newDisk.GetShardCnt())
	newDisk.store.Close()
}

func TestServerDisk_Info(t *testing.T) {
	diskID := proto.DiskID(1)
	disk, clearFunc, err := NewMockDisk(t, diskID, false)
	defer clearFunc()
	require.NoError(t, err)

	err = disk.d.SaveDiskInfo(ctx)
	require.Nil(t, err)

	info := disk.d.GetDiskInfo()
	require.NotNil(t, info)
}

func TestServerDisk_Raft(t *testing.T) {
	diskID := []proto.DiskID{1, 2, 3}
	disks, clearFunc, err := setUpRaftDisks(t, diskID)
	require.Nil(t, err)
	defer clearFunc()

	disk1, disk2, disk3 := disks[0], disks[1], disks[2]

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
		{DiskID: diskID[0], Suid: suid1},
		{DiskID: diskID[1], Suid: suid2},
		{DiskID: diskID[2], Suid: suid3},
	}
	version := proto.RouteVersion(1)
	rg := sharding.New(sharding.RangeType_RangeTypeHash, 1)

	// add shards
	require.NoError(t, disk1.AddShard(ctx, suid1, version, *rg, units))
	require.NoError(t, disk2.AddShard(ctx, suid2, version, *rg, units))
	require.NoError(t, disk3.AddShard(ctx, suid3, version, *rg, units))

	//  wait select leader disk
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

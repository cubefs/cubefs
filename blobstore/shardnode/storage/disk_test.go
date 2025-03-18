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
	"io"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
)

var atomDiskID = new(uint32)

func genDiskID(n uint32) []proto.DiskID {
	ids := make([]proto.DiskID, n)
	for i := range ids {
		ids[i] = proto.DiskID(atomic.AddUint32(atomDiskID, 1))
	}
	return ids
}

func setUpRaftDisks(t *testing.T, ids []proto.DiskID) ([]*Disk, func(), error) {
	disks := make([]*Disk, len(ids))
	closers := make([]func(), len(ids))
	for i := range ids {
		d, closer, err := NewMockDisk(t, ids[i])
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

func TestServerDisk_Basic(t *testing.T) {
	diskID := genDiskID(1)[0]
	disk, clearFunc, err := NewMockDisk(t, diskID)
	defer clearFunc()
	require.NoError(t, err)
	d := disk.GetDisk()

	// DiskInfo
	{
		err = d.SaveDiskInfo(ctx)
		require.Nil(t, err)

		info := d.GetDiskInfo()
		require.NotNil(t, info)

		d.SetDiskInfo(clustermgr.ShardNodeDiskInfo{
			ShardNodeDiskHeartbeatInfo: clustermgr.ShardNodeDiskHeartbeatInfo{
				DiskID: info.DiskID,
			},
		})
	}

	// db stats
	{
		kv_stats, err := d.DBStats(ctx, "kv")
		require.Nil(t, err)
		t.Log(kv_stats)

		raft_stats, err := d.DBStats(ctx, "raft")
		require.Nil(t, err)
		t.Log(raft_stats)

		_, err = d.DBStats(ctx, "")
		require.NotNil(t, err)
	}
}

func TestServerDisk_Shard(t *testing.T) {
	diskID := genDiskID(1)[0]
	disk, _, err := NewMockDisk(t, diskID)
	defer func() {
		os.RemoveAll(disk.d.cfg.DiskPath)
	}()
	require.NoError(t, err)

	d := disk.GetDisk()
	shardCnt := 2
	rgs := sharding.InitShardingRange(sharding.RangeType_RangeTypeHash, 1, shardCnt)

	suids := make([]proto.Suid, shardCnt)

	// Add Shard
	{
		for i := 0; i < shardCnt; i++ {
			suid := proto.EncodeSuid(proto.ShardID(i+1), 0, 0)
			suids[i] = suid

			err = disk.GetDisk().AddShard(ctx, suid, 0, *rgs[i], []clustermgr.ShardUnit{
				{DiskID: diskID, Suid: suid},
			})
			require.Nil(t, err)

			// add same suid
			err = disk.GetDisk().AddShard(ctx, suid, 0, *rgs[i], []clustermgr.ShardUnit{
				{DiskID: diskID, Suid: suid},
			})
			require.Nil(t, err)

			// add with same shardID
			_suid := proto.EncodeSuid(suid.ShardID(), 1, 0)
			err = disk.GetDisk().AddShard(ctx, _suid, 0, *rgs[i], []clustermgr.ShardUnit{
				{DiskID: diskID, Suid: _suid},
			})
			require.NotNil(t, err)
		}

		require.Equal(t, shardCnt, d.GetShardCnt())
	}

	// Update route version
	{
		routeVersion := proto.RouteVersion(1)
		for i := range suids {
			err = d.UpdateShardRouteVersion(ctx, suids[i], routeVersion)
			require.Nil(t, err)

			shard, err := d.GetShard(suids[i])
			require.Nil(t, err)
			require.NotNil(t, shard)

			require.Equal(t, routeVersion, shard.GetRouteVersion())
		}
	}

	// UpdateShard
	{
		// add with diff index and diskID, success
		suid := suids[0]
		err = disk.GetDisk().UpdateShard(ctx, suid, proto.ShardUpdateTypeAddMember, clustermgr.ShardUnit{
			Suid:    proto.EncodeSuid(suid.ShardID(), suid.Index()+1, 0),
			DiskID:  diskID + 1,
			Learner: true,
		})
		require.Nil(t, err)

		// add with diff index but exist diskID, fail
		err = disk.GetDisk().UpdateShard(ctx, suid, proto.ShardUpdateTypeAddMember, clustermgr.ShardUnit{
			Suid:    proto.EncodeSuid(suid.ShardID(), suid.Index()+2, 0),
			DiskID:  diskID,
			Learner: true,
		})
		require.True(t, strings.Contains(err.Error(), errors.ErrIllegalUpdateUnit.Error()))

		// add with diff shardID, fail
		err = disk.GetDisk().UpdateShard(ctx, suid, proto.ShardUpdateTypeAddMember, clustermgr.ShardUnit{
			Suid:    proto.EncodeSuid(suid.ShardID()+1, suid.Index(), 0),
			DiskID:  diskID + 1,
			Learner: true,
		})
		require.True(t, strings.Contains(err.Error(), errors.ErrIllegalUpdateUnit.Error()))

		// update with same shardID, index but diff diskID, fail
		err = disk.GetDisk().UpdateShard(ctx, suid, proto.ShardUpdateTypeUpdateMember, clustermgr.ShardUnit{
			Suid:    proto.EncodeSuid(suid.ShardID(), suid.Index(), 0),
			DiskID:  diskID + 1,
			Learner: true,
		})
		require.True(t, strings.Contains(err.Error(), errors.ErrIllegalUpdateUnit.Error()))

		// update with index not exist
		err = disk.GetDisk().UpdateShard(ctx, suid, proto.ShardUpdateTypeUpdateMember, clustermgr.ShardUnit{
			Suid:    proto.EncodeSuid(suid.ShardID(), suid.Index()+2, 0),
			DiskID:  diskID + 2,
			Learner: true,
		})
		require.True(t, strings.Contains(err.Error(), errors.ErrIllegalUpdateUnit.Error()))

		// remove unit but len(shard.Units) < 3, fail
		err = disk.GetDisk().UpdateShard(ctx, suid, proto.ShardUpdateTypeRemoveMember, clustermgr.ShardUnit{
			Suid:   suid,
			DiskID: diskID,
		})
		require.True(t, strings.Contains(err.Error(), errors.ErrNoEnoughRaftMember.Error()))
	}

	// RangeShard
	{
		cnt := 0
		d.RangeShard(func(s ShardHandler) bool {
			cnt++
			return true
		})
		require.Equal(t, shardCnt, cnt)
		cnt = 0

		d.SetBroken()
		d.RangeShardNoRWCheck(func(s ShardHandler) bool {
			cnt++
			return true
		})
		require.Equal(t, shardCnt, cnt)
	}

	// Disk reload shard from storage
	d.Close()
	reopenDisk, err := OpenDisk(ctx, d.cfg)
	require.Nil(t, err)

	reopenDisk.diskInfo.DiskID = diskID
	err = reopenDisk.Load(ctx)
	require.Nil(t, err)
	require.Equal(t, shardCnt, d.GetShardCnt())

	// DeleteShard
	{
		for i := range suids {
			err = reopenDisk.DeleteShard(ctx, suids[i], 0)
			require.Nil(t, err)
		}
		require.Equal(t, 0, reopenDisk.GetShardCnt())
	}
	reopenDisk.Close()
}

func TestServerDisk_Raft(t *testing.T) {
	diskID := genDiskID(3)
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
	diskID4 := genDiskID(1)[0]
	suid4 := proto.EncodeSuid(shardID, 0, 1)
	newUnit := clustermgr.ShardUnit{DiskID: diskID4, Suid: suid4, Learner: true}
	units = append(units, newUnit)
	d4, diskClean4, err := NewMockDisk(t, diskID4)
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

	err = shardLeader.TransferLeader(ctx, proto.DiskID(100))
	require.Equal(t, err, raft.ErrNodeNotFound)

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

	err = disks[leaderIdx].UpdateShard(ctx, shardLeader.GetSuid(), proto.ShardUpdateTypeRemoveMember, clustermgr.ShardUnit{
		DiskID: disks[delIdx].diskInfo.DiskID,
		Suid:   units[delIdx].GetSuid(),
	})
	require.Nil(t, err)

	for {
		if len(shardLeader.GetUnits()) == 3 {
			break
		}
	}
}

func TestServerDisk_RaftData(t *testing.T) {
	diskID := genDiskID(1)[0]
	disk, clearFunc, err := NewMockDisk(t, diskID)
	defer clearFunc()
	require.NoError(t, err)

	shardID := proto.ShardID(1)
	suid1 := proto.EncodeSuid(shardID, 0, 0)
	units := []clustermgr.ShardUnit{
		{DiskID: diskID, Suid: suid1},
	}
	version := proto.RouteVersion(1)
	rg := sharding.New(sharding.RangeType_RangeTypeHash, 1)

	d := disk.GetDisk()
	// add shard
	require.NoError(t, d.AddShard(ctx, suid1, version, *rg, units))

	var (
		shard ShardHandler
		_err  error
	)
	//  wait select leader disk
	for {
		shard, _err = d.GetShard(suid1)
		require.Nil(t, _err)
		stat, _ := shard.Stats(ctx)
		if stat.LeaderDiskID != proto.InvalidDiskID {
			break
		}
	}

	blobName := []byte("test_blob")
	h := OpHeader{
		RouteVersion: version,
		ShardKeys:    [][]byte{blobName},
	}
	b := proto.Blob{
		Name:     blobName,
		Location: proto.Location{},
		Sealed:   false,
	}

	key := blobName
	kv, err := InitKV(key, &io.LimitedReader{R: rpc2.Codec2Reader(&b), N: int64(b.Size())})
	if err != nil {
		return
	}

	b1, err := shard.CreateBlob(ctx, h, kv)
	require.Nil(t, err)
	require.Equal(t, b, b1)

	b1.Location.Size_ = 1024
	kv, err = InitKV(key, &io.LimitedReader{R: rpc2.Codec2Reader(&b), N: int64(b.Size())})
	if err != nil {
		return
	}

	// CreateBlob with same key, return existed value
	b11, err := shard.CreateBlob(ctx, h, kv)
	require.Nil(t, err)
	require.NotEqual(t, b1, b11)
	require.Equal(t, uint64(0), b11.Location.Size_)
}

func TestServerDisk_HandleRaftError(t *testing.T) {
	diskID := genDiskID(1)[0]
	disk, clearFunc, err := NewMockDisk(t, diskID)
	defer clearFunc()
	require.NoError(t, err)

	shardID := proto.ShardID(1)
	suid1 := proto.EncodeSuid(shardID, 0, 0)
	units := []clustermgr.ShardUnit{
		{DiskID: diskID, Suid: suid1},
	}
	version := proto.RouteVersion(1)
	rg := sharding.New(sharding.RangeType_RangeTypeHash, 1)

	d := disk.GetDisk()
	// add shard
	require.NoError(t, d.AddShard(ctx, suid1, version, *rg, units))

	d.handleRaftError(uint64(shardID), syscall.EIO)
	// handle another error will not fatal
	d.handleRaftError(uint64(shardID), errors.ErrKeyNotFound)
}

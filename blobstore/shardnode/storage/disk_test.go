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
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
)

//go:generate mockgen -source=../base/transport.go -destination=../mock/mock_transport.go -package=mock -mock_names Transport=MockTransport

func tempPath() (string, func()) {
	tmp := path.Join(os.TempDir(), fmt.Sprintf("shardserver_disk_%d", rand.Int31n(10000)+10000))
	return tmp, func() { os.RemoveAll(tmp) }
}

type mockDisk struct {
	d  *Disk
	tp *base.MockTransport
}

func newMockDisk(tb testing.TB) (*mockDisk, func()) {
	diskPath, pathClean := tempPath()
	var cfg DiskConfig
	cfg.DiskPath = diskPath
	cfg.StoreConfig.KVOption.CreateIfMissing = true
	cfg.StoreConfig.RaftOption.CreateIfMissing = true
	cfg.StoreConfig.KVOption.ColumnFamily = append(cfg.StoreConfig.KVOption.ColumnFamily, lockCF, dataCF, writeCF)
	cfg.StoreConfig.RaftOption.ColumnFamily = append(cfg.StoreConfig.RaftOption.ColumnFamily, raftWalCF)

	cfg.RaftConfig.NodeID = 1
	mockResolver := raft.NewMockAddressResolver(C(tb))
	cfg.RaftConfig.Resolver = mockResolver
	mockResolver.EXPECT().Resolve(A, A).Return(raft.NewMockAddr(C(tb)).EXPECT().String().Return("127.0.0.1:8080").AnyTimes(), nil).AnyTimes()
	tp := base.NewMockTransport(C(tb))
	cfg.Transport = tp
	tp.EXPECT().GetNode(A, A).Return(&clustermgr.ShardNodeInfo{}, nil).AnyTimes()
	tp.EXPECT().GetDisk(A, A).Return(&clustermgr.ShardNodeDiskInfo{}, nil).AnyTimes()

	disk, err := OpenDisk(ctx, cfg)
	require.NoError(tb, err)

	disk.diskInfo.DiskID = 1
	disk.diskInfo.Status = proto.DiskStatusNormal
	require.NoError(tb, disk.Load(ctx))
	return &mockDisk{d: disk, tp: tp}, func() {
		time.Sleep(time.Second)
		disk.raftManager.Close()
		disk.store.KVStore().Close()
		disk.store.RaftStore().Close()
		pathClean()
	}
}

func TestServerDisk_Open(t *testing.T) {
	tp := base.NewMockTransport(C(t))
	diskPath, pathClean := tempPath()
	defer pathClean()

	var cfg DiskConfig
	cfg.DiskPath = diskPath
	_panic := func() { require.Panics(t, func() { OpenDisk(ctx, cfg) }) }

	cfg.CheckMountPoint = true
	_panic()
	cfg.CheckMountPoint = false
	_, err := OpenDisk(ctx, cfg)
	require.Errorf(t, err, "")

	cfg.StoreConfig.KVOption.CreateIfMissing = true
	cfg.StoreConfig.RaftOption.CreateIfMissing = true

	cfg.Transport = tp
	disk, err := OpenDisk(ctx, cfg)
	require.NoError(t, err)
	disk.diskInfo.Status = proto.DiskStatusNormal

	require.NoError(t, disk.SaveDiskInfo(ctx))
	t.Logf("Disk info: %+v", disk.GetDiskInfo())
	disk.store.KVStore().Close()
	disk.store.RaftStore().Close()

	disk, err = OpenDisk(ctx, cfg)
	require.NoError(t, err)

	disk.store.KVStore().Close()
	disk.store.RaftStore().Close()

	f, err := disk.store.NewRawFS(sysRawFSPath).CreateRawFile(ctx, diskMetaFile)
	require.NoError(t, err)
	f.Write([]byte{'0'})
	_, err = OpenDisk(ctx, cfg)
	require.Errorf(t, err, "")
}

func TestServerDisk_Shard(t *testing.T) {
	d, diskClean := newMockDisk(t)
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
	require.Equal(t, uint64(1), s.GetEpoch())

	shardID2 := proto.ShardID(2)
	suid2 := proto.EncodeSuid(shardID2, 0, 0)

	require.NoError(t, d.d.AddShard(ctx, suid2, 1, *rg, []clustermgr.ShardUnit{{DiskID: 1}}))
	_, err = d.d.GetShard(suid2)
	require.NoError(t, err)

	d.d.RangeShard(func(s ShardHandler) bool {
		require.NoError(t, s.Checkpoint(ctx))
		return true
	})
	require.NoError(t, d.d.DeleteShard(ctx, suid2))
	require.NoError(t, d.d.DeleteShard(ctx, suid2))

	_, err = d.d.GetShard(suid2)
	require.Equal(t, apierr.ErrShardDoesNotExist, err)

	require.NoError(t, d.d.Load(ctx))
}

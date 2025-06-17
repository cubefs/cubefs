// Copyright 2024 The CubeFS Authors.
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
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/util"
)

var (
	A = gomock.Any()
	C = gomock.NewController

	_, ctx = trace.StartSpanFromContext(context.Background(), "TestingDisk")
)

type MockDisk struct {
	d  *Disk
	tp *base.MockTransport
}

func NewMockDisk(tb testing.TB, diskID proto.DiskID) (*MockDisk, func(), error) {
	diskPath, err := util.GenTmpPath()
	if err != nil {
		return nil, nil, err
	}
	pathClean := func() { os.RemoveAll(diskPath) }
	var cfg DiskConfig
	cfg.DiskPath = diskPath
	cfg.StoreConfig.KVOption.CreateIfMissing = true
	cfg.StoreConfig.RaftOption.CreateIfMissing = true
	cfg.StoreConfig.KVOption.ColumnFamily = append(cfg.StoreConfig.KVOption.ColumnFamily, lockCF, dataCF, writeCF)
	cfg.StoreConfig.RaftOption.ColumnFamily = append(cfg.StoreConfig.RaftOption.ColumnFamily, raftWalCF)

	// mock raft transport：for raft manager to resolve node address
	tp := base.NewMockTransport(C(tb))
	cfg.Transport = tp

	tp.EXPECT().GetNode(A, A).DoAndReturn(func(ctx context.Context, nodeID proto.NodeID) (*clustermgr.ShardNodeInfo, error) {
		raftHost := fmt.Sprintf("127.0.0.1:%d", 18080+uint32(nodeID))
		return &clustermgr.ShardNodeInfo{
			ShardNodeExtraInfo: clustermgr.ShardNodeExtraInfo{RaftHost: raftHost},
		}, nil
	}).AnyTimes()
	tp.EXPECT().GetDisk(A, A, A).DoAndReturn(func(ctx context.Context, diskID proto.DiskID, cache bool) (*clustermgr.ShardNodeDiskInfo, error) {
		return &clustermgr.ShardNodeDiskInfo{
			DiskInfo: clustermgr.DiskInfo{NodeID: proto.NodeID(diskID)},
		}, nil
	}).AnyTimes()

	// raft config
	cfg.RaftConfig.HeartbeatTick = 4
	cfg.RaftConfig.ElectionTick = 6
	cfg.RaftConfig.TransportConfig.Resolver = &AddressResolver{Transport: tp}
	cfg.RaftConfig.TransportConfig.Addr = fmt.Sprintf("127.0.0.1:%d", 18080+uint32(diskID))
	cfg.RaftConfig.Transport = raft.NewTransport(&raft.TransportConfig{
		Addr:     fmt.Sprintf("127.0.0.1:%d", 18080+uint32(diskID)),
		Resolver: &AddressResolver{Transport: tp},
	})

	// mock shard stat api
	shardTp := base.NewMockShardTransport(C(tb))
	shardTp.EXPECT().ResolveRaftAddr(A, A).Return("127.0.0.1:18080", nil).AnyTimes()
	shardTp.EXPECT().ResolveNodeAddr(A, A).Return("127.0.0.1:9100", nil).AnyTimes()
	shardTp.EXPECT().ShardStats(A, A, A).Return(shardnode.ShardStats{}, nil).AnyTimes()
	cfg.ShardBaseConfig.Transport = shardTp

	disk, err := OpenDisk(ctx, cfg)
	require.NoError(tb, err)

	shardTp.EXPECT().UpdateShard(A, A, A).DoAndReturn(func(ctx context.Context, host string, args shardnode.UpdateShardArgs) error {
		if args.ShardUpdateType == proto.ShardUpdateTypeRemoveMember {
			sd, err := disk.getShard(args.Unit.Suid)
			require.NoError(tb, err)
			sd.shardInfoMu.Lock()
			for i, u := range sd.shardInfoMu.Units {
				if u.Suid == args.Unit.Suid {
					sd.shardInfoMu.Units = append(sd.shardInfoMu.Units[:i], sd.shardInfoMu.Units[i+1:]...)
				}
			}
			sd.shardInfoMu.Unlock()
		}
		return nil
	}).AnyTimes()

	disk.SetDiskID(diskID)
	disk.diskInfo.Status = proto.DiskStatusNormal
	// load
	require.NoError(tb, disk.Load(ctx))
	return &MockDisk{d: disk, tp: tp}, func() {
		time.Sleep(time.Second)
		disk.Close()
		pathClean()
	}, nil
}

func (d *MockDisk) GetDisk() *Disk {
	return d.d
}

func (d *MockDisk) Close() {
	d.d.Close()
	os.RemoveAll(d.d.cfg.DiskPath)
}

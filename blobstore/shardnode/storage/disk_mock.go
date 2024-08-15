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
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/golang/mock/gomock"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/stretchr/testify/require"
)

var (
	A = gomock.Any()
	C = gomock.NewController

	_, ctx = trace.StartSpanFromContext(context.Background(), "Testing")
)

func tempPath() (string, func()) {
	rand.Seed(time.Now().Unix())
	tmp := path.Join(os.TempDir(), fmt.Sprintf("shardserver_disk_%d", rand.Int31n(10000)+10000))
	return tmp, func() { os.RemoveAll(tmp) }
}

type MockDisk struct {
	d  *Disk
	tp *base.MockTransport
}

func NewMockDisk(tb testing.TB) (*MockDisk, func()) {
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
	return &MockDisk{d: disk, tp: tp}, func() {
		time.Sleep(time.Second)
		disk.raftManager.Close()
		disk.store.KVStore().Close()
		disk.store.RaftStore().Close()
		pathClean()
	}
}

func (d *MockDisk) GetDisk() *Disk {
	return d.d
}

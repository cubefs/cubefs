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
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/store"
)

var (
	A = gomock.Any()
	C = gomock.NewController

	_, ctx = trace.StartSpanFromContext(context.Background(), "Testing")
)

func tempShardTestPath() (string, func()) {
	tmp := path.Join(os.TempDir(), fmt.Sprintf("shard_test_%d", rand.Int31n(10000)+10000))
	return tmp, func() { os.RemoveAll(tmp) }
}

type mockShard struct {
	shard         *shard
	shardSM       *shardSM
	mockRaftGroup *raft.MockGroup
	ctl           *gomock.Controller
}

func newMockShard(tb testing.TB) (*mockShard, func()) {
	// tmp dir for ut
	dir, pathClean := tempShardTestPath()
	ctl := C(tb)
	mockRaftGroup := raft.NewMockGroup(ctl)
	s, err := store.NewStore(ctx, &store.Config{
		Path: dir,
		KVOption: kvstore.Option{
			CreateIfMissing: true,
			ColumnFamily:    []kvstore.CF{lockCF, dataCF, writeCF},
		},
		RaftOption: kvstore.Option{
			CreateIfMissing: true,
			ColumnFamily:    []kvstore.CF{raftWalCF},
		},
	})
	require.Nil(tb, err)
	shardID := proto.Suid(1)
	shard := &shard{
		suid:      shardID,
		store:     s,
		shardKeys: &shardKeysGenerator{suid: shardID},
		raftGroup: mockRaftGroup,
		cfg: &ShardBaseConfig{
			TruncateWalLogInterval: uint64(1 << 16),
			RaftSnapTransmitConfig: RaftSnapshotTransmitConfig{
				BatchInflightNum:  64,
				BatchInflightSize: 1 << 20,
			},
		},
		shardInfoMu: struct {
			sync.RWMutex
			shardInfo

			leader             proto.DiskID
			lastStableIndex    uint64
			lastTruncatedIndex uint64
		}{
			leader: 1, shardInfo: shardInfo{
				ShardID: 1,
				Range:   *sharding.New(sharding.RangeType_RangeTypeHash, 1),
			},
		},
		diskID: 1,
	}

	return &mockShard{
			shard:         shard,
			shardSM:       (*shardSM)(shard),
			mockRaftGroup: mockRaftGroup,
			ctl:           ctl,
		}, func() {
			ctl.Finish()
			pathClean()
		}
}

func TestServerShard_Checkpoint(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()
	gomock.InOrder(mockShard.mockRaftGroup.EXPECT().Truncate(A, A).AnyTimes().Return(nil))
	err := mockShard.shard.Checkpoint(ctx)
	require.Nil(t, err)
}

func TestServerShard_Item(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()
	oldProtoItem := &shardnode.Item{
		ID: []byte{2},
		Fields: []shardnode.Field{
			{ID: 0, Value: []byte("string")},
		},
	}
	newProtoItem := &shardnode.Item{
		ID: []byte{1},
		Fields: []shardnode.Field{
			{ID: 0, Value: []byte("string1")},
		},
	}
	oldProtoItemBytes, _ := oldProtoItem.Marshal()

	oldShardOpHeader := OpHeader{ShardKeys: [][]byte{oldProtoItem.ID}}
	newShardOpHeader := OpHeader{ShardKeys: [][]byte{newProtoItem.ID}}

	// Insert
	gomock.InOrder(mockShard.mockRaftGroup.EXPECT().Propose(A, A).Return(raft.ProposalResponse{}, nil).AnyTimes())
	err := mockShard.shard.InsertItem(ctx, oldShardOpHeader, *oldProtoItem)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	err = mockShard.shard.InsertItem(ctx, oldShardOpHeader, *oldProtoItem)
	require.Equal(t, apierr.ErrShardNodeNotLeader, err)
	mockShard.shard.diskID = 1

	// Get
	_ = mockShard.shardSM.applyInsertItem(ctx, oldProtoItemBytes)
	_, err = mockShard.shard.GetItem(ctx, oldShardOpHeader, oldProtoItem.ID)
	require.Nil(t, err)
	/*_, err = mockShard.shard.GetItem(ctx, mockShard.shard.startIno-1)
	require.Equal(t, apierr.ErrInoMismatchShardRange, err)
	_, err = mockShard.shard.GetItem(ctx, mockShard.shard.startIno+proto.ShardRangeStepSize+1)
	require.Equal(t, apierr.ErrInoMismatchShardRange, err)*/

	// Update
	err = mockShard.shard.UpdateItem(ctx, newShardOpHeader, *newProtoItem)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	require.Equal(t, apierr.ErrShardNodeNotLeader, mockShard.shard.UpdateItem(ctx, newShardOpHeader, *newProtoItem))
	mockShard.shard.diskID = 1
	/*newProtoItem.Ino = mockShard.shard.startIno - 1
	require.Equal(t, apierr.ErrInoMismatchShardRange, mockShard.shard.UpdateItem(ctx, *newProtoItem))
	newProtoItem.Ino = mockShard.shard.startIno + proto.ShardRangeStepSize + 1
	require.Equal(t, apierr.ErrInoMismatchShardRange, mockShard.shard.UpdateItem(ctx, *newProtoItem))*/

	// Delete
	err = mockShard.shard.DeleteItem(ctx, oldShardOpHeader, oldProtoItem.ID)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	require.Equal(t, apierr.ErrShardNodeNotLeader, mockShard.shard.DeleteItem(ctx, newShardOpHeader, oldProtoItem.ID))
	mockShard.shard.diskID = 1
}

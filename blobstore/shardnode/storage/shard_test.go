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
	"io"
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
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/store"
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
	mockRaftGroup.EXPECT().Close().Return(nil)
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

	mockShardTp := base.NewMockShardTransport(C(tb))
	mockShardTp.EXPECT().ResolveRaftAddr(A, A).Return("", nil).AnyTimes()

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
			Transport: mockShardTp,
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
			shard.Close()
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

func TestServerShard_Key(t *testing.T) {
	g := shardKeysGenerator{suid: proto.EncodeSuid(1, 0, 1)}
	key := []byte("test")
	encodeKey := g.encodeItemKey(key)
	_key := g.decodeItemKey(encodeKey)
	require.Equal(t, key, _key)
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

	oldShardOpHeader := OpHeader{ShardKeys: [][]byte{oldProtoItem.ID}}
	newShardOpHeader := OpHeader{ShardKeys: [][]byte{newProtoItem.ID}}

	// Insert
	gomock.InOrder(mockShard.mockRaftGroup.EXPECT().Propose(A, A).Return(raft.ProposalResponse{}, nil).AnyTimes())

	oldkv, _ := InitKV(oldProtoItem.ID, &io.LimitedReader{R: rpc2.Codec2Reader(oldProtoItem), N: int64(oldProtoItem.Size())})
	err := mockShard.shard.Insert(ctx, oldShardOpHeader, oldkv)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	err = mockShard.shard.Insert(ctx, oldShardOpHeader, oldkv)
	require.Equal(t, apierr.ErrShardNodeNotLeader, err)
	mockShard.shard.diskID = 1

	// Get
	_ = mockShard.shardSM.applyInsertRaw(ctx, oldkv.Marshal())
	_, err = mockShard.shard.GetItem(ctx, oldShardOpHeader, oldProtoItem.ID)
	require.Nil(t, err)

	newkv, _ := InitKV(newProtoItem.ID, &io.LimitedReader{R: rpc2.Codec2Reader(newProtoItem), N: int64(newProtoItem.Size())})
	_ = mockShard.shardSM.applyInsertRaw(ctx, newkv.Marshal())
	_, err = mockShard.shard.GetItem(ctx, newShardOpHeader, newProtoItem.ID)
	require.Nil(t, err)

	// Update
	oldProtoItem.Fields[0].Value = []byte("new-string")
	updatekv, _ := InitKV(oldProtoItem.ID, &io.LimitedReader{R: rpc2.Codec2Reader(oldProtoItem), N: int64(oldProtoItem.Size())})
	err = mockShard.shard.Update(ctx, oldShardOpHeader, updatekv)
	require.Nil(t, err)

	// Update Item
	err = mockShard.shard.UpdateItem(ctx, newShardOpHeader, *newProtoItem)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	require.Equal(t, apierr.ErrShardNodeNotLeader, mockShard.shard.UpdateItem(ctx, newShardOpHeader, *newProtoItem))
	mockShard.shard.diskID = 1

	// Get Items
	oldItemShardKey := mockShard.shard.shardKeys.encodeItemKey(oldProtoItem.ID)
	newItemShardKey := mockShard.shard.shardKeys.encodeItemKey(newProtoItem.ID)
	itms, err := mockShard.shard.GetItems(ctx, oldShardOpHeader, [][]byte{oldItemShardKey, newItemShardKey})
	require.Nil(t, err)
	require.Equal(t, 2, len(itms))

	// Delete
	err = mockShard.shard.Delete(ctx, oldShardOpHeader, oldProtoItem.ID)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	require.Equal(t, apierr.ErrShardNodeNotLeader, mockShard.shard.Delete(ctx, newShardOpHeader, oldProtoItem.ID))
	mockShard.shard.diskID = 1
}

func TestServerShard_Stats(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()

	mockShard.mockRaftGroup.EXPECT().Stat().Return(&raft.Stat{}, nil)

	_, err := mockShard.shard.Stats(context.Background())
	require.Nil(t, err)

	index := mockShard.shard.GetAppliedIndex()
	require.Equal(t, uint64(0), index)

	stableIdx := mockShard.shard.GetStableIndex()
	require.Equal(t, uint64(0), stableIdx)
}

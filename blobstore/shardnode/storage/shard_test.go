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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	kvstore "github.com/cubefs/cubefs/blobstore/common/kvstorev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raft"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/shardnode/storage/store"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

const (
	brokenDiskID = proto.DiskID(1000)
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
	mockRaftGroup.EXPECT().Propose(A, A).Return(
		raft.ProposalResponse{
			Data: applyRet{traceLog: []string{"trace log"}},
		},
		nil).AnyTimes()
	mockRaftGroup.EXPECT().MemberChange(A, A).Return(nil).AnyTimes()
	mockRaftGroup.EXPECT().ReadIndex(A).Return(nil).AnyTimes()

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
	mockShardTp.EXPECT().ResolveNodeAddr(A, A).Return("", nil).AnyTimes()
	mockShardTp.EXPECT().ShardStats(A, A, A).DoAndReturn(
		func(ctx context.Context, host string, args shardnode.GetShardArgs) (shardnode.ShardStats, error) {
			if args.DiskID == brokenDiskID {
				return shardnode.ShardStats{}, apierr.ErrShardNodeDiskNotFound
			}
			return shardnode.ShardStats{}, nil
		},
	).AnyTimes()

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
	shard.shardState.readIndexFunc = func(ctx context.Context) error {
		return mockRaftGroup.ReadIndex(ctx)
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

func TestServerShard_ShardSplit(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()

	// init
	require.True(t, mockShard.shard.shardState.allowRW())

	go func() {
		for i := 0; i < 100; i++ {
			err := mockShard.shard.shardState.prepRWCheck(ctx)
			require.Nil(t, err)
			mockShard.shard.shardState.prepRWCheckDone()
		}
	}()

	mockShard.shard.shardState.startSplitting()
	mockShard.shard.shardState.splitStopWriting()
	// mock splitting
	time.Sleep(1 * time.Second)
	mockShard.shard.shardState.stopSplitting()
	mockShard.shard.shardState.splitStartWriting()
}

func TestServerShard_Checkpoint(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()
	mockShard.shard.SaveShardInfo(ctx, false, true)
	gomock.InOrder(mockShard.mockRaftGroup.EXPECT().Truncate(A, A).AnyTimes().Return(nil))
	err := mockShard.shard.Checkpoint(ctx)
	require.Nil(t, err)
}

func TestServerShard_Key(t *testing.T) {
	g := shardKeysGenerator{suid: proto.EncodeSuid(1, 0, 1)}
	key := []byte("test")
	encodeKey := g.encodeBlobKey(key)
	_key := g.decodeBlobKey(encodeKey)
	require.Equal(t, key, _key)
}

func TestSpace_Key(t *testing.T) {
	g := NewShardKeysGenerator(proto.EncodeSuid(proto.ShardID(1), 0, 0))
	g.EncodeShardInfoKey()
	g.EncodeShardDataMaxPrefix()
	g.EncodeShardDataPrefix()
}

func TestServerShard_Item(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()
	sk := mockShard.shard.shardKeys

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
	err := mockShard.shard.InsertItem(ctx, oldShardOpHeader, oldProtoItem.ID, *oldProtoItem)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	err = mockShard.shard.InsertItem(ctx, oldShardOpHeader, oldProtoItem.ID, *oldProtoItem)
	require.Equal(t, apierr.ErrShardNodeNotLeader, err)
	mockShard.shard.diskID = 1

	_interOldItem := protoItemToInternalItem(*oldProtoItem)
	oldkv, _ := initKV(sk.encodeItemKey(oldProtoItem.ID), &io.LimitedReader{R: rpc2.Codec2Reader(&_interOldItem), N: int64(_interOldItem.Size())})
	// Get
	_ = mockShard.shardSM.applyInsertItem(ctx, oldkv.Marshal())
	itm, err := mockShard.shard.GetItem(ctx, oldShardOpHeader, oldProtoItem.ID)
	require.Nil(t, err)
	require.Equal(t, itm.ID, oldProtoItem.ID)

	_interNewItem := protoItemToInternalItem(*newProtoItem)
	newkv, _ := initKV(sk.encodeItemKey(newProtoItem.ID), &io.LimitedReader{R: rpc2.Codec2Reader(&_interNewItem), N: int64(_interNewItem.Size())})
	_ = mockShard.shardSM.applyInsertItem(ctx, newkv.Marshal())
	_, err = mockShard.shard.GetItem(ctx, newShardOpHeader, newProtoItem.ID)
	require.Nil(t, err)

	// Update
	oldProtoItem.Fields[0].Value = []byte("new-string")
	_interOldItem = protoItemToInternalItem(*oldProtoItem)
	oldkv, _ = initKV(sk.encodeItemKey(oldProtoItem.ID), &io.LimitedReader{R: rpc2.Codec2Reader(&_interOldItem), N: int64(_interOldItem.Size())})
	err = mockShard.shardSM.applyUpdateItem(ctx, oldkv.Marshal())
	require.Nil(t, err)

	// Update Item
	err = mockShard.shard.UpdateItem(ctx, newShardOpHeader, newProtoItem.ID, *newProtoItem)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	require.Equal(t, apierr.ErrShardNodeNotLeader, mockShard.shard.UpdateItem(ctx, newShardOpHeader, newProtoItem.ID, *newProtoItem))
	mockShard.shard.diskID = 1

	// Get Items
	oldItemShardKey := mockShard.shard.shardKeys.encodeItemKey(oldProtoItem.ID)
	newItemShardKey := mockShard.shard.shardKeys.encodeItemKey(newProtoItem.ID)
	itms, err := mockShard.shard.GetItems(ctx, oldShardOpHeader, [][]byte{oldItemShardKey, newItemShardKey})
	require.Nil(t, err)
	require.Equal(t, 2, len(itms))

	// Delete
	err = mockShard.shard.DeleteItem(ctx, oldShardOpHeader, oldProtoItem.ID)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	require.Equal(t, apierr.ErrShardNodeNotLeader, mockShard.shard.DeleteItem(ctx, newShardOpHeader, oldProtoItem.ID))
	mockShard.shard.diskID = 1
}

func TestServerShard_Stats(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()

	mockShard.mockRaftGroup.EXPECT().Stat().Return(&raft.Stat{}, nil)

	_, err := mockShard.shard.Stats(context.Background(), true)
	require.Nil(t, err)

	index := mockShard.shard.GetAppliedIndex()
	require.Equal(t, uint64(0), index)
}

func TestServerShard_CheckAndClearShard(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()

	diskID := mockShard.shard.diskID
	mockShard.shard.shardInfoMu.Units = []clustermgr.ShardUnit{
		{
			Suid:   proto.EncodeSuid(1, 0, 0),
			DiskID: mockShard.shard.diskID,
		},
		{
			Suid:   proto.EncodeSuid(1, 1, 0),
			DiskID: brokenDiskID,
		},
		{
			Suid:   proto.EncodeSuid(1, 2, 0),
			DiskID: proto.DiskID(101),
		},
		{
			Suid:   proto.EncodeSuid(1, 1, 1),
			DiskID: proto.DiskID(102),
		},
	}

	mockShard.mockRaftGroup.EXPECT().Stat().Return(&raft.Stat{
		Peers: []raft.Peer{
			{NodeID: uint64(diskID), RecentActive: true},
			{NodeID: uint64(brokenDiskID), RecentActive: false},
			{NodeID: uint64(101), RecentActive: false},
			{NodeID: uint64(102), RecentActive: true},
		},
	}, nil)

	require.Nil(t, mockShard.shard.CheckAndClearShard(ctx))
}

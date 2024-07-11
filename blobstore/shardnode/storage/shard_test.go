package storage

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/apierr"
	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/common/sharding"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"
	"github.com/cubefs/inodedb/shardserver/store"
	"github.com/cubefs/inodedb/shardserver/vector"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func tempShardTestPath() (string, func()) {
	tmp := path.Join(os.TempDir(), fmt.Sprintf("shard_test_%d", rand.Int31n(10000)+10000))
	return tmp, func() { os.RemoveAll(tmp) }
}

type mockShard struct {
	shard           *shard
	shardSM         *shardSM
	mockVectorIndex *vector.MockVectorIndex
	mockRaftGroup   *raft.MockGroup
	ctl             *gomock.Controller
}

func newMockShard(tb testing.TB) (*mockShard, func()) {
	// tmp dir for ut
	dir, pathClean := tempShardTestPath()
	ctl := C(tb)
	mockVectorIndex := vector.NewMockVectorIndex(ctl)
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
	sid := proto.Sid(1)
	shardID := proto.ShardID(1)
	shard := &shard{
		sid:         sid,
		shardID:     shardID,
		store:       s,
		shardKeys:   &shardKeysGenerator{shardID: shardID},
		vectorIndex: mockVectorIndex,
		raftGroup:   mockRaftGroup,
		cfg: &ShardBaseConfig{
			AutoIDAllocRangeStep: 100,
		},
		shardMu: struct {
			sync.RWMutex
			shardInfo
			leader proto.DiskID
		}{
			leader: 1, shardInfo: shardInfo{
				ShardID: 1,
				Range:   *sharding.New(sharding.RangeType_RangeTypeHash, 1),
			},
		},
		diskID:         1,
		embeddingField: "f2",
	}

	ir := autoIDRange{0, 100}
	shard.autoIDRange.Store(&ir)
	return &mockShard{
			shard:           shard,
			shardSM:         (*shardSM)(shard),
			mockVectorIndex: mockVectorIndex,
			mockRaftGroup:   mockRaftGroup,
			ctl:             ctl,
		}, func() {
			ctl.Finish()
			pathClean()
		}
}

func TestServerShard_Checkpoint(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()
	gomock.InOrder(mockShard.mockVectorIndex.EXPECT().Dump(A).Return(nil))
	gomock.InOrder(mockShard.mockRaftGroup.EXPECT().Truncate(A, A).AnyTimes().Return(nil))
	err := mockShard.shard.Checkpoint(ctx)
	require.Nil(t, err)
}

func TestServerShard_Item(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()
	oldEmbedding := proto.Embedding{
		Elements: []float32{0.11, 0.22},
		Source:   "source",
	}
	oldEmbeddingBytes, _ := oldEmbedding.Marshal()
	oldProtoItem := &proto.Item{
		ID: []byte{2},
		Fields: []proto.Field{
			{Name: "f1", Value: []byte("string")},
			{Name: "f2", Value: oldEmbeddingBytes},
		},
	}
	// oldProtoItemBytes, _ := oldProtoItem.Marshal()
	newEmbedding := proto.Embedding{
		Elements: []float32{0.111, 0.222},
		Source:   "source1",
	}
	newEmbeddingBytes, _ := newEmbedding.Marshal()
	newProtoItem := &proto.Item{
		ID: []byte{1},
		Fields: []proto.Field{
			{Name: "f1", Value: []byte("string1")},
			{Name: "f2", Value: newEmbeddingBytes},
		},
	}

	oldShardOpHeader := ShardOpHeader{ShardKeys: [][]byte{oldProtoItem.ID}}
	newShardOpHeader := ShardOpHeader{ShardKeys: [][]byte{newProtoItem.ID}}

	// Insert
	gomock.InOrder(mockShard.mockRaftGroup.EXPECT().Propose(A, A).Return(raft.ProposalResponse{}, nil).AnyTimes())
	gomock.InOrder(mockShard.mockVectorIndex.EXPECT().ValidateBeforeInsert(A).Return(nil).AnyTimes())
	gomock.InOrder(mockShard.mockVectorIndex.EXPECT().Add(A, A).Return(nil).AnyTimes())
	err := mockShard.shard.InsertItem(ctx, oldShardOpHeader, *oldProtoItem)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	err = mockShard.shard.InsertItem(ctx, oldShardOpHeader, *oldProtoItem)
	require.Equal(t, apierr.ErrNotLeader, err)
	mockShard.shard.diskID = 1

	// Get
	//_ = mockShard.shardSM.applyInsertItem(ctx, oldProtoItemBytes)
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
	require.Equal(t, apierr.ErrNotLeader, mockShard.shard.UpdateItem(ctx, newShardOpHeader, *newProtoItem))
	mockShard.shard.diskID = 1
	/*newProtoItem.Ino = mockShard.shard.startIno - 1
	require.Equal(t, apierr.ErrInoMismatchShardRange, mockShard.shard.UpdateItem(ctx, *newProtoItem))
	newProtoItem.Ino = mockShard.shard.startIno + proto.ShardRangeStepSize + 1
	require.Equal(t, apierr.ErrInoMismatchShardRange, mockShard.shard.UpdateItem(ctx, *newProtoItem))*/

	// Delete
	err = mockShard.shard.DeleteItem(ctx, oldShardOpHeader, oldProtoItem.ID)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	require.Equal(t, apierr.ErrNotLeader, mockShard.shard.DeleteItem(ctx, newShardOpHeader, oldProtoItem.ID))
	mockShard.shard.diskID = 1
}

func TestServerShard_Link(t *testing.T) {
	mockShard, shardClean := newMockShard(t)
	defer shardClean()
	fields := []proto.Field{
		{Name: "f1", Value: []byte("string")},
	}
	l1 := proto.Link{Parent: []byte{1}, Name: []byte("link1"), Fields: fields}
	l1Byte, _ := l1.Marshal()
	item1 := &proto.Item{
		ID: []byte{1},
		Fields: []proto.Field{
			{Name: "f1", Value: []byte("string")},
		},
	}
	item2 := &proto.Item{
		ID: []byte{2},
		Fields: []proto.Field{
			{Name: "f1", Value: []byte("string")},
		},
	}
	pi1, _ := mockShard.shard.generateProposeItem(ctx, *item1)
	b1 := pi1.raw
	pi2, _ := mockShard.shard.generateProposeItem(ctx, *item2)
	b2 := pi2.raw
	_ = mockShard.shardSM.applyInsertItem(ctx, b1)
	_ = mockShard.shardSM.applyInsertItem(ctx, b2)

	shardOpHeader1 := ShardOpHeader{ShardKeys: [][]byte{item1.ID}}

	// link
	gomock.InOrder(mockShard.mockRaftGroup.EXPECT().Propose(A, A).Return(raft.ProposalResponse{}, nil).AnyTimes())
	err := mockShard.shard.Link(ctx, shardOpHeader1, l1)
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	require.Equal(t, apierr.ErrNotLeader, mockShard.shard.Link(ctx, shardOpHeader1, l1))
	mockShard.shard.diskID = 1
	/*l1.Parent = mockShard.shard.startIno + proto.ShardRangeStepSize + 1
	require.Equal(t, apierr.ErrInoMismatchShardRange, mockShard.shard.Link(ctx, l1))
	l1.Parent = 1
	l1.Child = mockShard.shard.startIno + proto.ShardRangeStepSize + 1
	require.Equal(t, apierr.ErrInoMismatchShardRange, mockShard.shard.Link(ctx, l1))
	l1.Child = 2*/

	// get link
	_ = mockShard.shardSM.applyLink(ctx, l1Byte)
	ret, err := mockShard.shard.GetLink(ctx, shardOpHeader1, proto.GetLink{
		Parent: l1.Parent,
		Name:   l1.Name,
	})
	require.Equal(t, l1, ret)
	require.Nil(t, err)
	/*_, err = mockShard.shard.GetLink(ctx, proto.GetLink{
		Parent: mockShard.shard.startIno + proto.ShardRangeStepSize + 1,
		Name:   "link1",
	})
	require.Equal(t, apierr.ErrInoMismatchShardRange, err)*/

	// list
	listRet, err := mockShard.shard.ListLink(ctx, shardOpHeader1, l1.Parent, l1.Name, 10)
	require.Nil(t, err)
	require.Equal(t, l1, listRet[0])
	require.Equal(t, 1, len(listRet))

	// unlink
	err = mockShard.shard.Unlink(ctx, shardOpHeader1, proto.Unlink{
		Parent: l1.Parent,
		Name:   l1.Name,
	})
	require.Nil(t, err)
	mockShard.shard.diskID = 2
	err = mockShard.shard.Unlink(ctx, shardOpHeader1, proto.Unlink{
		Parent: l1.Parent,
		Name:   l1.Name,
	})
	mockShard.shard.diskID = 1
	require.Equal(t, apierr.ErrNotLeader, err)
	/*err = mockShard.shard.Unlink(ctx, proto.Unlink{
		Parent: mockShard.shard.startIno + proto.ShardRangeStepSize + 1,
		Name:   "link1",
	})
	require.Equal(t, apierr.ErrInoMismatchShardRange, err)*/
}

func TestServerShard_NextAutoID(t *testing.T) {
	mockShard, cleanShard := newMockShard(t)
	defer cleanShard()
	mockShard.shard.autoIDRange = atomic.Value{}
	gomock.InOrder(mockShard.mockRaftGroup.EXPECT().Propose(A, A).Return(raft.ProposalResponse{Data: autoIDRange{start: 0, end: 100}}, nil))
	i, err := mockShard.shard.NextAutoID(ctx)
	require.Nil(t, err)
	require.Equal(t, uint64(1), i)
	mockShard.shard.autoIDRange = atomic.Value{}
	gomock.InOrder(mockShard.mockRaftGroup.EXPECT().Propose(A, A).Return(raft.ProposalResponse{}, errors.New("alloc ino range error")))
	_, err = mockShard.shard.NextAutoID(ctx)
	require.Equal(t, errors.New("alloc auto id range error"), err)
	mockShard.shard.autoIDRange = atomic.Value{}
	_, err = mockShard.shard.NextAutoID(ctx)
	require.Equal(t, apierr.ErrInodeLimitExceed, err)
}

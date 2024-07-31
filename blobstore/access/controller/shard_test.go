package controller

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/btree"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func TestShardController(t *testing.T) {
	ctx := context.Background()
	any := gomock.Any()
	errMock := errors.New("fake error")

	stopCh := make(chan struct{})
	ctr := gomock.NewController(&testing.T{})
	cmCli := mocks.NewMockClientAPI(ctr)
	cmCli.EXPECT().AuthSpace(any, any).Return(nil)
	cmCli.EXPECT().GetCatalogChanges(any, any).Return(&clustermgr.GetCatalogChangesRet{
		RouteVersion: 1,
	}, nil)

	s, err := NewShardController(shardCtrlConf{}, cmCli, stopCh)
	require.Nil(t, err)
	require.NotEqual(t, errMock, err)

	blobName := []byte("blob1")
	// empty tree
	_, err = s.GetShard(ctx, blobName)
	require.NotNil(t, err)

	shard := &shardInfo{
		ShardID: 1,
		Epoch:   1,
	}
	// rangePtr := sharding.New(sharding.RangeType_RangeTypeHash, 2)  // keys len=1; subs len=2. will panic
	rangePtr := sharding.New(sharding.RangeType_RangeTypeHash, 1)
	for i := range rangePtr.Subs {
		rangePtr.Subs[i].Min = uint64(i * 10)
		rangePtr.Subs[i].Max = uint64(i+1) * 10
	}
	shard.Range = *rangePtr
	svr := &shardControllerImpl{
		shards: make(map[proto.ShardID]*shardInfo),
		ranges: btree.New(defaultBTreeDegree),
	}

	// add one, not found blob
	svr.addShard(shard)
	_, err = svr.GetShard(ctx, blobName)
	require.NotNil(t, err)
	svr.delShard(shard)

	{
		// add 8 shard
		shards := make([]*shardInfo, 8)
		for i := 0; i < 8; i++ {
			shard := &shardInfo{
				ShardID: proto.ShardID(i + 1),
				Epoch:   1,
			}
			shard.Range = sharding.Range{
				Type: sharding.RangeType_RangeTypeHash,
				Subs: []sharding.SubRange{
					{Min: uint64(i * 10), Max: uint64(i+1) * 10},
					// {Min: 0, Max: math.MaxUint64},
				},
			}
			shards[i] = shard
		}
		shards[7].Range.Subs[0].Max = math.MaxUint64

		for i := 0; i < 8; i++ {
			svr.addShard(shards[i])
		}

		ret, err := svr.GetShard(ctx, []byte("blob1__xxx")) // expect 2 keys
		require.Nil(t, err)
		require.Equal(t, proto.ShardID(8), ret.ShardID)

		// require.Panics(t, func() {
		//	// will panic at hashrange.go Belong, not math array length; key len=1, subRange=2
		//	_, err = svr.GetShard(ctx, blobName)
		// })

		ret, err = svr.GetShard(ctx, []byte("blob2__yy"))
		require.Nil(t, err)
		require.Equal(t, proto.ShardID(8), ret.ShardID)

		// ret, err = svr.GetShard(ctx, []byte("blob1__xx__xx"))
		// require.NotNil(t, err)
		// require.Equal(t, proto.ShardID(0), ret.ShardID)
	}

	{
		si, ok := svr.getShardByID(1)
		require.True(t, ok)
		require.Equal(t, proto.ShardID(1), si.ShardID)
	}
}

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
	gAny := gomock.Any()
	errMock := errors.New("fake error")

	stopCh := make(chan struct{})
	ctr := gomock.NewController(&testing.T{})
	cmCli := mocks.NewMockClientAPI(ctr)
	cmCli.EXPECT().AuthSpace(gAny, gAny).Return(nil)
	cmCli.EXPECT().GetSpaceByName(gAny, gAny).Return(&clustermgr.Space{
		SpaceID: 1,
		Name:    "spaceTest",
	}, nil)
	retCatlog := &clustermgr.GetCatalogChangesRet{}
	retCatlog.RouteVersion = 1
	cmCli.EXPECT().GetCatalogChanges(gAny, gAny).Return(retCatlog, nil)

	s, err := NewShardController(shardCtrlConf{}, cmCli, stopCh)
	require.Nil(t, err)
	require.NotEqual(t, errMock, err)

	blobName := []byte("blob1")
	// empty tree
	_, err = s.GetShard(ctx, blobName)
	require.NotNil(t, err)

	sh := &shard{
		shardID: 1,
		version: 1,
	}
	// rangePtr := sharding.New(sharding.RangeType_RangeTypeHash, 2)  // keys len=1; subs len=2. will panic
	rangePtr := sharding.New(sharding.RangeType_RangeTypeHash, 1)
	for i := range rangePtr.Subs {
		rangePtr.Subs[i].Min = uint64(i * 10)
		rangePtr.Subs[i].Max = uint64(i+1) * 10
	}
	sh.rangeExt = *rangePtr
	svr := &shardControllerImpl{
		shards: make(map[proto.ShardID]*shard),
		ranges: btree.New(defaultBTreeDegree),
	}

	// add one, not found blob
	svr.addShard(sh)
	_, err = svr.GetShard(ctx, blobName)
	require.NotNil(t, err)
	svr.delShard(sh)

	{
		// add 8 shard
		shards := make([]*shard, 8)
		for i := 0; i < 8; i++ {
			shard := &shard{
				shardID: proto.ShardID(i + 1),
				version: 1,
			}
			shard.rangeExt = sharding.Range{
				Type: sharding.RangeType_RangeTypeHash,
				Subs: []sharding.SubRange{
					{Min: uint64(i * 10), Max: uint64(i+1) * 10},
					// {Min: 0, Max: math.MaxUint64},
				},
			}
			shards[i] = shard
		}
		shards[7].rangeExt.Subs[0].Max = math.MaxUint64

		for i := 0; i < 8; i++ {
			svr.addShard(shards[i])
		}

		ret, err := svr.GetShard(ctx, []byte("blob1__xxx")) // expect 2 keys
		require.Nil(t, err)
		require.Equal(t, proto.ShardID(8), ret.(*shard).shardID)

		// require.Panics(t, func() {
		//	// will panic at hashrange.go Belong, not math array length; key len=1, subRange=2
		//	_, err = svr.GetShard(ctx, blobName)
		// })

		ret, err = svr.GetShard(ctx, []byte("blob2__yy"))
		require.Nil(t, err)
		require.Equal(t, proto.ShardID(8), ret.(*shard).shardID)

		// ret, err = svr.GetShard(ctx, []byte("blob1__xx__xx"))
		// require.NotNil(t, err)
		// require.Equal(t, proto.ShardID(0), ret.ShardID)
	}

	{
		si, ok := svr.getShardByID(1)
		require.True(t, ok)
		require.Equal(t, proto.ShardID(1), si.shardID)
	}
}

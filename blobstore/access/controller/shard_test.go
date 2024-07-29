package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/btree"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func TestShardController(t *testing.T) {
	// any := gomock.Any()
	errMock := errors.New("fake error")

	stopCh := make(chan struct{})
	ctr := gomock.NewController(&testing.T{})
	cmCli := mocks.NewMockClientAPI(ctr)
	// cmCli.EXPECT().GetCatalogChanges(any, any).Return(&clustermgr.GetCatalogChangesRet{
	//	RouteVersion: 1,
	// }, nil)

	s, err := NewShardController(shardCtrlConf{}, cmCli, stopCh)
	require.Nil(t, err)
	require.NotEqual(t, errMock, err)

	ctx := context.Background()
	blobName := []byte("blob1")
	s.GetShard(ctx, blobName)

	shard := clustermgr.Shard{}
	svr := &shardControllerImpl{
		shards:  make(map[proto.ShardID]*shardInfo),
		ranges:  btree.New(defaultBTreeDegree),
		version: 1,
	}
	s.GetShard(ctx, blobName)
	svr.addShard(shard)
	svr.delShard(shard)
}

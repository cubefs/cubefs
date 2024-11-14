package stream

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func newStreamHandlerSuccess(t *testing.T) *Handler {
	ctr := gomock.NewController(t)
	gAny := gomock.Any()

	info := controller.ShardOpInfo{
		DiskID:       101,
		Suid:         proto.EncodeSuid(1, 0, 1),
		RouteVersion: 1,
	}

	shardInfo := NewMockShard(ctr)
	shardInfo.EXPECT().GetMember(gAny, gAny).Return(info).AnyTimes()

	shardMgr := NewMockShardController(ctr)
	shardMgr.EXPECT().GetShard(gAny, gAny).Return(shardInfo, nil).AnyTimes()
	shardMgr.EXPECT().GetSpaceID().Return(proto.SpaceID(1)).AnyTimes()
	shardMgr.EXPECT().UpdateRoute(gAny).Return(nil).AnyTimes()

	svrCtrl := NewMockServiceController(ctr)
	svrCtrl.EXPECT().GetShardnodeHost(gAny, gAny).Return(&controller.HostIDC{Host: "host"}, nil).AnyTimes()

	clu := NewMockClusterController(ctr)
	clu.EXPECT().GetShardController(gAny).Return(shardMgr, nil).AnyTimes()
	clu.EXPECT().GetServiceController(gAny).Return(svrCtrl, nil).Times(2)

	shardCli := mocks.NewMockShardnodeAccess(ctr)
	proxyClient := mocks.NewMockProxyClient(ctr)

	allCodeModes := CodeModePairs{
		codemode.EC3P3: CodeModePair{
			Policy: codemode.Policy{
				ModeName: codemode.EC3P3.Name(),
				MaxSize:  math.MaxInt64,
				Enable:   true,
			},
			Tactic: codemode.EC3P3.Tactic(),
		},
	}
	handler := &Handler{
		clusterController: clu,
		shardnodeClient:   shardCli,
		proxyClient:       proxyClient,

		maxObjectSize: 100,
		allCodeModes:  allCodeModes,
	}

	return handler
}

func TestStreamGetBlob(t *testing.T) {
	ctx := context.Background()
	ctr := gomock.NewController(t)
	gAny := gomock.Any()

	info := controller.ShardOpInfo{}

	shardInfo := NewMockShard(ctr)
	shardInfo.EXPECT().GetMember(gAny, gAny).Return(info).Times(2)

	shardMgr := NewMockShardController(ctr)
	shardMgr.EXPECT().GetShard(gAny, gAny).Return(shardInfo, nil).Times(2)
	shardMgr.EXPECT().GetSpaceID().Return(proto.SpaceID(1)).Times(2)
	shardMgr.EXPECT().UpdateRoute(gAny).Return(nil)

	svrCtrl := NewMockServiceController(ctr)
	svrCtrl.EXPECT().GetShardnodeHost(gAny, gAny).Return(&controller.HostIDC{Host: "host"}, nil).Times(2)

	clu := NewMockClusterController(ctr)
	clu.EXPECT().GetShardController(gAny).Return(shardMgr, nil).Times(3)
	clu.EXPECT().GetServiceController(gAny).Return(svrCtrl, nil).Times(2)

	blobName := []byte("blob1")
	blob := &proto.Blob{
		Name: blobName,
		Location: proto.Location{
			ClusterID: 1,
			CodeMode:  codemode.EC3P3,
			Size_:     1,
			SliceSize: 1,
			Crc:       1,
			Slices:    nil,
		},
	}
	ret := shardnode.GetBlobRet{
		Blob: *blob,
	}
	shardCli := mocks.NewMockShardnodeAccess(ctr)
	shardCli.EXPECT().GetBlob(gAny, gAny, gAny).Return(ret, errcode.ErrShardRouteVersionNeedUpdate)
	shardCli.EXPECT().GetBlob(gAny, gAny, gAny).Return(ret, nil)

	handler := &Handler{
		clusterController: clu,
		shardnodeClient:   shardCli,
	}
	args := acapi.GetBlobArgs{
		BlobName: blobName,
		Mode:     acapi.GetShardModeRandom,
	}

	loc, err := handler.GetBlob(ctx, &args)
	require.NoError(t, err)
	require.Equal(t, ret.Blob.Location, *loc)
}

func TestStreamBlobCreate(t *testing.T) {
	ctx := context.Background()
	gAny := gomock.Any()
	h := newStreamHandlerSuccess(t)

	args := acapi.CreateBlobArgs{
		BlobName:  []byte("blob-create"),
		CodeMode:  0,
		ClusterID: 0,
		Size:      10,
		SliceSize: 4,
		ShardKeys: nil,
	}

	ret := shardnode.CreateBlobRet{
		Blob: proto.Blob{
			Name: []byte("blob-create"),
			Location: proto.Location{
				ClusterID: 1,
				CodeMode:  codemode.EC3P3,
				Size_:     10,
				SliceSize: 4,
				Crc:       1,
				Slices:    nil,
			},
		},
	}
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().CreateBlob(gAny, gAny, gAny).Return(ret, errcode.ErrShardRouteVersionNeedUpdate)
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().CreateBlob(gAny, gAny, gAny).Return(ret, nil)
	h.clusterController.(*MockClusterController).EXPECT().ChooseOne().Return(&clustermgr.ClusterInfo{
		ClusterID: 1,
	}, nil)

	loc, err := h.CreateBlob(ctx, &args)
	require.NoError(t, err)
	require.Equal(t, ret.Blob.Location, *loc)
}

func TestStreamBlobDelete(t *testing.T) {
	ctx := context.Background()
	gAny := gomock.Any()
	h := newStreamHandlerSuccess(t)

	args := acapi.DelBlobArgs{
		BlobName:  []byte("blob-del"),
		ClusterID: 1,
		ShardKeys: nil,
	}

	blob := shardnode.GetBlobRet{
		Blob: proto.Blob{
			Name: []byte("blob-del"),
			Location: proto.Location{
				ClusterID: 1,
				CodeMode:  codemode.EC3P3,
			},
		},
	}
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().GetBlob(gAny, gAny, gAny).Return(blob, errcode.ErrShardRouteVersionNeedUpdate)
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().GetBlob(gAny, gAny, gAny).Return(blob, nil).Times(2)
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().DeleteBlob(gAny, gAny, gAny).Return(errcode.ErrShardRouteVersionNeedUpdate)
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().DeleteBlob(gAny, gAny, gAny).Return(nil)
	h.proxyClient.(*mocks.MockProxyClient).EXPECT().SendDeleteMsg(gAny, gAny, gAny).Return(nil)

	svrCtrl := NewMockServiceController(gomock.NewController(t))
	svrCtrl.EXPECT().GetServiceHost(gAny, gAny).Return("host", nil)
	svrCtrl.EXPECT().GetShardnodeHost(gAny, gAny).Return(&controller.HostIDC{Host: "host"}, nil)
	h.clusterController.(*MockClusterController).EXPECT().GetServiceController(gAny).Return(svrCtrl, nil).Times(2)

	err := h.DeleteBlob(ctx, &args)
	require.NoError(t, err)
}

func TestStreamBlobSeal(t *testing.T) {
	ctx := context.Background()
	gAny := gomock.Any()
	h := newStreamHandlerSuccess(t)

	args := acapi.SealBlobArgs{
		BlobName:  []byte("blob-seal"),
		ShardKeys: nil,
		ClusterID: 1,
		Slices:    make([]proto.Slice, 1),
	}

	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().SealBlob(gAny, gAny, gAny).Return(errcode.ErrShardRouteVersionNeedUpdate)
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().SealBlob(gAny, gAny, gAny).Return(nil)

	err := h.SealBlob(ctx, &args)
	require.NoError(t, err)
}

func TestStreamBlobList(t *testing.T) {
	ctx := context.Background()
	gAny := gomock.Any()
	errMock := errors.New("fake error")
	ctr := gomock.NewController(t)

	info := controller.ShardOpInfo{
		DiskID:       101,
		Suid:         proto.EncodeSuid(1, 0, 1),
		RouteVersion: 1,
	}

	shardInfo := NewMockShard(ctr)
	shardInfo.EXPECT().GetMember(gAny, gAny).Return(info).Times(3)

	shardMgr := NewMockShardController(ctr)
	shardMgr.EXPECT().GetShardByID(gAny, gAny).Return(shardInfo, nil).Times(3)
	shardMgr.EXPECT().GetSpaceID().Return(proto.SpaceID(1)).Times(3)
	shardMgr.EXPECT().UpdateRoute(gAny).Return(nil).Times(3)

	svrCtrl := NewMockServiceController(ctr)
	svrCtrl.EXPECT().GetShardnodeHost(gAny, gAny).Return(&controller.HostIDC{Host: "host"}, nil).Times(3)

	clu := NewMockClusterController(ctr)
	clu.EXPECT().GetShardController(gAny).Return(shardMgr, nil).Times(3 * 2)
	clu.EXPECT().GetServiceController(gAny).Return(svrCtrl, nil).Times(3)

	h := &Handler{
		clusterController: clu,
		shardnodeClient:   mocks.NewMockShardnodeAccess(ctr),
	}

	args := acapi.ListBlobArgs{
		ClusterID: 1,
		ShardID:   1,
		Prefix:    []byte("test-"),
		Marker:    []byte("test-blob-1"),
		Count:     4,
	}
	// list one shard
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().ListBlob(gAny, gAny, gAny).Return(shardnode.ListBlobRet{}, errcode.ErrShardRouteVersionNeedUpdate).Times(3)
	ret, err := h.ListBlob(ctx, &args)
	require.NotNil(t, err)
	require.ErrorIs(t, errcode.ErrShardRouteVersionNeedUpdate, err)
	require.Equal(t, 0, len(ret.Blobs))

	// list one shard, 3 blob
	shardInfo.EXPECT().GetMember(gAny, gAny).Return(info)
	shardMgr.EXPECT().GetShardByID(gAny, gAny).Return(shardInfo, nil)
	shardMgr.EXPECT().GetSpaceID().Return(proto.SpaceID(1))
	svrCtrl.EXPECT().GetShardnodeHost(gAny, gAny).Return(&controller.HostIDC{Host: "host"}, nil)
	clu.EXPECT().GetShardController(gAny).Return(shardMgr, nil)
	clu.EXPECT().GetServiceController(gAny).Return(svrCtrl, nil)
	listRet := shardnode.ListBlobRet{
		Blobs: []proto.Blob{
			{Name: []byte("test-blob-1")},
			{Name: []byte("test-blob-2")},
			{Name: []byte("test-blob-3")},
		},
		NextMarker: nil,
	}
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().ListBlob(gAny, gAny, gAny).Return(listRet, nil)

	ret, err = h.ListBlob(ctx, &args)
	require.NoError(t, err)
	require.Equal(t, []byte(nil), ret.NextMarker)
	require.Equal(t, 3, len(ret.Blobs))

	// list all
	shards := make([]controller.Shard, 4)
	ranges := sharding.InitShardingRange(sharding.RangeType_RangeTypeHash, 1, 3)
	for i := range shards {
		shards[i] = NewMockShard(ctr)
		shards[i].(*MockShard).EXPECT().GetShardID().Return(proto.ShardID(i + 1)).AnyTimes()
		shards[i].(*MockShard).EXPECT().GetRange().Return(*ranges[i]).AnyTimes()
		shards[i].(*MockShard).EXPECT().GetMember(gAny, gAny).Return(info).AnyTimes()
	}

	shardMgr.EXPECT().GetFisrtShard(gAny).Return(shards[0], nil).Times(1)
	shardMgr.EXPECT().GetNextShard(gAny, gAny).Return(shards[1], nil)
	shardMgr.EXPECT().GetNextShard(gAny, gAny).Return(shards[2], nil)
	shardMgr.EXPECT().GetSpaceID().Return(proto.SpaceID(1)).Times(2)

	svrCtrl.EXPECT().GetShardnodeHost(gAny, gAny).Return(&controller.HostIDC{Host: "host"}, nil).Times(2)
	h.clusterController.(*MockClusterController).EXPECT().GetShardController(gAny).Return(shardMgr, nil).Times(1)
	h.clusterController.(*MockClusterController).EXPECT().GetServiceController(gAny).Return(svrCtrl, nil).Times(2)
	listRet = shardnode.ListBlobRet{
		Blobs: []proto.Blob{
			{Name: []byte("test-blob-1")},
			{Name: []byte("test-blob-2")},
		},
		NextMarker: nil,
	}
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().ListBlob(gAny, gAny, gAny).Return(listRet, nil).Times(2)

	args.ShardID = 0
	args.Marker = nil
	args.Count = 4
	ret, err = h.ListBlob(ctx, &args)
	expectMarker := acapi.ListBlobEncodeMarker{
		Range:  *ranges[2],
		Marker: args.Marker,
	}
	require.NoError(t, err)
	require.Equal(t, 4, len(ret.Blobs))

	actual := acapi.ListBlobEncodeMarker{}
	err = actual.Unmarshal(ret.NextMarker)
	require.NoError(t, err)
	require.Equal(t, expectMarker, actual) // string(ret.NextMarker))

	// list all, from next shard 3
	args.ShardID = 0
	args.Count = 2
	args.Marker = ret.NextMarker

	shardMgr.EXPECT().GetSpaceID().Return(proto.SpaceID(1))
	shardMgr.EXPECT().GetShardByRange(gAny, expectMarker.Range).Return(shards[2], nil)
	shardMgr.EXPECT().GetNextShard(gAny, gAny).Return(nil, errMock)
	svrCtrl.EXPECT().GetShardnodeHost(gAny, gAny).Return(&controller.HostIDC{Host: "host"}, nil)
	h.clusterController.(*MockClusterController).EXPECT().GetShardController(gAny).Return(shardMgr, nil).Times(1)
	h.clusterController.(*MockClusterController).EXPECT().GetServiceController(gAny).Return(svrCtrl, nil)
	listRet.NextMarker = nil
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().ListBlob(gAny, gAny, gAny).Return(listRet, nil)

	ret, err = h.ListBlob(ctx, &args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)
	require.Equal(t, 0, len(ret.Blobs))

	// list all, access until last shard not enough count
	lastEndMarker := acapi.ListBlobEncodeMarker{
		Range:  *ranges[2], // total count 4
		Marker: args.Marker,
	}
	lastEnd, err := lastEndMarker.Marshal()
	require.NoError(t, err)
	args.ShardID = 0
	args.Count = 100
	args.Marker = lastEnd

	h.clusterController.(*MockClusterController).EXPECT().GetShardController(gAny).Return(shardMgr, nil)
	shardMgr.EXPECT().GetShardByRange(gAny, lastEndMarker.Range).Return(shards[2], nil)
	shardMgr.EXPECT().GetSpaceID().Return(proto.SpaceID(1)).Times(2)
	shardMgr.EXPECT().GetNextShard(gAny, gAny).Return(shards[3], nil)
	shardMgr.EXPECT().GetNextShard(gAny, gAny).Return(nil, nil)

	svrCtrl.EXPECT().GetShardnodeHost(gAny, gAny).Return(&controller.HostIDC{Host: "host"}, nil).Times(2)
	h.clusterController.(*MockClusterController).EXPECT().GetServiceController(gAny).Return(svrCtrl, nil).Times(2)
	listRet.NextMarker = nil
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().ListBlob(gAny, gAny, gAny).Return(listRet, nil).Times(2)

	ret, err = h.ListBlob(ctx, &args)
	require.NoError(t, err)
	require.Equal(t, 4, len(ret.Blobs))
	// endOneMarker := acapi.ListBlobEncodeMarker{
	//	Range:  sharding.Range{}, // *ranges[3],
	//	Marker: args.Marker,
	// }
	// endOne, err := endOneMarker.Marshal()
	// require.NoError(t, err)
	require.Equal(t, []byte(nil), ret.NextMarker)
}

func TestStreamBlobAlloc(t *testing.T) {
	ctx := context.Background()
	gAny := gomock.Any()
	h := newStreamHandlerSuccess(t)

	args := acapi.AllocSliceArgs{
		BlobName:  []byte("blob-seal"),
		ShardKeys: nil,
		ClusterID: 1,
		CodeMode:  1,
		Size:      1,
		FailSlice: proto.Slice{},
	}

	ret := shardnode.AllocSliceRet{
		Slices: make([]proto.Slice, 1),
	}
	ret.Slices[0].Vid = 1
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().AllocSlice(gAny, gAny, gAny).Return(ret, errcode.ErrShardRouteVersionNeedUpdate)
	h.shardnodeClient.(*mocks.MockShardnodeAccess).EXPECT().AllocSlice(gAny, gAny, gAny).Return(ret, nil)

	loc, err := h.AllocSlice(ctx, &args)
	require.NoError(t, err)
	require.NotNil(t, loc)
	require.Equal(t, 1, len(loc.Slices))
	require.Equal(t, proto.Vid(1), loc.Slices[0].Vid)
}

func TestStreamBlobOther(t *testing.T) {
	ctx := context.Background()
	gAny := gomock.Any()
	ctr := gomock.NewController(t)

	svrCtrl := NewMockServiceController(ctr)
	svrCtrl.EXPECT().PunishShardnode(gAny, gAny, gAny).Times(2)
	svrCtrl.EXPECT().GetShardnodeHost(gAny, gAny).Return(&controller.HostIDC{Host: "host"}, nil).Times(1)

	shardMgr := NewMockShardController(ctr)
	shardMgr.EXPECT().UpdateRoute(gAny).Return(nil).Times(2)
	shardMgr.EXPECT().UpdateShard(gAny, gAny).Return(nil)

	clu := NewMockClusterController(ctr)
	clu.EXPECT().GetServiceController(gAny).Return(svrCtrl, nil).Times(3)
	clu.EXPECT().GetShardController(gAny).Return(shardMgr, nil).Times(4)

	h := &Handler{
		clusterController: clu,
	}

	interrupt := h.punishAndUpdate(ctx, &punishArgs{
		ShardOpHeader: shardnode.ShardOpHeader{},
		clusterID:     0,
		host:          "",
		err:           errcode.ErrDiskBroken,
	})
	require.Equal(t, true, interrupt)

	interrupt = h.punishAndUpdate(ctx, &punishArgs{
		err: errcode.ErrShardNodeDiskNotFound,
	})
	require.Equal(t, false, interrupt)

	interrupt = h.punishAndUpdate(ctx, &punishArgs{
		err: errcode.ErrShardDoesNotExist,
	})
	require.Equal(t, false, interrupt)

	shardnodeClient := mocks.NewMockShardnodeAccess(ctr)
	shardnodeClient.EXPECT().GetShardStats(gAny, gAny, gAny).Return(shardnode.ShardStats{LeaderDiskID: 11}, nil).Times(2)
	h.shardnodeClient = shardnodeClient
	h.ShardnodeRetryTimes = defaultShardnodeRetryTimes
	interrupt = h.punishAndUpdate(ctx, &punishArgs{
		err: errcode.ErrShardNodeNotLeader,
	})
	require.Equal(t, false, interrupt)
}

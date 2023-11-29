package blobnode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func TestDataInspect(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	ds1 := NewMockDiskAPI(ctr)
	ds2 := NewMockDiskAPI(ctr)
	svr := &Service{
		Disks:   map[proto.DiskID]core.DiskAPI{11: ds1, 22: ds2},
		ctx:     context.Background(),
		closeCh: make(chan struct{}),
	}
	cfg := DataInspectConf{IntervalSec: 100, RateLimit: 2}

	clusterMgrCli := mocks.NewMockClientAPI(ctr)
	clusterMgrCli.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)
	switchMgr := taskswitch.NewSwitchMgr(clusterMgrCli)
	mgr, err := NewDataInspectMgr(svr, cfg, switchMgr)
	svr.inspectMgr = mgr
	require.NoError(t, err)
	require.Equal(t, cfg.IntervalSec, mgr.conf.IntervalSec)

	{
		ds1.EXPECT().ID().Times(2).Return(proto.DiskID(11))
		ds1.EXPECT().ListChunks(any).Return(nil, errMock)
		ds1.EXPECT().Status().Return(proto.DiskStatusNormal)
		ds2.EXPECT().ID().Times(2).Return(proto.DiskID(22))
		ds2.EXPECT().ListChunks(any).Return(nil, errMock)
		ds2.EXPECT().Status().Return(proto.DiskStatusNormal)
		// close(svr.closeCh)
		mgr.inspectAllDisks(ctx)

		flag := mgr.getSwitch()
		require.False(t, flag)
	}

	{
		var wg sync.WaitGroup
		wg.Add(1)

		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Times(2).Return(proto.Vuid(1001))
		cs.EXPECT().ID().Times(2).Return(bnapi.ChunkId{})
		cs.EXPECT().Disk().Return(ds1)
		cs.EXPECT().Read(any, any).Return(int64(0), nil)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 1}}, proto.BlobID(123456), nil)
		ds1.EXPECT().ID().Times(1).Return(proto.DiskID(11))
		ds1.EXPECT().ListChunks(any).Return([]core.VuidMeta{{Vuid: proto.Vuid(1001)}}, nil)
		ds1.EXPECT().GetChunkStorage(any).Return(cs, true)

		mgr.inspectDisk(ctx, ds1, &wg)
	}

	{
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Times(2).Return(proto.Vuid(1001))
		cs.EXPECT().ID().Times(2).Return(bnapi.ChunkId{})
		cs.EXPECT().Disk().Return(ds1)
		ds1.EXPECT().ID().Times(1).Return(proto.DiskID(11))

		batchShards := make([]*bnapi.ShardInfo, 2*speedUpCnt)
		for i := range batchShards {
			batchShards[i] = &bnapi.ShardInfo{Bid: proto.BlobID(i + 1), Size: 8}
		}
		cs.EXPECT().ListShards(any, any, any, any).Return(batchShards, proto.InValidBlobID, nil)
		cs.EXPECT().Read(any, any).Return(int64(8), nil).Times(2 * speedUpCnt)

		mgr.setAllDiskRateForce(2 * minRateLimit)
		mgr.conf.RateLimit = 2 * minRateLimit * 2 * 2
		_, err = mgr.inspectChunk(ctx, cs)
		require.NoError(t, err)
		require.Equal(t, mgr.conf.RateLimit, int(mgr.limits[proto.DiskID(11)].Limit()))
	}

	{
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Times(2).Return(bnapi.ChunkId{})
		cs.EXPECT().Disk().Return(ds1)
		cs.EXPECT().Read(any, any).Return(int64(0), nil)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Times(1).Return(proto.DiskID(11))

		close(mgr.svr.closeCh)
		mgr.limits[proto.DiskID(11)].SetLimit(2)
		mgr.limits[proto.DiskID(11)].SetBurst(4)
		_, err = mgr.inspectChunk(ctx, cs)
		require.NotNil(t, err)
		require.Equal(t, "context canceled", err.Error())
	}

	{
		rc := &rpc.Context{Request: &http.Request{}, Writer: &httptest.ResponseRecorder{}}
		mgr.svr.GetInspectStat(rc)
		require.Equal(t, cfg.IntervalSec, mgr.conf.IntervalSec)
	}
}

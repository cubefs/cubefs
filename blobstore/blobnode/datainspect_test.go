package blobnode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
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

	getter := mocks.NewMockAccessor(ctr)
	getter.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)
	getter.EXPECT().SetConfig(any, any, any).AnyTimes().Return(nil)
	switchMgr := taskswitch.NewSwitchMgr(getter)
	mgr, err := NewDataInspectMgr(svr, cfg, switchMgr)
	svr.inspectMgr = mgr
	require.NoError(t, err)
	require.Equal(t, cfg.IntervalSec, mgr.conf.IntervalSec)

	ds1.EXPECT().IsWritable().AnyTimes().Return(true)
	ds2.EXPECT().IsWritable().AnyTimes().Return(true)

	{
		ds1.EXPECT().ID().Times(2).Return(proto.DiskID(11))
		ds1.EXPECT().ListChunks(any).Return(nil, errMock)
		ds2.EXPECT().ID().Times(2).Return(proto.DiskID(22))
		ds2.EXPECT().ListChunks(any).Return(nil, errMock)
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
		cs.EXPECT().ID().Times(2).Return(clustermgr.ChunkID{})
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
		cs.EXPECT().ID().Times(2).Return(clustermgr.ChunkID{})
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
		// inspect single chunk, cancel parent ctx
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1)
		cs.EXPECT().Read(any, any).Return(int64(0), nil).Times(0)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		pCtx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = mgr.inspectChunk(pCtx, cs)
		require.NotNil(t, err)
		require.ErrorIs(t, err, context.Canceled)
	}

	{
		// inspect single chunk, closed ctx
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1)
		cs.EXPECT().Read(any, any).Return(int64(0), nil).Times(0)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		close(mgr.svr.closeCh)
		mgr.limits[proto.DiskID(11)].SetLimit(2)
		mgr.limits[proto.DiskID(11)].SetBurst(4)
		_, err = mgr.inspectChunk(ctx, cs)
		require.NotNil(t, err)
		require.ErrorIs(t, err, errServiceClosed)
	}

	{
		rc := &rpc.Context{Request: &http.Request{}, Writer: &httptest.ResponseRecorder{}}
		mgr.svr.GetInspectStat(rc)
		require.Equal(t, cfg.IntervalSec, mgr.conf.IntervalSec)
	}
}

func TestInspectChunk_NoGoroutineLeak(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	// build service and manager
	ds := NewMockDiskAPI(ctr)
	svr := &Service{
		Disks:   map[proto.DiskID]core.DiskAPI{11: ds},
		ctx:     context.Background(),
		closeCh: make(chan struct{}),
	}
	getter := mocks.NewMockAccessor(ctr)
	getter.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)
	getter.EXPECT().SetConfig(any, any, any).AnyTimes().Return(nil)
	switchMgr := taskswitch.NewSwitchMgr(getter)
	mgr, err := NewDataInspectMgr(svr, DataInspectConf{IntervalSec: 1, RateLimit: 1024 * 1024}, switchMgr)
	require.NoError(t, err)
	mgr.svr = svr
	svr.inspectMgr = mgr

	// limiter entry (avoid nil access if shards present)
	ds.EXPECT().ID().AnyTimes().Return(proto.DiskID(11))
	mgr.setLimiters([]core.DiskAPI{ds})

	// chunk mock: empty shard list so inspectChunk returns quickly
	cs := NewMockChunkAPI(ctr)
	cs.EXPECT().Vuid().AnyTimes().Return(proto.Vuid(1001))
	cs.EXPECT().ID().AnyTimes().Return(clustermgr.ChunkID{})
	cs.EXPECT().Disk().AnyTimes().Return(ds)
	cs.EXPECT().ListShards(any, any, any, any).AnyTimes().Return([]*bnapi.ShardInfo{}, proto.InValidBlobID, nil)

	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		_, err = mgr.inspectChunk(ctx, cs)
		require.NoError(t, err)
	}
	// allow scheduler to settle
	// (if a leak existed via a background goroutine, goroutine count would keep growing)
	// small sleep to stabilize
	// not too long to avoid slowing CI
	// 50 iterations are enough to detect growth

	after := runtime.NumGoroutine()
	// tolerate a small delta for unrelated goroutines
	require.LessOrEqual(t, after, before+5)
}

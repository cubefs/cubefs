package blobnode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/util/errors"
)

func newDataInspectMgr(t *testing.T, conf DataInspectConf, svr *Service) *DataInspectMgr {
	ctr := gomock.NewController(t)

	getter := mocks.NewMockAccessor(ctr)
	getter.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)
	getter.EXPECT().SetConfig(any, any, any).AnyTimes().Return(nil)
	switchMgr := taskswitch.NewSwitchMgr(getter)

	mgr, err := NewDataInspectMgr(svr, conf, switchMgr)
	require.NoError(t, err)
	require.NotNil(t, mgr)

	// mocker inspect record
	recorder := mocks.NewMockRecordLogEncoder(ctr)
	mgr.recorder = recorder

	return mgr
}

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

	var err error
	var bads []bnapi.BadShard
	cfg := DataInspectConf{IntervalSec: 100, RateLimit: 2}

	// empty config Record log
	getter := mocks.NewMockAccessor(ctr)
	getter.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)
	mgr, err := NewDataInspectMgr(svr, cfg, taskswitch.NewSwitchMgr(getter))
	require.NoError(t, err)
	require.NotNil(t, mgr)

	mgr = newDataInspectMgr(t, cfg, svr)
	svr.inspectMgr = mgr
	require.Equal(t, cfg.IntervalSec, mgr.conf.IntervalSec)

	ds1.EXPECT().IsWritable().AnyTimes().Return(true)
	ds2.EXPECT().IsWritable().AnyTimes().Return(true)

	{
		// inspect all disks
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
		ds1.EXPECT().ListChunks(any).Return(nil, errMock)
		ds2.EXPECT().ID().Return(proto.DiskID(22)).AnyTimes()
		ds2.EXPECT().ListChunks(any).Return(nil, errMock)
		ds1.EXPECT().DiskInfo().Times(1)
		ds2.EXPECT().DiskInfo().Times(1)
		mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(any).Times(1)
		mgr.inspectAllDisks(ctx)

		flag := mgr.getSwitch()
		require.False(t, flag)
	}

	{
		// inspect single disk
		var wg sync.WaitGroup
		wg.Add(1)

		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1).Times(2)
		cs.EXPECT().Read(any, any).Return(int64(0), nil)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 1}}, proto.BlobID(123456), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
		ds1.EXPECT().ListChunks(any).Return([]core.VuidMeta{{Vuid: proto.Vuid(1001)}}, nil)
		ds1.EXPECT().GetChunkStorage(any).Return(cs, true)
		ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{})

		mgr.inspectDisk(ds1, &wg)
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
		mgr.limits[proto.DiskID(11)].SetLimit(4)
		mgr.limits[proto.DiskID(11)].SetBurst(6)
		bads, err = mgr.inspectChunk(ctx, cs)
		require.NotNil(t, err)
		require.ErrorIs(t, err, errServiceClosed)
		require.Equal(t, 0, len(bads))
	}

	{
		rc := &rpc.Context{Request: &http.Request{}, Writer: &httptest.ResponseRecorder{}}
		mgr.svr.GetInspectStat(rc)
		require.Equal(t, cfg.IntervalSec, mgr.conf.IntervalSec)
	}

	{
		// inspect find error, report metric
		mgr.limits[proto.DiskID(11)].SetLimit(100)
		mgr.limits[proto.DiskID(11)].SetBurst(200)
		mgr.svr.closeCh = make(chan struct{})
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1)
		cs.EXPECT().Read(any, any).Return(int64(0), errMock)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		// bad bid report metric
		cs.EXPECT().Disk().Return(ds1).Times(1 + 1)
		// cs.EXPECT().Vuid().Return(proto.Vuid(1001))
		ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{}).Times(1 + 1)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
		mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(any).Times(1)

		bads, err = mgr.inspectChunk(ctx, cs)
		require.NoError(t, err)
		require.Equal(t, 1, len(bads))
	}

	{
		// inspect already delete shard, file does not exist
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1).Times(1)
		cs.EXPECT().Read(any, any).Return(int64(0), os.ErrNotExist)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		bads, err = mgr.inspectChunk(ctx, cs)
		require.NoError(t, err)
		require.Equal(t, 1, len(bads))

		// no such bid
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1).Times(1)
		cs.EXPECT().Read(any, any).Return(int64(0), bloberr.ErrNoSuchBid)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		bads, err = mgr.inspectChunk(ctx, cs)
		require.NoError(t, err)
		require.Equal(t, 1, len(bads))
	}

	{
		// scanShards EIO
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1).Times(3)
		cs.EXPECT().Read(any, any).Return(int64(0), syscall.EIO)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		// bad bid report metric
		ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{}).Times(1 + 1)
		mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(any).Times(1)

		bads, err = mgr.inspectChunk(ctx, cs)
		require.ErrorIs(t, syscall.EIO, err)
		require.Equal(t, 1, len(bads))
	}

	close(svr.closeCh)
	mgr.conf.IntervalSec = 5
	mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Close().Times(1)
	mgr.loopDataInspect()
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

func TestDataInspectMetric(t *testing.T) {
	ctx := context.Background()
	ctr := gomock.NewController(t)
	ds1 := NewMockDiskAPI(ctr)
	svr := &Service{
		Disks:   map[proto.DiskID]core.DiskAPI{11: ds1},
		ctx:     context.Background(),
		closeCh: make(chan struct{}),
	}

	cfg := DataInspectConf{IntervalSec: 100, RateLimit: 2}
	mgr := newDataInspectMgr(t, cfg, svr)
	svr.inspectMgr = mgr
	defer close(svr.closeCh)

	// no bad blob
	const total = 10
	cs := NewMockChunkAPI(ctr)
	bads := make([]bnapi.BadShard, total)
	for i := range bads {
		bads[i] = bnapi.BadShard{
			DiskID: 11,
			Vuid:   proto.Vuid(1001),
			Bid:    proto.BlobID(i + 1),
			Err:    os.ErrNotExist,
		}
	}

	badBidCnt := mgr.reportBatchBadShards(ctx, cs, bads)
	require.Equal(t, 0, badBidCnt)

	// some bad blob
	cs = NewMockChunkAPI(ctr)
	bads = make([]bnapi.BadShard, total)
	err1 := errors.New("fake mock error 111")
	err2 := errors.New("fake mock error 222")
	err3 := os.ErrNotExist

	expectCnt := 0
	for i := range bads {
		bads[i] = bnapi.BadShard{
			DiskID: 11,
			Vuid:   proto.Vuid(1001),
			Bid:    proto.BlobID(i + 1),
		}
		if i%3 == 0 {
			bads[i].Err = err1
			expectCnt++
		} else if i%3 == 1 {
			bads[i].Err = err2
			expectCnt++
		} else {
			bads[i].Err = err3
		}
	}

	ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{
		DiskInfo: clustermgr.DiskInfo{
			ClusterID: 1,
			Idc:       "idc",
			Rack:      "rack",
			Host:      "host",
			Path:      "",
		},
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: 11},
	}).Times(1 + 2)
	cs.EXPECT().Disk().Return(ds1).Times(1 + 2)
	cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
	mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(any).Times(2)

	badBidCnt = mgr.reportBatchBadShards(ctx, cs, bads)
	require.Equal(t, expectCnt, badBidCnt)

	// one shard bad
	badBid := proto.BlobID(1234)
	ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{
		DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: 11},
	}).Times(2)
	cs.EXPECT().Disk().Return(ds1).Times(2)
	cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
	mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(any)

	mgr.reportBadShard(ctx, cs, badBid, errMock)
}

func TestDataInspectRecord(t *testing.T) {
	ctx := context.Background()
	ctr := gomock.NewController(t)
	ds1 := NewMockDiskAPI(ctr)
	svr := &Service{
		Disks:   map[proto.DiskID]core.DiskAPI{11: ds1},
		ctx:     context.Background(),
		closeCh: make(chan struct{}),
	}
	cfg := DataInspectConf{IntervalSec: 100, RateLimit: 2}

	mgr := newDataInspectMgr(t, cfg, svr)
	require.Equal(t, uint64(0), mgr.round)

	mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(gomock.Any()).DoAndReturn(func(record interface{}) error {
		roundRec, ok := record.(roundRecord)
		require.True(t, ok, "record should be roundRecord type")
		require.Equal(t, uint64(0), roundRec.Round)
		require.Greater(t, roundRec.Timestamp, int64(0))
		return nil
	})
	mgr.recordInspectStartPoint(ctx)

	{
		// test inspectAllDisks round++
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
		ds1.EXPECT().IsWritable().Return(true).AnyTimes()
		ds1.EXPECT().ListChunks(any).Return([]core.VuidMeta{}, errMock)
		ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{})

		mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(gomock.Any()).DoAndReturn(func(record interface{}) error {
			roundRec, ok := record.(roundRecord)
			require.True(t, ok)
			require.Equal(t, uint64(0), roundRec.Round)
			return nil
		})
		mgr.inspectAllDisks(ctx)
		require.Equal(t, uint64(1), mgr.round)

		// next run inspectAllDisks, check round++
		ds1.EXPECT().ListChunks(any).Return([]core.VuidMeta{}, errMock)
		ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{})
		mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(gomock.Any()).DoAndReturn(func(record interface{}) error {
			roundRec, ok := record.(roundRecord)
			require.True(t, ok)
			require.Equal(t, uint64(1), roundRec.Round)
			return nil
		})
		mgr.inspectAllDisks(ctx)
		require.Equal(t, uint64(2), mgr.round)
	}

	{
		// test record log
		workDir, err := os.MkdirTemp(os.TempDir(), "TestDataInspect")
		require.NoError(t, err)
		defer os.RemoveAll(workDir)

		recordDir := filepath.Join(workDir, "inspect_dir")
		rl, err := recordlog.NewEncoder(&recordlog.Config{Dir: recordDir})
		require.NoError(t, err)
		mgr.recorder = rl

		mgr.recordInspectStartPoint(ctx)
		mgr.round++
		mgr.recordInspectStartPoint(ctx)

		// check file
		files, err := os.ReadDir(recordDir)
		require.NoError(t, err)
		require.Greater(t, len(files), 0, "should have at least one file in record directory")

		var latestFile os.DirEntry
		for _, file := range files {
			if !file.IsDir() {
				latestFile = file
				break
			}
		}
		require.NotNil(t, latestFile, "should find at least one record file")
		fileInfo, err := latestFile.Info()
		require.NoError(t, err)
		require.Greater(t, fileInfo.Size(), int64(0), "record file should have content written")

		// check file content
		filePath := filepath.Join(recordDir, latestFile.Name())
		content, err := os.ReadFile(filePath)
		require.NoError(t, err)
		require.Greater(t, len(content), 0, "file content should not be empty")

		contentStr := string(content)
		require.Contains(t, contentStr, "round", "file content should contain round field")
		require.Contains(t, contentStr, "timestamp", "file content should contain timestamp field")
	}
}

func TestDataInspectMgr_ConcurrentAccess(t *testing.T) {
	const diskCnt = 60
	const concurrentCnt = 1000
	mgr := &DataInspectMgr{}

	diskIDs := make([]proto.DiskID, diskCnt)
	for i := 0; i < diskCnt; i++ {
		diskIDs[i] = proto.DiskID(i + 1)
		// mgr.progress[diskIDs[i]] = 0
		mgr.progress.Store(diskIDs[i], 0)
	}

	var writeWg sync.WaitGroup
	for _, diskID := range diskIDs {
		writeWg.Add(1)
		go func(did proto.DiskID) {
			defer writeWg.Done()
			for i := 0; i < concurrentCnt; i++ {
				// mgr.progress[did] = i % 100
				mgr.progress.Store(did, i%100)
			}
		}(diskID)
	}

	var readWg sync.WaitGroup
	for i := 0; i < 2; i++ {
		readWg.Add(1)
		go func() {
			defer readWg.Done()
			for j := 0; j < concurrentCnt; j++ {
				progressCopy := make(map[proto.DiskID]int)
				// for k, v := range mgr.progress {
				//	progressCopy[k] = v
				// }
				mgr.progress.Range(func(k, v interface{}) bool {
					progressCopy[k.(proto.DiskID)] = v.(int)
					return true
				})
				_ = len(progressCopy)
			}
		}()
	}

	// wait all done, and no panic(concurrent write map, concurrent iteration map)
	writeWg.Wait()
	readWg.Wait()
}

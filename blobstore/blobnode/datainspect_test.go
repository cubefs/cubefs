package blobnode

import (
	"context"
	"hash/crc32"
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
	"golang.org/x/time/rate"

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
		// inspect single disk: BatchRead succeeds, no CRC mismatch → 0 bad shards
		// cs.Disk() called twice: once in inspectDisk (IsWritable check), once in inspectChunk
		var wg sync.WaitGroup
		wg.Add(1)

		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1).Times(2)
		ds1.EXPECT().GetConfig().Return(&core.Config{RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1024 * 1024}}).Times(1)
		cs.EXPECT().BatchRead(any, any).Return(int64(0), nil)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 1}}, proto.BlobID(123456), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
		ds1.EXPECT().ListChunks(any).Return([]core.VuidMeta{{Vuid: proto.Vuid(1001)}}, nil)
		ds1.EXPECT().GetChunkStorage(any).Return(cs, true)
		ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{})

		mgr.inspectDisk(ds1, &wg)
	}

	{
		// inspect single chunk, cancel parent ctx: context already cancelled before batch starts
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1)
		cs.EXPECT().BatchRead(any, any).Times(0)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		pCtx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = mgr.inspectChunk(pCtx, cs)
		require.NotNil(t, err)
		require.ErrorIs(t, err, context.Canceled)
	}

	{
		// inspect single chunk, closed ctx: closeCh fires in batch loop before BatchRead
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1)
		cs.EXPECT().BatchRead(any, any).Times(0)
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
		// inspect find error, report metric:
		// BatchRead returns non-EIO error → fallback per-shard → Read fails → bad shard
		mgr.limits[proto.DiskID(11)].SetLimit(100)
		mgr.limits[proto.DiskID(11)].SetBurst(200)
		mgr.svr.closeCh = make(chan struct{})
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1)
		ds1.EXPECT().GetConfig().Return(&core.Config{RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1024 * 1024}}).Times(1)
		cs.EXPECT().BatchRead(any, any).Return(int64(0), errMock)
		cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 1}, nil)
		cs.EXPECT().Read(any, any).Return(int64(0), errMock)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		// bad bid report metric (reportBatchBadShards: cs.Disk() × 2, DiskInfo × 2)
		cs.EXPECT().Disk().Return(ds1).Times(1 + 1)
		ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{}).Times(1 + 1)
		mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(any).Times(1)

		bads, err = mgr.inspectChunk(ctx, cs)
		require.NoError(t, err)
		require.Equal(t, 1, len(bads))
	}

	{
		// inspect already delete shard: BatchRead returns non-EIO → fallback → Read returns
		// IsShardDeleted error → skip, 0 bad shards
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1).Times(1)
		ds1.EXPECT().GetConfig().Return(&core.Config{RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1024 * 1024}}).Times(1)
		cs.EXPECT().BatchRead(any, any).Return(int64(0), errMock)
		cs.EXPECT().Read(any, any).Return(int64(0), os.ErrNotExist)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		bads, err = mgr.inspectChunk(ctx, cs)
		require.NoError(t, err)
		require.Equal(t, 0, len(bads))

		// no such bid
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1).Times(1)
		ds1.EXPECT().GetConfig().Return(&core.Config{RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1024 * 1024}}).Times(1)
		cs.EXPECT().BatchRead(any, any).Return(int64(0), errMock)
		cs.EXPECT().Read(any, any).Return(int64(0), bloberr.ErrNoSuchBid)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		bads, err = mgr.inspectChunk(ctx, cs)
		require.NoError(t, err)
		require.Equal(t, 0, len(bads))
	}

	{
		// BatchRead IO error: skip the batch, continue inspection → 0 bad shards, nil error.
		// Behavior change from per-shard path: EIO no longer terminates chunk inspection.
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1).Times(1)
		ds1.EXPECT().GetConfig().Return(&core.Config{RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1024 * 1024}}).Times(1)
		cs.EXPECT().BatchRead(any, any).Return(int64(0), syscall.EIO)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		bads, err = mgr.inspectChunk(ctx, cs)
		require.NoError(t, err)
		require.Equal(t, 0, len(bads))
	}

	{
		// ErrBidNotMatch: the failing shard is re-inspected per-shard; remaining shards (none
		// here) continue as a new batch. failIdx=0 because the mock BatchRead never writes to
		// the crcWriter, so crcWriter.idx stays 0. Read returns errMock → 1 bad shard.
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().Disk().Return(ds1).Times(1)
		ds1.EXPECT().GetConfig().Return(&core.Config{RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1024 * 1024}}).Times(1)
		cs.EXPECT().BatchRead(any, any).Return(int64(0), bloberr.ErrBidNotMatch)
		cs.EXPECT().Read(any, any).Return(int64(0), errMock)
		cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 1}, nil)
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{{Bid: 123456, Size: 8}}, proto.BlobID(123456+1), nil)
		ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

		// reportBatchBadShards calls cs.Disk() twice (DiskInfo for metric + record log)
		cs.EXPECT().Disk().Return(ds1).Times(2)
		ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{}).Times(2)
		mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(any).Times(1)

		bads, err = mgr.inspectChunk(ctx, cs)
		require.NoError(t, err)
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
	mgr, err := NewDataInspectMgr(svr, DataInspectConf{IntervalSec: 10, RateLimit: 1024 * 1024}, switchMgr)
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

	const testSomeGoroutines = 50
	for i := 0; i < testSomeGoroutines; i++ {
		_, err = mgr.inspectChunk(ctx, cs)
		require.NoError(t, err)
	}

	// allow scheduler to settle
	// (if a leak existed via a background goroutine, goroutine count would keep growing)
	// small sleep to stabilize, not too long to avoid slowing CI, 50 iterations are enough to detect growth
	after := runtime.NumGoroutine()
	// tolerate a small delta for unrelated goroutines
	const tolerance = 5
	require.LessOrEqual(t, after, before+tolerance)
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

	// reportBadShard with an ignored error (shard deleted) → early return, no metric reported
	mgr.reportBadShard(ctx, cs, badBid, os.ErrNotExist)
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

	{
		// recordBadBids: Encode returns error → only logs, no panic
		mgr2 := newDataInspectMgr(t, cfg, svr)
		recorder2 := mgr2.recorder.(*mocks.MockRecordLogEncoder)
		recorder2.EXPECT().Encode(any).Return(errMock)
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Disk().Return(ds1)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{})
		mgr2.recordBadBids(ctx, cs, []string{"1", "2"}, "some error")
	}

	{
		// recordInspectStartPoint: Encode returns error → only logs, no panic
		mgr3 := newDataInspectMgr(t, cfg, svr)
		recorder3 := mgr3.recorder.(*mocks.MockRecordLogEncoder)
		recorder3.EXPECT().Encode(any).Return(errMock)
		mgr3.recordInspectStartPoint(ctx)
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

func TestInspectShard_MetaDoubleCheck(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	errMismatch := errors.New("crc32block: mismatched checksum")

	// service and manager
	ds := NewMockDiskAPI(ctr)
	svr := &Service{Disks: map[proto.DiskID]core.DiskAPI{11: ds}, ctx: context.Background(), closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 1, RateLimit: 128 * 1024}, svr)

	ds.EXPECT().ID().AnyTimes().Return(proto.DiskID(11))

	// base shard info
	si := &bnapi.ShardInfo{Bid: proto.BlobID(1), Vuid: proto.Vuid(1001), Size: 8}

	// case 1: ReadShardMeta returns os.ErrNotExist -> skip bid error
	cs := NewMockChunkAPI(ctr)
	cs.EXPECT().Disk().Return(ds).AnyTimes()
	cs.EXPECT().ReadShardMeta(any, any).Return(nil, os.ErrNotExist)
	cs.EXPECT().Read(any, any).Return(int64(1), errMismatch)
	cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
	{
		err := mgr.inspectShard(ctx, cs, si)
		require.NoError(t, err)
	}

	// case 2: ReadShardMeta returns ErrNoSuchBid -> skip bid error
	cs.EXPECT().ReadShardMeta(any, any).Return(nil, bloberr.ErrNoSuchBid)
	cs.EXPECT().Read(any, any).Return(int64(1), errMismatch)
	{
		err := mgr.inspectShard(ctx, cs, si)
		require.NoError(t, err)
	}

	// case 3: ReadShardMeta returns meta with Size==0 -> skip bid error
	cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 0}, nil)
	cs.EXPECT().Read(any, any).Return(int64(1), errMismatch)
	{
		err := mgr.inspectShard(ctx, cs, si)
		require.NoError(t, err)
	}

	// case 4: ReadShardMeta returns errMock
	cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 1}, errMock)
	cs.EXPECT().Read(any, any).Return(int64(1), errMismatch)
	{
		err := mgr.inspectShard(ctx, cs, si)
		require.NotNil(t, err)
		require.ErrorIs(t, err, errMismatch)
	}

	// case 5: normal, read shard meta and data, all success
	cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 1}, nil).Times(0)
	cs.EXPECT().Read(any, any).Return(int64(1), nil)
	{
		err := mgr.inspectShard(ctx, cs, si)
		require.NoError(t, err)
	}

	// case 6: read shard data error, but meta ok.
	cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 1}, nil)
	cs.EXPECT().Read(any, any).Return(int64(1), errMismatch)
	{
		err := mgr.inspectShard(ctx, cs, si)
		require.NotNil(t, err)
		require.ErrorIs(t, err, errMismatch)
	}

	// case 7: read shard data error, it is deleted
	cs.EXPECT().Read(any, any).Return(int64(1), bloberr.ErrNoSuchBid)
	{
		err := mgr.inspectShard(ctx, cs, si)
		require.NoError(t, err)
	}
}

func TestInspect_ClearProgress(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	// service and manager
	ds1 := NewMockDiskAPI(ctr)
	ds2 := NewMockDiskAPI(ctr)
	ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds2.EXPECT().ID().Return(proto.DiskID(22)).AnyTimes()
	svr := &Service{Disks: map[proto.DiskID]core.DiskAPI{11: ds1, 22: ds2}, ctx: ctx, closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 1, RateLimit: 128 * 1024}, svr)
	snapshot := make(map[proto.DiskID]int)

	// first round inspection
	mgr.prepareDiskInspectionState([]core.DiskAPI{ds1, ds2})
	mgr.progress.Store(proto.DiskID(11), 100)
	mgr.progress.Store(proto.DiskID(22), 3)
	mgr.progress.Range(func(k, v interface{}) bool {
		snapshot[k.(proto.DiskID)] = v.(int)
		return true
	})
	require.Equal(t, 2, len(snapshot))
	require.Equal(t, 100, snapshot[11])
	require.Equal(t, 3, snapshot[22])

	// mock replace broken disk, bad disk progress
	mgr.progress.Store(proto.DiskID(33), 9)
	mgr.progress.Range(func(k, v interface{}) bool {
		snapshot[k.(proto.DiskID)] = v.(int)
		return true
	})
	require.Equal(t, 3, len(snapshot))
	require.Equal(t, 100, snapshot[11])
	require.Equal(t, 3, snapshot[22])
	require.Equal(t, 9, snapshot[33])

	// next round inspection
	ds3 := NewMockDiskAPI(ctr)
	ds3.EXPECT().ID().Return(proto.DiskID(33)).AnyTimes()
	mgr.prepareDiskInspectionState([]core.DiskAPI{ds3, ds2})
	snapshot = make(map[proto.DiskID]int)
	mgr.progress.Range(func(k, v interface{}) bool {
		snapshot[k.(proto.DiskID)] = v.(int)
		return true
	})
	require.Equal(t, 2, len(snapshot))
	for diskID := range snapshot {
		require.Equal(t, 0, snapshot[diskID])
	}
	_, ok := snapshot[proto.DiskID(11)]
	require.False(t, ok)
}

// ---------------------------------------------------------------------------
// Unit tests for batchCRCWriter
// ---------------------------------------------------------------------------

func TestBatchCRCWriter(t *testing.T) {
	okHeader := func() []byte {
		var h bnapi.ShardsHeader
		h.Set(http.StatusOK)
		return h[:]
	}
	errHeader := func() []byte {
		var h bnapi.ShardsHeader
		h.Set(http.StatusNotFound)
		return h[:]
	}

	{
		// empty shard list: all writes are silently consumed, idx stays 0
		w := newBatchCRCWriter(nil)
		n, err := w.Write([]byte{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Empty(t, w.badBids)
	}

	{
		// single shard, correct CRC: no bad bids
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		si := &bnapi.ShardInfo{Bid: 100, Size: int64(len(data)), Crc: crc32.ChecksumIEEE(data)}
		w := newBatchCRCWriter([]*bnapi.ShardInfo{si})
		w.Write(okHeader())
		w.Write(data)
		require.Empty(t, w.badBids)
		require.Equal(t, 1, w.idx)
	}

	{
		// single shard, CRC mismatch: bid added to badBids
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		si := &bnapi.ShardInfo{Bid: 200, Size: int64(len(data)), Crc: 0xDEADBEEF}
		w := newBatchCRCWriter([]*bnapi.ShardInfo{si})
		w.Write(okHeader())
		w.Write(data)
		require.Equal(t, []proto.BlobID{200}, w.badBids)
	}

	{
		// non-200 header: idx unchanged (real BatchRead returns ErrBidNotMatch after the
		// header write, so no payload bytes follow; handleBidNotMatch uses idx as failIdx)
		si := &bnapi.ShardInfo{Bid: 300, Size: 8, Crc: 0}
		w := newBatchCRCWriter([]*bnapi.ShardInfo{si})
		w.Write(errHeader())
		require.Empty(t, w.badBids)
		require.Equal(t, 0, w.idx)
	}

	{
		// two shards: first has CRC mismatch, second is correct
		data0 := []byte{0xFF, 0xFE, 0xFD, 0xFC}
		data1 := []byte{0x0A, 0x0B, 0x0C, 0x0D}
		si0 := &bnapi.ShardInfo{Bid: 400, Size: 4, Crc: 0}
		si1 := &bnapi.ShardInfo{Bid: 401, Size: 4, Crc: crc32.ChecksumIEEE(data1)}
		w := newBatchCRCWriter([]*bnapi.ShardInfo{si0, si1})
		w.Write(okHeader())
		w.Write(data0)
		w.Write(okHeader())
		w.Write(data1)
		require.Equal(t, []proto.BlobID{400}, w.badBids)
		require.Equal(t, 2, w.idx)
	}

	{
		// data split across multiple Write calls (simulate chunked CRC-block writes)
		data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
		si := &bnapi.ShardInfo{Bid: 500, Size: 8, Crc: crc32.ChecksumIEEE(data)}
		w := newBatchCRCWriter([]*bnapi.ShardInfo{si})
		w.Write(okHeader())
		w.Write(data[:4])
		w.Write(data[4:])
		require.Empty(t, w.badBids)
	}
}

// ---------------------------------------------------------------------------
// Unit tests for splitIntoBatches
// ---------------------------------------------------------------------------

func TestSplitIntoBatches(t *testing.T) {
	{
		// nil input: returns nil
		require.Empty(t, splitIntoBatches(nil, 100))
	}

	{
		// all shards filtered: Size==0, NopData, Inline
		shards := []*bnapi.ShardInfo{
			{Bid: 1, Size: 0, Offset: 0},
			{Bid: 2, Size: 10, NopData: true, Offset: 100},
			{Bid: 3, Size: 10, Inline: true, Offset: 200},
		}
		require.Empty(t, splitIntoBatches(shards, 1000))
	}

	{
		// partial filter: only the valid shard (Size>0, not NopData, not Inline) passes
		shards := []*bnapi.ShardInfo{
			{Bid: 1, Size: 0, Offset: 0},
			{Bid: 2, Size: 10, NopData: true, Offset: 100},
			{Bid: 3, Size: 10, Inline: true, Offset: 200},
			{Bid: 4, Size: 10, Offset: 300},
		}
		batches := splitIntoBatches(shards, 1000)
		require.Len(t, batches, 1)
		require.Len(t, batches[0], 1)
		require.Equal(t, proto.BlobID(4), batches[0][0].Bid)
	}

	{
		// out-of-order offsets: output must be sorted ascending
		shards := []*bnapi.ShardInfo{
			{Bid: 3, Size: 10, Offset: 300},
			{Bid: 1, Size: 10, Offset: 100},
			{Bid: 2, Size: 10, Offset: 200},
		}
		batches := splitIntoBatches(shards, 10000)
		require.Len(t, batches, 1)
		offsets := make([]int64, len(batches[0]))
		for i, si := range batches[0] {
			offsets[i] = si.Offset
		}
		require.Equal(t, []int64{100, 200, 300}, offsets)
	}

	{
		// size-based splitting: each 60 bytes, maxSize=100 → 3 separate batches
		// logic: len(cur)>0 && curSize+si.Size > maxSize → flush cur
		shards := []*bnapi.ShardInfo{
			{Bid: 1, Size: 60, Offset: 0},
			{Bid: 2, Size: 60, Offset: 100},
			{Bid: 3, Size: 60, Offset: 200},
		}
		batches := splitIntoBatches(shards, 100)
		require.Len(t, batches, 3)
		require.Equal(t, proto.BlobID(1), batches[0][0].Bid)
		require.Equal(t, proto.BlobID(2), batches[1][0].Bid)
		require.Equal(t, proto.BlobID(3), batches[2][0].Bid)
	}

	{
		// two shards fit in one batch, third starts a new batch
		// si[0]=60, si[1]=60 → 120>100 → flush [si0]; si[2]=30 → 60+30=90≤100 → [si1,si2]
		shards := []*bnapi.ShardInfo{
			{Bid: 1, Size: 60, Offset: 0},
			{Bid: 2, Size: 60, Offset: 100},
			{Bid: 3, Size: 30, Offset: 200},
		}
		batches := splitIntoBatches(shards, 100)
		require.Len(t, batches, 2)
		require.Len(t, batches[0], 1)
		require.Equal(t, proto.BlobID(1), batches[0][0].Bid)
		require.Len(t, batches[1], 2)
		require.Equal(t, proto.BlobID(2), batches[1][0].Bid)
		require.Equal(t, proto.BlobID(3), batches[1][1].Bid)
	}
}

// ---------------------------------------------------------------------------
// inspectBatch: CRC mismatch detection (BatchRead succeeds, crcWriter finds bad bids)
// ---------------------------------------------------------------------------

func TestInspectBatch_CRCMismatch(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	ds := NewMockDiskAPI(ctr)
	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().GetConfig().Return(&core.Config{
		RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1024 * 1024},
	}).AnyTimes()

	svr := &Service{closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 1, RateLimit: 1024 * 1024}, svr)
	lmt := rate.NewLimiter(rate.Limit(1024*1024), 2*1024*1024)

	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	correctCRC := crc32.ChecksumIEEE(data)

	writeOneShard := func(bs *core.BatchShard, statusOK bool, payload []byte) {
		var hdr bnapi.ShardsHeader
		if statusOK {
			hdr.Set(http.StatusOK)
		} else {
			hdr.Set(http.StatusNotFound)
		}
		bs.Writer.Write(hdr[:])
		if statusOK {
			bs.Writer.Write(payload)
		}
	}

	{
		// NewBatchShardReader fails (duplicate Offset) → fallback per-shard for all shards.
		// si0: Read OK → healthy; si1: Read fails → bad shard.
		si0 := &bnapi.ShardInfo{Bid: 9001, Vuid: proto.Vuid(1001), Size: 8, Offset: 100}
		si1 := &bnapi.ShardInfo{Bid: 9002, Vuid: proto.Vuid(1001), Size: 8, Offset: 100} // same offset as si0
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().BatchRead(any, any).Times(0) // never reached
		cs.EXPECT().Read(any, any).Return(int64(8), nil)
		cs.EXPECT().Read(any, any).Return(int64(0), errMock)
		cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 1}, nil)

		bads, ioErr := mgr.inspectBatch(ctx, cs, ds, []*bnapi.ShardInfo{si0, si1}, lmt)
		require.NoError(t, ioErr)
		require.Len(t, bads, 1)
		require.Equal(t, proto.BlobID(9002), bads[0].Bid)
	}

	{
		// CRC mismatch → re-inspect via inspectShard → Read fails → bad shard reported
		si := &bnapi.ShardInfo{Bid: 1001, Vuid: proto.Vuid(1001), Size: 8, Offset: 100, Crc: correctCRC + 1}
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().BatchRead(any, any).DoAndReturn(func(_ context.Context, bs *core.BatchShard) (int64, error) {
			writeOneShard(bs, true, data)
			return int64(len(data)), nil
		})
		cs.EXPECT().Read(any, any).Return(int64(0), errMock)
		cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 1}, nil)

		bads, ioErr := mgr.inspectBatch(ctx, cs, ds, []*bnapi.ShardInfo{si}, lmt)
		require.NoError(t, ioErr)
		require.Len(t, bads, 1)
		require.Equal(t, proto.BlobID(1001), bads[0].Bid)
	}

	{
		// CRC mismatch → re-inspect → Read returns shard-deleted error → skip, 0 bad shards
		si := &bnapi.ShardInfo{Bid: 1002, Vuid: proto.Vuid(1001), Size: 8, Offset: 100, Crc: correctCRC + 1}
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().BatchRead(any, any).DoAndReturn(func(_ context.Context, bs *core.BatchShard) (int64, error) {
			writeOneShard(bs, true, data)
			return int64(len(data)), nil
		})
		cs.EXPECT().Read(any, any).Return(int64(0), os.ErrNotExist)

		bads, ioErr := mgr.inspectBatch(ctx, cs, ds, []*bnapi.ShardInfo{si}, lmt)
		require.NoError(t, ioErr)
		require.Empty(t, bads)
	}
}

// ---------------------------------------------------------------------------
// handleBidNotMatch: advanced cases (failIdx > 0, failIdx+1 < len, healthy re-inspect)
// ---------------------------------------------------------------------------

func TestHandleBidNotMatch_Advanced(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	ds := NewMockDiskAPI(ctr)
	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().GetConfig().Return(&core.Config{
		RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1024 * 1024},
	}).AnyTimes()

	svr := &Service{closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 1, RateLimit: 1024 * 1024}, svr)
	lmt := rate.NewLimiter(rate.Limit(1024*1024), 2*1024*1024)

	data0 := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22}
	data2 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	crc0 := crc32.ChecksumIEEE(data0)
	crc2 := crc32.ChecksumIEEE(data2)

	writeShardOK := func(bs *core.BatchShard, payload []byte) {
		var hdr bnapi.ShardsHeader
		hdr.Set(http.StatusOK)
		bs.Writer.Write(hdr[:])
		bs.Writer.Write(payload)
	}
	writeShardErr := func(bs *core.BatchShard) {
		var hdr bnapi.ShardsHeader
		hdr.Set(http.StatusNotFound)
		bs.Writer.Write(hdr[:])
	}

	{
		// failIdx=1: si0 read OK but CRC mismatch, si1 has BidNotMatch header.
		// Expected:
		//   si0 re-inspected → Read fails → bad shard
		//   si1 re-inspected (fallback) → Read succeeds → healthy, not counted
		si0 := &bnapi.ShardInfo{Bid: 2001, Vuid: proto.Vuid(1001), Size: 8, Offset: 0, Crc: 0xDEADBEEF}
		si1 := &bnapi.ShardInfo{Bid: 2002, Vuid: proto.Vuid(1001), Size: 8, Offset: 100, Crc: 0}
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().BatchRead(any, any).DoAndReturn(func(_ context.Context, bs *core.BatchShard) (int64, error) {
			writeShardOK(bs, data0) // si0: CRC mismatch (expected 0xDEADBEEF, actual crc32(data0))
			writeShardErr(bs)       // si1: non-200 → ErrBidNotMatch
			return int64(12), bloberr.ErrBidNotMatch
		})
		// si0 re-inspect: Read fails
		cs.EXPECT().Read(any, any).Return(int64(0), errMock)
		cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 1}, nil)
		// si1 re-inspect (fallback): Read succeeds → healthy
		cs.EXPECT().Read(any, any).Return(int64(1), nil)

		bads, ioErr := mgr.inspectBatch(ctx, cs, ds, []*bnapi.ShardInfo{si0, si1}, lmt)
		require.NoError(t, ioErr)
		require.Len(t, bads, 1)
		require.Equal(t, proto.BlobID(2001), bads[0].Bid)
	}

	{
		// failIdx=1 with remaining shards: si0 OK (correct CRC), si1 BidNotMatch, si2 OK (correct CRC).
		// Expected:
		//   si0: no re-inspect (CRC matched, not in badBids)
		//   si1 re-inspected (fallback) → Read fails → bad shard
		//   si2 continues as new recursive batch → BatchRead succeeds, CRC OK → 0 bad shards
		si0 := &bnapi.ShardInfo{Bid: 3001, Vuid: proto.Vuid(1001), Size: 8, Offset: 0, Crc: crc0}
		si1 := &bnapi.ShardInfo{Bid: 3002, Vuid: proto.Vuid(1001), Size: 8, Offset: 100, Crc: 0}
		si2 := &bnapi.ShardInfo{Bid: 3003, Vuid: proto.Vuid(1001), Size: 8, Offset: 200, Crc: crc2}
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		gomock.InOrder(
			// first call: si0+si1+si2 batch; stops at si1 header with ErrBidNotMatch
			cs.EXPECT().BatchRead(any, any).DoAndReturn(func(_ context.Context, bs *core.BatchShard) (int64, error) {
				writeShardOK(bs, data0) // si0: correct CRC → not in badBids
				writeShardErr(bs)       // si1: BidNotMatch
				return int64(12), bloberr.ErrBidNotMatch
			}),
			// second call (recursive inspectBatch for si2)
			cs.EXPECT().BatchRead(any, any).DoAndReturn(func(_ context.Context, bs *core.BatchShard) (int64, error) {
				writeShardOK(bs, data2) // si2: correct CRC → no bad shards
				return int64(len(data2)), nil
			}),
		)
		// si1 fallback re-inspect: Read fails
		cs.EXPECT().Read(any, any).Return(int64(0), errMock)
		cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 1}, nil)

		bads, ioErr := mgr.inspectBatch(ctx, cs, ds, []*bnapi.ShardInfo{si0, si1, si2}, lmt)
		require.NoError(t, ioErr)
		require.Len(t, bads, 1)
		require.Equal(t, proto.BlobID(3002), bads[0].Bid)
	}

	{
		// failIdx=2: si0 correct CRC (not in badBids), si1 wrong CRC (in badBids), si2 BidNotMatch.
		// reInspectCRCMismatches is called with shards[:2]=[si0,si1] and badBids=[si1.Bid].
		// si0 is NOT in badBids → the !isBad continue branch is exercised.
		// si1 IS in badBids → re-inspected, Read fails → bad shard.
		// si2 is the BidNotMatch shard → fallback single-shard inspect, Read succeeds → healthy.
		data1 := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
		wrongCRC1 := crc32.ChecksumIEEE(data1) + 1
		si0 := &bnapi.ShardInfo{Bid: 4001, Vuid: proto.Vuid(1001), Size: 8, Offset: 0, Crc: crc0}
		si1 := &bnapi.ShardInfo{Bid: 4002, Vuid: proto.Vuid(1001), Size: 8, Offset: 100, Crc: wrongCRC1}
		si2 := &bnapi.ShardInfo{Bid: 4003, Vuid: proto.Vuid(1001), Size: 8, Offset: 200}
		cs := NewMockChunkAPI(ctr)
		cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
		cs.EXPECT().BatchRead(any, any).DoAndReturn(func(_ context.Context, bs *core.BatchShard) (int64, error) {
			writeShardOK(bs, data0) // si0: correct CRC
			writeShardOK(bs, data1) // si1: CRC mismatch (stored wrongCRC1)
			writeShardErr(bs)       // si2: BidNotMatch header
			return int64(24), bloberr.ErrBidNotMatch
		})
		// si1 re-inspect (in badBids): Read fails
		cs.EXPECT().Read(any, any).Return(int64(0), errMock)
		cs.EXPECT().ReadShardMeta(any, any).Return(&core.ShardMeta{Size: 1}, nil)
		// si2 fallback (BidNotMatch shard): Read succeeds → healthy
		cs.EXPECT().Read(any, any).Return(int64(8), nil)

		bads, ioErr := mgr.inspectBatch(ctx, cs, ds, []*bnapi.ShardInfo{si0, si1, si2}, lmt)
		require.NoError(t, ioErr)
		require.Len(t, bads, 1)
		require.Equal(t, proto.BlobID(4002), bads[0].Bid)
	}
}

// ---------------------------------------------------------------------------
// inspectChunk: multi-page shard scanning
// ---------------------------------------------------------------------------

func TestInspectChunk_MultiPage(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	ds := NewMockDiskAPI(ctr)
	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().GetConfig().Return(&core.Config{
		RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1024 * 1024},
	}).Times(2) // two batches, one per page
	ds.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{}).AnyTimes()

	svr := &Service{closeCh: make(chan struct{})}

	// Build mgr directly with an always-enabled switch to avoid the SwitchMgr background
	// goroutine re-disabling the switch (it calls GetConfig→"" which triggers Disable).
	recorder, _ := recordlog.NewEncoder(nil) // NopEncoder; no Encode calls expected
	mgr := &DataInspectMgr{
		conf: DataInspectConf{
			IntervalSec:   1,
			RateLimit:     1024 * 1024,
			BatchReadSize: 16 << 20,
		},
		limits:     make(map[proto.DiskID]*rate.Limiter),
		svr:        svr,
		taskSwitch: taskswitch.NewEnabledTaskSwitch(),
		recorder:   recorder,
	}
	mgr.limits[proto.DiskID(11)] = rate.NewLimiter(rate.Limit(1024*1024), 2*1024*1024)

	cs := NewMockChunkAPI(ctr)
	cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
	cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
	cs.EXPECT().Disk().Return(ds).AnyTimes()

	si1 := &bnapi.ShardInfo{Bid: 4001, Size: 8, Offset: 0}
	si2 := &bnapi.ShardInfo{Bid: 4002, Size: 8, Offset: 100}

	// page 1 returns next=BlobID(4002), triggering a second ListShards call
	gomock.InOrder(
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{si1}, proto.BlobID(4002), nil),
		cs.EXPECT().ListShards(any, any, any, any).Return([]*bnapi.ShardInfo{si2}, proto.InValidBlobID, nil),
	)
	// BatchRead is called once per page (no CRC mismatch, mock writes nothing → crcWriter sees no badBids)
	cs.EXPECT().BatchRead(any, any).Return(int64(0), nil).Times(2)

	bads, err := mgr.inspectChunk(ctx, cs)
	require.NoError(t, err)
	require.Empty(t, bads)
}

// ---------------------------------------------------------------------------
// inspectBatch: rate-limiter WaitN returns error when context is cancelled
// ---------------------------------------------------------------------------

func TestInspectBatch_RateLimitCtxCancel(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	ds := NewMockDiskAPI(ctr)
	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()

	cs := NewMockChunkAPI(ctr)
	cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
	// BatchRead and GetConfig must NOT be called: rate-limit loop returns before reaching them
	cs.EXPECT().BatchRead(any, any).Times(0)
	ds.EXPECT().GetConfig().Times(0)

	svr := &Service{closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 1, RateLimit: 1}, svr)

	// burst=1, rate=1/s: first WaitN(ctx,1) consumes the burst token immediately (no wait),
	// second WaitN(ctx,1) needs ~1s but ctx is already done → returns context.Canceled.
	lmt := rate.NewLimiter(rate.Limit(1), 1)

	si := &bnapi.ShardInfo{Bid: 5001, Size: 2, Offset: 0} // needs 2 tokens
	bads, ioErr := mgr.inspectBatch(ctx, cs, ds, []*bnapi.ShardInfo{si}, lmt)
	require.Nil(t, bads)
	require.ErrorIs(t, ioErr, context.Canceled)
}

// ---------------------------------------------------------------------------
// inspectBatch: nil limiter must not panic (getLimiter returns nil for unknown disk)
// ---------------------------------------------------------------------------

func TestInspectBatch_NilLimiter(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	ds := NewMockDiskAPI(ctr)
	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().GetConfig().Return(&core.Config{
		RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1024 * 1024},
	}).AnyTimes()

	svr := &Service{closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 1, RateLimit: 1024 * 1024}, svr)

	si := &bnapi.ShardInfo{Bid: 6001, Vuid: proto.Vuid(1001), Size: 8, Offset: 0}
	cs := NewMockChunkAPI(ctr)
	cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
	cs.EXPECT().BatchRead(any, any).Return(int64(0), nil)

	// lmt == nil: rate-limiting loop is skipped, BatchRead is still called, no panic
	bads, ioErr := mgr.inspectBatch(ctx, cs, ds, []*bnapi.ShardInfo{si}, nil)
	require.NoError(t, ioErr)
	require.Empty(t, bads)
}

func TestInspectChunk_NilLimiter(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	ds := NewMockDiskAPI(ctr)
	ds.EXPECT().ID().Return(proto.DiskID(99)).AnyTimes()
	ds.EXPECT().GetConfig().Return(&core.Config{
		RuntimeConfig: core.RuntimeConfig{BatchBufferSize: 1024 * 1024},
	}).AnyTimes()

	svr := &Service{closeCh: make(chan struct{})}
	// disk 99 is intentionally absent from mgr.limits so getLimiter returns nil
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 1, RateLimit: 1024 * 1024}, svr)

	cs := NewMockChunkAPI(ctr)
	cs.EXPECT().Vuid().Return(proto.Vuid(9001)).AnyTimes()
	cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
	cs.EXPECT().Disk().Return(ds).AnyTimes()
	cs.EXPECT().ListShards(any, any, any, any).Return(
		[]*bnapi.ShardInfo{{Bid: 6002, Size: 8, Offset: 0}}, proto.InValidBlobID, nil,
	)
	cs.EXPECT().BatchRead(any, any).Return(int64(0), nil)

	// getLimiter(ds) == nil: no panic, inspection completes normally
	bads, err := mgr.inspectChunk(ctx, cs)
	require.NoError(t, err)
	require.Empty(t, bads)
}

// ---------------------------------------------------------------------------
// inspectDisk: cs.Disk().IsWritable() returns false mid-inspection → early return
// ---------------------------------------------------------------------------

func TestInspectDisk_BrokenMidInspect(t *testing.T) {
	ctr := gomock.NewController(t)

	cs := NewMockChunkAPI(ctr)
	ds := NewMockDiskAPI(ctr)

	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{}).AnyTimes()
	ds.EXPECT().ListChunks(any).Return([]core.VuidMeta{{Vuid: proto.Vuid(1001)}}, nil)
	ds.EXPECT().GetChunkStorage(any).Return(cs, true)

	// cs.Disk() is called for the IsWritable check inside the chunk loop
	cs.EXPECT().Disk().Return(ds).Times(1)
	ds.EXPECT().IsWritable().Return(false) // disk broken → inspectDisk returns immediately

	// neither BatchRead nor ListShards should be reached
	cs.EXPECT().BatchRead(any, any).Times(0)
	cs.EXPECT().ListShards(any, any, any, any).Times(0)

	svr := &Service{closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 1, RateLimit: 1024 * 1024}, svr)

	var wg sync.WaitGroup
	wg.Add(1)
	mgr.inspectDisk(ds, &wg)
}

// ---------------------------------------------------------------------------
// waitNextRoundInspect: timer fires (NextRoundSec=0)
// ---------------------------------------------------------------------------

func TestWaitNextRoundInspect(t *testing.T) {
	svr := &Service{closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 1, RateLimit: 1}, svr)
	// NextRoundSec=0 → timer duration is 0 → fires immediately
	mgr.waitNextRoundInspect()
}

// ---------------------------------------------------------------------------
// inspectDisk: extra edge cases (ChunkStatusRelease, GetChunkStorage not found, inspectChunk error)
// ---------------------------------------------------------------------------

func TestInspectDisk_ExtraEdgeCases(t *testing.T) {
	ctr := gomock.NewController(t)

	ds := NewMockDiskAPI(ctr)
	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{}).AnyTimes()

	svr := &Service{closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 1, RateLimit: 1024 * 1024}, svr)

	{
		// chunk.Status == ChunkStatusRelease → skip, no GetChunkStorage call
		// chunk.Status != ChunkStatusRelease but GetChunkStorage returns not found → skip
		cs := NewMockChunkAPI(ctr)
		ds.EXPECT().ListChunks(any).Return([]core.VuidMeta{
			{Vuid: proto.Vuid(1001), Status: clustermgr.ChunkStatusRelease},
			{Vuid: proto.Vuid(1002), Status: clustermgr.ChunkStatusNormal},
		}, nil)
		ds.EXPECT().GetChunkStorage(proto.Vuid(1002)).Return(cs, false) // not found
		cs.EXPECT().BatchRead(any, any).Times(0)
		cs.EXPECT().ListShards(any, any, any, any).Times(0)

		var wg sync.WaitGroup
		wg.Add(1)
		mgr.inspectDisk(ds, &wg)
	}

	{
		// inspectChunk returns error (ListShards fails) → inspectDisk logs and returns
		// This also covers the scanShards ListShards error path (line 479-481).
		cs := NewMockChunkAPI(ctr)
		ds.EXPECT().ListChunks(any).Return([]core.VuidMeta{
			{Vuid: proto.Vuid(2001), Status: clustermgr.ChunkStatusNormal},
		}, nil)
		ds.EXPECT().GetChunkStorage(proto.Vuid(2001)).Return(cs, true)
		cs.EXPECT().Disk().Return(ds).Times(2) // inspectDisk IsWritable check + inspectChunk ds := cs.Disk()
		ds.EXPECT().IsWritable().Return(true)
		cs.EXPECT().Vuid().Return(proto.Vuid(2001)).AnyTimes()
		cs.EXPECT().ID().Return(clustermgr.ChunkID{}).AnyTimes()
		cs.EXPECT().ListShards(any, any, any, any).Return(nil, proto.InValidBlobID, errMock)

		var wg sync.WaitGroup
		wg.Add(1)
		mgr.inspectDisk(ds, &wg)
	}
}

// ---------------------------------------------------------------------------
// HTTP handlers: SetInspectRate, CleanInspectMetric, setAllDiskRateForce
// ---------------------------------------------------------------------------

func TestInspectHTTPHandlers(t *testing.T) {
	ctr := gomock.NewController(t)

	ds := NewMockDiskAPI(ctr)
	ds.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{}).AnyTimes()

	svr := &Service{
		Disks:   map[proto.DiskID]core.DiskAPI{11: ds},
		ctx:     context.Background(),
		closeCh: make(chan struct{}),
	}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 1, RateLimit: 1 << 20}, svr)
	mgr.setLimiters([]core.DiskAPI{ds}) // populate limits so setAllDiskRateForce iterates the map
	svr.inspectMgr = mgr

	ts := httptest.NewServer(NewHandler(svr))
	defer ts.Close()
	client := &http.Client{}

	do := func(method, url string) int {
		req, err := http.NewRequest(method, url, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}

	// SetInspectRate: parse error (rate=abc cannot be converted to int)
	require.NotEqual(t, http.StatusOK, do(http.MethodPost, ts.URL+"/inspect/rate/abc"))

	// SetInspectRate: rate too small
	require.NotEqual(t, http.StatusOK, do(http.MethodPost, ts.URL+"/inspect/rate/1"))

	// SetInspectRate: success (also exercises setAllDiskRateForce with populated limits)
	require.Equal(t, http.StatusOK, do(http.MethodPost, ts.URL+"/inspect/rate/1048576"))

	// CleanInspectMetric: parse error (diskid missing → "do not omit" error)
	require.NotEqual(t, http.StatusOK, do(http.MethodPost, ts.URL+"/inspect/cleanmetric"))

	// CleanInspectMetric: invalid diskid=0
	require.NotEqual(t, http.StatusOK, do(http.MethodPost, ts.URL+"/inspect/cleanmetric?diskid=0"))

	// CleanInspectMetric: disk not found
	require.NotEqual(t, http.StatusOK, do(http.MethodPost, ts.URL+"/inspect/cleanmetric?diskid=99"))

	// CleanInspectMetric: success
	require.Equal(t, http.StatusOK, do(http.MethodPost, ts.URL+"/inspect/cleanmetric?diskid=11"))
}

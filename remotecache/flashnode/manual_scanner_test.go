package flashnode

import (
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/remotecache"
	"github.com/cubefs/cubefs/util/routinepool"
	"github.com/cubefs/cubefs/util/unboundedchan"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

type mockCacheStreamer struct{}

func (m *mockCacheStreamer) PrepareCacheRequests(offset, size uint64, data []byte, gen uint64) ([]*remotecache.CacheReadRequest, error) {
	req := &remotecache.CacheReadRequest{
		CacheReadRequest: proto.CacheReadRequest{
			CacheRequest: &proto.CacheRequest{
				FixedFileOffset: offset,
			},
			Size_: uint64(size),
		},
	}
	return []*remotecache.CacheReadRequest{req}, nil
}

func (m *mockCacheStreamer) GetFlashGroup(fixedFileOffset uint64) (uint32, *remotecache.FlashGroup, uint32) {
	return 0, &remotecache.FlashGroup{
		FlashGroupInfo: &proto.FlashGroupInfo{
			Hosts: []string{"127.0.0.1"},
		},
	}, 0
}

func testManualScanner(t *testing.T) {
	rc := &stream.RemoteCache{}
	rc.PrepareCh = make(chan *stream.PrepareRemoteCacheRequest, 1024)
	scanner := &ManualScanner{
		ID:        "test_manual_scan_id",
		Volume:    "test_vol",
		flashNode: flashServer,
		mw:        NewMockMetaWrapper(),
		adminTask: &proto.AdminTask{
			Response: &proto.FlashNodeManualTaskResponse{},
		},
		dirChan:        unboundedchan.NewUnboundedChan(defaultUnboundedChanInitCapacity),
		fileChan:       make(chan interface{}, 2*flashServer.handlerFileRoutineNumPerTask),
		dirRPool:       routinepool.NewRoutinePool(flashServer.scanRoutineNumPerTask),
		fileRPool:      routinepool.NewRoutinePool(flashServer.handlerFileRoutineNumPerTask),
		currentStat:    &proto.ManualTaskStatistics{FlashNode: flashServer.localAddr},
		limiter:        rate.NewLimiter(rate.Limit(flashServer.manualScanLimitPerSecond), _defaultManualScanLimitBurst),
		prepareLimiter: rate.NewLimiter(rate.Limit(flashServer.prepareLimitPerSecond), int(flashServer.prepareLimitPerSecond/2)),
		flowLimiter:    rate.NewLimiter(rate.Inf, 0),
		createTime:     time.Now(),
		receiveStopC:   make(chan struct{}),
		stopC:          make(chan struct{}),
		receivePauseC:  make(chan struct{}),
		receiveResumeC: make(chan struct{}),
		pause:          0,
		pauseCond:      sync.NewCond(&sync.Mutex{}),
		semaphore:      make(chan struct{}, 2000),
		timeoutStopCh:  make(chan struct{}),
		ec:             NewMockExtentClient(),
		RemoteCache:    rc,
		manualTask: &proto.FlashManualTask{
			Id:       "test_manual_scan_id",
			VolName:  "test_vol",
			Action:   proto.FlashManualWarmupAction,
			Status:   int(proto.Flash_Task_Running),
			TopoName: proto.DefaultTopoName,
		},
	}

	originalGetCacheStreamer := getCacheStreamer
	getCacheStreamer = func(ec ExtentApi, inode uint64) (CacheStreamer, error) {
		return &mockCacheStreamer{}, nil
	}
	defer func() {
		getCacheStreamer = originalGetCacheStreamer
	}()

	err := scanner.Start()
	require.NoError(t, err)
	time.Sleep(time.Second * 5)
	require.Equal(t, int64(4), scanner.currentStat.TotalFileScannedNum)
	require.Equal(t, int64(2), scanner.currentStat.TotalFileCachedNum)
	require.Equal(t, int64(4), scanner.currentStat.TotalDirScannedNum)
	require.Equal(t, int64(7000), scanner.currentStat.TotalCacheSize)
	require.Equal(t, int64(0), scanner.currentStat.LastCacheSize)
	require.Equal(t, int64(0), scanner.currentStat.ErrorCacheNum)
	require.Equal(t, int64(0), scanner.currentStat.ErrorReadDirNum)
}

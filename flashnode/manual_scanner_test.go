package flashnode

import (
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/util/routinepool"
	"github.com/cubefs/cubefs/util/unboundedchan"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

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
		createTime:     time.Now(),
		receiveStopC:   make(chan struct{}),
		stopC:          make(chan struct{}),
		receivePauseC:  make(chan struct{}),
		receiveResumeC: make(chan struct{}),
		pause:          0,
		pauseCond:      sync.NewCond(&sync.Mutex{}),
		ec:             NewMockExtentClient(),
		RemoteCache:    rc,
		manualTask: &proto.FlashManualTask{
			Id:      "test_manual_scan_id",
			VolName: "test_vol",
			Action:  proto.FlashManualWarmupAction,
			Status:  int(proto.Flash_Task_Running),
		},
	}
	err := scanner.Start()
	require.NoError(t, err)
	time.Sleep(time.Second * 5)
	require.Equal(t, true, scanner.DoneScanning())
	require.Equal(t, int64(4), scanner.currentStat.TotalFileScannedNum)
	require.Equal(t, int64(2), scanner.currentStat.TotalFileCachedNum)
	require.Equal(t, int64(4), scanner.currentStat.TotalDirScannedNum)
	require.Equal(t, int64(300), scanner.currentStat.TotalCacheSize)
	require.Equal(t, int64(0), scanner.currentStat.LastCacheSize)
	require.Equal(t, int64(0), scanner.currentStat.ErrorCacheNum)
	require.Equal(t, int64(0), scanner.currentStat.ErrorReadDirNum)
}

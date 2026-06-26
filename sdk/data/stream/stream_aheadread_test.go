package stream

import (
	"bytes"
	"container/list"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/manager"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
)

type testSimpleClient struct {
	latestVer uint64
}

func (c *testSimpleClient) GetFlowInfo() (*proto.ClientReportLimitInfo, bool) {
	return nil, false
}

func (c *testSimpleClient) UpdateFlowInfo(*proto.LimitRsp2Client) {}

func (c *testSimpleClient) SetClientID(uint64) error { return nil }

func (c *testSimpleClient) UpdateLatestVer(verList *proto.VolVersionInfoList) error {
	if verList != nil && len(verList.VerList) > 0 {
		c.latestVer = verList.VerList[len(verList.VerList)-1].Ver
	}
	return nil
}

func (c *testSimpleClient) GetReadVer() uint64 { return 0 }

func (c *testSimpleClient) GetLatestVer() uint64 { return c.latestVer }

func (c *testSimpleClient) GetVerMgr() *proto.VolVersionInfoList { return nil }

func (c *testSimpleClient) UpdateRemoteCacheConfig(*proto.SimpleVolView) {}

func newTestStreamerWithAheadRead(t *testing.T, partitionID uint64) (*Streamer, *AheadReadCache) {
	t.Helper()

	w := &wrapper.Wrapper{}
	w.SimpleClient = &testSimpleClient{}
	w.InitInnerReq(true)
	// Preload a DataPartition to avoid fetching from master
	dp := &wrapper.DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: partitionID,
		},
	}
	dp.ClientWrapper = w
	wrapper.InsertPartitionForTest(w, dp)

	client := &ExtentClient{}
	client.dataWrapper = w
	client.extentConfig = &ExtentConfig{}
	client.streamRetryTimeout = time.Second

	// Enable AheadRead cache
	arc := NewAheadReadCache(true, 16*util.MB, 100000, 2)

	s := &Streamer{}
	s.client = client
	s.inode = 12345
	s.aheadReadEnable = true
	s.isOpen = true
	// Initialize extents to avoid nil pointer in getNextExtent
	s.extents = NewExtentCache(s.inode)
	// Construct AheadReadWindow (no background goroutine needed)
	s.aheadReadWindow = &AheadReadWindow{
		cache:    arc,
		streamer: s,
		taskC:    make(chan *AheadReadTask, arc.winCnt),
	}
	s.aheadReadBlockSize = util.CacheReadBlockSize
	return s, arc
}

func newTestStreamerWithAheadReadAndWriteSupport(t *testing.T, partitionID uint64) (*Streamer, *AheadReadCache) {
	t.Helper()

	s, arc := newTestStreamerWithAheadRead(t, partitionID)
	s.client.writeLimiter = rate.NewLimiter(rate.Inf, 128)
	s.client.LimitManager = manager.NewLimitManager(s.client)
	s.client.appendExtentKey = func(uint64, uint64, proto.ExtentKey, []proto.ExtentKey, bool, uint8, bool) (int, error) {
		return 0, nil
	}
	s.client.splitExtentKey = func(uint64, uint64, proto.ExtentKey, uint8) error {
		return nil
	}
	s.client.truncate = func(uint64, uint64, string) error {
		return nil
	}
	s.client.getExtents = func(uint64, bool, bool, bool) (uint64, uint64, []proto.ExtentKey, error) {
		return 1, 0, nil, nil
	}
	s.dirtylist = NewDirtyExtentList()
	s.handler = &ExtentHandler{}
	return s, arc
}

func putCacheBlock(arc *AheadReadCache, inode, partitionID, extentID uint64, cacheOffset int, availSize int, fill byte) string {
	key := createAheadBlockKey(inode, partitionID, extentID, 0, cacheOffset)
	bv := &AheadReadBlock{}
	bv.inode = inode
	bv.partitionId = partitionID
	bv.extentId = extentID
	bv.offset = uint64(cacheOffset)
	bv.size = uint64(util.CacheReadBlockSize)
	bv.data = make([]byte, util.CacheReadBlockSize)
	for i := 0; i < availSize; i++ {
		bv.data[i] = fill
	}
	bv.time = time.Now().Unix()
	bv.key = key
	atomic.StoreUint64(&bv.readBytes, uint64(availSize))
	atomic.StoreUint32(&bv.state, AheadReadBlockStateInit)
	arc.blockCache.Store(key, bv)
	return key
}

func TestAheadRead_FullHit_SingleBlock(t *testing.T) {
	s, arc := newTestStreamerWithAheadRead(t, 1)
	defer arc.Stop()

	// Prepare a cache block starting at 0, 2MB available, fill 'A'
	avail := 2 * util.MB
	putCacheBlock(arc, s.inode, 1, 100, 0, avail, 'A')

	reqSize := 1 * util.MB
	offset := 512 * util.KB
	reqData := make([]byte, reqSize)
	ek := &proto.ExtentKey{PartitionId: 1, ExtentId: 100, FileOffset: 0, ExtentOffset: 0, Size: 8 * util.MB}
	req := &ExtentRequest{FileOffset: offset, Size: reqSize, Data: reqData, ExtentKey: ek}

	read, err := s.aheadRead(req, 0)
	if err != nil {
		t.Fatalf("aheadRead error: %v", err)
	}
	if read != reqSize {
		t.Fatalf("read size mismatch, want %d, got %d", reqSize, read)
	}
	for i := 0; i < reqSize; i++ {
		if reqData[i] != 'A' {
			t.Fatalf("unexpected data at %d, want 'A', got %v", i, reqData[i])
		}
	}
}

func TestAheadRead_PartialHit_SingleBlock(t *testing.T) {
	s, arc := newTestStreamerWithAheadRead(t, 2)
	defer arc.Stop()

	// Cache block [0, 800KB) available, fill 'A'
	avail := 800 * util.KB
	putCacheBlock(arc, s.inode, 2, 200, 0, avail, 'A')

	reqSize := 1 * util.MB
	offset := 512 * util.KB
	reqData := make([]byte, reqSize)
	ek := &proto.ExtentKey{PartitionId: 2, ExtentId: 200, FileOffset: 0, ExtentOffset: 0, Size: 8 * util.MB}
	req := &ExtentRequest{FileOffset: offset, Size: reqSize, Data: reqData, ExtentKey: ek}

	read, err := s.aheadRead(req, 0)
	if err != nil {
		t.Fatalf("aheadRead error: %v", err)
	}
	// Only 800KB-512KB=288KB should hit from cache
	want := 288 * util.KB
	if read != want {
		t.Fatalf("read size mismatch, want %d, got %d", want, read)
	}
	for i := 0; i < want; i++ {
		if reqData[i] != 'A' {
			t.Fatalf("unexpected data at %d, want 'A', got %v", i, reqData[i])
		}
	}
}

func TestAheadRead_CrossBlocks_FullHit(t *testing.T) {
	s, arc := newTestStreamerWithAheadRead(t, 3)
	defer arc.Stop()

	// Prepare two consecutive cache blocks:
	// Block0: [0, 4MB) fully available, fill 'A'
	putCacheBlock(arc, s.inode, 3, 300, 0, util.CacheReadBlockSize, 'A')
	// Block1: [4MB, 4MB+512KB) available, fill 'B'
	putCacheBlock(arc, s.inode, 3, 300, util.CacheReadBlockSize, 512*util.KB, 'B')

	reqSize := 1 * util.MB
	offset := util.CacheReadBlockSize - 512*util.KB // 3.5MB
	reqData := make([]byte, reqSize)
	ek := &proto.ExtentKey{PartitionId: 3, ExtentId: 300, FileOffset: 0, ExtentOffset: 0, Size: 8 * util.MB}
	req := &ExtentRequest{FileOffset: offset, Size: reqSize, Data: reqData, ExtentKey: ek}

	read, err := s.aheadRead(req, 0)
	if err != nil {
		t.Fatalf("aheadRead error: %v", err)
	}
	if read != reqSize {
		t.Fatalf("read size mismatch, want %d, got %d", reqSize, read)
	}
	// First 512KB from block0 ('A'), next 512KB from block1 ('B')
	for i := 0; i < 512*util.KB; i++ {
		if reqData[i] != 'A' {
			t.Fatalf("unexpected data A at %d, got %v", i, reqData[i])
		}
	}
	for i := 512 * util.KB; i < reqSize; i++ {
		if reqData[i] != 'B' {
			t.Fatalf("unexpected data B at %d, got %v", i, reqData[i])
		}
	}
}

func TestAheadRead_DoTask_ReadFailed(t *testing.T) {
	s, arc := newTestStreamerWithAheadRead(t, 4)
	defer arc.Stop()

	// Create a task with an invalid host to simulate read failure
	dp := &wrapper.DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: 4,
			LeaderAddr:  "127.0.0.1:1",
			Hosts:       []string{"127.0.0.1:1", "127.0.0.1:2"}, // invalid hosts
		},
	}
	ek := &proto.ExtentKey{PartitionId: 4, ExtentId: 400, FileOffset: 0, ExtentOffset: 0, Size: 8 * util.MB}
	p := NewReadPacket(ek, 0, util.CacheReadBlockSize, s.inode, 0, false)
	req := &ExtentRequest{
		FileOffset: 0,
		Size:       util.CacheReadBlockSize,
		ExtentKey:  ek,
	}

	task := &AheadReadTask{
		p:         p,
		dp:        dp,
		time:      time.Now(),
		req:       req,
		cacheSize: util.CacheReadBlockSize,
		cacheType: "test",
		logTime:   &time.Time{},
		reqID:     "req-1",
		poolId:    0,
		retry:     MaxCacheBlockRetry + 1, // set to max to avoid pushing back to taskC
	}

	key := createAheadBlockKey(s.inode, 4, 400, 0, 0)

	// Ensure block is not in cache before
	if _, ok := arc.blockCache.Load(key); ok {
		t.Fatalf("block should not be in cache")
	}

	// Call doTask directly
	s.aheadReadWindow.doTask(task)

	// Block should be deleted from cache
	if _, ok := arc.blockCache.Load(key); ok {
		t.Fatalf("block should be deleted from cache after read failure")
	}
}

func TestAheadRead_DoTask_FollowerReadUsesRotatedHosts(t *testing.T) {
	s, arc := newTestStreamerWithAheadRead(t, 41)
	defer arc.Stop()
	s.client.dataWrapper.InitFollowerRead(true)

	dp := &wrapper.DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: 41,
			LeaderAddr:  "127.0.0.1:1",
			Hosts:       []string{"127.0.0.1:1", "127.0.0.1:2"},
		},
	}
	ek := &proto.ExtentKey{PartitionId: 41, ExtentId: 4100, FileOffset: 0, ExtentOffset: 0, Size: 8 * util.MB}
	p := NewReadPacket(ek, 0, util.CacheReadBlockSize, s.inode, 0, false)
	req := &ExtentRequest{
		FileOffset: 0,
		Size:       util.CacheReadBlockSize,
		ExtentKey:  ek,
	}

	task := &AheadReadTask{
		p:         p,
		dp:        dp,
		time:      time.Now(),
		req:       req,
		cacheSize: util.CacheReadBlockSize,
		cacheType: "test",
		logTime:   &time.Time{},
		reqID:     "req-rotate",
		poolId:    0,
		retry:     MaxCacheBlockRetry + 1,
	}

	s.aheadReadWindow.doTask(task)
}

func TestAheadRead_GetAheadReadTask_FollowerReadFlag(t *testing.T) {
	s, arc := newTestStreamerWithAheadRead(t, 42)
	defer arc.Stop()
	s.client.dataWrapper.InitFollowerRead(true)

	dp := &wrapper.DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: 42,
			LeaderAddr:  "127.0.0.1:1",
			Hosts:       []string{"127.0.0.1:1", "127.0.0.1:2"},
		},
	}
	ek := &proto.ExtentKey{PartitionId: 42, ExtentId: 4200, FileOffset: 0, ExtentOffset: 0, Size: 8 * util.MB}
	req := &ExtentRequest{
		FileOffset: 0,
		Size:       util.CacheReadBlockSize,
		ExtentKey:  ek,
	}

	task := s.aheadReadWindow.getAheadReadTask(dp, req, 1, util.CacheReadBlockSize, 0)
	if task == nil {
		t.Fatal("expected task")
	}
	if task.p.Opcode != proto.OpStreamFollowerRead {
		t.Fatalf("expected follower read opcode, got %v", task.p.Opcode)
	}
}

func TestAheadRead_GetAheadReadTask_DisableFollowerReadOnOverwrite(t *testing.T) {
	s, arc := newTestStreamerWithAheadRead(t, 43)
	defer arc.Stop()
	s.client.dataWrapper.InitFollowerRead(true)

	req := &ExtentRequest{
		FileOffset: 0,
		Size:       util.CacheReadBlockSize,
		ExtentKey: &proto.ExtentKey{
			PartitionId:  43,
			ExtentId:     4300,
			FileOffset:   0,
			ExtentOffset: 0,
			Size:         8 * util.MB,
		},
	}
	s.aheadReadWindow.evictCacheBlock(req)

	dp := &wrapper.DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: 43,
			LeaderAddr:  "127.0.0.1:1",
			Hosts:       []string{"127.0.0.1:1", "127.0.0.1:2"},
		},
	}

	task := s.aheadReadWindow.getAheadReadTask(dp, req, 1, util.CacheReadBlockSize, 0)
	if task == nil {
		t.Fatal("expected task")
	}
	if task.p.Opcode != proto.OpStreamRead {
		t.Fatalf("expected overwrite to disable follower read, got %v", task.p.Opcode)
	}
}

func TestAheadRead_DoTask_DisableFollowerReadOnOverwrite(t *testing.T) {
	s, arc := newTestStreamerWithAheadRead(t, 44)
	defer arc.Stop()
	s.client.dataWrapper.InitFollowerRead(true)
	atomic.StoreUint64(&rrIdx, 1)
	t.Cleanup(func() {
		atomic.StoreUint64(&rrIdx, 0)
	})

	ek := &proto.ExtentKey{PartitionId: 44, ExtentId: 4400, FileOffset: 0, ExtentOffset: 0, Size: 8 * util.MB}
	req := &ExtentRequest{FileOffset: 0, Size: util.CacheReadBlockSize, ExtentKey: ek}
	s.aheadReadWindow.evictCacheBlock(req)

	var gotHost string
	patches := gomonkey.ApplyFunc(sendToNode, func(host string, p *Packet, key, reqID string, getReply GetReplyFunc) error {
		gotHost = host
		return nil
	})
	defer patches.Reset()

	dp := &wrapper.DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: 44,
			LeaderAddr:  "127.0.0.1:1",
			Hosts:       []string{"127.0.0.1:1", "127.0.0.1:2"},
		},
	}
	p := NewReadPacket(ek, 0, util.CacheReadBlockSize, s.inode, 0, true)
	task := &AheadReadTask{
		p:         p,
		dp:        dp,
		time:      time.Now(),
		req:       req,
		cacheSize: util.CacheReadBlockSize,
		cacheType: "test",
		logTime:   &time.Time{},
		reqID:     "req-overwrite",
		poolId:    0,
		retry:     MaxCacheBlockRetry + 1,
	}

	s.aheadReadWindow.doTask(task)

	if gotHost != dp.LeaderAddr {
		t.Fatalf("expected leader host to be selected first, got %q want %q", gotHost, dp.LeaderAddr)
	}
}

func TestOverwriteInodeCacheBranches(t *testing.T) {
	_, err := log.InitLog("", "stream_cov_overwrite", log.DebugLevel, nil, log.DefaultLogLeftSpaceLimitRatio)
	if err != nil {
		t.Fatalf("init log: %v", err)
	}
	t.Cleanup(func() {
		log.SetLogLevelV2(log.InfoLevel)
	})

	c := &overwriteInodeCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  time.Hour,
		maxElements: 1,
	}

	if c.isOverwrite(100) {
		t.Fatal("missing inode should not be marked as overwrite")
	}

	c.mark(100)
	if !c.isOverwrite(100) {
		t.Fatal("marked inode should be reported as overwrite")
	}

	c.mark(100)
	c.mark(101)

	expired := &overwriteInodeCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  time.Hour,
		maxElements: 2,
	}
	expiredEl := expired.lruList.PushFront(&overwriteInodeEntry{
		ino:    200,
		expiry: time.Now().Add(-time.Second).Unix(),
	})
	expired.cache[200] = expiredEl
	if expired.isOverwrite(200) {
		t.Fatal("expired inode should not be reported as overwrite")
	}

	foreground := &overwriteInodeCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  time.Hour,
		maxElements: 4,
	}
	for i := 0; i < 10; i++ {
		ino := uint64(300 + i)
		foreground.cache[ino] = foreground.lruList.PushFront(&overwriteInodeEntry{
			ino:    ino,
			expiry: time.Now().Add(-time.Second).Unix(),
		})
	}
	foreground.evict(true)
	if len(foreground.cache) != 0 {
		t.Fatalf("foreground eviction should clear entries, got %d", len(foreground.cache))
	}

	fastEvict := &overwriteInodeCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  time.Hour,
		maxElements: 16,
	}
	for i := 0; i < 11; i++ {
		ino := uint64(400 + i)
		el := fastEvict.lruList.PushFront(&overwriteInodeEntry{
			ino:    ino,
			expiry: time.Now().Add(-time.Second).Unix(),
		})
		fastEvict.cache[ino] = el
	}
	fastEvict.evict(false)
	if len(fastEvict.cache) != 0 {
		t.Fatalf("background-style eviction should clear expired entries, got %d", len(fastEvict.cache))
	}

	unexpired := &overwriteInodeCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  time.Hour,
		maxElements: 16,
	}
	for i := 0; i < 10; i++ {
		ino := uint64(500 + i)
		el := unexpired.lruList.PushFront(&overwriteInodeEntry{
			ino:    ino,
			expiry: time.Now().Add(-time.Second).Unix(),
		})
		unexpired.cache[ino] = el
	}
	keepEl := unexpired.lruList.PushFront(&overwriteInodeEntry{
		ino:    999,
		expiry: time.Now().Add(time.Hour).Unix(),
	})
	unexpired.cache[999] = keepEl
	unexpired.evict(false)
	if _, ok := unexpired.cache[999]; !ok {
		t.Fatal("unexpired inode should remain after eviction pass")
	}

	earlyReturn := &overwriteInodeCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  time.Hour,
		maxElements: 16,
	}
	earlyEl := earlyReturn.lruList.PushFront(&overwriteInodeEntry{
		ino:    600,
		expiry: time.Now().Add(time.Hour).Unix(),
	})
	earlyReturn.cache[600] = earlyEl
	earlyReturn.evict(false)
	if len(earlyReturn.cache) != 1 {
		t.Fatalf("unexpired eviction early return should keep entry, got %d", len(earlyReturn.cache))
	}
}

func TestOverwriteInodeCacheBackgroundEviction(t *testing.T) {
	c := &overwriteInodeCache{
		cache:   make(map[uint64]*list.Element),
		lruList: list.New(),
	}
	ticks := make(chan time.Time, 1)
	patches := gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
		return &time.Ticker{C: ticks}
	})
	defer patches.Reset()

	done := make(chan struct{})
	go func() {
		c.backgroundEviction()
		close(done)
	}()

	ticks <- time.Now()
	close(ticks)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("backgroundEviction did not exit")
	}
}

func TestAheadRead_EvictAllBlocksMarksOverwriteAndClearsCache(t *testing.T) {
	s, arc := newTestStreamerWithAheadRead(t, 45)
	defer arc.Stop()

	key := putCacheBlock(arc, s.inode, 45, 4500, 0, util.CacheReadBlockSize, 'E')
	if _, ok := arc.blockCache.Load(key); !ok {
		t.Fatal("expected cache block before evictAllBlocks")
	}

	s.aheadReadWindow.evictAllBlocks()

	if _, ok := arc.blockCache.Load(key); ok {
		t.Fatal("cache block should be cleared by evictAllBlocks")
	}
	if !arc.overwriteInodes.isOverwrite(s.inode) {
		t.Fatal("evictAllBlocks should mark inode as overwrite")
	}
}

func TestAheadRead_BackgroundTaskTickerStop(t *testing.T) {
	// This test verifies that backgroundAheadReadTask stops its ticker
	// when the streamer is closed, covering the defer ticker.Stop() line.
	arc := NewAheadReadCache(true, 16*util.MB, 100000, 2)

	s := &Streamer{}
	s.inode = 99999
	s.isOpen = false // stream is closed so backgroundAheadReadTask will exit on ticker

	arw := &AheadReadWindow{
		taskC:    make(chan *AheadReadTask, arc.winCnt),
		cache:    arc,
		streamer: s,
	}

	// Start backgroundAheadReadTask — it will see isOpen==false on the next
	// ticker tick and return, which triggers defer ticker.Stop().
	done := make(chan struct{})
	go func() {
		arw.backgroundAheadReadTask()
		close(done)
	}()

	// Wait for the goroutine to exit (it should exit within ~1s ticker interval)
	select {
	case <-done:
		// backgroundAheadReadTask exited, defer ticker.Stop() was executed
	case <-time.After(5 * time.Second):
		t.Fatal("backgroundAheadReadTask did not exit within 5s, defer ticker.Stop() may not have been called")
	}

	arc.Stop()
}

func TestAheadRead_EvictCacheBlock(t *testing.T) {
	s, arc := newTestStreamerWithAheadRead(t, 5)
	defer arc.Stop()

	// Prepare a cache block in Init state
	key := putCacheBlock(arc, s.inode, 5, 500, 0, util.CacheReadBlockSize, 'A')

	// Verify it's in cache
	if _, ok := arc.blockCache.Load(key); !ok {
		t.Fatalf("block should be in cache")
	}

	req := &ExtentRequest{
		FileOffset: 0,
		Size:       util.CacheReadBlockSize,
		ExtentKey:  &proto.ExtentKey{PartitionId: 5, ExtentId: 500, FileOffset: 0, ExtentOffset: 0, Size: 8 * util.MB},
	}

	s.aheadReadWindow.evictCacheBlock(req)

	// Verify it's deleted from cache
	if _, ok := arc.blockCache.Load(key); ok {
		t.Fatalf("block should be deleted from cache after evictCacheBlock")
	}
}

func TestAheadRead_EvictCacheBlock_MultiBlockAndZeroSize(t *testing.T) {
	s, arc := newTestStreamerWithAheadRead(t, 6)
	defer arc.Stop()

	firstKey := putCacheBlock(arc, s.inode, 6, 600, 0, util.CacheReadBlockSize, 'A')
	secondKey := putCacheBlock(arc, s.inode, 6, 600, util.CacheReadBlockSize, util.CacheReadBlockSize, 'B')

	s.aheadReadWindow.evictCacheBlock(&ExtentRequest{
		FileOffset: 0,
		Size:       util.CacheReadBlockSize + 1,
		ExtentKey:  &proto.ExtentKey{PartitionId: 6, ExtentId: 600, FileOffset: 0, ExtentOffset: 0, Size: 8 * util.MB},
	})

	if _, ok := arc.blockCache.Load(firstKey); ok {
		t.Fatalf("first block should be evicted")
	}
	if _, ok := arc.blockCache.Load(secondKey); ok {
		t.Fatalf("second block should be evicted")
	}

	zeroKey := putCacheBlock(arc, s.inode, 6, 601, 0, util.CacheReadBlockSize, 'C')
	s.aheadReadWindow.evictCacheBlock(&ExtentRequest{
		FileOffset: 0,
		Size:       0,
		ExtentKey:  &proto.ExtentKey{PartitionId: 6, ExtentId: 601, FileOffset: 0, ExtentOffset: 0, Size: 8 * util.MB},
	})
	if _, ok := arc.blockCache.Load(zeroKey); !ok {
		t.Fatalf("zero-size request should not evict cache blocks")
	}
}

func TestAheadRead_EvictCacheBlock_DebugAndMiss(t *testing.T) {
	_, err := log.InitLog("", "stream_cov", log.DebugLevel, nil, log.DefaultLogLeftSpaceLimitRatio)
	if err != nil {
		t.Fatalf("init log: %v", err)
	}
	t.Cleanup(func() {
		log.SetLogLevelV2(log.InfoLevel)
	})

	s, arc := newTestStreamerWithAheadRead(t, 9)
	defer arc.Stop()

	firstKey := putCacheBlock(arc, s.inode, 9, 900, 0, util.CacheReadBlockSize, 'Z')
	s.aheadReadWindow.evictCacheBlock(&ExtentRequest{
		FileOffset: 0,
		Size:       util.CacheReadBlockSize*2 - 1,
		ExtentKey:  &proto.ExtentKey{PartitionId: 9, ExtentId: 900, FileOffset: 0, ExtentOffset: 0, Size: 8 * util.MB},
	})

	if _, ok := arc.blockCache.Load(firstKey); ok {
		t.Fatalf("first block should be evicted in debug path")
	}
}

func TestStreamerWriteLazyInitAheadReadWindow(t *testing.T) {
	s, arc := newTestStreamerWithAheadReadAndWriteSupport(t, 7)
	defer arc.Stop()
	if proto.Buffers == nil {
		proto.InitBufferPool(32768)
	}
	s.client.AheadRead = arc
	s.aheadReadWindow = nil

	ec := &proto.ExtentKey{
		PartitionId:  7,
		ExtentId:     700,
		FileOffset:   0,
		ExtentOffset: 0,
		Size:         uint32(util.CacheReadBlockSize),
	}
	s.extents.Append(ec, true)
	s.extents.SetSize(uint64(util.CacheReadBlockSize), false)
	s.verSeq = 1
	s.minReadAheadSize = 1

	cacheKey := putCacheBlock(arc, s.inode, 7, 700, 0, util.CacheReadBlockSize, 'D')

	dp := &wrapper.DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{
			PartitionID: 7,
			LeaderAddr:  "",
			Hosts:       []string{},
			Status:      proto.ReadWrite,
		},
		Metrics: wrapper.NewDataPartitionMetrics(),
	}
	dp.ClientWrapper = s.client.dataWrapper
	wrapper.InsertPartitionForTest(s.client.dataWrapper, dp)

	listenerAddr, closeFn := startOverwriteReplyServer(t, proto.OpOk, new(int32), 1)
	defer closeFn()
	dp.LeaderAddr = listenerAddr
	dp.Hosts = []string{listenerAddr}

	data := bytes.Repeat([]byte("a"), 1024)
	req := &ExtentRequest{
		FileOffset:  0,
		Size:        len(data),
		Data:        data,
		ExtentKey:   ec,
		CreateNewEk: false,
	}

	total, err := s.write(data, req.FileOffset, req.Size, 0, nil, 0, false)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	if total != req.Size {
		t.Fatalf("write size mismatch, want %d, got %d", req.Size, total)
	}
	if s.aheadReadWindow == nil {
		t.Fatalf("ahead read window should be initialized lazily")
	}
	if _, ok := arc.blockCache.Load(cacheKey); ok {
		t.Fatalf("cache block should be evicted after write")
	}
}

func TestStreamerTruncateLazyInitAheadReadWindow(t *testing.T) {
	s, arc := newTestStreamerWithAheadReadAndWriteSupport(t, 8)
	defer arc.Stop()

	s.extents.SetSize(uint64(2*util.CacheReadBlockSize), false)
	s.minReadAheadSize = 1
	s.client.AheadRead = arc
	s.aheadReadWindow = nil
	s.handler = nil

	if err := s.truncate(0, "/tmp/test-truncate"); err != nil {
		t.Fatalf("truncate error: %v", err)
	}
	if s.aheadReadWindow == nil {
		t.Fatalf("ahead read window should be initialized lazily on truncate")
	}
}

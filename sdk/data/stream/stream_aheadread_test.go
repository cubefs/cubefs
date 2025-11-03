package stream

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
)

func newTestStreamerWithAheadRead(t *testing.T, partitionID uint64) (*Streamer, *AheadReadCache) {
	t.Helper()

	w := &wrapper.Wrapper{}
	w.InitInnerReq(true)
	// Preload a DataPartition to avoid fetching from master
	dp := &wrapper.DataPartition{}
	dp.PartitionID = partitionID
	dp.ClientWrapper = w
	wrapper.InsertPartitionForTest(w, dp)

	client := &ExtentClient{}
	client.dataWrapper = w
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

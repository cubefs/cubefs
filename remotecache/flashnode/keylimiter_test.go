package flashnode

import (
	"context"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/remotecache/flashnode/cachengine"
)

func TestWaitForRateLimit(t *testing.T) {
	disk := &cachengine.Disk{
		Path:   "/tmp/test",
		Status: proto.ReadWrite,
	}

	keyRateLimitThreshold := int32(1024 * 1024)
	keyLimiterFlow := int32(100)
	allocSize := uint64(2 * 1024 * 1024)

	block := cachengine.NewCacheBlockV2("/cfs_test/tmpfs", "test", "testkey", allocSize, "127.0.0.1", disk, keyRateLimitThreshold, keyLimiterFlow)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := block.WaitForRateLimit(ctx, 10)
	if err != nil {
		t.Fatalf("Expected no error when KeyLimiter is available, got: %v", err)
	}

	smallAllocSize := uint64(512 * 1024)
	block2 := cachengine.NewCacheBlockV2("/cfs_test/tmpfs", "test", "testkey2", smallAllocSize, "127.0.0.1", disk, keyRateLimitThreshold, keyLimiterFlow)

	err = block2.WaitForRateLimit(ctx, 10)
	if err != nil {
		t.Fatalf("Expected no error when KeyLimiter is nil, got: %v", err)
	}
}

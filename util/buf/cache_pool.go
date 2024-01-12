package buf

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
)

var (
	cacheTotalLimit int64
	cacheRateLimit  = rate.NewLimiter(rate.Limit(16), 16)
	cacheCount      int64
	CachePool       *FileCachePool
)

func newWriterCachePool(blockSize int) *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			if atomic.LoadInt64(&cacheCount) >= cacheTotalLimit {
				ctx := context.Background()
				cacheRateLimit.Wait(ctx)
			}
			return make([]byte, blockSize)
		},
	}
}

type FileCachePool struct {
	pool *sync.Pool
}

func InitCachePool(blockSize int) {
	if blockSize == 0 {
		return
	}
	CachePool = &FileCachePool{}
	cacheTotalLimit = int64((4 * util.GB) / blockSize)
	CachePool.pool = newWriterCachePool(blockSize)
}

func (fileCachePool *FileCachePool) Get() []byte {
	atomic.AddInt64(&cacheCount, 1)
	return fileCachePool.pool.Get().([]byte)
}

func (fileCachePool *FileCachePool) Put(data []byte) {
	log.LogInfof("action[FileCachePool.put] %v", fileCachePool)
	log.LogInfof("action[FileCachePool.put] pool %v", fileCachePool.pool)
	atomic.AddInt64(&cacheCount, -1)
	fileCachePool.pool.Put(data)
}

package buf

import (
	"github.com/cubefs/cubefs/util"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	"sync"
	"sync/atomic"
)

var cacheTotalLimit int64
var cacheRateLimit = rate.NewLimiter(rate.Limit(16), 16)
var cacheCount int64
var CachePool *FileCachePool

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
	atomic.AddInt64(&cacheCount, -1)
	fileCachePool.pool.Put(data)
}

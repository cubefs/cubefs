package buf

import (
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
)

var (
	bcacheTotalLimit int64
	bcacheRateLimit  = rate.NewLimiter(rate.Limit(1), 16)
	bcacheCount      int64
	BCachePool       *FileBCachePool
)

func newBlockCachePool(blockSize int) *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			if atomic.LoadInt64(&bcacheCount) >= bcacheTotalLimit {
				log.LogWarnf("FileBCachePool: bcacheCount=(%v),bcacheTotalLimit=(%v)", atomic.LoadInt64(&bcacheCount), bcacheTotalLimit)
				ctx := context.Background()
				bcacheRateLimit.Wait(ctx)
			}
			return make([]byte, blockSize)
		},
	}
}

type FileBCachePool struct {
	pool *sync.Pool
}

func InitbCachePool(blockSize int) {
	if blockSize == 0 {
		return
	}
	BCachePool = &FileBCachePool{}
	bcacheTotalLimit = int64((4 * util.GB) / blockSize)
	BCachePool.pool = newBlockCachePool(blockSize)
}

func (fileCachePool *FileBCachePool) Get() []byte {
	atomic.AddInt64(&bcacheCount, 1)
	return fileCachePool.pool.Get().([]byte)
}

func (fileCachePool *FileBCachePool) Put(data []byte) {
	atomic.AddInt64(&bcacheCount, -1)
	fileCachePool.pool.Put(data)
}

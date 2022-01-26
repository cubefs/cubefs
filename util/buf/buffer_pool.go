package buf

import (
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/util"
)

const (
	HeaderBufferPoolSize = 8192
)

var tinyBuffersTotalLimit int64 = 4096
var tinyBuffersCount int64
var buffersRateLimit = rate.NewLimiter(rate.Limit(16), 16)

func NewTinyBufferPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			if atomic.LoadInt64(&tinyBuffersCount) >= tinyBuffersTotalLimit {
				ctx := context.Background()
				buffersRateLimit.Wait(ctx)
			}
			return make([]byte, util.DefaultTinySizeLimit)
		},
	}
}

// BufferPool defines the struct of a buffered pool with 4 objects.
type BufferPool struct {
	pools    [2]chan []byte
	tinyPool *sync.Pool
}

// NewBufferPool returns a new buffered pool.
func NewBufferPool() (bufferP *BufferPool) {
	bufferP = &BufferPool{}
	bufferP.pools[0] = make(chan []byte, HeaderBufferPoolSize)
	bufferP.pools[1] = make(chan []byte, HeaderBufferPoolSize)
	bufferP.tinyPool = NewTinyBufferPool()
	return bufferP
}

func (bufferP *BufferPool) get(index int, size int) (data []byte) {
	select {
	case data = <-bufferP.pools[index]:
		return
	default:
		return make([]byte, size)
	}
}

// Get returns the data based on the given size. Different size corresponds to different object in the pool.
func (bufferP *BufferPool) Get(size int) (data []byte, err error) {
	if size == util.PacketHeaderSize {
		return bufferP.get(0, size), nil
	} else if size == util.BlockSize {
		return bufferP.get(1, size), nil
	} else if size == util.DefaultTinySizeLimit {
		atomic.AddInt64(&tinyBuffersCount, 1)
		return bufferP.tinyPool.Get().([]byte), nil
	}
	return nil, fmt.Errorf("can only support 45 or 65536 bytes")
}

func (bufferP *BufferPool) put(index int, data []byte) {
	select {
	case bufferP.pools[index] <- data:
		return
	default:
		return
	}
}

// Put puts the given data into the buffer pool.
func (bufferP *BufferPool) Put(data []byte) {
	if data == nil {
		return
	}
	size := len(data)
	if size == util.PacketHeaderSize {
		bufferP.put(0, data)
	} else if size == util.BlockSize {
		bufferP.put(1, data)
	} else if size == util.DefaultTinySizeLimit {
		bufferP.tinyPool.Put(data)
		atomic.AddInt64(&tinyBuffersCount, -1)
	}
	return
}

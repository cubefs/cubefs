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
	InvalidLimit         = 0
)

var tinyBuffersTotalLimit int64 = 4096
var NormalBuffersTotalLimit int64
var HeadBuffersTotalLimit int64

var tinyBuffersCount int64
var normalBuffersCount int64
var headBuffersCount int64

var buffersRateLimit = rate.NewLimiter(rate.Limit(16), 16)
var normalBuffersRateLimit = rate.NewLimiter(rate.Limit(16), 16)
var headBuffersRateLimit = rate.NewLimiter(rate.Limit(16), 16)

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

func NewHeadBufferPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			if HeadBuffersTotalLimit != InvalidLimit && atomic.LoadInt64(&headBuffersCount) >= HeadBuffersTotalLimit {
				ctx := context.Background()
				headBuffersRateLimit.Wait(ctx)
			}
			return make([]byte, util.PacketHeaderSize)
		},
	}
}

func NewNormalBufferPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			if NormalBuffersTotalLimit != InvalidLimit && atomic.LoadInt64(&normalBuffersCount) >= NormalBuffersTotalLimit {
				ctx := context.Background()
				normalBuffersRateLimit.Wait(ctx)
			}
			return make([]byte, util.BlockSize)
		},
	}
}

// BufferPool defines the struct of a buffered pool with 4 objects.
type BufferPool struct {
	pools      [2]chan []byte
	tinyPool   *sync.Pool
	headPool   *sync.Pool
	normalPool *sync.Pool
}

// NewBufferPool returns a new buffered pool.
func NewBufferPool() (bufferP *BufferPool) {
	bufferP = &BufferPool{}
	bufferP.pools[0] = make(chan []byte, HeaderBufferPoolSize)
	bufferP.pools[1] = make(chan []byte, HeaderBufferPoolSize)
	bufferP.tinyPool = NewTinyBufferPool()
	bufferP.headPool = NewHeadBufferPool()
	bufferP.normalPool = NewNormalBufferPool()
	return bufferP
}

func (bufferP *BufferPool) get(index int, size int) (data []byte) {
	select {
	case data = <-bufferP.pools[index]:
		return
	default:
		if index == 0 {
			return bufferP.headPool.Get().([]byte)
		} else {
			return bufferP.normalPool.Get().([]byte)
		}
	}
}

// Get returns the data based on the given size. Different size corresponds to different object in the pool.
func (bufferP *BufferPool) Get(size int) (data []byte, err error) {
	if size == util.PacketHeaderSize {
		atomic.AddInt64(&headBuffersCount, 1)
		return bufferP.get(0, size), nil
	} else if size == util.BlockSize {
		atomic.AddInt64(&normalBuffersCount, 1)
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
		if index == 0 {
			bufferP.headPool.Put(data)
		} else {
			bufferP.normalPool.Put(data)
		}
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
		atomic.AddInt64(&headBuffersCount, -1)
	} else if size == util.BlockSize {
		bufferP.put(1, data)
		atomic.AddInt64(&normalBuffersCount, -1)
	} else if size == util.DefaultTinySizeLimit {
		bufferP.tinyPool.Put(data)
		atomic.AddInt64(&tinyBuffersCount, -1)
	}
	return
}

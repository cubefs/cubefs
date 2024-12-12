package common

import (
	"fmt"
	"math"
	"rdma_test/common/context"
	"rdma_test/common/rate"
	"sync"
	"sync/atomic"
	"time"
)

const (
	HeaderBufferPoolSize = 8192
	InvalidLimit         = 0
)

var ReadBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 32*1024)
		return b
	},
}

var tinyBuffersTotalLimit int64 = 4096
var NormalBuffersTotalLimit int64
var HeadBuffersTotalLimit int64

var tinyBuffersCount int64
var normalBuffersCount int64
var headBuffersCount int64

var normalBufAllocId uint64
var headBufAllocId uint64

var normalBufFreecId uint64
var headBufFreeId uint64

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
			return make([]byte, DefaultTinySizeLimit)
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
			return make([]byte, PacketHeaderSize)
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
			return make([]byte, BlockSize)
		},
	}
}

func NewBinaryPool() *BinaryPool {
	pools := make([]*sync.Pool, 32)
	for i := 0; i < 32; i++ {
		pools[i] = &sync.Pool{
			New: func() interface{} {
				buff := make([]byte, 1<<i)
				return &buff
			},
		}
	}
	return &BinaryPool{pools: pools}
}

func (bp *BinaryPool) Get(size int) []byte {
	var buff *[]byte
	l := int(math.Ceil(math.Log2(float64(size))))
	for buff == nil {
		buff = bp.pools[l].Get().(*[]byte)
		time.Sleep(10 * time.Millisecond)
	}
	return (*buff)[:size]
}

func (bp *BinaryPool) Put(buff []byte) {
	l := int(math.Ceil(math.Log2(float64(cap(buff)))))
	bp.pools[l].Put(&buff)
}

type BinaryPool struct {
	pools []*sync.Pool
}

// BufferPool defines the struct of a buffered pool with 4 objects.
type BufferPool struct {
	headPools   []chan []byte
	normalPools []chan []byte
	tinyPool    *sync.Pool
	headPool    *sync.Pool
	normalPool  *sync.Pool
	binaryPool  *BinaryPool
}

var (
	slotCnt = uint64(16)
)

// NewBufferPool returns a new buffered pool.
func NewBufferPool() (bufferP *BufferPool) {
	bufferP = &BufferPool{}

	bufferP.headPools = make([]chan []byte, slotCnt)
	bufferP.normalPools = make([]chan []byte, slotCnt)
	for i := 0; i < int(slotCnt); i++ {
		bufferP.headPools[i] = make(chan []byte, HeaderBufferPoolSize/slotCnt)
		bufferP.normalPools[i] = make(chan []byte, HeaderBufferPoolSize/slotCnt)
	}

	bufferP.tinyPool = NewTinyBufferPool()
	bufferP.headPool = NewHeadBufferPool()
	bufferP.normalPool = NewNormalBufferPool()
	bufferP.binaryPool = NewBinaryPool()
	return bufferP
}
func (bufferP *BufferPool) getHead(id uint64) (data []byte) {
	select {
	case data = <-bufferP.headPools[id%slotCnt]:
		return
	default:
		return bufferP.headPool.Get().([]byte)
	}
}

func (bufferP *BufferPool) getNoraml(id uint64) (data []byte) {
	select {
	case data = <-bufferP.normalPools[id%slotCnt]:
		return
	default:
		return bufferP.normalPool.Get().([]byte)
	}
}

// Get returns the data based on the given size. Different size corresponds to different object in the pool.
func (bufferP *BufferPool) Get(size int) (data []byte, err error) {
	if size == PacketHeaderSize {
		atomic.AddInt64(&headBuffersCount, 1)
		id := atomic.AddUint64(&headBufAllocId, 1)
		return bufferP.getHead(id), nil
	} else if size == BlockSize {
		atomic.AddInt64(&normalBuffersCount, 1)
		id := atomic.AddUint64(&normalBufAllocId, 1)
		return bufferP.getNoraml(id), nil
	} else if size == DefaultTinySizeLimit {
		atomic.AddInt64(&tinyBuffersCount, 1)
		return bufferP.tinyPool.Get().([]byte), nil
	} else {
		buff := bufferP.binaryPool.Get(size)
		if len(buff) != size {
			return nil, fmt.Errorf("we only can allocate buffer which less than 1 << 31 bytes")
		}
		return buff, nil
	}
}

func (bufferP *BufferPool) putHead(index int, data []byte) {
	select {
	case bufferP.headPools[index] <- data:
		return
	default:
		bufferP.headPool.Put(data)
	}
}

func (bufferP *BufferPool) putNormal(index int, data []byte) {
	select {
	case bufferP.normalPools[index] <- data:
		return
	default:
		bufferP.normalPool.Put(data)
	}
}

// Put puts the given data into the buffer pool.
func (bufferP *BufferPool) Put(data []byte) {
	if data == nil {
		return
	}
	size := len(data)
	if size == PacketHeaderSize {
		atomic.AddInt64(&headBuffersCount, -1)
		id := atomic.AddUint64(&headBufFreeId, 1)
		bufferP.putHead(int(id%slotCnt), data)
	} else if size == BlockSize {
		atomic.AddInt64(&normalBuffersCount, -1)
		id := atomic.AddUint64(&normalBufFreecId, 1)
		bufferP.putNormal(int(id%slotCnt), data)
	} else if size == DefaultTinySizeLimit {
		bufferP.tinyPool.Put(data)
		atomic.AddInt64(&tinyBuffersCount, -1)
	} else {
		bufferP.binaryPool.Put(data)
	}
	return
}

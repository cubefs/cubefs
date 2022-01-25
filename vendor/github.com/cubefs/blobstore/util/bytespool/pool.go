package bytespool

import "sync"

func createAllocFunc(size int) func() interface{} {
	return func() interface{} {
		return make([]byte, size)
	}
}

// The following parameters controls the size of buffer pools.
// There are numPools pools. Starting from 1k size, the size of each pool is sizeMulti of the previous one.
const (
	zeroSize int = 1 << 14 // 16K

	// 1K - 4K - 16K - 64K - 256K - 1M
	numPools      = 6
	sizeMulti     = 4
	startSize int = 1 << 10 // 1K
	maxSize   int = 1 << 20 // 1M
)

var (
	zero = make([]byte, zeroSize)

	pool     [numPools]sync.Pool
	poolSize [numPools]int
)

func init() {
	size := startSize
	for i := 0; i < numPools; i++ {
		pool[i] = sync.Pool{
			New: createAllocFunc(size),
		}
		poolSize[i] = size
		size *= sizeMulti
	}
}

// GetPool returns a sync.Pool that generates bytes array with at least the given size.
// It may return nil if no such pool exists.
func GetPool(size int) *sync.Pool {
	for idx, ps := range poolSize {
		if size <= ps {
			return &pool[idx]
		}
	}
	return nil
}

// Alloc returns a byte slice with at least the given size.
// Minimum size of returned slice is startSize.
// Make a byte slice if oversize.
func Alloc(size int) []byte {
	pool := GetPool(size)
	if pool != nil {
		b := pool.Get().([]byte)
		return b[:size]
	}
	return make([]byte, size)
}

// Free puts a byte slice into the internal pool.
// Discard the byte slice if oversize.
func Free(b []byte) {
	size := int(cap(b))
	if size > maxSize {
		return
	}

	b = b[0:cap(b)]
	for i := numPools - 1; i >= 0; i-- {
		if size >= poolSize[i] {
			pool[i].Put(b) // nolint: staticcheck
			return
		}
	}
}

// Zero clean up the byte slice b to zero
func Zero(b []byte) {
	for len(b) > 0 {
		n := copy(b, zero)
		b = b[n:]
	}
}

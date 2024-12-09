package bytespool

import "sync"

func newBytes(size int) func() interface{} {
	return func() interface{} {
		return make([]byte, size)
	}
}

const (
	zeroSize int = 1 << 14 // 16K

	// 1K - 2K - 4K - 8K - 16K - 32K - 64K - 128K - 256K - 512K - 1M
	numPools      = 11
	sizeStep      = 2
	startSize int = 1 << 10 // 1K
	maxSize   int = 1 << 20 // 1M
)

var (
	zero = make([]byte, zeroSize)

	pools    [numPools]sync.Pool
	poolSize [numPools]int
)

func init() {
	size := startSize
	for ii := 0; ii < numPools; ii++ {
		pools[ii] = sync.Pool{
			New: newBytes(size),
		}
		poolSize[ii] = size
		size *= sizeStep
	}
}

// GetPool returns a sync.Pool that generates bytes slice with the size.
// Return nil if no such pool exists.
func GetPool(size int) *sync.Pool {
	for idx, psize := range poolSize {
		if size <= psize {
			return &pools[idx]
		}
	}
	return nil
}

// Alloc returns a bytes slice with the size.
// Make a new bytes slice if oversize.
func Alloc(size int) []byte {
	if pool := GetPool(size); pool != nil {
		b := pool.Get().([]byte)
		return b[:size]
	}
	return make([]byte, size)
}

// Free puts the bytes slice into suitable pool.
// Discard the bytes slice if oversize.
func Free(b []byte) {
	size := cap(b)
	if size > maxSize {
		return
	}

	b = b[0:size]
	for ii := numPools - 1; ii >= 0; ii-- {
		if size >= poolSize[ii] {
			pools[ii].Put(b) // nolint: staticcheck
			return
		}
	}
}

// Zero clean up the bytes slice b to zero.
func Zero(b []byte) {
	for len(b) > 0 {
		n := copy(b, zero)
		b = b[n:]
	}
}

package buf

type BytePool struct {
	c        chan []byte
	size     int
	capacity int
}

func NewBytePool(capacity, size int) (bp *BytePool) {
	return &BytePool{
		c:        make(chan []byte, capacity),
		size:     size,
		capacity: capacity,
	}
}

func (bp *BytePool) Get() (b []byte) {
	select {
	case b = <-bp.c:
		// reuse existing buffer
	default:
		// create new buffer
		b = make([]byte, bp.size)
	}
	return
}

// Put returns the given Buffer to the BytePool.
func (bp *BytePool) Put(b []byte) {
	if cap(b) < bp.capacity {
		// someone tried to put back a too small buffer, discard it
		return
	}

	select {
	case bp.c <- b[:bp.size]:
		// buffer went back into pool
	default:
		// buffer didn't go back into pool, just discard
	}
}

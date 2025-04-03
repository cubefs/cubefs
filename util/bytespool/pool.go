package bytespool

import (
	"bytes"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

var (
	GetPool = bytespool.GetPool
	Alloc   = bytespool.Alloc
	Free    = bytespool.Free
	Zero    = bytespool.Zero
)

// NewBuffer returns empty buffer with sized capacity.
func NewBuffer(size int) *bytes.Buffer {
	b := bytes.NewBuffer(Alloc(size))
	b.Reset()
	return b
}

// FreeBuffer free the underlying bytes.
func FreeBuffer(b *bytes.Buffer) {
	b.Reset()
	Free(b.Bytes())
}

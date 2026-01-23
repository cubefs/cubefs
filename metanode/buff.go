package metanode

import (
	"sync"

	"github.com/cubefs/cubefs/util/buf"
)

const (
	inodeBufSize  = 40960 // size about 128G
	dentryBufSize = 1024
)

var inodeBufPool = sync.Pool{
	New: func() interface{} {
		return buf.NewByteBufEx(inodeBufSize)
	},
}

var dentryBufPool = sync.Pool{
	New: func() interface{} {
		return buf.NewByteBufEx(dentryBufSize)
	},
}

var readBufPool = sync.Pool{
	New: func() interface{} {
		return buf.NewReadByteBuf()
	},
}

// GetInodeBuf retrieves an inode buffer from the pool
func GetInodeBuf() *buf.ByteBufExt {
	return inodeBufPool.Get().(*buf.ByteBufExt)
}

// PutInodeBuf returns an inode buffer to the pool
// NOTE: the byte data may be modified after calling PutInodeBuf, so do not use the byte data after calling PutInodeBuf
func PutInodeBuf(buf *buf.ByteBufExt) {
	if buf != nil {
		buf.Reset()
		inodeBufPool.Put(buf)
	}
}

// GetDentryBuf retrieves a dentry buffer from the pool
func GetDentryBuf() *buf.ByteBufExt {
	return dentryBufPool.Get().(*buf.ByteBufExt)
}

// PutDentryBuf returns a dentry buffer to the pool
func PutDentryBuf(buf *buf.ByteBufExt) {
	if buf != nil {
		buf.Reset()
		dentryBufPool.Put(buf)
	}
}

// GetReadBuf retrieves a read buffer from the pool
func GetReadBuf(raw []byte) *buf.ReadByteBuff {
	rBuf := readBufPool.Get().(*buf.ReadByteBuff)
	rBuf.SetData(raw)
	return rBuf
}

// PutReadBuf returns a read buffer to the pool
func PutReadBuf(buf *buf.ReadByteBuff) {
	if buf != nil {
		buf.Reset()
		readBufPool.Put(buf)
	}
}

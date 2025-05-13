package metanode

import (
	"sync"

	"github.com/cubefs/cubefs/util/buf"
)

const inodeBufSize = 40960 // size about 128G
const dentryBufSize = 1024

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

func GetInodeBuf() *buf.ByteBufExt {
	return inodeBufPool.Get().(*buf.ByteBufExt)
}

func PutInodeBuf(buf *buf.ByteBufExt) {
	buf.Reset()
	inodeBufPool.Put(buf)
}

func GetDentryBuf() *buf.ByteBufExt {
	return dentryBufPool.Get().(*buf.ByteBufExt)
}

func PutDentryBuf(buf *buf.ByteBufExt) {
	buf.Reset()
	dentryBufPool.Put(buf)
}

var readBufPool = sync.Pool{
	New: func() interface{} {
		return buf.NewReadByteBuf()
	},
}

func GetReadBuf(raw []byte) *buf.ReadByteBuff {
	rBuf := readBufPool.Get().(*buf.ReadByteBuff)
	rBuf.SetData(raw)
	return rBuf
}

func PutReadBuf(buf *buf.ReadByteBuff) {
	buf.Reset()
	readBufPool.Put(buf)
}

package buf

import (
	"fmt"
	"github.com/tiglabs/baudstorage/util"
	"sync"
)

var (
	Buffers = NewBufferPool()
)

type BufferPool struct {
	pools [3]*sync.Pool
}

func NewBufferPool() (bufferP *BufferPool) {
	bufferP = &BufferPool{}
	bufferP.pools[0] = &sync.Pool{New: func() interface{} {
		return make([]byte, util.PacketHeaderSize)
	}}
	bufferP.pools[1] = &sync.Pool{New: func() interface{} {
		return make([]byte, util.BlockSize)
	}}
	bufferP.pools[2] = &sync.Pool{New: func() interface{} {
		return make([]byte, util.ReadBlockSize)
	}}

	return bufferP
}

func (bufferP *BufferPool) Get(size int) (data []byte, err error) {
	if size == util.PacketHeaderSize {
		return bufferP.pools[0].Get().([]byte), nil
	} else if size == util.BlockSize {
		return bufferP.pools[1].Get().([]byte), nil
	} else if size == util.ReadBlockSize {
		return bufferP.pools[2].Get().([]byte), nil
	}
	return nil, fmt.Errorf("can only support 45 or 65536 bytes")
}

func (bufferP *BufferPool) Put(data []byte) {
	if data == nil {
		return
	}
	size := len(data)
	if size != util.BlockSize && size != util.PacketHeaderSize {
		return
	}
	if size == util.PacketHeaderSize {
		bufferP.pools[0].Put(data)
	} else if size == util.BlockSize {
		bufferP.pools[1].Put(data)
	} else if size == util.ReadBlockSize {
		bufferP.pools[2].Put(data)
	}

	return
}

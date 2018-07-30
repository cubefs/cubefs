package fuse

import (
	"sync"
)

const (
	NumOfBlockPool = 9
)

const (
	BlockSize = 4096
)

const (
	PoolSize4K = BlockSize * (1 << iota)
	PoolSize8K
	PoolSize16K
	PoolSize32K
	PoolSize64K
	PoolSize128K
	PoolSize256K
	PoolSize512K
	PoolSize1024K
)

const (
	PoolSizeWithHeader4K = BlockSize*(1<<iota) + OutHeaderSize
	PoolSizeWithHeader8K
	PoolSizeWithHeader16K
	PoolSizeWithHeader32K
	PoolSizeWithHeader64K
	PoolSizeWithHeader128K
	PoolSizeWithHeader256K
	PoolSizeWithHeader512K
	PoolSizeWithHeader1024K
)

var ReadBlockPool = [NumOfBlockPool]*sync.Pool{}

func InitReadBlockPool() {
	ReadBlockPool[0] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSizeWithHeader4K)
	}}
	ReadBlockPool[1] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSizeWithHeader8K)
	}}
	ReadBlockPool[2] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSizeWithHeader16K)
	}}
	ReadBlockPool[3] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSizeWithHeader32K)
	}}
	ReadBlockPool[4] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSizeWithHeader64K)
	}}
	ReadBlockPool[5] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSizeWithHeader128K)
	}}
	ReadBlockPool[6] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSizeWithHeader256K)
	}}
	ReadBlockPool[7] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSizeWithHeader512K)
	}}
	ReadBlockPool[8] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSizeWithHeader1024K)
	}}
}

func GetBlockBuf(size int) []byte {
	var data []byte
	switch size {
	case PoolSize4K:
		data = ReadBlockPool[0].Get().([]byte)
	case PoolSize8K:
		data = ReadBlockPool[1].Get().([]byte)
	case PoolSize16K:
		data = ReadBlockPool[2].Get().([]byte)
	case PoolSize32K:
		data = ReadBlockPool[3].Get().([]byte)
	case PoolSize64K:
		data = ReadBlockPool[4].Get().([]byte)
	case PoolSize128K:
		data = ReadBlockPool[5].Get().([]byte)
	case PoolSize256K:
		data = ReadBlockPool[6].Get().([]byte)
	case PoolSize512K:
		data = ReadBlockPool[7].Get().([]byte)
	case PoolSize1024K:
		data = ReadBlockPool[8].Get().([]byte)
	default:
		data = make([]byte, OutHeaderSize+size)
	}
	return data
}

func PutBlockBuf(data []byte) {
	switch len(data) {
	case PoolSizeWithHeader4K:
		ReadBlockPool[0].Put(data)
	case PoolSizeWithHeader8K:
		ReadBlockPool[1].Put(data)
	case PoolSizeWithHeader16K:
		ReadBlockPool[2].Put(data)
	case PoolSizeWithHeader32K:
		ReadBlockPool[3].Put(data)
	case PoolSizeWithHeader64K:
		ReadBlockPool[4].Put(data)
	case PoolSizeWithHeader128K:
		ReadBlockPool[5].Put(data)
	case PoolSizeWithHeader256K:
		ReadBlockPool[6].Put(data)
	case PoolSizeWithHeader512K:
		ReadBlockPool[7].Put(data)
	case PoolSizeWithHeader1024K:
		ReadBlockPool[8].Put(data)
	default:
		return
	}
}

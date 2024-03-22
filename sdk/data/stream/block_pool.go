package stream

import (
	"sync"

	"github.com/cubefs/cubefs/util/log"
)

const (
	NumOfBlockPool = 10
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
	PoolSize2048K
)

var ReadBlockPool = [NumOfBlockPool]*sync.Pool{}

func InitReadBlockPool() {
	ReadBlockPool[0] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSize4K)
	}}
	ReadBlockPool[1] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSize8K)
	}}
	ReadBlockPool[2] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSize16K)
	}}
	ReadBlockPool[3] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSize32K)
	}}
	ReadBlockPool[4] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSize64K)
	}}
	ReadBlockPool[5] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSize128K)
	}}
	ReadBlockPool[6] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSize256K)
	}}
	ReadBlockPool[7] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSize512K)
	}}
	ReadBlockPool[8] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSize1024K)
	}}
	ReadBlockPool[9] = &sync.Pool{New: func() interface{} {
		return make([]byte, PoolSize2048K)
	}}
}

func PutBlockBuf(data []byte) {
	switch len(data) {
	case PoolSize4K:
		ReadBlockPool[0].Put(data)
	case PoolSize8K:
		ReadBlockPool[1].Put(data)
	case PoolSize16K:
		ReadBlockPool[2].Put(data)
	case PoolSize32K:
		ReadBlockPool[3].Put(data)
	case PoolSize64K:
		ReadBlockPool[4].Put(data)
	case PoolSize128K:
		ReadBlockPool[5].Put(data)
	case PoolSize256K:
		ReadBlockPool[6].Put(data)
	case PoolSize512K:
		ReadBlockPool[7].Put(data)
	case PoolSize1024K:
		ReadBlockPool[8].Put(data)
	case PoolSize2048K:
		ReadBlockPool[9].Put(data)
	default:
		log.LogWarnf("PutBlockBuf: size(%v)", len(data))
		return
	}
}

func GetBlockBuf(size int) []byte {
	var data []byte
	allocSize := computeAllocSize(size)
	switch allocSize {
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
	case PoolSize2048K:
		data = ReadBlockPool[9].Get().([]byte)
	default:
		log.LogInfof("GetBlockBuf: make size(%v)", size)
		data = make([]byte, size)
	}
	if log.EnableDebug() {
		log.LogDebugf("GetBlockBuf: make size(%v) alloc(%v)", size, allocSize)
	}
	return data
}

func computeAllocSize(size int) int {
	if size <= PoolSize4K {
		return PoolSize4K
	} else if size <= PoolSize8K {
		return PoolSize8K
	} else if size <= PoolSize16K {
		return PoolSize16K
	} else if size <= PoolSize32K {
		return PoolSize32K
	} else if size <= PoolSize64K {
		return PoolSize64K
	} else if size <= PoolSize128K {
		return PoolSize128K
	} else if size <= PoolSize256K {
		return PoolSize256K
	} else if size <= PoolSize512K {
		return PoolSize512K
	} else if size <= PoolSize1024K {
		return PoolSize1024K
	} else if size <= PoolSize2048K {
		return PoolSize2048K
	} else {
		return size
	}
}

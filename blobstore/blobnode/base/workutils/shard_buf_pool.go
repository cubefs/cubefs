// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package workutils

import (
	"sync"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// ErrMemNotEnough not enough memory
var ErrMemNotEnough = errors.New("not enough memory")

// BufPoolConfig buf pool config used for data buff
type BufPoolConfig struct {
	BufSizeByte int `json:"buf_size_byte"`
	PoolSize    int `json:"pool_size"`
}

// ByteBufferPool byte buffer pool
type ByteBufferPool struct {
	pool      *sync.Pool
	poolSize  int
	allocSize int

	bufSize int
	l       sync.Mutex
}

// NewByteBufferPool returns byte buffer pool
func NewByteBufferPool(bufSize int, poolSize int) *ByteBufferPool {
	return &ByteBufferPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, bufSize)
			},
		},
		poolSize:  poolSize,
		allocSize: 0,

		bufSize: bufSize,
	}
}

// Get selects an arbitrary item from the Pool, removes it from the
// Pool, and returns it to the caller
func (p *ByteBufferPool) Get() ([]byte, error) {
	p.l.Lock()
	defer p.l.Unlock()

	if p.allocSize >= p.poolSize {
		return nil, ErrMemNotEnough
	}

	p.allocSize++
	return p.pool.Get().([]byte), nil
}

// Put adds x to the pool.
func (p *ByteBufferPool) Put(buf []byte) {
	p.l.Lock()
	defer p.l.Unlock()

	if len(buf) != p.bufSize {
		log.Panicf("unexpected size of buf: len buf[%d], p.bufSize[%d]", len(buf), p.bufSize)
	}

	if p.allocSize <= 0 {
		panic("unexpect:buffer should alloc yet")
	}

	p.allocSize--
	p.pool.Put(buf) // nolint: staticcheck
}

// GetBufSize returns buffer size
func (p *ByteBufferPool) GetBufSize() int {
	return p.bufSize
}

var (
	// BigBufPool used for balance/repair/diskdrop...
	BigBufPool *ByteBufferPool
	// SmallBufPool used for shard repair
	SmallBufPool *ByteBufferPool
)

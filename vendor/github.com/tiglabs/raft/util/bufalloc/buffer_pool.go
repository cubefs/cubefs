// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bufalloc

import (
	"math/rand"
	"sync"

	"github.com/tiglabs/raft/util"
)

const (
	baseSize = 15
	bigSize  = 256 * util.KB
)

var buffPool *bufferPool

func init() {
	buffPool = &bufferPool{
		baseline: [...]int{64, 128, 256, 512, util.KB, 2 * util.KB, 4 * util.KB, 8 * util.KB, 16 * util.KB, 32 * util.KB, 64 * util.KB, 128 * util.KB, 256 * util.KB, 512 * util.KB, util.MB},
	}
	for i, n := range buffPool.baseline {
		buffPool.pool[i] = createPool(n)
	}
	buffPool.pool[baseSize] = createPool(0)
}

func createPool(n int) (opool *optimizePool) {
	opool=new(optimizePool)
	for i:=0;i<MaxPoolCnt;i++ {
		opool.poolArr[i] = &sync.Pool{
			New: func() interface{} {
				if n == 0 || n > bigSize {
					return &ibuffer{}
				}
				return &ibuffer{buf: makeSlice(n)}
			},
		}
	}
	return
}

const (
	MaxPoolCnt = 32
)

type optimizePool struct {
	poolArr [MaxPoolCnt]*sync.Pool
}

func (op *optimizePool)Get() interface{}{
	index:=rand.Int()%MaxPoolCnt
	return op.poolArr[index].Get()
}

func (op *optimizePool)Put(x interface{}){
	index:=rand.Int()%MaxPoolCnt
	op.poolArr[index].Put(x)
}

type bufferPool struct {
	baseline [baseSize]int
	pool     [baseSize + 1]*optimizePool
}

func (p *bufferPool) getPoolNum(n int) int {
	for i, x := range p.baseline {
		if n <= x {
			return i
		}
	}
	return baseSize
}

func (p *bufferPool) getBuffer(n int) Buffer {
	num := p.getPoolNum(n)
	pool := p.pool[num]
	buf := pool.Get().(Buffer)
	if buf.Cap() < n {
		// return old buffer to pool
		buffPool.putBuffer(buf)
		buf = &ibuffer{buf: makeSlice(n)}
	}
	buf.Reset()
	return buf
}

func (p *bufferPool) putBuffer(buf Buffer) {
	num := p.getPoolNum(buf.Cap())
	pool := p.pool[num]
	pool.Put(buf)
}

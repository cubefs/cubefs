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

package resourcepool

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// cache in chan pool will not be released by runtime.GC().
// pool chan length dynamicly changes by EMA(Exponential Moving Average).
// redundant buffers will released by GC.

const maxMemorySize = 1 << 32 // 4G

// type SliceHeader is 24 bytes.
// buffered channel malloc memory in one call.
// `makechan` see more at: https://github.com/golang/go/blob/master/src/runtime/chan.go
//
// limit max channel memory to 96MB (24 * (1<<22)),
// can reduce to 32MB if using type *[]byte.
const maxChanSize = 1 << 22 // 4m

var releaseInterval int64 = int64(time.Minute) * 2

// SetReleaseInterval set release interval duration
func SetReleaseInterval(duration time.Duration) {
	if duration > time.Millisecond*100 {
		atomic.StoreInt64(&releaseInterval, int64(duration))
	}
}

type chPool struct {
	chBuffer    chan []byte
	newBuffer   func() []byte
	capacity    int
	concurrence int32
	waitOnLimit bool

	closeCh   chan struct{}
	closeOnce sync.Once

	// mu guards wait-on-limit updates of concurrence. A caller that cannot
	// fit holds the lock and sleeps on cond; it does not sit in a waiter queue
	// and it does not own any slot until the whole request fits.
	mu   sync.Mutex
	cond *sync.Cond
}

// NewChanPool return Pool with capacity.
// A negative capacity uses a default capacity based on maxMemorySize and buffer size.
func NewChanPool(newFunc func() []byte, capacity int, waitOnLimit bool) Pool {
	chCap := capacity
	if chCap < 0 {
		buf := newFunc()
		chCap = maxMemorySize / len(buf)
	}
	if chCap > maxChanSize {
		chCap = maxChanSize
	}

	pool := &chPool{
		chBuffer:    make(chan []byte, chCap),
		newBuffer:   newFunc,
		capacity:    chCap,
		waitOnLimit: waitOnLimit,
		closeCh:     make(chan struct{}),
	}
	pool.cond = sync.NewCond(&pool.mu)
	runtime.SetFinalizer(pool, func(p *chPool) {
		p.closeOnce.Do(func() {
			close(p.closeCh)
		})
	})

	go pool.loopRelease()
	return pool
}

// loopRelease release redundant buffers in chan.
// check EMA concurrence per round of release interval duration.
// release the redundant buffers per release interval duration.
//
// reserve 30% redundancy of capacity
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// |      length of buffer chan        |     concurrence     |
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// |  redundant  |            capacity            | reserved |
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// | to release  |          buffers keep in memory           |
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
func (p *chPool) loopRelease() {
	const emaRound = 5

	ticker := time.NewTicker(time.Duration(atomic.LoadInt64(&releaseInterval)) / emaRound)
	defer ticker.Stop()

	var (
		turn     int
		capacity int32
	)
	for {
		select {
		case <-p.closeCh:
			for {
				select {
				case <-p.chBuffer:
				default:
					return
				}
			}
		case <-ticker.C:
			nowConc := atomic.LoadInt32(&p.concurrence)
			capacity = ema(nowConc, capacity)
			if turn = (turn + 1) % emaRound; turn != 0 {
				continue
			}

			capa := capacity * 13 / 10
			redundant := len(p.chBuffer) + int(nowConc-capa)
			if redundant <= 0 {
				continue
			}
			has := true
			for ii := 0; has && ii < redundant; ii++ {
				select {
				case <-p.chBuffer:
				default:
					has = false
				}
			}
		}
	}
}

func (p *chPool) Get(ctx context.Context) (interface{}, error) {
	if !p.waitOnLimit {
		return p.getImmediate()
	}
	if err := p.reserve(ctx, 1); err != nil {
		return nil, err
	}
	return p.takeBuf(), nil
}

// GetN reserves n buffers under the pool lock. With wait-on-limit the caller
// sleeps until all n fit, and owns none of them while asleep.
func (p *chPool) GetN(ctx context.Context, n int) ([]interface{}, error) {
	if n <= 0 {
		return nil, nil
	}
	if !p.waitOnLimit {
		bufs := make([]interface{}, n)
		for i := range bufs {
			buf, err := p.getImmediate()
			if err != nil {
				for _, allocated := range bufs[:i] {
					p.Put(allocated)
				}
				return nil, err
			}
			bufs[i] = buf
		}
		return bufs, nil
	}
	if err := p.reserve(ctx, n); err != nil {
		return nil, err
	}
	bufs := make([]interface{}, n)
	for i := range bufs {
		bufs[i] = p.takeBuf()
	}
	return bufs, nil
}

func (p *chPool) getImmediate() (interface{}, error) {
	atomic.AddInt32(&p.concurrence, 1)
	return p.takeBuf(), nil
}

func (p *chPool) takeBuf() []byte {
	select {
	case buf := <-p.chBuffer:
		return buf
	default:
		return p.newBuffer()
	}
}

// reserve owns n slots, or returns before taking any.
// The caller holds mu and sleeps on cond until the batch fits. A later
// request that already fits is not lined up behind it.
func (p *chPool) reserve(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.capacity > 0 && n > p.capacity {
		return ErrPoolLimit
	}

	p.mu.Lock()
	if err := p.waitLocked(ctx, n); err != nil {
		p.mu.Unlock()
		return err
	}
	atomic.AddInt32(&p.concurrence, int32(n))
	p.mu.Unlock()
	return nil
}

// waitLocked sleeps until n slots fit. The caller holds mu.
// ctx cancel and pool close wake the sleep; neither path keeps the slots.
func (p *chPool) waitLocked(ctx context.Context, n int) error {
	if p.slotsFree(n) {
		return p.closedOrCanceled(ctx)
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
		case <-p.closeCh:
		case <-done:
			return
		}
		p.mu.Lock()
		p.cond.Broadcast()
		p.mu.Unlock()
	}()

	for !p.slotsFree(n) {
		if err := p.closedOrCanceled(ctx); err != nil {
			return err
		}
		p.cond.Wait()
	}
	return p.closedOrCanceled(ctx)
}

func (p *chPool) closedOrCanceled(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-p.closeCh:
		return ErrPoolClosed
	default:
		return nil
	}
}

func (p *chPool) slotsFree(n int) bool {
	if p.capacity <= 0 {
		return true
	}
	return int(atomic.LoadInt32(&p.concurrence))+n <= p.capacity
}

func (p *chPool) Put(x interface{}) {
	buf, ok := x.([]byte)
	if !ok {
		return
	}

	select {
	case p.chBuffer <- buf:
	default:
	}
	if !p.waitOnLimit {
		atomic.AddInt32(&p.concurrence, -1)
		return
	}
	p.mu.Lock()
	atomic.AddInt32(&p.concurrence, -1)
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *chPool) Cap() int {
	return p.capacity
}

func (p *chPool) Len() int {
	return int(atomic.LoadInt32(&p.concurrence))
}

func (p *chPool) Idle() int {
	return len(p.chBuffer)
}

func ema(val, lastVal int32) int32 {
	return (val*2 + lastVal*8) / 10
}

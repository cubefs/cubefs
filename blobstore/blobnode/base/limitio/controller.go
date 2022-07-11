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

package limitio

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	tickWindow      = 100 * time.Millisecond
	minLimitPerTick = 1
)

type Controller interface {
	Assign(int64) (int64, bool)
	Fill(int64)
	UpdateCapacity(ratePerSecond int)
	Close() error
}

type AtomicController struct {
	remain   int64 // read/write
	capacity int64 // read/write
	limit    int64 // limits per second. readonly
	hit      uint64
	cond     *sync.Cond
	done     chan struct{}
	once     *sync.Once
}

func (c *AtomicController) UpdateCapacity(ratePerSecond int) {
	if ratePerSecond <= 0 {
		return
	}
	limit := int64(ratePerSecond * int(tickWindow) / int(time.Second))
	if limit < minLimitPerTick {
		limit = minLimitPerTick
	}
	atomic.StoreInt64(&c.capacity, limit)
}

func (c *AtomicController) fastAssign(size int64) (n int64, get bool) {
	var curr int64

	curr = atomic.LoadInt64(&c.remain)
	for {
		var remain int64
		if curr == 0 {
			break
		}
		if curr >= size {
			remain, n = curr-size, size
		} else {
			remain, n = 0, curr
		}
		swapped := atomic.CompareAndSwapInt64(&c.remain, curr, remain)
		if swapped {
			return n, true
		}
		curr = atomic.LoadInt64(&c.remain)
	}

	return
}

func (c *AtomicController) Assign(size int64) (n int64, limited bool) {
	// fast path
	if n, get := c.fastAssign(size); get {
		return n, false
	}

	// slow path
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
retry:
	atomic.AddUint64(&c.hit, 1)
	for atomic.LoadInt64(&c.remain) == 0 {
		c.cond.Wait()
	}
	n, get := c.fastAssign(size)
	if !get {
		goto retry
	}
	return n, true
}

func (c *AtomicController) Fill(size int64) {
	if size <= 0 {
		return
	}

	var curr int64
	curr = atomic.LoadInt64(&c.remain)
	for {
		var remain int64

		capacity := atomic.LoadInt64(&c.capacity)
		remain += curr + size

		if remain > capacity {
			remain = capacity
		}
		swapped := atomic.CompareAndSwapInt64(&c.remain, curr, remain)
		if swapped {
			break
		}
		curr = atomic.LoadInt64(&c.remain)
	}
	c.cond.Broadcast()
}

func (c *AtomicController) run() {
	ticker := time.NewTicker(tickWindow)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			capacity := atomic.LoadInt64(&c.capacity)
			atomic.StoreInt64(&c.remain, capacity)
			c.cond.Broadcast()
		}
	}
}

func (c *AtomicController) Close() error {
	c.once.Do(func() {
		c.done <- struct{}{}
	})
	return nil
}

func NewController(limitPerSec int64) *AtomicController {
	capacity := limitPerSec * int64(tickWindow) / int64(time.Second)
	if capacity < minLimitPerTick {
		capacity = minLimitPerTick
	}

	c := &AtomicController{
		remain:   capacity,
		capacity: capacity,
		limit:    limitPerSec,
		cond:     sync.NewCond(new(sync.Mutex)),
		done:     make(chan struct{}, 1),
		once:     &sync.Once{},
	}
	go c.run()
	return c
}

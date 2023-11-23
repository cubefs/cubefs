// Copyright 2023 The CubeFS Authors.
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

// Package ratelimit RateLimit Algorithm Based on Token Bucket
package flowctrl

import (
	"io"
	"sync"
	"time"
)

const (
	tokenWindow  = 50 * time.Millisecond
	minWindowCap = 64
)

type Controller struct {
	capacity      int
	threshold     int
	cond          *sync.Cond
	done          chan struct{}
	ratePerSecond int
	closeOnce     *sync.Once
}

func NewController(ratePerSecond int) *Controller {
	capacity := ratePerSecond * int(tokenWindow) / int(time.Second)
	if capacity < minWindowCap {
		capacity = minWindowCap
	}
	self := &Controller{
		ratePerSecond: ratePerSecond,
		threshold:     capacity,
		capacity:      capacity,
		cond:          sync.NewCond(new(sync.Mutex)),
		done:          make(chan struct{}, 1),
		closeOnce:     &sync.Once{},
	}
	// produce token asynchronously
	go self.run(capacity)
	return self
}

func (self *Controller) acquire(size int) int {
	self.cond.L.Lock()
	for self.capacity == 0 {
		self.cond.Wait()
	}
	if size > self.capacity {
		size = self.capacity
	}
	self.capacity -= size
	self.cond.L.Unlock()
	return size
}

func (self *Controller) fill(size int) {
	if size <= 0 {
		return
	}
	self.cond.L.Lock()
	self.capacity += size
	if self.capacity > self.threshold {
		self.capacity = self.threshold
	}
	self.cond.L.Unlock()
	self.cond.Broadcast()
}

func (self *Controller) run(capacity int) {
	t := time.NewTicker(tokenWindow)
	for {
		select {
		case <-t.C:
			self.cond.L.Lock()
			self.capacity = capacity
			self.cond.L.Unlock()
			self.cond.Broadcast()
		case <-self.done:
			t.Stop()
			return
		}
	}
}

// Close release token-producing goroutine
func (self *Controller) Close() error {
	self.closeOnce.Do(func() {
		self.done <- struct{}{}
	})
	return nil
}

func (self *Controller) Reader(underlying io.Reader) io.Reader {
	return &rateReader{
		underlying: underlying,
		c:          self,
	}
}

func (self *Controller) Writer(underlying io.Writer) io.Writer {
	return &rateWriter{
		underlying: underlying,
		c:          self,
	}
}

func (self *Controller) GetRateLimit() int {
	return self.ratePerSecond
}

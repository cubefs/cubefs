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

package count

import (
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/util/limit"
)

const minusOne = ^uint32(0)

type countLimit struct {
	limit   uint32
	current uint32
}

// New returns limiter with concurrent n
func New(n int) limit.Limiter {
	return &countLimit{limit: uint32(n)}
}

func (l *countLimit) Running() int {
	return int(atomic.LoadUint32(&l.current))
}

func (l *countLimit) Acquire(keys ...interface{}) error {
	if atomic.AddUint32(&l.current, 1) > l.limit {
		atomic.AddUint32(&l.current, minusOne)
		return limit.ErrLimited
	}
	return nil
}

func (l *countLimit) Release(keys ...interface{}) {
	atomic.AddUint32(&l.current, minusOne)
}

type blockingCountLimit struct {
	ch chan struct{}
}

// NewBlockingCount returns limiter with concurrent n
// Blocking acquire if no available concurrence
func NewBlockingCount(n int) limit.Limiter {
	ch := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		ch <- struct{}{}
	}
	return &blockingCountLimit{ch: ch}
}

func (l *blockingCountLimit) Running() int {
	return cap(l.ch) - len(l.ch)
}

func (l *blockingCountLimit) Acquire(keys ...interface{}) error {
	<-l.ch
	return nil
}

func (l *blockingCountLimit) Release(keys ...interface{}) {
	l.ch <- struct{}{}
}

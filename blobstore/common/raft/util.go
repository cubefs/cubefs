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

package raft

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

func newIDGenerator(nodeId uint64, now time.Time) *idGenerator {
	prefix := nodeId << 48
	unixMilli := uint64(now.UnixNano()) / uint64(time.Millisecond/time.Nanosecond)
	suffix := lowBit(unixMilli, 40) << 8
	return &idGenerator{
		prefix: prefix,
		suffix: suffix,
	}
}

// idGenerator generate unique id for propose request
// generated id contains these part of below
// | prefix   | suffix              |
// | 2 bytes  | 5 bytes   | 1 byte  |
// | memberID | timestamp | cnt     |
type idGenerator struct {
	// high order 2 bytes
	prefix uint64
	// low order 6 bytes
	suffix uint64
}

// Next generates a id that is unique.
func (g *idGenerator) Next() uint64 {
	suffix := atomic.AddUint64(&g.suffix, 1)
	return g.prefix | lowBit(suffix, 48)
}

func newNotify(timeoutCtx context.Context) notify {
	n := *notifyPool.Get().(*notify)
	n.timeoutCtx = timeoutCtx
	return n
}

type notify struct {
	ch         chan proposalResult
	timeoutCtx context.Context
}

func (n notify) Notify(ret proposalResult) {
	select {
	case n.ch <- ret:
	default:
	}
}

func (n notify) Wait(ctx context.Context) (ret proposalResult, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-n.timeoutCtx.Done():
		err = errors.New("notify timeout")
		return
	case ret = <-n.ch:
		return
	}
}

// Release put notify back into pool
// Note that don't Release Wait error notify into pool
func (n notify) Release() {
	notifyPool.Put(&n)
}

// reuse with notify pool
var notifyPool = sync.Pool{
	New: func() interface{} {
		return &notify{
			ch: make(chan proposalResult, 1),
		}
	},
}

func lowBit(x uint64, n uint) uint64 {
	return x & (math.MaxUint64 >> (64 - n))
}

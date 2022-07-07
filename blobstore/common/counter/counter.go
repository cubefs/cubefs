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

package counter

import (
	"sync/atomic"

	"github.com/benbjohnson/clock"
)

const (
	// SLOT counter resulting stored.
	SLOT int64 = 20

	// interval default is one minute.
	interval int64 = 60
)

// time clock just for testing mock.
var time = clock.New()

// Counter resulting count in default interval period.
// It is thread safe.
type Counter struct {
	counts     [SLOT]int64
	timestamps [SLOT]int64
}

// Add counts once in now.
func (c *Counter) Add() {
	c.AddN(1)
}

// AddN adds n in now.
func (c *Counter) AddN(n int) {
	now := time.Now().Unix() / interval
	index := now % SLOT

	if ts := atomic.LoadInt64(&c.timestamps[index]); ts == now {
		atomic.AddInt64(&c.counts[index], int64(n))
	} else {
		atomic.StoreInt64(&c.counts[index], int64(n))
		atomic.StoreInt64(&c.timestamps[index], now)
	}
}

// Show returns result from oldest to newest.
func (c *Counter) Show() (counts [SLOT]int) {
	now := time.Now().Unix() / interval
	index := now % SLOT

	validTs := now - SLOT
	for ii := int64(0); ii < SLOT; ii++ {
		idx := (index + 1 + ii) % SLOT
		if ts := atomic.LoadInt64(&c.timestamps[idx]); ts > validTs {
			counts[ii] = int(atomic.LoadInt64(&c.counts[idx]))
		}
	}
	return
}

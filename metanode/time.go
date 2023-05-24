// Copyright 2018 The CubeFS Authors.
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

package metanode

import (
	"sync"
	"sync/atomic"
	"time"
)

// Now
var Now = NewNowTime()

// NowTime defines the current time.
type NowTime struct {
	sync.RWMutex
	now      time.Time
	timeUnix int64
}

// GetCurrentTime returns the current time.
func (t *NowTime) GetCurrentTime() (now time.Time) {
	t.RLock()
	now = t.now
	t.RUnlock()
	return
}

// GetCurrentTimeUnix returns the current time unix
func (t *NowTime) GetCurrentTimeUnix() int64 {
	return atomic.LoadInt64(&t.timeUnix)
}

// UpdateTime updates the stored time.
func (t *NowTime) UpdateTime(now time.Time) {
	t.Lock()
	t.now = now
	t.Unlock()
}

// UpdateTimeUnix updates the stored time unix.
func (t *NowTime) UpdateTimeUnix(now int64) {
	atomic.StoreInt64(&t.timeUnix, now)
}

// NewNowTime returns a new NowTime.
func NewNowTime() *NowTime {
	return &NowTime{
		now:      time.Now(),
		timeUnix: time.Now().Unix(),
	}
}

func init() {
	go func() {
		for {
			time.Sleep(time.Second)
			now := time.Now()
			Now.UpdateTime(now)
			Now.UpdateTimeUnix(now.Unix())
		}
	}()
}

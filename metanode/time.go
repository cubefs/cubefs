// Copyright 2018 The Cubefs Authors.
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
	"time"
)

// Now
var Now = NewNowTime()

// NowTime defines the current time.
type NowTime struct {
	sync.RWMutex
	now time.Time
}

// GetCurrentTime returns the current time.
func (t *NowTime) GetCurrentTime() (now time.Time) {
	t.RLock()
	now = t.now
	t.RUnlock()
	return
}

// UpdateTime updates the stored time.
func (t *NowTime) UpdateTime(now time.Time) {
	t.Lock()
	t.now = now
	t.Unlock()
}

// NewNowTime returns a new NowTime.
func NewNowTime() *NowTime {
	return &NowTime{
		now: time.Now(),
	}
}

func init() {
	go func() {
		for {
			time.Sleep(time.Second)
			now := time.Now()
			Now.UpdateTime(now)
		}
	}()
}

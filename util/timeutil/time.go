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

package timeutil

import (
	"sync"
	"sync/atomic"
	"time"
)

// GetCurrentTime returns the current time.
func GetCurrentTime() (now time.Time) {
	return n.getCurrentTime()
}

// GetCurrentTimeUnix returns the current time unix.
func GetCurrentTimeUnix() int64 {
	return n.getCurrentTimeUnix()
}

// now
var n = newNowTime()

// nowTime defines the current time.
type nowTime struct {
	sync.RWMutex
	now      time.Time
	timeUnix int64
}

// GetCurrentTime returns the current time.
func (t *nowTime) getCurrentTime() (now time.Time) {
	t.RLock()
	now = t.now
	t.RUnlock()
	return
}

// GetCurrentTimeUnix returns the current time unix.
func (t *nowTime) getCurrentTimeUnix() int64 {
	return atomic.LoadInt64(&t.timeUnix)
}

// updateTime updates the stored time.
func (t *nowTime) updateTime(now time.Time) {
	t.Lock()
	t.now = now
	t.Unlock()
}

// updateTimeUnix updates the stored time unix.
func (t *nowTime) updateTimeUnix(now int64) {
	atomic.StoreInt64(&t.timeUnix, now)
}

// newNowTime returns a new nowTime.
func newNowTime() *nowTime {
	return &nowTime{
		now:      time.Now(),
		timeUnix: time.Now().Unix(),
	}
}

func init() {
	go func() {
		for {
			time.Sleep(time.Second)
			now := time.Now()
			n.updateTime(now)
			n.updateTimeUnix(now.Unix())
		}
	}()
}

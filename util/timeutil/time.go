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
	"sync/atomic"
	"time"
)

// GetCurrentTime returns the current time.
func GetCurrentTime() time.Time {
	return now.time.Load().(time.Time)
}

// GetCurrentTimeUnix returns the current time unix.
func GetCurrentTimeUnix() int64 {
	return atomic.LoadInt64(&now.timeUnix)
}

var now = newNowTime()

// nowTime defines the current time.
type nowTime struct {
	time     atomic.Value // store time.Time
	timeUnix int64
}

// newNowTime returns a new nowTime.
func newNowTime() *nowTime {
	n := time.Now()
	t := &nowTime{timeUnix: n.Unix()}
	t.time.Store(n)

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			<-ticker.C
			n := time.Now()
			t.time.Store(n)
			atomic.StoreInt64(&t.timeUnix, n.Unix())
		}
	}()
	return t
}

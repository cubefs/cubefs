// Copyright 2024 The CubeFS Authors.
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

package util

import (
	"runtime/debug"
	"time"

	"github.com/cubefs/cubefs/util/loadutil"
)

type RecycleTimer struct {
	interval        time.Duration
	recyclePercent  float64
	minRecycleLimit uint64
	panicHook       func(r interface{})
	statHook        func(totalPercent float64, currentProcess uint64, currentGoHeap uint64)
	stopC           chan interface{}
}

func (r *RecycleTimer) emitPanicHook(p interface{}) {
	if handle := r.panicHook; handle != nil {
		handle(p)
	}
}

func (r *RecycleTimer) emitStatHook(totalPercent float64, currentProcess uint64, currentGoHeap uint64) {
	if handle := r.statHook; handle != nil {
		handle(totalPercent, currentProcess, currentGoHeap)
	}
}

func (r *RecycleTimer) start() (err error) {
	total, err := loadutil.GetTotalMemory()
	if err != nil {
		return
	}
	go func() {
		defer func() {
			if p := recover(); p != nil {
				r.emitPanicHook(p)
			}
		}()

		timer := time.NewTicker(r.interval)
		defer timer.Stop()
		for {
			select {
			case <-r.stopC:
				return
			case <-timer.C:
				used, err := loadutil.GetUsedMemory()
				if err != nil {
					return
				}
				percent := float64(used) / float64(total)
				if percent >= r.recyclePercent {
					used, err = loadutil.GetCurrentProcessMemory()
					if err != nil {
						return
					}
					goHeapSize := loadutil.GetGoInUsedHeap()
					r.emitStatHook(percent, used, goHeapSize)
					if used > goHeapSize+r.minRecycleLimit {
						debug.FreeOSMemory()
					}
				}
			}
		}
	}()
	return
}

func (r *RecycleTimer) Stop() {
	close(r.stopC)
}

func (r *RecycleTimer) SetPanicHook(handle func(r interface{})) {
	r.panicHook = handle
}

func (r *RecycleTimer) SetStatHook(handle func(totalPercent float64, currentProcess uint64, currentGoHeap uint64)) {
	r.statHook = handle
}

func NewRecycleTimer(interval time.Duration, recyclePercent float64, minRecycleLimit uint64) (r *RecycleTimer, err error) {
	r = &RecycleTimer{
		interval:        interval,
		recyclePercent:  recyclePercent,
		minRecycleLimit: minRecycleLimit,
		stopC:           make(chan interface{}),
	}
	err = r.start()
	if err != nil {
		r.Stop()
		return
	}
	return
}

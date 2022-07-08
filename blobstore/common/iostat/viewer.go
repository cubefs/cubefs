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

package iostat

import (
	"sync/atomic"
	"time"
)

type IOViewer interface {
	ReadStat() *StatData
	WriteStat() *StatData
	Update()
	Close()
}

type StatIterator struct {
	Stat     *Stat                 // share memory
	LastStat atomic.Value          // :Stat , last stat
	IoStat   [maxType]atomic.Value // :StatData
	lastTime int64                 // nanosecond
}

type Viewer struct {
	StatIterator
	done chan struct{}
}

func (iter *StatIterator) Update() {
	if iter == nil || iter.Stat == nil {
		return
	}

	var currStat Stat

	iter.Stat.CopyReadStat(&currStat)
	iter.Stat.CopyWriteStat(&currStat)

	curTime := time.Now().UnixNano()
	lastTime := atomic.LoadInt64(&iter.lastTime)

	interval := curTime - lastTime
	if interval <= 0 {
		interval = 1
	}

	for i := 0; i < maxType; i++ {
		var rt StatData
		var lastStat Stat

		v := iter.LastStat.Load()
		if v != nil {
			lastStat = *(v.(*Stat))
		}

		last := &lastStat.items[i]
		item := &currStat.items[i]

		count := item.cnt - last.cnt
		if count <= 0 {
			count = 1
		}

		rt.Iops = (item.cnt - last.cnt) * 1e9 / uint64(interval)
		rt.Bps = (item.size - last.size) * 1e9 / uint64(interval)
		rt.Avgrq = (item.size - last.size) / count
		rt.Avgqu = item.inque
		rt.Await = int64((item.latencies - last.latencies) / count)

		iter.IoStat[i].Store(&rt)
	}

	iter.LastStat.Store(&currStat)
	atomic.StoreInt64(&iter.lastTime, curTime)
}

func (iov *Viewer) run(interval int64) {
	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-iov.done:
			return
		case <-ticker.C:
			iov.Update()
		}
	}
}

func (iov *Viewer) ReadStat() *StatData {
	st := iov.IoStat[readStatType].Load()
	if st == nil {
		return &StatData{}
	}
	return st.(*StatData)
}

func (iov *Viewer) WriteStat() *StatData {
	st := iov.IoStat[writeStatType].Load()
	if st == nil {
		return &StatData{}
	}
	return st.(*StatData)
}

func (iov *Viewer) Close() {
	close(iov.done)
}

func NewIOViewer(stat *Stat, intervalMs int64) (iov *Viewer) {
	iov = &Viewer{}
	iov.Stat = stat

	if intervalMs <= 0 {
		intervalMs = 500 // 500 ms
	}

	go iov.run(intervalMs)

	return iov
}

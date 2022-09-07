// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import "container/list"

type openFunc func(logFileName) (*logEntryFile, error)

type logFileCache struct {
	capacity int

	l *list.List
	m map[logFileName]*list.Element // key是seq

	f openFunc
}

func newLogFileCache(capacity int, f openFunc) *logFileCache {
	return &logFileCache{
		capacity: capacity,
		l:        list.New(),
		m:        make(map[logFileName]*list.Element, capacity),
		f:        f,
	}
}

func (lc *logFileCache) Get(name logFileName) (lf *logEntryFile, err error) {
	e, ok := lc.m[name]
	if ok {
		lf = (e.Value).(*logEntryFile)
		lc.l.MoveToFront(e)
		return
	}

	// 不存在打开新的
	lf, err = lc.f(name)
	if err != nil {
		return
	}
	// 缓存
	e = lc.l.PushFront(lf)
	lc.m[name] = e

	// keep capacity
	if err = lc.keepCapacity(); err != nil {
		return nil, err
	}
	return
}

func (lc *logFileCache) Put(name logFileName, lf *logEntryFile) (err error) {
	_, ok := lc.m[name]
	if ok {
		return
	}
	e := lc.l.PushFront(lf)
	lc.m[name] = e

	// keep capacity
	if err = lc.keepCapacity(); err != nil {
		return err
	}
	return
}

func (lc *logFileCache) Delete(name logFileName, close bool) error {
	e, ok := lc.m[name]
	if !ok {
		return nil
	}

	lf := e.Value.(*logEntryFile)
	if close {
		lf.DecreaseRef()
		if err := lf.Close(); err != nil {
			return err
		}
	}
	delete(lc.m, lf.Name())
	lc.l.Remove(e)
	return nil
}

func (lc *logFileCache) keepCapacity() (err error) {
	for lc.l.Len() > lc.capacity {
		e := lc.l.Back()
		df := (e.Value).(*logEntryFile)
		if err = lc.Delete(df.Name(), true); err != nil {
			return
		}
	}
	return
}

func (lc *logFileCache) Close() (err error) {
	for _, e := range lc.m {
		f := (e.Value).(*logEntryFile)
		err = f.Close()
	}
	return
}

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

package wal

import "container/list"

type openFunc func(logName) (*logFile, error)

type logFileCache struct {
	capacity int
	l        *list.List
	m        map[logName]*list.Element
	f        openFunc
}

func newLogFileCache(capacity int, f openFunc) *logFileCache {
	return &logFileCache{
		capacity: capacity,
		l:        list.New(),
		m:        make(map[logName]*list.Element, capacity),
		f:        f,
	}
}

func (lc *logFileCache) Get(name logName) (lf *logFile, err error) {
	e, ok := lc.m[name]
	if ok {
		lf = (e.Value).(*logFile)
		lc.l.MoveToFront(e)
		return
	}

	lf, err = lc.f(name)
	if err != nil {
		return
	}
	e = lc.l.PushFront(lf)
	lc.m[name] = e

	for lc.l.Len() > lc.capacity {
		e = lc.l.Back()
		df := (e.Value).(*logFile)
		lc.Delete(df.Name())
	}
	return
}

func (lc *logFileCache) Delete(name logName) error {
	e, ok := lc.m[name]
	if !ok {
		return nil
	}

	delete(lc.m, name)
	lc.l.Remove(e)
	return nil
}

func (lc *logFileCache) Close() (err error) {
	for name, e := range lc.m {
		delete(lc.m, name)
		lc.l.Remove(e)
	}
	return
}

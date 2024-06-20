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

package atomicutil

import "sync/atomic"

type Int64 struct {
	v int64
}

func (i *Int64) Load() (v int64) {
	v = atomic.LoadInt64(&i.v)
	return
}

func (i *Int64) Store(v int64) {
	atomic.StoreInt64(&i.v, v)
}

func (i *Int64) CompareAndSwap(old int64, new int64) (swaped bool) {
	swaped = atomic.CompareAndSwapInt64(&i.v, old, new)
	return
}

func (i *Int64) Add(v int64) (new int64) {
	new = atomic.AddInt64(&i.v, v)
	return
}

func (i *Int64) Sub(v int64) (new int64) {
	new = atomic.AddInt64(&i.v, -v)
	return
}

func (i *Int64) Swap(v int64) (old int64) {
	old = atomic.SwapInt64(&i.v, v)
	return
}

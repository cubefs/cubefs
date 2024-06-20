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

type Int32 struct {
	v int32
}

func (i *Int32) Load() (v int32) {
	v = atomic.LoadInt32(&i.v)
	return
}

func (i *Int32) Store(v int32) {
	atomic.StoreInt32(&i.v, v)
}

func (i *Int32) CompareAndSwap(old int32, new int32) (swaped bool) {
	swaped = atomic.CompareAndSwapInt32(&i.v, old, new)
	return
}

func (i *Int32) Add(v int32) (new int32) {
	new = atomic.AddInt32(&i.v, v)
	return
}

func (i *Int32) Sub(v int32) (new int32) {
	new = atomic.AddInt32(&i.v, -v)
	return
}

func (i *Int32) Swap(v int32) (old int32) {
	old = atomic.SwapInt32(&i.v, v)
	return
}

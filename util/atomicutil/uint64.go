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

type Uint64 struct {
	v uint64
}

func (i *Uint64) Load() (v uint64) {
	v = atomic.LoadUint64(&i.v)
	return
}

func (i *Uint64) Store(v uint64) {
	atomic.StoreUint64(&i.v, v)
}

func (i *Uint64) CompareAndSwap(old uint64, new uint64) (swaped bool) {
	swaped = atomic.CompareAndSwapUint64(&i.v, old, new)
	return
}

func (i *Uint64) Add(v uint64) (new uint64) {
	new = atomic.AddUint64(&i.v, v)
	return
}

func (i *Uint64) Sub(v int64) (new uint64) {
	new = atomic.AddUint64(&i.v, ^uint64(v-1))
	return
}

func (i *Uint64) Swap(v uint64) (old uint64) {
	old = atomic.SwapUint64(&i.v, v)
	return
}

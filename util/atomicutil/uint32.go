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

type Uint32 struct {
	v uint32
}

func (i *Uint32) Load() (v uint32) {
	v = atomic.LoadUint32(&i.v)
	return
}

func (i *Uint32) Store(v uint32) {
	atomic.StoreUint32(&i.v, v)
}

func (i *Uint32) CompareAndSwap(old uint32, new uint32) (swaped bool) {
	swaped = atomic.CompareAndSwapUint32(&i.v, old, new)
	return
}

func (i *Uint32) Add(v uint32) (new uint32) {
	new = atomic.AddUint32(&i.v, v)
	return
}

func (i *Uint32) Sub(v int32) (new uint32) {
	new = atomic.AddUint32(&i.v, ^uint32(v-1))
	return
}

func (i *Uint32) Swap(v uint32) (old uint32) {
	old = atomic.SwapUint32(&i.v, v)
	return
}

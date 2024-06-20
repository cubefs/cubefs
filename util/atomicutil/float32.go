// Copyright 2023 The CubeFS Authors.
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

import (
	"math"
	"sync/atomic"
)

type Float32 struct {
	val uint32
}

func (f *Float32) Load() float32 {
	return math.Float32frombits(atomic.LoadUint32(&f.val))
}

func (f *Float32) Store(val float32) {
	atomic.StoreUint32(&f.val, math.Float32bits(val))
}

func (f *Float32) CompareAndSwap(old float32, new float32) (swaped bool) {
	swaped = atomic.CompareAndSwapUint32(&f.val, math.Float32bits(old), math.Float32bits(new))
	return
}

func (f *Float32) Swap(new float32) (old float32) {
	old = math.Float32frombits(atomic.SwapUint32(&f.val, math.Float32bits(new)))
	return
}

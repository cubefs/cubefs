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

type Float64 struct {
	val uint64
}

func (f *Float64) Load() float64 {
	return math.Float64frombits(atomic.LoadUint64(&f.val))
}

func (f *Float64) Store(val float64) {
	atomic.StoreUint64(&f.val, math.Float64bits(val))
}

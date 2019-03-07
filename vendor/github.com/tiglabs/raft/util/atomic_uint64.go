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

package util

import "sync/atomic"

type AtomicUInt64 struct {
	v uint64
}

func (a *AtomicUInt64) Get() uint64 {
	return atomic.LoadUint64(&a.v)
}

func (a *AtomicUInt64) Set(v uint64) {
	atomic.StoreUint64(&a.v, v)
}

func (a *AtomicUInt64) Add(v uint64) uint64 {
	return atomic.AddUint64(&a.v, v)
}

func (a *AtomicUInt64) Incr() uint64 {
	return atomic.AddUint64(&a.v, 1)
}

func (a *AtomicUInt64) CompareAndSwap(o, n uint64) bool {
	return atomic.CompareAndSwapUint64(&a.v, o, n)
}

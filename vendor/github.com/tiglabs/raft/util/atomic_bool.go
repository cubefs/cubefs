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

type AtomicBool struct {
	v int32
}

func (b *AtomicBool) Get() bool {
	return atomic.LoadInt32(&b.v) != 0
}

func (b *AtomicBool) Set(newValue bool) {
	atomic.StoreInt32(&b.v, boolToInt(newValue))
}

func (b *AtomicBool) CompareAndSet(expect, update bool) bool {
	return atomic.CompareAndSwapInt32(&b.v, boolToInt(expect), boolToInt(update))
}

func boolToInt(v bool) int32 {
	if v {
		return 1
	} else {
		return 0
	}
}

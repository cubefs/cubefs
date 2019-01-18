// Copyright 2018 The Container File System Authors.
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

package metanode

import (
	"sync"
)

// AtomicBool returns the bool value atomically.
type AtomicBool struct {
	sync.RWMutex
	applyID uint64
	status  bool
}

// NewAtomicBool returns a new AtomicBool.
func NewAtomicBool() *AtomicBool {
	return &AtomicBool{}
}

// SetTrue set true to bool value in AtomicBool.
func (b *AtomicBool) SetTrue(appID uint64) {
	b.Lock()
	b.applyID = appID
	b.status = true
	b.Unlock()
}

// Bool returns the bool value stored in AtomicBool.
func (b *AtomicBool) Bool() bool {
	b.RLock()
	ok := b.status
	b.RUnlock()
	return ok
}

// SetTrue set false to the bool value in AtomicBool.
func (b *AtomicBool) SetFalse(appID uint64) {
	b.Lock()
	if b.applyID <= appID {
		b.status = false
	}
	b.Unlock()
}

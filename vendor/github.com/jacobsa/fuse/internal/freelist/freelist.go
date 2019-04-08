// Copyright 2015 Google Inc. All Rights Reserved.
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

package freelist

import "unsafe"

// A freelist for arbitrary pointers. Not safe for concurrent access.
type Freelist struct {
	list []unsafe.Pointer
}

// Get an element from the freelist, returning nil if empty.
func (fl *Freelist) Get() (p unsafe.Pointer) {
	l := len(fl.list)
	if l == 0 {
		return
	}

	p = fl.list[l-1]
	fl.list = fl.list[:l-1]

	return
}

// Contribute an element back to the freelist.
func (fl *Freelist) Put(p unsafe.Pointer) {
	fl.list = append(fl.list, p)
}

// Copyright 2022 The CubeFS Authors.
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

package limit

import "errors"

// ErrLimited limited error for non-blocking
var ErrLimited = errors.New("limit exceeded")

// Limiter to limit all by key
type Limiter interface {
	// returns how many holder are running
	// return -1 if u donot want to implement this
	Running() int

	// Acquire by this keys, returns error if no available resource
	// Panic if key is unhashable type necessarily
	Acquire(keys ...interface{}) error

	// Release this keys holder
	// Panic if not acquire yet necessarily
	// Panic if key is unhashable type necessarily
	Release(keys ...interface{})
}

// ResettableLimiter resetable limiter
type ResettableLimiter interface {
	Limiter

	// Reset the available resource
	Reset(n int)
}

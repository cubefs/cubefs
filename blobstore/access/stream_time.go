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

package access

import (
	"fmt"
	"sync/atomic"
	"time"
)

type timeReadWrite struct {
	a int64 // alloc buffer
	r int64 // PUT: read from client,  GET: read from blobnode
	w int64 // PUT: write to blobnode, GET: write to client
}

func (t *timeReadWrite) IncA(dur time.Duration) {
	atomic.AddInt64(&t.a, int64(dur))
}

func (t *timeReadWrite) IncR(dur time.Duration) {
	atomic.AddInt64(&t.r, int64(dur))
}

func (t *timeReadWrite) IncW(dur time.Duration) {
	atomic.AddInt64(&t.w, int64(dur))
}

// String within milliseconds
func (t *timeReadWrite) String() string {
	a := atomic.LoadInt64(&t.a) / 1e6
	r := atomic.LoadInt64(&t.r) / 1e6
	w := atomic.LoadInt64(&t.w) / 1e6
	return fmt.Sprintf("a_%d_r_%d_w_%d", a, r, w)
}

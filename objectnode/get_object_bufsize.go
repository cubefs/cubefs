// Copyright 2018 The CubeFS Authors.
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

package objectnode

import (
	"sync/atomic"

	"github.com/cubefs/cubefs/util"
)

// getObjectBufSize controls the per-iteration buffer size of the
// GET-object inner loop in Volume.read. See the doc on
// configGetObjectBufSize in server.go for what raising this buys and
// what it costs. The variable is process-global because every Volume
// in this ObjectNode shares the same read path.
//
// Default is 2*util.BlockSize = 256 KiB, the historical value. Set
// once at startup from handleStart; read on every GET. atomic.Int32
// is Go 1.19+ — we're on 1.17, so use the package-level helpers.
var getObjectBufSizeBytes int64 = int64(2 * util.BlockSize)

// setGetObjectBufSize overrides the GET-object loop buffer size.
// Called from handleStart when the operator sets configGetObjectBufSize
// to a positive value. Zero or negative values are rejected so a
// misconfigured operator can't accidentally hang the GET path on a
// zero-byte read loop.
func setGetObjectBufSize(n int) {
	if n <= 0 {
		return
	}
	atomic.StoreInt64(&getObjectBufSizeBytes, int64(n))
}

// getObjectBufSize returns the current GET-object loop buffer size in
// bytes. Hot path — called once per Volume.read invocation, not per
// loop iteration.
func getObjectBufSize() int {
	return int(atomic.LoadInt64(&getObjectBufSizeBytes))
}

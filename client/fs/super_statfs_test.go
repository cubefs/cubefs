// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

package fs

import (
	"testing"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
)

func TestFillStatfsInodes(t *testing.T) {
	for _, used := range []uint64{0, 100, 1 << 40} {
		var resp fuse.StatfsResponse
		fillStatfs(&resp, 1<<30, 1<<20, used)
		if resp.Files < resp.Ffree {
			t.Fatalf("used=%d: Files(%d) < Ffree(%d), df -i would report negative IUsed", used, resp.Files, resp.Ffree)
		}
		if got := resp.Files - resp.Ffree; got != used {
			t.Fatalf("used=%d: Files-Ffree=%d", used, got)
		}
	}
}

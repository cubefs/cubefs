// Copyright 2025 The CubeFS Authors.
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHasInvalidPathComponent(t *testing.T) {
	invalid := []string{
		".",
		"..",
		"../a",
		"./a",
		"a/..",
		"a/.",
		"a/../b",
		"a/./b",
		"a/../",
		"a//../b",
	}
	for _, key := range invalid {
		require.True(t, hasInvalidPathComponent(key), "key %q should be rejected", key)
	}

	valid := []string{
		"a",
		"a/b",
		"a/b/",
		"a.b/c",
		"..a",
		"a..",
		"a...b",
		"dir/.hidden",
		"dir/..config",
		"a/b.../c.",
	}
	for _, key := range valid {
		require.False(t, hasInvalidPathComponent(key), "key %q should be accepted", key)
	}
}

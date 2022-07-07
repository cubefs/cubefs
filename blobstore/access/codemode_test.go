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

package access_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
)

func TestAccessStreamCodeModePairs(t *testing.T) {
	m := access.CodeModePairs{
		codemode.EC6P6: access.CodeModePair{
			Policy: codemode.Policy{
				ModeName: codemode.EC6P6.Name(),
				MinSize:  1 << 10,
				MaxSize:  1 << 20,
				Enable:   true,
			},
			Tactic: codemode.EC6P6.Tactic(),
		},
		codemode.EC6P10L2: access.CodeModePair{
			Policy: codemode.Policy{
				ModeName: codemode.EC6P10L2.Name(),
				MinSize:  1 << 30,
				MaxSize:  1 << 40,
				Enable:   true,
			},
			Tactic: codemode.EC6P10L2.Tactic(),
		},
	}

	cases := []struct {
		size    int64
		isPanic bool
		mode    codemode.CodeMode
	}{
		{-1, true, 0},
		{0, true, 0},
		{1 << 8, true, 0},
		{1 << 10, false, codemode.EC6P6},
		{1 << 14, false, codemode.EC6P6},
		{1 << 20, false, codemode.EC6P6},
		{1 << 25, true, 0},
		{1 << 30, false, codemode.EC6P10L2},
		{1 << 40, false, codemode.EC6P10L2},
		{1 << 50, true, 0},
	}
	for _, cs := range cases {
		if cs.isPanic {
			require.Panics(t, func() { m.SelectCodeMode(cs.size) })
		} else {
			require.Equal(t, cs.mode, m.SelectCodeMode(cs.size))
		}
	}
}

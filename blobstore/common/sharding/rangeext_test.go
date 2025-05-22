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

package sharding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRange(t *testing.T) {
	subRangeCountMap := map[int][][]byte{
		1: {[]byte{1}},
		2: {[]byte{1}, []byte{2}},
	}
	rt := RangeType_RangeTypeHash

	for subRangeCount := range subRangeCountMap {
		r := New(rt, subRangeCount)

		require.False(t, r.IsEmpty())

		ci := NewCompareItem(rt, subRangeCountMap[subRangeCount])
		require.True(t, r.Belong(ci))

		for splitIndex := 0; splitIndex < subRangeCount; splitIndex++ {
			subs, err := r.Split(splitIndex)
			require.NoError(t, err)
			require.False(t, subs[0].IsEmpty())
			require.False(t, subs[1].IsEmpty())

			require.True(t, r.Contain(&subs[0]))
			require.True(t, r.Contain(&subs[1]))
			require.False(t, subs[0].Contain(r))
			require.False(t, subs[1].Contain(r))
			require.False(t, subs[0].Contain(&subs[1]))
			require.False(t, subs[1].Contain(&subs[0]))

			b := r.MaxBoundary()
			require.Equal(t, false, b.Less(subs[0].MaxBoundary()))
			require.Equal(t, false, b.Less(subs[1].MaxBoundary()))
			require.Equal(t, true, subs[0].MaxBoundary().Less(subs[1].MaxBoundary()))
		}

	}
}

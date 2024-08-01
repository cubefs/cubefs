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

func TestInitRange(t *testing.T) {
	expected := []int{0, 1, 2, 3, 4, 5, 10, 100, 1000, 1024, 2000}
	for _, expected := range expected {
		ss := InitShardingRange(RangeType_RangeTypeHash, 2, expected)
		require.LessOrEqual(t, expected, len(ss))
		require.LessOrEqual(t, len(ss), expected*2)
		for _, r := range ss {
			require.False(t, r.IsEmpty())
		}
	}
}

func TestCompareItem(t *testing.T) {
	inputs2 := [][][][]byte{
		{{[]byte{1}}, {[]byte{1}}},
		{{[]byte{1}}, {[]byte{2}}},
		{{[]byte{2}}, {[]byte{1}}},
		{{[]byte{1}, []byte{1}}, {[]byte{1}, []byte{1}}},
		{{[]byte{1}, []byte{1}}, {[]byte{1}, []byte{2}}},
		{{[]byte{1}, []byte{2}}, {[]byte{1}, []byte{1}}},
	}
	outputs2 := []bool{
		false,
		false,
		true,
		false,
		false,
		true,
	}

	for i := range inputs2 {
		ci1 := NewCompareItem(RangeType_RangeTypeHash, inputs2[i][0])
		ci2 := NewCompareItem(RangeType_RangeTypeHash, inputs2[i][1])
		b := ci1.GetBoundary()
		ret := b.Less(ci2.GetBoundary())
		require.Equal(t, outputs2[i], ret)
	}
}

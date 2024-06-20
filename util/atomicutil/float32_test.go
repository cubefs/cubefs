// Copyright 2023 The CubeFS Authors.
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

package atomicutil_test

import (
	"testing"

	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/stretchr/testify/require"
)

func TestAtomicFloat32(t *testing.T) {
	f := atomicutil.Float32{}
	testVal := float32(1.0)
	f.Store(testVal)
	require.Equal(t, testVal, f.Load())
	require.True(t, f.CompareAndSwap(testVal, 0))
	require.EqualValues(t, 0, f.Swap(testVal))
	require.EqualValues(t, testVal, f.Load())
}

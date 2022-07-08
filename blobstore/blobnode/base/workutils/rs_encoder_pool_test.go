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

package workutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReEncoderPool(t *testing.T) {
	pool := EncoderPoolInst()
	pool2 := EncoderPoolInst()
	require.Equal(t, pool, pool2)

	encoderN3M2 := pool.GetEncoder(3, 2)
	encoderN5M3 := pool.GetEncoder(5, 3)
	require.NotEqual(t, encoderN3M2, encoderN5M3)

	encoderN5M3Copy := pool.GetEncoder(5, 3)
	require.Equal(t, encoderN5M3, encoderN5M3Copy)
}

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

	"github.com/cubefs/cubefs/blobstore/common/codemode"
)

func TestEncoderPool(t *testing.T) {
	modes := codemode.GetAllCodeModes()
	for _, mode := range modes {
		_, err := GetEncoder(mode)
		require.NoError(t, err)
	}

	encode1, err := GetEncoder(codemode.EC15P12)
	require.NoError(t, err)
	encoder2, err := GetEncoder(codemode.EC15P12)
	require.NoError(t, err)
	require.Equal(t, encode1, encoder2)

	encoder3, err := GetEncoder(3)
	require.NoError(t, err)
	encoder4, err := GetEncoder(4)
	require.NoError(t, err)
	require.NotEqual(t, encoder3, encoder4)
}

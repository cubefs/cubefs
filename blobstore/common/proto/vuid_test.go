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

package proto

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVuid(t *testing.T) {
	_, err := NewVuid(1, 1, 0)
	require.Error(t, err)

	vid := Vid(rand.Uint32())
	index := uint8(rand.Intn(256))
	epoch := uint32(rand.Int31n(MaxEpoch-MinEpoch) + MinEpoch)
	vuid, err := NewVuid(vid, index, epoch)
	vuidPre := EncodeVuidPrefix(vid, index)
	require.NoError(t, err)
	require.True(t, vuid.IsValid())

	require.Equal(t, vuidPre, vuid.VuidPrefix())
	require.Equal(t, vid, vuidPre.Vid())
	require.Equal(t, index, vuidPre.Index())
	require.Equal(t, vid, vuid.Vid())
	require.Equal(t, index, vuid.Index())
	require.Equal(t, epoch, vuid.Epoch())
}

func TestVuidMulti(t *testing.T) {
	for range [1000]struct{}{} {
		TestVuid(t)
	}
}

func TestDecodeVuid(t *testing.T) {
	for _, vuid := range []Vuid{425335980033, 116131889167} {
		t.Log(vuid.Vid(), vuid.VuidPrefix(), vuid.Index(), vuid.Epoch(), vuid.ToString())
		require.Equal(t, vuid, EncodeVuid(vuid.VuidPrefix(), vuid.Epoch()))
	}
}

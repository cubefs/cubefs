// Copyright 2024 The CubeFS Authors.
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

func TestSuid(t *testing.T) {
	shardID := ShardID(rand.Uint32())
	index := uint8(rand.Intn(256))
	epoch := uint32(rand.Int31n(MaxEpoch-MinEpoch) + MinEpoch)
	suid := EncodeSuid(shardID, index, epoch)
	suidPre := EncodeSuidPrefix(shardID, index)

	require.True(t, suid.IsValid())
	require.Equal(t, suidPre, suid.SuidPrefix())
	require.Equal(t, shardID, suidPre.ShardID())
	require.Equal(t, index, suidPre.Index())
	require.Equal(t, shardID, suid.ShardID())
	require.Equal(t, index, suid.Index())
	require.Equal(t, epoch, suid.Epoch())
}

func TestSuidMulti(t *testing.T) {
	for range [1000]struct{}{} {
		TestSuid(t)
	}
}

func TestDecodeSuid(t *testing.T) {
	for _, suid := range []Suid{425335980033, 116131889167} {
		t.Log(suid.ShardID(), suid.SuidPrefix(), suid.Index(), suid.Epoch())
		require.Equal(t, suid, EncodeSuid(suid.ShardID(), suid.Index(), suid.Epoch()))
	}
}

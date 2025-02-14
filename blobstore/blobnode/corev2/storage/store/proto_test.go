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

package store

import (
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"testing"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/stretchr/testify/require"
)

func TestSliceMeta(t *testing.T) {
	sm := &SliceMeta{
		Index:           1,
		ChunkEpoch:      1,
		LastBlockCrcRaw: [crcSize]byte{1, 1, 1, 1},
		Vuid:            22,
		ID:              10001,
		ShardMeta: core.ShardMeta{
			Version: 1,
			Flag:    bnapi.ShardStatusNormal,
			Offset:  0,
			Size:    1024,
			Crc:     4,
		},
	}
	size := sm.GetSize()
	raw := make([]byte, size)
	err := sm.MarshalTo(raw)
	require.NoError(t, err)

	_sm := &SliceMeta{}
	require.NoError(t, _sm.Unmarshal(raw))
	require.EqualValues(t, sm, _sm)
}

func TestChunkMeta(t *testing.T) {
	cm := ChunkMeta{
		Index: 1,
		Epoch: 1,
		VuidMeta: core.VuidMeta{
			Vuid:    1,
			ChunkID: clustermgr.ChunkID{},
			Status:  clustermgr.ChunkStatusNormal,
		},
	}
	chunkMetaBuff := make([]byte, 4<<10)
	err := cm.MarshalTo(chunkMetaBuff)
	require.NoError(t, err)

	_cm := ChunkMeta{}
	err = _cm.Unmarshal(chunkMetaBuff)
	t.Log(_cm)
	require.NoError(t, err)
	require.EqualValues(t, _cm, cm)
}

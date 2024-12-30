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

package store

import (
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/stretchr/testify/require"
)

func TestChunkMetaMarshal(t *testing.T) {
	vuid := proto.Vuid(101)
	cm := &ChunkMeta{
		Index: 1,
		Epoch: 1,
		VuidMeta: core.VuidMeta{
			Vuid:    101,
			ChunkID: clustermgr.NewChunkID(vuid),
			Status:  clustermgr.ChunkStatusNormal,
		},
	}

	raw := make([]byte, 1024)
	require.NoError(t, cm.MarshalTo(raw))

	_cm := &ChunkMeta{}
	require.NoError(t, _cm.Unmarshal(raw))

	require.Equal(t, _cm, cm)
}

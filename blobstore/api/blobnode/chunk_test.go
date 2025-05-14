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

package blobnode

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func TestIsValidChunkId(t *testing.T) {
	id := clustermgr.InvalidChunkID
	require.Equal(t, false, IsValidChunkID(id))

	id = clustermgr.ChunkID{0x1}
	require.Equal(t, true, IsValidChunkID(id))
}

func TestChunkIdNew(t *testing.T) {
	chunkid := clustermgr.NewChunkID(101)
	require.Equal(t, clustermgr.ChunkIDLength, len(chunkid))
	require.NotEqual(t, clustermgr.InvalidChunkID, chunkid)

	expectedVuid := chunkid.VolumeUnitId()
	require.Equal(t, expectedVuid, proto.Vuid(101))

	chunkname := chunkid.String()
	require.Equal(t, clustermgr.ChunkIDEncodeLen, len(chunkname))

	arrs := strings.Split(chunkname, "-")
	require.Equal(t, 2, len(arrs))
	require.Equal(t, "0000000000000065", arrs[0])
}

func TestChunkId_Marshal(t *testing.T) {
	chunkid := clustermgr.NewChunkID(101)

	data, err := chunkid.Marshal()
	require.NoError(t, err)
	require.Equal(t, clustermgr.ChunkIDEncodeLen, len(data))
	log.Infof("data:%s", data)

	var newchunk clustermgr.ChunkID
	err = newchunk.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, chunkid, newchunk)
}

func TestChunkId_MarshalJSON(t *testing.T) {
	chunkid := clustermgr.NewChunkID(101)

	data, err := json.Marshal(chunkid)
	require.NoError(t, err)
	require.Equal(t, clustermgr.ChunkIDEncodeLen+2, len(data))

	log.Infof("data:%s", data)

	var newchunk clustermgr.ChunkID
	err = json.Unmarshal(data, &newchunk)
	require.NoError(t, err)
	require.Equal(t, chunkid, newchunk)
}

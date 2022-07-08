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

package clustermgr

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestChunkReportArgs(t *testing.T) {
	vuid1, _ := proto.NewVuid(1, 0, 1)
	vuid2, _ := proto.NewVuid(1, 1, 1)
	args := ReportChunkArgs{
		ChunkInfos: []blobnode.ChunkInfo{
			{
				Id:     blobnode.NewChunkId(vuid1),
				Vuid:   vuid1,
				DiskID: 1,
				Free:   1,
				Used:   1,
				Size:   1,
			},
			{
				Id:     blobnode.NewChunkId(vuid2),
				Vuid:   vuid2,
				DiskID: 2,
				Free:   2,
				Used:   2,
				Size:   2,
			},
		},
	}
	b, err := args.Encode()
	assert.NoError(t, err)

	decodeArgs := &ReportChunkArgs{}
	err = decodeArgs.Decode(bytes.NewReader(b))
	assert.NoError(t, err)
	assert.Equal(t, len(args.ChunkInfos), len(decodeArgs.ChunkInfos))
	for i := 0; i < len(args.ChunkInfos); i++ {
		assert.Equal(t, args.ChunkInfos[i].Id, decodeArgs.ChunkInfos[i].Id)
		assert.Equal(t, args.ChunkInfos[i].Vuid, decodeArgs.ChunkInfos[i].Vuid)
		assert.Equal(t, args.ChunkInfos[i].DiskID, decodeArgs.ChunkInfos[i].DiskID)
		assert.Equal(t, args.ChunkInfos[i].Free, decodeArgs.ChunkInfos[i].Free)
		assert.Equal(t, args.ChunkInfos[i].Used, decodeArgs.ChunkInfos[i].Used)
		assert.Equal(t, args.ChunkInfos[i].Size, decodeArgs.ChunkInfos[i].Size)
	}
}

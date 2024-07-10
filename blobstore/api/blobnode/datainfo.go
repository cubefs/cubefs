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
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type ChunkInfo struct {
	Id         ChunkId      `json:"id"`
	Vuid       proto.Vuid   `json:"vuid"`
	DiskID     proto.DiskID `json:"diskid"`
	Total      uint64       `json:"total"`  // ChunkSize
	Used       uint64       `json:"used"`   // user data size
	Free       uint64       `json:"free"`   // ChunkSize - Used
	Size       uint64       `json:"size"`   // Chunk File Size (logic size)
	Status     ChunkStatus  `json:"status"` // normal、readOnly
	Compacting bool         `json:"compacting"`
}

type ShardInfo struct {
	Vuid   proto.Vuid   `json:"vuid"`
	Bid    proto.BlobID `json:"bid"`
	Size   int64        `json:"size"`
	Crc    uint32       `json:"crc"`
	Flag   ShardStatus  `json:"flag"` // 1:normal,2:markDelete
	Inline bool         `json:"inline"`
}

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
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type DiskHeartBeatInfo struct {
	DiskID       proto.DiskID `json:"disk_id"`
	Used         int64        `json:"used"`           // disk used space
	Free         int64        `json:"free"`           // remaining free space on the disk
	Size         int64        `json:"size"`           // total physical disk space
	MaxChunkCnt  int64        `json:"max_chunk_cnt"`  // note: maintained by clustermgr
	FreeChunkCnt int64        `json:"free_chunk_cnt"` // note: maintained by clustermgr
	UsedChunkCnt int64        `json:"used_chunk_cnt"` // current number of chunks on the disk
}

type DiskInfo struct {
	ClusterID    proto.ClusterID  `json:"cluster_id"`
	Idc          string           `json:"idc"`
	Rack         string           `json:"rack"`
	Host         string           `json:"host"`
	Path         string           `json:"path"`
	Status       proto.DiskStatus `json:"status"` // normal、broken、repairing、repaired、dropped
	Readonly     bool             `json:"readonly"`
	CreateAt     time.Time        `json:"create_time"`
	LastUpdateAt time.Time        `json:"last_update_time"`
	DiskHeartBeatInfo
}

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

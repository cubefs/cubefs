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

import core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"

type (
	layoutInfo struct {
		LogArenaStart  uint64 `json:"log_arena_start"`
		ChunkMetaStart uint64 `json:"chunk_meta_start"`
		SliceMetaStart uint64 `json:"slice_meta_start"`
		SliceDataStart uint64 `json:"slice_data_start"`
		MaxChunkCount  uint64 `json:"max_chunk_count"`
	}
)

type superBlock struct {
	DiskMeta   core.DiskMeta `json:"disk_meta"`
	LayoutInfo layoutInfo    `json:"layout_info"`
	CheckSum   uint32        `json:"check_sum"`
}

// Marshal encode superBlock into []byte with 4KB align and padding
func (s *superBlock) Marshal() ([]byte, error) {
	// todo: calculate checksum automatically

	return nil, nil
}

func (s *superBlock) Unmarshal(raw []byte) error {
	return nil
}

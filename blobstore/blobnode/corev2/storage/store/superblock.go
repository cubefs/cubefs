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
	"bytes"
	"encoding/binary"
	"encoding/json"

	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/util"
)

const _superBlockMagicSize = 4

var _superBlockMagic = [_superBlockMagicSize]byte{0xab, 0xcd, 0xef, 0xcc}

type (
	layoutInfo struct {
		LogArenaStart  uint64 `json:"log_arena_start"`
		ChunkMetaStart uint64 `json:"chunk_meta_start"`
		SliceMetaStart uint64 `json:"slice_meta_start"`
		SliceDataStart uint64 `json:"slice_data_start"`
		MaxChunkCount  uint64 `json:"max_chunk_count"`
		MaxSliceCount  uint64 `json:"max_slice_count"`
	}
)

type superBlock struct {
	DiskMeta   core.DiskMeta `json:"disk_meta"`
	LayoutInfo layoutInfo    `json:"layout_info"`
	Crc        uint32        `json:"crc"`
}

// Marshal encode superBlock into []byte with 4KB align and padding
func (s *superBlock) Marshal() ([]byte, error) {
	// todo: calculate checksum automatically
	// todo: use binary encode replace json, it's not matter now.

	buf := util.AllocAlignedBlock(int(rawStoreFormatV1Layout.superBlockSize), deviceSectorSize)
	raw, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	copy(buf, _superBlockMagic[:])
	binary.BigEndian.PutUint32(buf[_superBlockMagicSize:], uint32(len(raw)))
	copy(buf[_superBlockMagicSize+4:], raw)

	return buf, nil
}

func (s *superBlock) Unmarshal(raw []byte) error {
	// todo: calculate checksum automatically
	// todo: use binary encode replace json, it's not matter now.

	// still not format now
	if !bytes.Equal(raw[:_superBlockMagicSize], _superBlockMagic[:]) {
		return nil
	}

	size := binary.BigEndian.Uint32(raw[_superBlockMagicSize:])
	return json.Unmarshal(raw[_superBlockMagicSize+4:_superBlockMagicSize+4+size], s)
}

func (s *superBlock) IsFormatted() bool {
	return s.DiskMeta.Registered
}

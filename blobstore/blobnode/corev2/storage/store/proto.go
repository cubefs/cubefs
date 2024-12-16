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

type (
	chunkIndex   uint32
	sliceIndex   uint32
	logHeaderVer uint64
)

const (
	initLogHeaderVer logHeaderVer = 1
	deviceSectorSize              = 512
)

type rawStoreFormatLayout struct {
	startOffset    uint64
	superBlockSize uint64
	logArenaSize   uint64
	chunkArenaSize uint64
	chunkMetaSize  uint64
	sliceMetaSize  uint64
	sliceSize      uint64
	blockSize      uint64
}

var rawStoreFormatV1Layout = rawStoreFormatLayout{
	startOffset:    0,
	superBlockSize: 4 << 20,
	logArenaSize:   64 << 20,
	chunkArenaSize: 16<<30 + 2<<20,
	chunkMetaSize:  4 << 10,
	sliceMetaSize:  512,
	// 512: every block(32KB) with 4 byte crc
	sliceSize: 4<<20 + 512,
	blockSize: 32 << 10,
}

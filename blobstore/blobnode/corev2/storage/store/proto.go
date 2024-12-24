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
	"unsafe"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type (
	chunkIndex    uint32
	sliceIndex    uint32
	logHeaderVer  uint64
	logRecordType uint8
)

const (
	initLogHeaderVer       logHeaderVer  = 1
	deviceSectorSize                     = 512
	logRecordTypeSliceMeta logRecordType = 1
)

type rawStoreFormatLayout struct {
	startOffset    uint64
	superBlockSize uint64
	logArenaSize   uint64
	logRecordSize  uint64
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
	logRecordSize:  4 << 10,
	chunkArenaSize: 16<<30 + 2<<20,
	chunkMetaSize:  4 << 10,
	sliceMetaSize:  512,
	// 512: every block(32KB) with 4 byte crc
	sliceSize: 4<<20 + 512,
	blockSize: 32 << 10,
}

type SliceMeta struct {
	// slice index in the physical device layout, as ShardMeta record offset, this filed can be removed
	Index sliceIndex
	// record chunk's epoch,
	// when chunk delete and open reuse, the slice epoch is mismatch with chunk's epoch and add into slice free list
	ChunkEpoch uint32
	// LastBlockCrc hold last block increment checksum raw, it'll flush into the tail of slice data as block write full
	LastBlockCrc uint32
	// Vuid means which chunk manage this slice
	Vuid proto.Vuid
	// LastSectorCrc hold last device sector increment checksum raw
	//LastSectorCrc []byte
	ID proto.BlobID

	core.ShardMeta
}

// Marshal Slice into 512 byte
func (s *SliceMeta) Marshal() ([]byte, error) {
	return nil, nil
}

func (s *SliceMeta) MarshalTo(dest []byte) (err error) {
	return nil
}

func (s *SliceMeta) GetSize() int {
	return int(unsafe.Sizeof(s))
}

func (s *SliceMeta) Unmarshal(raw []byte) error {
	return nil
}

func (s *SliceMeta) Reset() {
	*s = SliceMeta{
		Index:     s.Index,
		ShardMeta: core.ShardMeta{Flag: blobnode.ShardStatusMarkDelete},
	}
}

func newSlice(sm *SliceMeta) *slice {
	return &slice{sm: sm}
}

type slice struct {
	// sm is the same pointer to the store's slice meta to save memory cost
	sm         *SliceMeta
	lastSector [deviceSectorSize]byte
}

func (s *slice) GetShardMeta() *SliceMeta {
	return s.sm
}

type logSliceMeta struct {
	*SliceMeta
	err error
}

func (l logSliceMeta) Size() uint16 {
	return uint16(l.SliceMeta.GetSize())
}

func (l logSliceMeta) MarshalTo(raw []byte) error {
	return l.SliceMeta.MarshalTo(raw)
}

func (l logSliceMeta) Unmarshal(raw []byte) error {
	return l.SliceMeta.Unmarshal(raw)
}

func (l logSliceMeta) NotifyError(err error) {
	if l.err == nil {
		l.err = err
	}
}

func (l logSliceMeta) Error() error {
	return l.err
}

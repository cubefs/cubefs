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
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type (
	chunkIndex    uint32
	sliceIndex    uint32
	logHeaderVer  uint64
	logHeaderFlag uint8
	logRecordType uint8
)

const (
	initLogHeaderVer logHeaderVer = 1
	crcSize                       = 4
	deviceSectorSize              = 512
	_SliceMetaSize                = 32 + 32

	logHeaderFlagUnCheckpoint   logHeaderFlag = 0
	logHeaderFlagCheckpointDone logHeaderFlag = 1

	logRecordTypeSliceMeta logRecordType = 1

	_sliceMetaMagicSize = 4
)

var _sliceMetaMagic = [_sliceMetaMagicSize]byte{0xab, 0xcd, 0xef, 0xcc}

type rawStoreFormatLayout struct {
	startOffset    uint64
	superBlockSize uint64
	logArenaSize   uint64
	logHeaderSize  uint64
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
	// 4K log header + 64MB log record arena
	logArenaSize:   64<<20 + 4<<10,
	logHeaderSize:  4 << 10,
	logRecordSize:  4 << 10,
	chunkArenaSize: 16 << 30,
	chunkMetaSize:  4 << 10,
	sliceMetaSize:  512,
	// every block(32KB-4) with 4 byte crc
	sliceSize: 4 << 20,
	blockSize: 32 << 10,
}

func newSliceMeta(index sliceIndex) *SliceMeta {
	return &SliceMeta{Index: index}
}

type SliceMeta struct {
	// todo: need slice meta checksum

	// slice index in the physical device layout, as ShardMeta record offset, this filed can be removed
	Index sliceIndex
	// record chunk's epoch,
	// when chunk delete and open reuse, the slice epoch is mismatch with chunk's epoch and add into slice free list
	ChunkEpoch uint32
	// LastBlockCrc hold last block increment checksum raw, it'll flush into the tail of slice data as block write full
	//LastBlockCrc uint32
	// LastBlockCrcRaw hold last block increment checksum raw, it'll flush into the tail of slice data as block write full
	LastBlockCrcRaw [crcSize]byte
	// Vuid means which chunk manage this slice
	Vuid proto.Vuid
	// LastSectorCrc hold last device sector increment checksum raw
	// LastSectorCrc []byte
	ID proto.BlobID

	core.ShardMeta
}

func (s *SliceMeta) MarshalTo(dest []byte) (err error) {
	if len(dest) < s.GetSize() {
		return fmt.Errorf("marshal buffer not enough: %d-%d", len(dest), s.GetSize())
	}

	copy(dest, _sliceMetaMagic[:])
	binary.BigEndian.PutUint32(dest[_sliceMetaMagicSize:], uint32(s.Index))
	binary.BigEndian.PutUint32(dest[_sliceMetaMagicSize+4:], s.ChunkEpoch)
	copy(dest[_sliceMetaMagicSize+8:], s.LastBlockCrcRaw[:])
	//binary.BigEndian.PutUint32(dest[_sliceMetaMagicSize+8:], s.LastBlockCrc)
	binary.BigEndian.PutUint64(dest[_sliceMetaMagicSize+12:], uint64(s.Vuid))
	binary.BigEndian.PutUint64(dest[_sliceMetaMagicSize+20:], uint64(s.ID))
	if err := s.ShardMeta.MarshalTo(dest[_sliceMetaMagicSize+28:]); err != nil {
		return err
	}

	return nil
}

func (s *SliceMeta) GetSize() int {
	return _SliceMetaSize
}

func (s *SliceMeta) Unmarshal(raw []byte) error {
	if len(raw) < s.GetSize() {
		return fmt.Errorf("unmarshal buffer not enough: %d-%d", len(raw), s.GetSize())
	}

	// todo: add crc checksum to ensure this slice is valid
	if !bytes.Equal(raw[:_sliceMetaMagicSize], _sliceMetaMagic[:]) {
		return nil
	}

	s.Index = sliceIndex(binary.BigEndian.Uint32(raw[_sliceMetaMagicSize:]))
	s.ChunkEpoch = binary.BigEndian.Uint32(raw[_sliceMetaMagicSize+4:])
	copy(s.LastBlockCrcRaw[:], raw[_sliceMetaMagicSize+8:])
	//s.LastBlockCrc = binary.BigEndian.Uint32(raw[_sliceMetaMagicSize+8:])
	s.Vuid = proto.Vuid(binary.BigEndian.Uint64(raw[_sliceMetaMagicSize+12:]))
	s.ID = proto.BlobID(binary.BigEndian.Uint64(raw[_sliceMetaMagicSize+20:]))

	return s.ShardMeta.Unmarshal(raw[_sliceMetaMagicSize+28:])
}

func (s *SliceMeta) IsEmpty() bool {
	return s.Vuid == 0 && s.Flag == blobnode.ShardStatusDefault
}

func (s *SliceMeta) ResetToDelete() {
	*s = SliceMeta{
		Index:     s.Index,
		ShardMeta: core.ShardMeta{Flag: blobnode.ShardStatusMarkDelete},
	}
}

func (s *SliceMeta) IsNormal() bool {
	return s.Flag == blobnode.ShardStatusNormal
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

func newLogSliceMeta(sm *SliceMeta) logSliceMeta {
	return logSliceMeta{
		SliceMeta: sm,
		notify:    errorChPool.Get().(chan error),
	}
}

type logSliceMeta struct {
	*SliceMeta
	notify chan error
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
	l.notify <- err
}

func (l logSliceMeta) Error() error {
	return <-l.notify
}

func (l logSliceMeta) Free() {
	errorChPool.Put(l.notify)
}

var errorChPool = sync.Pool{New: func() interface{} {
	return make(chan error)
}}

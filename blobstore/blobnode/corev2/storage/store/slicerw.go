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
	"hash/crc32"
	"io"
	"sync"

	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/iouring"
)

type sliceReader struct {
	next  uint32
	slice *slice
	read  *core.Shard
	// max slice writable size
	sliceSize uint32
	ioEngine  iouring.Engine
}

func (s *sliceReader) Read(b []byte) (n int, err error) {
	if s.next >= s.read.Size {
		return 0, io.EOF
	}
	n = len(b)
	if s.next+uint32(n) > s.sliceSize {
		return 0, io.ErrUnexpectedEOF
	}

	err = s.ioEngine.Read(b, uint64(s.read.Offset+s.read.From)+uint64(s.next), len(b))
	if err != nil {
		return 0, err
	}
	s.next += uint32(n)

	// fix last block crc
	if s.next+uint32(n) == s.read.Size {
		sm := s.slice.GetShardMeta()
		lastBlockCrcRaw := b[n-crcSize:]
		lastBlockCrcRaw = append(lastBlockCrcRaw, byte(sm.LastBlockCrc>>24), byte(sm.LastBlockCrc>>16), byte(sm.LastBlockCrc>>8), byte(sm.LastBlockCrc))
	}

	return
}

func (s *sliceReader) Close() error {
	sliceReaderPool.Put(s)
	return nil
}

type sliceWriter struct {
	slice *slice
	// append hold append write object
	append *core.Shard
	// shard next write offset
	next uint32
	// max slice writable size
	sliceSize uint32
	// max block writable size
	blockSize uint32
	// lastBlockCrc hold last block increment checksum raw, it'll flush into the tail of slice data as block write full
	lastBlockCrc uint32
	// lastSector hold last sector of this slice
	lastSector [deviceSectorSize]byte
	ioEngine   iouring.Engine
}

func (s *sliceWriter) Write(b []byte) (n int, err error) {
	if s.next >= s.sliceSize {
		return 0, io.EOF
	}
	toWrite := uint32(len(b))
	// fix toWrite by actual size
	if s.next+toWrite > s.append.Size {
		toWrite = s.append.Size - s.next
	}
	if s.next+toWrite > s.sliceSize {
		return 0, io.ErrUnexpectedEOF
	}

	sm := s.slice.GetShardMeta()

	lastSectorDataSize := (sm.Size + s.next) % 512
	// 1.refill last sector data
	if lastSectorDataSize > 0 {
		copy(b[:lastSectorDataSize], s.lastSector[:lastSectorDataSize-crcSize])
	}

	// 2.calculate last block crc
	lastBlockDataSize := (sm.Size + s.next) % s.blockSize
	end := toWrite - crcSize
	if lastBlockDataSize-crcSize+toWrite >= s.blockSize {
		end = s.blockSize - (lastBlockDataSize - crcSize)
	}
	lastBlockCrc := crc32.Update(s.lastBlockCrc, crc32.IEEETable, b[:end])
	lastBlockCrcRaw := b[end:][:0]
	lastBlockCrcRaw = append(lastBlockCrcRaw, byte(lastBlockCrc>>24), byte(lastBlockCrc>>16), byte(lastBlockCrc>>8), byte(lastBlockCrc))

	// 3.calculate new block crc
	// check if this is the last write
	if s.next+toWrite == s.append.Size {
		s.lastBlockCrc = crc32.ChecksumIEEE(b[end+crcSize : toWrite-crcSize])
		lastBlockCrcRaw = b[toWrite-crcSize:][:0]
		lastBlockCrcRaw = append(lastBlockCrcRaw, byte(s.lastBlockCrc>>24), byte(s.lastBlockCrc>>16), byte(s.lastBlockCrc>>8), byte(s.lastBlockCrc))
	} else {
		s.lastBlockCrc = crc32.ChecksumIEEE(b[end+crcSize : toWrite])
	}

	// 4.dispatch io write into device
	err = s.ioEngine.Write(b, uint64(sm.Offset)+uint64(sm.Size)+uint64(s.next), len(b))
	if err != nil {
		return
	}
	n = int(toWrite)
	s.next += uint32(n)

	// 5.update last sector by copy
	copy(s.lastSector[:], b[len(b)-deviceSectorSize:])
	return
}

func (s *sliceWriter) Close() error {
	sliceWriterPool.Put(s)
	return nil
}

var sliceReaderPool = sync.Pool{New: func() interface{} {
	return &sliceReader{}
}}

var sliceWriterPool = sync.Pool{New: func() interface{} {
	return &sliceWriter{}
}}

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
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	core "github.com/cubefs/cubefs/blobstore/blobnode/corev2"
	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/iouring"
	"github.com/cubefs/cubefs/blobstore/util"
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
	if uint32(s.read.From)+s.next >= s.read.Size {
		return 0, io.EOF
	}
	n = len(b)
	padtail := util.AlignedTail(s.read.Size, deviceSectorSize)
	if maxsize := s.read.Size + padtail - uint32(s.read.From) - s.next; n > int(maxsize) {
		n = int(maxsize)
		b = b[:n]
	}
	if uint32(s.read.From)+s.next+uint32(n) > s.sliceSize {
		return 0, io.ErrUnexpectedEOF
	}

	fmt.Println("read from: ", s.read.From, "read to: ", s.read.To, "read buff: ", n)

	err = s.ioEngine.Read(b, uint64(s.read.Offset+s.read.From)+uint64(s.next), len(b))
	if err != nil {
		return 0, err
	}

	// fmt.Println("read data: ", b)

	// as slice read and append write may happen concurrently and last block crc in disk will be covered
	// during append write, we should fix last block crc when read to the end of the slice with memory last block crc
	if uint32(s.read.From)+s.next+uint32(n) >= s.read.Size {
		sm := s.slice.GetShardMeta()
		lastBlockCrcRaw := b[s.read.Size-uint32(s.read.From)-s.next-crcSize:]
		copy(lastBlockCrcRaw, sm.LastBlockCrcRaw[:])
		// lastBlockCrcRaw = append(lastBlockCrcRaw, byte(sm.LastBlockCrc>>24), byte(sm.LastBlockCrc>>16), byte(sm.LastBlockCrc>>8), byte(sm.LastBlockCrc))
	}
	s.next += uint32(n)

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
	lastBlockCrc    uint32
	lastBlockCrcRaw []byte
	// lastSector hold last sector of this slice
	lastSector [deviceSectorSize]byte
	ioEngine   iouring.Engine
}

/*func (s *sliceWriter) Write(b []byte) (n int, err error) {
sm := s.slice.GetShardMeta()
if s.next >= s.sliceSize {
	return 0, io.EOF
}

appendDataSize, newDataEnd := uint32(len(b)), uint32(len(b))
// fix toWrite by actual size
if s.next+appendDataSize > s.append.Size {
	appendDataSize = s.append.Size - s.next
	newDataEnd = appendDataSize
}
if s.next+appendDataSize > s.sliceSize {
	return 0, io.ErrUnexpectedEOF
}

newDataStart := uint32(0)
lastSectorDataSize := (sm.Size + s.next) % deviceSectorSize
// 1.refill last sector data
if lastSectorDataSize > 0 {
	copy(b, s.lastSector[:lastSectorDataSize-crcSize])
	// b: [last sector data|new append data]
	// newDataStart should start from the end of last sector data
	// newDataEnd should include last sector data and new append data
	newDataStart = lastSectorDataSize - crcSize
	newDataEnd += newDataStart
}

// 2.calculate last block crc
lastBlockDataSize := (sm.Size + s.next) % s.blockSize
if sm.Size%s.blockSize != 0 {
	// last written size is aligned with block size, then sm.Size should decrease with crcSize
	lastBlockDataSize = (sm.Size - crcSize + s.next) % s.blockSize
}
if lastBlockDataSize-crcSize+toWrite >= s.blockSize {
	end := s.blockSize - (lastBlockDataSize - crcSize)
	lastBlockCrc := crc32.Update(s.lastBlockCrc, crc32.IEEETable, b[:end])
	lastBlockCrcRaw := b[end:][:0]
	lastBlockCrcRaw = append(lastBlockCrcRaw, byte(lastBlockCrc>>24), byte(lastBlockCrc>>16), byte(lastBlockCrc>>8), byte(lastBlockCrc))
	s.lastBlockCrc = 0
	newDataStart = end
}
// update last block crc when there is anymore data after last block filled
if toWrite > newDataStart {
	s.lastBlockCrc = crc32.Update(s.lastBlockCrc, crc32.IEEETable, b[newDataStart:toWrite-crcSize])
}

// 3.calculate new block crc
// check if this is the last write, do persistence for the last block crc when there is anymore data after last block filled
if s.next+appendDataSize == s.append.Size && toWrite > newDataStart {
	lastBlockCrcRaw := b[toWrite-crcSize:][:0]
	lastBlockCrcRaw = append(lastBlockCrcRaw, byte(s.lastBlockCrc>>24), byte(s.lastBlockCrc>>16), byte(s.lastBlockCrc>>8), byte(s.lastBlockCrc))
}*/

// deleted! 2.calculate last block crc
/*lastBlockDataSize := (sm.Size + s.next) % s.blockSize
end := toWrite - crcSize
if lastBlockDataSize-crcSize+toWrite >= s.blockSize {
	end = s.blockSize - (lastBlockDataSize - crcSize)
}
lastBlockCrc := crc32.Update(s.lastBlockCrc, crc32.IEEETable, b[:end])
lastBlockCrcRaw := b[end:][:0]
lastBlockCrcRaw = append(lastBlockCrcRaw, byte(lastBlockCrc>>24), byte(lastBlockCrc>>16), byte(lastBlockCrc>>8), byte(lastBlockCrc))

// deleted! 3.calculate new block crc
// check if this is the last write
if s.next+toWrite == s.append.Size {
	s.lastBlockCrc = crc32.ChecksumIEEE(b[end+crcSize : toWrite-crcSize])
	lastBlockCrcRaw = b[toWrite-crcSize:][:0]
	lastBlockCrcRaw = append(lastBlockCrcRaw, byte(s.lastBlockCrc>>24), byte(s.lastBlockCrc>>16), byte(s.lastBlockCrc>>8), byte(s.lastBlockCrc))
} else {
	s.lastBlockCrc = crc32.ChecksumIEEE(b[end+crcSize : toWrite])
}*/

// 4.dispatch io write into device
/*err = s.ioEngine.Write(b, uint64(sm.Offset)+uint64(sm.Size)+uint64(s.next), len(b))
	if err != nil {
		return
	}

	s.next += appendDataSize
	n = len(b)

	// 5.update last sector by copy
	if n > deviceSectorSize {
		copy(s.lastSector[:], b[n-deviceSectorSize:])
	} else {
		copy(s.lastSector[:], b)
	}

	return
}*/

func (s *sliceWriter) Write(b []byte) (n int, err error) {
	if len(b) > 1<<20 || len(b)%deviceSectorSize != 0 {
		panic(fmt.Sprintf("invalid buffer length: %d", len(b)))
	}

	sm := s.slice.GetShardMeta()
	if sm.Size+s.next >= s.sliceSize {
		return 0, io.EOF
	}

	fmt.Println("next: ", s.next, "write buffer len: ", len(b))
	// fmt.Println("next: ", s.next, "write buffer len: ", len(b), "before write data: ", b)
	// os.WriteFile("before-"+strconv.Itoa(int(sm.Size)), b, 0644)

	bufferSize := uint32(len(b))
	appendDataSize, newDataEnd := bufferSize, bufferSize
	// fix toWrite by actual size
	if s.next+appendDataSize > s.append.Size {
		appendDataSize = s.append.Size - s.next
		newDataEnd = appendDataSize
	}

	newDataStart := uint32(0)
	lastBlockDataSize := (sm.Size + s.next) % s.blockSize
	if sm.Size%s.blockSize != 0 {
		// last written size is not aligned with block size, then sm.Size should decrease with crcSize
		lastBlockDataSize = (sm.Size - crcSize + s.next) % s.blockSize
	}

	writtenSize := sm.Size + s.next + appendDataSize
	if lastBlockDataSize > 0 {
		writtenSize -= crcSize
	}
	if writtenSize > s.sliceSize {
		return 0, io.ErrUnexpectedEOF
	}

	// refill last sector data
	var lastSectorDataSize uint32
	if lastBlockDataSize > 0 {
		lastSectorDataSize = lastBlockDataSize % deviceSectorSize
		// copy(b, s.lastSector[:lastSectorDataSize-crcSize])
		// copy(b, s.lastSector[:lastSectorDataSize])
		// b: [last sector data|new append data]
		// newDataStart should start from the end of last sector data
		// newDataEnd should include last sector data and new append data
		// newDataStart = lastSectorDataSize - crcSize
		newDataStart = lastSectorDataSize
		if newDataEnd < bufferSize {
			newDataEnd += newDataStart
		}
	}

	// restWrittenSize := s.append.Size - s.next - (newDataEnd - newDataStart)

	// calculate last write full block crc
	lastBlockDataStart := newDataStart
	//lastBlockUnalignedDataSize := lastBlockDataSize
	/*if lastBlockDataSize > 0 {
		lastBlockUnalignedDataSize -= crcSize
	}*/
	fmt.Println("last block data size: ", lastBlockDataSize, "last block data start: ", lastBlockDataStart, "new data end: ", newDataEnd)

	/*if lastBlockDataSize > 0 {
		// last write is not aligned with block size, we should calculate last block crc by append data
		if lastBlockDataSize+(newDataEnd-newDataStart) >= s.blockSize {
			if s.next == 0 {
				// one block: [data    |    crc]
				//             32KB-4  |     4
				//                     end
				end := s.blockSize - crcSize - lastBlockDataSize
				lastBlockCrc := crc32.Update(s.lastBlockCrc, crc32.IEEETable, b[newDataStart:newDataStart+end])
				lastBlockCrcRaw := b[newDataStart+end:]
				crcToRaw(lastBlockCrcRaw, lastBlockCrc)
				lastBlockDataStart += end + crcSize
				s.lastBlockCrc = 0
			}
		} else {
			if s.next == 0 {
				s.lastBlockCrc = crc32.Update(s.lastBlockCrc, crc32.IEEETable, b[lastBlockDataStart:newDataEnd-crcSize])
				fmt.Println("slice writer last block crc: ", s.lastBlockCrc, s.next, appendDataSize, s.append.Size)
				// check if this is the last write, do persistence for the last block crc when there is anymore data after last block filled
				// if restWrittenSize <= 0 {
				lastBlockCrcRaw := b[newDataEnd-crcSize:]
				crcToRaw(lastBlockCrcRaw, s.lastBlockCrc)
				//}
			}
		}
	}*/

	// update last block crc when this is the last write and there is anymore data after last block filled
	//if /*newDataEnd > lastBlockDataStart && */ restWrittenSize <= 0 {
	//	lastBlockCrcRaw := b[newDataEnd-crcSize:]
	//	s.lastBlockCrc = rawToCRC(lastBlockCrcRaw)
	//}

	// dispatch io write into device
	// offset怎么对齐，next按上面定义是每次write的新增数据大小，不包括前面补齐512扇区的部分，同时也要减去覆盖crc的4字节
	sectorIdx := (uint64(sm.Offset) + uint64(sm.Size) + uint64(s.next)) / deviceSectorSize
	// 当前一次append的有效数据在倒数第二个sector或者刚好508个字节时，这里sectorIdx要减1，否则会导致数据crc没有覆盖导致没有写入。
	if lastSectorDataSize >= 508 {
		sectorIdx -= 1
	}
	// 最近一次写入未对齐，则扇区前移至前一个sector开始，如果对齐，则不需要-1
	/*if (uint64(sm.Offset)+uint64(sm.Size)+uint64(s.next))%deviceSectorSize > 0 {
		sectorIdx -= 1
	}*/

	// fmt.Println("after write data: ", b)
	// os.WriteFile("after-"+strconv.Itoa(int(sm.Size)), b, 0644)

	err = s.ioEngine.Write(b, sectorIdx*deviceSectorSize, len(b))
	if err != nil {
		return
	}
	/*n = int(toWrite)
	s.next += uint32(n)*/

	s.next += newDataEnd - newDataStart
	// 上次未对齐，则next需要减一下crc值多余部分
	/*if lastBlockDataSize > 0 && s.next == 0 {
		s.next -= crcSize
	}*/

	restWrittenSize := s.append.Size - s.next
	if restWrittenSize <= 4 {
		rawSize := uint32(len(s.lastBlockCrcRaw))
		s.lastBlockCrcRaw = append(s.lastBlockCrcRaw, b[newDataEnd-newDataStart-crcSize-restWrittenSize-rawSize:]...)
		if rawSize == 0 {
			copy(s.lastSector[:], b[bufferSize-deviceSectorSize:])
		}
	}

	n = len(b)
	/*// 5.update last sector by copy when this is the last write of this append
	if restWrittenSize <= 0 {
		if n > deviceSectorSize {
			lastSectorDataSize := (newDataEnd - deviceSectorSize) % deviceSectorSize
			if lastSectorDataSize > 0 && lastSectorDataSize <= 3 {
				// copy(s.lastSector[:], b[n-deviceSectorSize:])
				// 剩余尾部数据大于508以上，则有1-3个字节的crc落在最后一个sector上，此时lastSector要从倒数第二个sector开始取。否则直接取最后一个sector就行
				copy(s.lastSector[:], b[n-2*deviceSectorSize:n-deviceSectorSize])
			} else {
				copy(s.lastSector[:], b[n-deviceSectorSize:])
			}
		} else {
			copy(s.lastSector[:], b)
		}
	}*/

	return
}

func (s *sliceWriter) WriteStable(b []byte) (n int, err error) {
	if len(b) > 1<<20 || len(b)%deviceSectorSize != 0 {
		panic(fmt.Sprintf("invalid buffer length: %d", len(b)))
	}

	sm := s.slice.GetShardMeta()
	if sm.Size+s.next >= s.sliceSize {
		return 0, io.EOF
	}

	fmt.Println("next: ", s.next, "write buffer len: ", len(b))
	// fmt.Println("next: ", s.next, "write buffer len: ", len(b), "before write data: ", b)
	// os.WriteFile("before-"+strconv.Itoa(int(sm.Size)), b, 0644)

	bufferSize := uint32(len(b))
	appendDataSize, newDataEnd := bufferSize, bufferSize
	// fix toWrite by actual size
	if s.next+appendDataSize > s.append.Size {
		appendDataSize = s.append.Size - s.next
		newDataEnd = appendDataSize
	}

	newDataStart := uint32(0)
	lastBlockDataSize := (sm.Size + s.next) % s.blockSize
	if sm.Size%s.blockSize != 0 {
		// last written size is not aligned with block size, then sm.Size should decrease with crcSize
		lastBlockDataSize = (sm.Size - crcSize + s.next) % s.blockSize
	}

	writtenSize := sm.Size + s.next + appendDataSize
	if lastBlockDataSize > 0 {
		writtenSize -= crcSize
	}
	if writtenSize > s.sliceSize {
		return 0, io.ErrUnexpectedEOF
	}

	// refill last sector data
	var lastSectorDataSize uint32
	if lastBlockDataSize > 0 {
		lastSectorDataSize = lastBlockDataSize % deviceSectorSize
		// copy(b, s.lastSector[:lastSectorDataSize-crcSize])
		copy(b, s.lastSector[:lastSectorDataSize])
		// b: [last sector data|new append data]
		// newDataStart should start from the end of last sector data
		// newDataEnd should include last sector data and new append data
		// newDataStart = lastSectorDataSize - crcSize
		newDataStart = lastSectorDataSize
		if newDataEnd < bufferSize {
			newDataEnd += newDataStart
		}
	}

	restWrittenSize := s.append.Size - s.next - (newDataEnd - newDataStart)

	// calculate last write full block crc
	lastBlockDataStart := newDataStart
	//lastBlockUnalignedDataSize := lastBlockDataSize
	/*if lastBlockDataSize > 0 {
		lastBlockUnalignedDataSize -= crcSize
	}*/
	fmt.Println("last block data size: ", lastBlockDataSize, "last block data start: ", lastBlockDataStart, "new data end: ", newDataEnd)

	if lastBlockDataSize > 0 {
		// last write is not aligned with block size, we should calculate last block crc by append data
		if lastBlockDataSize+(newDataEnd-newDataStart) >= s.blockSize {
			if s.next == 0 {
				// one block: [data    |    crc]
				//             32KB-4  |     4
				//                     end
				end := s.blockSize - crcSize - lastBlockDataSize
				lastBlockCrc := crc32.Update(s.lastBlockCrc, crc32.IEEETable, b[newDataStart:newDataStart+end])
				lastBlockCrcRaw := b[newDataStart+end:]
				crcToRaw(lastBlockCrcRaw, lastBlockCrc)
				lastBlockDataStart += end + crcSize
				s.lastBlockCrc = 0
			}
			// update last block crc when this is the last write and there is anymore data after last block filled
			/*if newDataEnd > lastBlockDataStart && restWrittenSize <= 0 {
				lastBlockCrcRaw := b[newDataEnd-crcSize:]
				s.lastBlockCrc = rawToCRC(lastBlockCrcRaw)
			}*/
		} else {
			if s.next == 0 {
				s.lastBlockCrc = crc32.Update(s.lastBlockCrc, crc32.IEEETable, b[lastBlockDataStart:newDataEnd-crcSize])
				fmt.Println("slice writer last block crc: ", s.lastBlockCrc, s.next, appendDataSize, s.append.Size)
				// check if this is the last write, do persistence for the last block crc when there is anymore data after last block filled
				// if restWrittenSize <= 0 {
				lastBlockCrcRaw := b[newDataEnd-crcSize:]
				crcToRaw(lastBlockCrcRaw, s.lastBlockCrc)
				//}
			}
			/*if restWrittenSize <= 0 {
				lastBlockCrcRaw := b[newDataEnd-crcSize:]
				s.lastBlockCrc = rawToCRC(lastBlockCrcRaw)
			}*/
		}
	} /* else {
		if restWrittenSize <= 0 {
			// last write is aligned with block size, then just get last block crc from b
			lastBlockCrcRaw := b[newDataEnd-crcSize:]
			s.lastBlockCrc = rawToCRC(lastBlockCrcRaw)
		}
	}*/

	// update last block crc when this is the last write and there is anymore data after last block filled
	if /*newDataEnd > lastBlockDataStart && */ restWrittenSize <= 0 {
		lastBlockCrcRaw := b[newDataEnd-crcSize:]
		s.lastBlockCrc = rawToCRC(lastBlockCrcRaw)
	}

	// dispatch io write into device
	// offset怎么对齐，next按上面定义是每次write的新增数据大小，不包括前面补齐512扇区的部分，同时也要减去覆盖crc的4字节
	sectorIdx := (uint64(sm.Offset) + uint64(sm.Size) + uint64(s.next)) / deviceSectorSize
	// 当前一次append的有效数据在倒数第二个sector或者刚好508个字节时，这里sectorIdx要减1，否则会导致数据crc没有覆盖导致没有写入。
	if lastSectorDataSize >= 508 {
		sectorIdx -= 1
	}
	// 最近一次写入未对齐，则扇区前移至前一个sector开始，如果对齐，则不需要-1
	/*if (uint64(sm.Offset)+uint64(sm.Size)+uint64(s.next))%deviceSectorSize > 0 {
		sectorIdx -= 1
	}*/

	// fmt.Println("after write data: ", b)
	// os.WriteFile("after-"+strconv.Itoa(int(sm.Size)), b, 0644)

	err = s.ioEngine.Write(b, sectorIdx*deviceSectorSize, len(b))
	if err != nil {
		return
	}
	/*n = int(toWrite)
	s.next += uint32(n)*/

	s.next += newDataEnd - newDataStart
	// 上次未对齐，则next需要减一下crc值多余部分
	/*if lastBlockDataSize > 0 && s.next == 0 {
		s.next -= crcSize
	}*/

	n = len(b)
	// 5.update last sector by copy when this is the last write of this append
	if restWrittenSize <= 0 {
		if n > deviceSectorSize {
			lastSectorDataSize := (newDataEnd - deviceSectorSize) % deviceSectorSize
			if lastSectorDataSize > 0 && lastSectorDataSize <= 3 {
				// copy(s.lastSector[:], b[n-deviceSectorSize:])
				// 剩余尾部数据大于508以上，则有1-3个字节的crc落在最后一个sector上，此时lastSector要从倒数第二个sector开始取。否则直接取最后一个sector就行
				copy(s.lastSector[:], b[n-2*deviceSectorSize:n-deviceSectorSize])
			} else {
				copy(s.lastSector[:], b[n-deviceSectorSize:])
			}
		} else {
			copy(s.lastSector[:], b)
		}
	}

	return
}

func (s *sliceWriter) WriteOld(b []byte) (n int, err error) {
	if len(b) > 1<<20 || len(b)%deviceSectorSize != 0 {
		panic(fmt.Sprintf("invalid buffer length: %d", len(b)))
	}

	sm := s.slice.GetShardMeta()
	if sm.Size+s.next >= s.sliceSize {
		return 0, io.EOF
	}

	fmt.Println("next: ", s.next, "write buffer len: ", len(b))
	// fmt.Println("next: ", s.next, "write buffer len: ", len(b), "before write data: ", b)
	// os.WriteFile("before-"+strconv.Itoa(int(sm.Size)), b, 0644)

	bufferSize := uint32(len(b))
	appendDataSize, newDataEnd := bufferSize, bufferSize
	// fix toWrite by actual size
	if s.next+appendDataSize > s.append.Size {
		appendDataSize = s.append.Size - s.next
		newDataEnd = appendDataSize
	}

	newDataStart := uint32(0)
	lastBlockDataSize := (sm.Size + s.next) % s.blockSize
	if sm.Size%s.blockSize != 0 {
		// last written size is not aligned with block size, then sm.Size should decrease with crcSize
		lastBlockDataSize = (sm.Size - crcSize + s.next) % s.blockSize
	}

	writtenSize := sm.Size + s.next + appendDataSize
	if lastBlockDataSize > 0 {
		writtenSize -= crcSize
	}
	if writtenSize > s.sliceSize {
		return 0, io.ErrUnexpectedEOF
	}

	// refill last sector data
	if lastBlockDataSize > 0 {
		lastSectorDataSize := lastBlockDataSize % deviceSectorSize
		// copy(b, s.lastSector[:lastSectorDataSize-crcSize])
		copy(b, s.lastSector[:lastSectorDataSize])
		// b: [last sector data|new append data]
		// newDataStart should start from the end of last sector data
		// newDataEnd should include last sector data and new append data
		// newDataStart = lastSectorDataSize - crcSize
		newDataStart = lastSectorDataSize
		if newDataEnd < bufferSize {
			newDataEnd += newDataStart
		}
	}

	restWrittenSize := s.append.Size - s.next - (newDataEnd - newDataStart)

	// calculate last write full block crc
	lastBlockDataStart := newDataStart
	//lastBlockUnalignedDataSize := lastBlockDataSize
	/*if lastBlockDataSize > 0 {
		lastBlockUnalignedDataSize -= crcSize
	}*/
	fmt.Println("last block data size: ", lastBlockDataSize, "last block data start: ", lastBlockDataStart, "new data end: ", newDataEnd)

	if lastBlockDataSize > 0 {
		// last write is not aligned with block size, we should calculate last block crc by append data
		if lastBlockDataSize+(newDataEnd-newDataStart) >= s.blockSize {
			if s.next == 0 {
				// one block: [data    |    crc]
				//             32KB-4  |     4
				//                     end
				end := s.blockSize - crcSize - lastBlockDataSize
				lastBlockCrc := crc32.Update(s.lastBlockCrc, crc32.IEEETable, b[newDataStart:newDataStart+end])
				lastBlockCrcRaw := b[newDataStart+end:]
				crcToRaw(lastBlockCrcRaw, lastBlockCrc)
				lastBlockDataStart += end + crcSize
				s.lastBlockCrc = 0
			}

			// update last block crc when this is the last write and there is anymore data after last block filled
			if newDataEnd > lastBlockDataStart && restWrittenSize <= 0 {
				lastBlockCrcRaw := b[newDataEnd-crcSize:]
				s.lastBlockCrc = rawToCRC(lastBlockCrcRaw)
			}
		} else {
			if s.next == 0 {
				s.lastBlockCrc = crc32.Update(s.lastBlockCrc, crc32.IEEETable, b[lastBlockDataStart:newDataEnd-crcSize])
				fmt.Println("slice writer last block crc: ", s.lastBlockCrc, s.next, appendDataSize, s.append.Size)
				// check if this is the last write, do persistence for the last block crc when there is anymore data after last block filled
				// if restWrittenSize <= 0 {
				lastBlockCrcRaw := b[newDataEnd-crcSize:]
				crcToRaw(lastBlockCrcRaw, s.lastBlockCrc)
				//}
			}
			if restWrittenSize <= 0 {
				lastBlockCrcRaw := b[newDataEnd-crcSize:]
				s.lastBlockCrc = rawToCRC(lastBlockCrcRaw)
			}
		}
	} else {
		if restWrittenSize <= 0 {
			// last write is aligned with block size, then just get last block crc from b
			lastBlockCrcRaw := b[newDataEnd-crcSize:]
			s.lastBlockCrc = rawToCRC(lastBlockCrcRaw)
		}
	}

	// dispatch io write into device
	// offset怎么对齐，next按上面定义是每次write的新增数据大小，不包括前面补齐512扇区的部分，同时也要减去覆盖crc的4字节
	sectorIdx := (uint64(sm.Offset) + uint64(sm.Size) + uint64(s.next)) / deviceSectorSize
	// 最近一次写入未对齐，则扇区前移至前一个sector开始，如果对齐，则不需要-1
	/*if (uint64(sm.Offset)+uint64(sm.Size)+uint64(s.next))%deviceSectorSize > 0 {
		sectorIdx -= 1
	}*/

	// fmt.Println("after write data: ", b)
	// os.WriteFile("after-"+strconv.Itoa(int(sm.Size)), b, 0644)

	err = s.ioEngine.Write(b, sectorIdx*deviceSectorSize, len(b))
	if err != nil {
		return
	}
	/*n = int(toWrite)
	s.next += uint32(n)*/

	s.next += newDataEnd - newDataStart
	// 上次未对齐，则next需要减一下crc值多余部分
	/*if lastBlockDataSize > 0 && s.next == 0 {
		s.next -= crcSize
	}*/

	n = len(b)
	// 5.update last sector by copy when this is the last write of this append
	if restWrittenSize <= 0 {
		if n > deviceSectorSize {
			copy(s.lastSector[:], b[n-deviceSectorSize:])
		} else {
			copy(s.lastSector[:], b)
		}
	}

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
	return &sliceWriter{
		lastBlockCrcRaw: make([]byte, 0, crcSize),
	}
}}

func crcToRaw(b []byte, crc uint32) {
	b[0] = byte(crc >> 24)
	b[1] = byte(crc >> 16)
	b[2] = byte(crc >> 8)
	b[3] = byte(crc)
}

func rawToCRC(b []byte) uint32 {
	return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
}

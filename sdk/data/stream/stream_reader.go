package stream

import (
	"fmt"
	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/sdk/data"
	"github.com/chubaoio/cbfs/util"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
	"io"
	"sync"
	"sync/atomic"
)

type ReadRequest struct {
	data       []byte
	offset     int
	size       int
	canRead    int
	err        error
	cond       *sync.Cond
	isResponse bool
}

type StreamReader struct {
	inode      uint64
	w          *data.Wrapper
	readers    []*ExtentReader
	getExtents GetExtentsFunc
	extents    *proto.StreamKey
	fileSize   uint64
}

func NewStreamReader(inode uint64, w *data.Wrapper, getExtents GetExtentsFunc) (stream *StreamReader, err error) {
	stream = new(StreamReader)
	stream.inode = inode
	stream.w = w
	stream.getExtents = getExtents
	stream.extents = proto.NewStreamKey(inode)
	stream.extents.Extents, err = stream.getExtents(inode)
	if err != nil {
		return
	}
	var offset int
	var reader *ExtentReader
	for _, key := range stream.extents.Extents {
		if reader, err = NewExtentReader(inode, offset, key, stream.w); err != nil {
			return nil, errors.Annotatef(err, "NewStreamReader inode[%v] "+
				"key[%v] dp not found error", inode, key)
		}
		stream.readers = append(stream.readers, reader)
		offset += int(key.Size)
	}
	stream.fileSize = stream.extents.Size()
	return
}

func (stream *StreamReader) toString() (m string) {
	return fmt.Sprintf("inode[%v] fileSize[%v] extents[%v] ",
		stream.inode, stream.fileSize, stream.extents)
}

func (stream *StreamReader) initCheck(offset, size int) (canread int, err error) {
	if size > util.ExtentSize {
		return 0, io.EOF
	}
	if offset+size <= int(stream.fileSize) {
		return size, nil
	}
	newStreamKey := proto.NewStreamKey(stream.inode)
	newStreamKey.Extents, err = stream.getExtents(stream.inode)

	if err == nil {
		err = stream.updateLocalReader(newStreamKey)
	}
	if err != nil {
		return 0, err
	}

	if offset > int(stream.fileSize) {
		return 0, io.EOF
	}
	if offset+size > int(stream.fileSize) {
		return int(stream.fileSize) - offset, io.EOF
	}

	return size, nil
}

func (stream *StreamReader) updateLocalReader(newStreamKey *proto.StreamKey) (err error) {
	var (
		newOffSet int
		r         *ExtentReader
	)
	readers := make([]*ExtentReader, 0)
	oldReaders := stream.readers
	oldReaderCnt := len(stream.readers)
	for index, key := range newStreamKey.Extents {
		if index < oldReaderCnt-1 {
			newOffSet += int(key.Size)
			continue
		} else if index == oldReaderCnt-1 {
			oldReaders[index].updateKey(key)
			newOffSet += int(key.Size)
			continue
		} else if index > oldReaderCnt-1 {
			if r, err = NewExtentReader(stream.inode, newOffSet, key, stream.w); err != nil {
				return errors.Annotatef(err, "NewStreamReader inode[%v] key[%v] "+
					"dp not found error", stream.inode, key)
			}
			readers = append(readers, r)
			newOffSet += int(key.Size)
			continue
		}
	}
	stream.fileSize = newStreamKey.Size()
	stream.extents = newStreamKey
	stream.readers = append(stream.readers, readers...)

	return nil
}

func (stream *StreamReader) read(data []byte, offset int, size int) (canRead int, err error) {
	var keyCanRead int
	keyCanRead, err = stream.initCheck(offset, size)
	if keyCanRead <= 0 || (err != nil && err != io.EOF) {
		return
	}
	readers, readerOffset, readerSize := stream.GetReader(offset, size)
	for index := 0; index < len(readers); index++ {
		r := readers[index]
		err = r.read(data[canRead:canRead+readerSize[index]], readerOffset[index], readerSize[index], offset, size)
		if err != nil {
			err = errors.Annotatef(err, "UserRequest{inode[%v] FileSize[%v] "+
				"Offset[%v] Size[%v]} readers{ [%v] Offset[%v] Size[%v] occous error}",
				stream.inode, stream.fileSize, offset, size, r.toString(), readerOffset[index],
				readerSize[index])
			log.LogErrorf(err.Error())
			return canRead, err
		}
		canRead += readerSize[index]
	}
	if canRead < size && err == nil {
		return canRead, io.EOF
	}

	return
}

func (stream *StreamReader) predictExtent(offset, size int) (startIndex int) {
	startIndex = offset >> 28
	r := stream.readers[startIndex]
	if int(atomic.LoadUint64(&r.startInodeOffset)) <= offset && int(atomic.LoadUint64(&r.endInodeOffset)) >= offset+size {
		return startIndex
	}
	index := startIndex - 1
	if index >= 0 {
		r = stream.readers[index]
		if int(atomic.LoadUint64(&r.startInodeOffset)) <= offset && int(atomic.LoadUint64(&r.endInodeOffset)) >= offset+size {
			return index
		}

	}
	index = startIndex - 2
	if index >= 0 {
		r = stream.readers[index]
		if int(atomic.LoadUint64(&r.startInodeOffset)) <= offset && int(atomic.LoadUint64(&r.endInodeOffset)) >= offset+size {
			return index
		}
	}

	return 0
}

func (stream *StreamReader) GetReader(offset, size int) (readers []*ExtentReader, readersOffsets []int, readersSize []int) {
	readers = make([]*ExtentReader, 0)
	readersOffsets = make([]int, 0)
	readersSize = make([]int, 0)
	startIndex := stream.predictExtent(offset, size)
	for _, r := range stream.readers[startIndex:] {
		if size <= 0 {
			break
		}
		if int(atomic.LoadUint64(&r.startInodeOffset)) > offset || int(atomic.LoadUint64(&r.endInodeOffset)) <= offset {
			continue
		}
		var (
			currReaderSize   int
			currReaderOffset int
		)
		if int(atomic.LoadUint64(&r.endInodeOffset)) >= offset+size {
			currReaderOffset = offset - int(atomic.LoadUint64(&r.startInodeOffset))
			currReaderSize = size
			offset += currReaderSize
			size -= currReaderSize
		} else {
			currReaderOffset = offset - int(atomic.LoadUint64(&r.startInodeOffset))
			currReaderSize = int(r.key.Size) - currReaderOffset
			offset = int(atomic.LoadUint64(&r.endInodeOffset))
			size = size - currReaderSize
		}
		if currReaderSize == 0 {
			continue
		}
		readersSize = append(readersSize, currReaderSize)
		readersOffsets = append(readersOffsets, currReaderOffset)
		readers = append(readers, r)
		if size <= 0 {
			break
		}
	}

	return
}

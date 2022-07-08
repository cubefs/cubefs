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

package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

type RecordReadAt interface {
	ReadAt(offset int64) (rec record, err error)
}

const defaultReadBufferedSize = 512

type recordReader struct {
	br         *bufio.Reader
	sr         io.ReaderAt
	offset     int64
	filename   string
	typeLenBuf []byte
}

func newRecordReader(f *os.File) *recordReader {
	return &recordReader{
		br:         bufio.NewReaderSize(f, defaultReadBufferedSize),
		sr:         f,
		filename:   f.Name(),
		typeLenBuf: make([]byte, 8),
	}
}

func (r *recordReader) Read() (recStartOffset int64, rec record, err error) {
	recStartOffset = r.offset

	// read record type and data len
	_, err = io.ReadFull(r.br, r.typeLenBuf)
	if err != nil {
		return
	}
	rec.dataLen = binary.BigEndian.Uint64(r.typeLenBuf)
	// read record and crc
	crc := crc32.NewIEEE()
	tr := io.TeeReader(r.br, crc)
	data := bytespool.Alloc(int(rec.dataLen) + 4)
	defer func() {
		if err != nil {
			bytespool.Free(data)
		}
	}()
	_, err = io.ReadFull(tr, data[0:int(rec.dataLen)])
	if err != nil {
		return
	}

	_, err = io.ReadFull(r.br, data[rec.dataLen:])
	if err != nil {
		return
	}

	// decode crc
	rec.recType = recordType(data[0])
	rec.crc = binary.BigEndian.Uint32(data[rec.dataLen:])
	rec.data = data[:rec.dataLen]
	// checksum
	if rec.crc != crc.Sum32() {
		err = NewLogError(r.filename, recStartOffset, "crc mismatch")
		return
	}

	r.offset += (8 + int64(rec.dataLen) + 4)

	return
}

func (r *recordReader) ReadAt(offset int64) (rec record, err error) {
	// read record type and data len
	n, err := r.sr.ReadAt(r.typeLenBuf, offset)
	if err != nil {
		if err == io.EOF {
			err = NewLogError(r.filename, offset, "too small record datalen")
		}
		return
	}
	rec.dataLen = binary.BigEndian.Uint64(r.typeLenBuf)

	// read data and crc
	data := bytespool.Alloc(int(rec.dataLen) + 4)
	defer func() {
		if err != nil {
			bytespool.Free(data)
		}
	}()
	_, err = r.sr.ReadAt(data, offset+int64(n))
	if err != nil {
		if err == io.EOF {
			err = NewLogError(r.filename, offset, "data size unmatch or too small crc")
		}
		return
	}

	// type
	rec.recType = recordType(data[0])
	// data
	rec.data = data[:rec.dataLen]
	// decode crc
	rec.crc = binary.BigEndian.Uint32(data[rec.dataLen:])
	// checksum
	crc := crc32.ChecksumIEEE(data[0:rec.dataLen])
	if rec.crc != crc {
		err = NewLogError(r.filename, offset, "crc mismatch")
		return
	}

	return
}

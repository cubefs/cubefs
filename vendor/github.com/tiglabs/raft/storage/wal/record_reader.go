// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"bufio"
	"encoding/binary"
	"io"
	"math"
	"os"

	"github.com/tiglabs/raft/util"
)

// 初始化完成之后，读取记录只能调用ReadAt方法
type recordReadAt interface {
	ReadAt(offset int64) (rec record, err error)
}

const defaultReadBufferedSize = 128 * 1024

type bufferedReader struct {
	r *bufio.Reader
}

func newBufferedReader(f *os.File) *bufferedReader {
	return &bufferedReader{
		r: bufio.NewReaderSize(f, defaultReadBufferedSize),
	}
}

func (br *bufferedReader) Read(p []byte) (total int, err error) {
	n := 0
	for {
		n, err = br.r.Read(p)
		if err != nil {
			return
		}

		total += n

		switch {
		case n == len(p):
			return
		case n < len(p):
			p = p[n:]
		default:
			panic("invalid read buffer")
		}
	}
}

type recordReader struct {
	br     *bufferedReader
	offset int64 // 当期记录的起始位置

	sr io.ReaderAt // 随机IO

	filename string

	typeLenBuf []byte
}

func newRecordReader(f *os.File) *recordReader {
	return &recordReader{
		br:         newBufferedReader(f),
		sr:         f,
		filename:   f.Name(),
		typeLenBuf: make([]byte, 9), // 1字节类型+8字节dataLen
	}
}

// 顺序读
func (r *recordReader) Read() (recStartOffset int64, rec record, err error) {
	recStartOffset = r.offset

	// read record type and data len
	n, err := r.br.Read(r.typeLenBuf)
	if err != nil {
		return
	}
	if n != len(r.typeLenBuf) {
		if n < 1 {
			err = NewCorruptError(r.filename, recStartOffset, "too small record type")
		} else {
			err = NewCorruptError(r.filename, recStartOffset, "too small record datalen")
		}
		return
	}

	// Decode and validate record type
	rec.recType = recordType(r.typeLenBuf[0])
	if !rec.recType.Valid() {
		err = NewCorruptError(r.filename, recStartOffset, "illegal record type")
		return
	}

	// Decode and validate record data length
	rec.dataLen = binary.BigEndian.Uint64(r.typeLenBuf[1:])
	if rec.dataLen+4 <= 0 || rec.dataLen > math.MaxUint32 {
		err = NewCorruptError(r.filename, recStartOffset, "illegal data length")
		return
	}

	// read data and crc
	// WARN：不可以用buffer pool，因为log entry等decode时没有进行拷贝
	rec.data = make([]byte, rec.dataLen+4)
	n, err = r.br.Read(rec.data)
	if err != nil {
		return
	}
	if uint64(n) != rec.dataLen+4 {
		err = NewCorruptError(r.filename, recStartOffset, "data size unmatch or too small crc")
		return
	}

	// decode crc
	rec.crc = binary.BigEndian.Uint32(rec.data[len(rec.data)-4:])
	// truncate crc
	rec.data = rec.data[:len(rec.data)-4]
	// checksum
	crc := util.NewCRC(rec.data)
	if rec.crc != crc.Value() {
		err = NewCorruptError(r.filename, recStartOffset, "crc mismatch")
		return
	}

	r.offset += (1 + 8 + int64(rec.dataLen) + 4)

	return
}

// 随机读指定offset
func (r *recordReader) ReadAt(offset int64) (rec record, err error) {
	defer func() {
		if err == io.EOF {
			err = NewCorruptError(r.filename, offset, "unexpected eof")
		}
	}()

	// read record type and data len
	n, err := r.sr.ReadAt(r.typeLenBuf, offset)
	if err != nil {
		return
	}
	if n != len(r.typeLenBuf) {
		if n < 1 {
			err = NewCorruptError(r.filename, offset, "too small record type")
		} else {
			err = NewCorruptError(r.filename, offset, "too small record datalen")
		}
		return
	}
	rec.recType = recordType(r.typeLenBuf[0])
	rec.dataLen = binary.BigEndian.Uint64(r.typeLenBuf[1:])

	// read data and crc
	if readLen := int(rec.dataLen + 4); readLen <= 0 || readLen > math.MaxInt32 {
		err = NewCorruptError(r.filename, offset, "illegal data length")
		return
	}
	rec.data = make([]byte, rec.dataLen+4)
	n, err = r.sr.ReadAt(rec.data, offset+int64(n))
	if err != nil {
		return
	}
	if uint64(n) != rec.dataLen+4 {
		err = NewCorruptError(r.filename, offset, "data size unmatch or too small crc")
		return
	}

	// decode crc
	rec.crc = binary.BigEndian.Uint32(rec.data[len(rec.data)-4:])
	// truncate crc
	rec.data = rec.data[:len(rec.data)-4]
	// checksum
	crc := util.NewCRC(rec.data)
	if rec.crc != crc.Value() {
		err = NewCorruptError(r.filename, offset, "crc mismatch")
		return
	}

	return
}

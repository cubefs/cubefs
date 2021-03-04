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
	"io"

	"encoding/binary"
	"fmt"
)

// 日志文件({seq}.log)格式：
// [log record]
//    ...
// [log record]
// [index record]
// [footer record]

// ErrCorrupt error
type ErrCorrupt struct {
	filename string
	offset   int64
	reason   string
}

func (e *ErrCorrupt) Error() string {
	return fmt.Sprintf("corrput data at %s:%d (%v)", e.filename, e.offset, e.reason)
}

// NewCorruptError new
func NewCorruptError(filename string, offset int64, reason string) *ErrCorrupt {
	return &ErrCorrupt{
		filename: filename,
		offset:   offset,
		reason:   reason,
	}
}

func IsErrCorrupt(err error) (is bool) {
	if err == nil {
		return
	}
	_, is = err.(*ErrCorrupt)
	return
}

type recordType uint8

const (
	recTypeLogEntry recordType = 1
	recTypeIndex    recordType = 2
	recTypeFooter   recordType = 3
)

func (rt recordType) Valid() bool {
	switch rt {
	case recTypeLogEntry, recTypeIndex, recTypeFooter:
		return true
	default:
	}
	return false
}

func (rt recordType) String() string {
	switch rt {
	case recTypeLogEntry:
		return "type-log"
	case recTypeIndex:
		return "type-index"
	case recTypeFooter:
		return "type-footer"
	default:
		return fmt.Sprintf("type-unknown(%d)", uint8(rt))
	}
}

var footerMagic = []byte{'\xf9', '\xbf', '\x3e', '\x0a', '\xd3', '\xc5', '\xcc', '\x3f'}

// record格式
type record struct {
	recType recordType // 字节类型
	dataLen uint64     // 八字节大端数据长度
	data    []byte     // []byte recordData.Encode()
	crc     uint32     // 固定四字节
}

// 一个record写入时最多需要多少字节的空间
func recordSize(data recordData) int {
	return 1 + 8 + int(data.Size()) + 4
}

type recordData interface {
	Encode(w io.Writer) error
	Size() uint64
}

type footerRecord struct {
	indexOffset uint64
	magic       []byte
}

func (fr footerRecord) Encode(w io.Writer) (err error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, fr.indexOffset)
	if _, err = w.Write(buf); err != nil {
		return
	}
	if _, err = w.Write(footerMagic); err != nil {
		return
	}
	return nil
}

func (fr footerRecord) Size() uint64 {
	return 16
}

func (fr *footerRecord) Decode(data []byte) {
	fr.indexOffset = binary.BigEndian.Uint64(data)
	fr.magic = data[8 : 8+len(footerMagic)]
}

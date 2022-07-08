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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

// wal format:
// |record|
// |  ... |
// |record|
// |index |
// |footer|

// logError error
type logError struct {
	filename string
	offset   int64
	reason   string
}

func (e *logError) Error() string {
	return fmt.Sprintf("invalid data at %s:%d (%s)", e.filename, e.offset, e.reason)
}

func IsLogError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*logError)
	return ok
}

// NewCorruptError new
func NewLogError(filename string, offset int64, reason string) *logError {
	return &logError{
		filename: filename,
		offset:   offset,
		reason:   reason,
	}
}

type Entry struct {
	pb.Entry
}

type recordType uint8

const (
	EntryType recordType = iota + 1
	IndexType
	FooterType
)

func (rt recordType) String() string {
	switch rt {
	case EntryType:
		return "entry"
	case IndexType:
		return "index"
	case FooterType:
		return "footer"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(rt))
	}
}

var magic = []byte{'\xf9', '\xbf', '\x3e', '\x0a', '\xd3', '\xc5', '\xcc', '\x3f'}

// record format
type record struct {
	dataLen uint64 // content len, include recType and data
	recType recordType
	data    []byte // []byte recordData.Encode()
	crc     uint32
}

func recordSize(r Recorder) int {
	return 8 + 1 + int(r.Size()) + 4
}

type Recorder interface {
	Encode(w io.Writer) error
	Size() uint64
}

type footer struct {
	indexOffset uint64
	magic       [8]byte
}

func (f *footer) Encode(w io.Writer) (err error) {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf, f.indexOffset)
	copy(buf[8:], magic)
	if _, err = w.Write(buf); err != nil {
		return
	}
	return nil
}

func (f *footer) Size() uint64 {
	return 16
}

func (f *footer) Decode(data []byte) {
	f.indexOffset = binary.BigEndian.Uint64(data)
	copy(f.magic[:], data[8:8+len(magic)])
}

func (entry *Entry) Encode(w io.Writer) (err error) {
	buf := bytespool.Alloc(int(entry.Size()))
	defer bytespool.Free(buf)
	if _, err = entry.MarshalTo(buf); err != nil {
		return err
	}
	if _, err = w.Write(buf); err != nil {
		return
	}
	return nil
}

func (entry *Entry) Size() uint64 {
	return uint64(entry.Entry.Size())
}

func (entry *Entry) Decode(data []byte) error {
	return entry.Unmarshal(data)
}

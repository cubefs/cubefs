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
	"encoding/binary"
	"os"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/util"
	"github.com/cubefs/cubefs/depends/tiglabs/raft/util/bufalloc"
)

const initialBufferSize = 1024 * 32
const flushTriggerSize = 1024 * 1024

type recordWriter struct {
	f      *os.File
	buf    bufalloc.Buffer
	u64Buf []byte
	u32Buf []byte
	offset int64
}

func newRecordWriter(f *os.File) *recordWriter {
	return &recordWriter{
		f:      f,
		u64Buf: make([]byte, 8),
		u32Buf: make([]byte, 4),
	}
}

func (w *recordWriter) Write(recType recordType, data recordData) error {
	if w.buf == nil {
		w.buf = bufalloc.AllocBuffer(initialBufferSize)
	}

	w.buf.Grow(recordSize(data))
	// write record type
	w.buf.WriteByte(byte(recType))
	// write data size
	binary.BigEndian.PutUint64(w.u64Buf, data.Size())
	w.buf.Write(w.u64Buf)
	// write data
	prevLen := w.buf.Len()
	data.Encode(w.buf)
	if uint64(w.buf.Len()-prevLen) != data.Size() {
		panic("fbase/raft/logstorage: unexpected data size when decode " + recType.String())
	}
	// write crc
	crc := util.NewCRC(w.buf.Bytes()[w.buf.Len()-int(data.Size()):])
	binary.BigEndian.PutUint32(w.u32Buf, crc.Value())
	w.buf.Write(w.u32Buf)

	w.offset += int64(recordSize(data))

	if err := w.tryToFlush(); err != nil {
		return err
	}

	return nil
}

func (w *recordWriter) tryToFlush() error {
	if w.buf != nil && w.buf.Len() >= flushTriggerSize {
		return w.Flush()
	}
	return nil
}

func (w *recordWriter) Offset() int64 {
	return w.offset
}

func (w *recordWriter) Truncate(offset int64) error {
	if err := w.f.Truncate(offset); err != nil {
		return err
	}
	w.offset = offset
	_, err := w.f.Seek(offset, os.SEEK_SET)
	return err
}

func (w *recordWriter) Flush() error {
	if w.buf != nil && w.buf.Len() > 0 {
		_, err := w.buf.WriteTo(w.f)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *recordWriter) Sync() error {
	if err := w.Flush(); err != nil {
		return err
	}

	return w.f.Sync()
}

// 关闭写
func (w *recordWriter) Close() error {
	if err := w.Sync(); err != nil {
		return err
	}
	if w.buf != nil {
		bufalloc.FreeBuffer(w.buf)
	}
	return nil
}

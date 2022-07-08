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
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const flushTriggerSize = 1024 * 1024

type recordWriter struct {
	file *os.File
	buf  *bytes.Buffer
	b    []byte
	off  int64
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func newRecordWriter(f *os.File) *recordWriter {
	b := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	return &recordWriter{
		file: f,
		buf:  b,
		b:    make([]byte, 8),
	}
}

func (rw *recordWriter) Write(recType recordType, rec Recorder) error {
	// write record size
	binary.BigEndian.PutUint64(rw.b, rec.Size()+1)
	rw.buf.Write(rw.b)
	crc := crc32.NewIEEE()
	mw := io.MultiWriter(rw.buf, crc)
	// write record type
	mw.Write([]byte{byte(recType)})
	// write data
	rec.Encode(mw)
	// write crc
	binary.BigEndian.PutUint32(rw.b, crc.Sum32())
	rw.buf.Write(rw.b[0:4])

	rw.off += int64(recordSize(rec))

	if err := rw.tryFlush(); err != nil {
		return err
	}

	return nil
}

func (rw *recordWriter) Truncate(off int64) error {
	if err := rw.file.Truncate(off); err != nil {
		return err
	}
	if _, err := rw.file.Seek(off, io.SeekStart); err != nil {
		return err
	}
	rw.off = off
	return nil
}

func (rw *recordWriter) Offset() int64 {
	return rw.off
}

func (rw *recordWriter) tryFlush() error {
	if rw.buf.Len() >= flushTriggerSize {
		return rw.Flush()
	}
	return nil
}

func (rw *recordWriter) Flush() error {
	if rw.buf.Len() > 0 {
		if _, err := rw.buf.WriteTo(rw.file); err != nil {
			return err
		}
	}
	return nil
}

func (rw *recordWriter) Sync() error {
	if err := rw.Flush(); err != nil {
		return err
	}

	return rw.file.Sync()
}

func (rw *recordWriter) Close() error {
	err := rw.Sync()
	bufPool.Put(rw.buf)
	rw.buf = nil
	return err
}

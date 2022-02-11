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

package util

import (
	"bufio"
	"errors"
	"io"
)

var (
	maxEmptyReads      = 100
	err_reader_isnil   = errors.New("BufferReader: reader is nil!")
	err_negative_count = errors.New("BufferReader: read return negative count!")
	err_no_progress    = errors.New("BufferReader: multiple Read calls return no data or error!")
	err_too_large      = errors.New("BufferReader: make byte slice too large!")
)

type BufferReader struct {
	buf    []byte
	reader io.Reader
	size   int
	r, w   int
	err    error
}

func NewBufferReader(reader io.Reader, size int) *BufferReader {
	return &BufferReader{
		reader: reader,
		size:   size,
		buf:    make([]byte, size),
	}
}

func (br *BufferReader) Reset() {
	if br.w > br.r {
		copy(br.buf, br.buf[br.r:br.w])
	}
	br.w = br.w - br.r
	br.r = 0
}

func (br *BufferReader) ReadFull(min int) (data []byte, err error) {
	if br.reader == nil {
		return nil, err_reader_isnil
	}
	if min == 0 {
		err = br.err
		br.err = nil
		return make([]byte, 0, 0), err
	}

	if min > (cap(br.buf) - br.r) {
		br.Grow(min)
	}
	for (br.w-br.r) < min && err == nil {
		br.fill()
		err = br.err
	}
	if (br.w - br.r) >= min {
		data = br.buf[br.r : br.r+min]
		br.r = br.r + min
		err = nil
	} else {
		data = br.buf[br.r:br.w]
		br.r = br.w
		err = br.err
		br.err = nil
	}
	return
}

func (br *BufferReader) fill() {
	if br.w >= cap(br.buf) {
		br.Grow(br.w - br.r)
	}

	for i := maxEmptyReads; i > 0; i-- {
		n, err := br.reader.Read(br.buf[br.w:])
		if n < 0 {
			panic(err_negative_count)
		}
		br.w = br.w + n
		if err != nil {
			br.err = err
			return
		}
		if n > 0 {
			return
		}
	}
	br.err = err_no_progress
}

func (br *BufferReader) Grow(n int) {
	defer func() {
		if recover() != nil {
			panic(err_too_large)
		}
	}()

	var buf []byte = nil
	if n > br.size {
		buf = make([]byte, n)
	} else {
		buf = make([]byte, br.size)
	}

	if br.w > br.r {
		copy(buf, br.buf[br.r:br.w])
	}
	br.w = br.w - br.r
	br.r = 0
	br.buf = buf
}

type BufferWriter struct {
	*bufio.Writer
}

func NewBufferWriter(wr io.Writer, size int) *BufferWriter {
	return &BufferWriter{
		Writer: bufio.NewWriterSize(wr, size),
	}
}

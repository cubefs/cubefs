// Copyright 2019 The ChubaoFS Authors.
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

package objectnode

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var (
	ErrUnexpectedChunkEnd  = errors.New("unexpected chunk end")
	ErrUnexpectedChunkSize = errors.New("unexpected chunk size")
)

type chunkedReader struct {
	readCloser io.ReadCloser
	chunkSize  int
	pos        int
	bof        bool
	eof        bool
}

func (r *chunkedReader) Close() error {
	return r.readCloser.Close()
}

func (r *chunkedReader) Read(p []byte) (n int, err error) {

	if r.eof {
		n = 0
		err = io.EOF
		return
	}

	if r.pos >= r.chunkSize {
		if err = r.nextChunk(); err != nil {
			return
		}
		if r.eof {
			n = 0
			err = io.EOF
			return
		}
	}
	length := len(p)
	if r.chunkSize-r.pos < length {
		length = r.chunkSize - r.pos
	}
	if n, err = r.readCloser.Read(p[:length]); err != nil {
		return
	}
	r.pos += n
	return
}

func (r *chunkedReader) nextChunk() (err error) {
	if !r.bof {
		if err = r.readCRLF(); err != nil {
			return
		}
	}
	if r.chunkSize, err = r.readChunkSize(); err != nil {
		return
	}
	r.bof = false
	r.pos = 0
	if r.chunkSize == 0 {
		r.eof = true
	}
	return
}

func (r *chunkedReader) readCRLF() (err error) {

	var tmp = make([]byte, 2)
	if _, err = r.readCloser.Read(tmp); err != nil {
		return
	}
	if cr, lf := int(tmp[0]), int(tmp[1]); (cr != '\r') || (lf != '\n') {
		err = fmt.Errorf("CRLF expected and end of chunk: %v/%v", cr, lf)
		return
	}
	return
}

func (r *chunkedReader) readByteToInt() (i int, err error) {
	var tmp = make([]byte, 1)
	if _, err = r.readCloser.Read(tmp); err != nil {
		return
	}
	i = int(tmp[0])
	return
}

func (r *chunkedReader) readChunkSize() (size int, err error) {
	var buf = make([]byte, 0)
	var state int
	for state != -1 {
		var b int
		if b, err = r.readByteToInt(); err != nil {
			return
		}
		if b == -1 {
			err = ErrUnexpectedChunkEnd
			return
		}
		switch state {
		case 0:
			switch b {
			case '\r':
				state = 1
				break

			case '"':
				state = 2

			default:
				buf = append(buf, byte(b))
			}
			break

		case 1:
			if b == '\n' {
				state = -1
			} else {
				err = ErrUnexpectedChunkSize
				return
			}
			break

		case 2:
			switch b {
			case '\\':
				if b, err = r.readByteToInt(); err != nil {
					return
				}
				buf = append(buf, byte(b))
				break

			case '"':
				state = 0

			default:
				buf = append(buf, byte(b))
			}
			break

		default:
			panic("assertion fail")
		}
	}
	var str = string(buf)
	if sepIndex := strings.Index(str, ";"); sepIndex > 0 {
		str = str[:sepIndex]
	}
	str = strings.TrimSpace(str)

	var result int64
	if result, err = strconv.ParseInt(str, 16, 64); err != nil {
		return
	}
	size = int(result)
	return
}

func NewChunkedReader(reader io.ReadCloser) io.ReadCloser {
	return &chunkedReader{
		readCloser: reader,
		pos:        0,
		bof:        true,
		eof:        false,
	}
}

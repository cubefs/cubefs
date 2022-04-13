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

package crc32block

import (
	"io"
)

type ReaderError struct {
	error
}

type WriterError struct {
	error
}

type Encoder struct {
	block blockUnit // block buffer
}

type limitEncoderReader struct {
	reader io.Reader
	block  blockUnit
	remain int64
	i, j   int
	err    error
}

type encoderReader struct {
	reader io.Reader //
	block  blockUnit //
	i, j   int       // block[i:j]
	err    error     //
}

func (enc *Encoder) Encode(from io.Reader, limitSize int64, to io.Writer) (n int64, err error) {
	if !isValidBlockLen(int64(enc.block.length())) {
		panic(ErrInvalidBlock)
	}

	encSize := EncodeSize(limitSize, int64(enc.block.length()))

	reader := &limitEncoderReader{reader: from, block: enc.block, remain: limitSize}

	return io.CopyN(to, reader, encSize)
}

func (r *limitEncoderReader) Read(b []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	for len(b) > 0 {
		if r.i == r.j {
			if r.remain == 0 {
				return n, io.EOF
			}
			if r.err = r.nextBlock(); r.err != nil {
				if n > 0 {
					return n, nil
				}
				return n, r.err
			}
		}

		readn := copy(b, r.block[r.i:r.j])
		r.i += readn

		b = b[readn:]
		n += readn
	}
	return
}

func (r *limitEncoderReader) nextBlock() (err error) {
	blockPayloadLen := r.block.payload()

	needn := blockPayloadLen
	if r.remain < int64(blockPayloadLen) {
		needn = int(r.remain)
	}

	block := blockUnit(r.block[:crc32Len+needn])

	n, err := io.ReadFull(r.reader, block[crc32Len:])
	if err != nil {
		return ReaderError{err}
	}

	r.i = 0
	r.j = crc32Len + n

	blockUnit(r.block[r.i:r.j]).writeCrc()
	r.remain -= int64(block.payload())

	return nil
}

func (r *encoderReader) Read(b []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	for len(b) > 0 {
		if r.i == r.j {
			if r.err = r.nextBlock(); r.err != nil {
				if n > 0 {
					return n, nil
				}
				return n, r.err
			}
		}

		readn := copy(b, r.block[r.i:r.j])
		r.i += readn

		b = b[readn:]
		n += readn
	}
	return
}

func (r *encoderReader) nextBlock() (err error) {
	n, err := readFullOrToEnd(r.reader, r.block[crc32Len:])
	if err != nil {
		return err
	}

	r.i = 0
	r.j = crc32Len + n

	blockUnit(r.block[r.i:r.j]).writeCrc()

	return nil
}

func NewEncoder(block []byte) (enc *Encoder, err error) {
	if block != nil && !isValidBlockLen(int64(len(block))) {
		return nil, ErrInvalidBlock
	}
	if block == nil {
		block = make([]byte, defaultCrc32BlockSize)
	}

	return &Encoder{block: block}, nil
}

// NewEncoderReader returns io.Reader
//
// Deprecated: no reused buffer, use NewBodyEncoder to instead.
func NewEncoderReader(r io.Reader) io.Reader {
	block := make([]byte, defaultCrc32BlockSize)
	return &encoderReader{block: block, reader: r}
}

func NewLimitEncoderReader(r io.Reader, limitSize int64) (enc *limitEncoderReader) {
	block := make([]byte, defaultCrc32BlockSize)
	enc = &limitEncoderReader{reader: r, block: block, remain: limitSize}
	return
}

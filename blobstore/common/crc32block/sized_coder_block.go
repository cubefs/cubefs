// Copyright 2024 The CubeFS Authors.
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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// NewSizedBlockEncoder return block sized encoder.
func NewSizedBlockEncoder(rc io.ReadCloser, actualSize, blockSize int64) io.ReadCloser {
	return NewSizedCoder(rc, actualSize, 0, blockSize, ModeBlockEncode, false)
}

// NewSizedBlockDecoder return block sized decoder.
func NewSizedBlockDecoder(rc io.ReadCloser, actualSize, blockSize int64) io.ReadCloser {
	return NewSizedCoder(rc, actualSize, 0, blockSize, ModeBlockDecode, false)
}

// NewSizedRangeBlockDecoder returns ranged crc32 block decoder, [from, to).
// The from should be in the first block of reader.
func NewSizedRangeBlockDecoder(rc io.ReadCloser, actualSize, from, to, blockSize int64) (int64, int64, io.ReadCloser) {
	return newSizedRangeDecoder(rc, actualSize, from, to, blockSize, ModeBlockDecode, false)
}

// encodeBlock writes crc and origin data to the parameter p.
// the parameter p must be no less than a block.
// p: [crc of payload : payload]
func (r *sizedCoder) encodeBlock(p []byte) (nn int, err error) {
	if r.remain <= 0 {
		return 0, io.EOF
	}

	n := r.payload + crc32.Size
	if extra := n - crc32.Size - r.remain; extra >= 0 {
		n -= extra
	}
	if len(p) < n {
		return 0, fmt.Errorf("crc32block: encode block buffer %d, but %d", n, len(p))
	}
	_, err = io.ReadFull(r.ReadCloser, p[crc32.Size:n])
	if err != nil {
		return 0, ReaderError{fmt.Errorf("crc32block: encode block read %s", err.Error())}
	}

	r.crc32.Write(p[crc32.Size:n])
	binary.LittleEndian.PutUint32(p, r.crc32.Sum32())
	r.crc32.Reset()

	nn += n
	r.remain -= n - crc32.Size
	return
}

// decodeBlock writes crc, origin head and data to the parameter p.
// the parameter p must be no less than a block.
// p: [crc of payload : payload]
func (r *sizedCoder) decodeBlock(p []byte) (nn int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remain <= 0 {
		return 0, io.EOF
	}

	n := r.payload + crc32.Size
	if extra := n - crc32.Size - r.remain; extra >= 0 {
		n -= extra
	}
	if len(p) < n {
		return 0, fmt.Errorf("crc32block: decode block buffer %d, but %d", n, len(p))
	}
	_, err = io.ReadFull(r.ReadCloser, p[:n])
	if err != nil {
		return 0, fmt.Errorf("crc32block: decode block read %s", err.Error())
	}

	r.crc32.Write(p[crc32.Size:n])
	act := r.crc32.Sum32()
	r.crc32.Reset()
	if binary.LittleEndian.Uint32(p) != act {
		r.err = ErrMismatchedCrc
		return 0, r.err
	}

	nn += n
	r.remain -= n - crc32.Size
	return
}

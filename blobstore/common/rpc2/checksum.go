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

package rpc2

// original |                body                 |
// encoded  | payload1+cell | payload2+cell |   ...    |
// len(payload1) = len(payload2) = blockSize, cell = hash.Hash.Size()

import (
	"bytes"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
)

const DefaultBlockSize = 64 << 10

var algorithms = map[ChecksumAlgorithm]func() hash.Hash{
	ChecksumAlgorithm_Crc_IEEE: func() hash.Hash { return crc32.NewIEEE() },
}

func (cd ChecksumDirection) IsUpload() bool {
	return cd == ChecksumDirection_Duplex || cd == ChecksumDirection_Upload
}

func (cd ChecksumDirection) IsDownload() bool {
	return cd == ChecksumDirection_Duplex || cd == ChecksumDirection_Download
}

func (cb *ChecksumBlock) EncodeSize(originalSize int64) int64 {
	if cb == nil {
		return originalSize
	}
	hasher := algorithms[cb.Algorithm]()
	payload := int64(cb.BlockSize)
	blocks := (originalSize + (payload - 1)) / payload
	return originalSize + int64(hasher.Size())*blocks
}

func (cb *ChecksumBlock) Hasher() hash.Hash {
	return algorithms[cb.Algorithm]()
}

func (cb *ChecksumBlock) Readable(b []byte) any {
	switch cb.Algorithm {
	case ChecksumAlgorithm_Crc_IEEE:
		return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	default:
		return nil
	}
}

func unmarshalBlock(b []byte) (*ChecksumBlock, error) {
	var block ChecksumBlock
	if err := block.Unmarshal(b); err != nil {
		return nil, fmt.Errorf("rpc2: internal checksum %s", err.Error())
	}
	if _, exist := algorithms[block.Algorithm]; !exist || block.BlockSize == 0 {
		return nil, fmt.Errorf("rpc2: checksum(%s) not implements", block.String())
	}
	return &block, nil
}

func checksumError(block ChecksumBlock, exp, act []byte) *Error {
	return &Error{
		Status: 400,
		Reason: "Checksum",
		Detail: fmt.Sprintf("rpc2: internal checksum algorithm(%s) direction(%s) exp(%v) act(%v)",
			block.Algorithm.String(), block.Direction.String(), block.Readable(exp), block.Readable(act)),
	}
}

// body encoder and decoder
type edBody struct {
	block  ChecksumBlock
	encode bool
	hasher hash.Hash

	remain int
	nx     int // block index
	cx     int // cell index, -1 means no sum cached
	cell   []byte
	err    error

	Body
}

func newEdBody(block ChecksumBlock, body Body, remain int, encode bool) *edBody {
	hasher := block.Hasher()
	return &edBody{
		block:  block,
		encode: encode,
		hasher: hasher,

		remain: remain,
		cx:     -1,
		cell:   make([]byte, hasher.Size()),

		Body: body,
	}
}

// encodeRead the parameter p is sumx.FreamWrite.data
func (r *edBody) encodeRead(p []byte) (nn int, err error) {
	var n int

	if r.cx >= 0 { // has remaining checksum
		n = copy(p, r.cell[r.cx:])
		nn += n
		r.cx += n
		if r.cx < r.hasher.Size() {
			return
		}
		r.cx = -1
		p = p[n:]
	}
	if r.remain <= 0 {
		if nn == 0 {
			err = io.EOF
		}
		return
	}

	blockSize := int(r.block.BlockSize)
	tryRead := blockSize - r.nx
	if tryRead > r.remain {
		tryRead = r.remain
	}

	if len(p) < tryRead {
		n, err = r.Body.Read(p)
	} else {
		n, err = r.Body.Read(p[:tryRead])
	}
	r.hasher.Write(p[:n])
	nn += n
	r.nx += n
	r.remain -= n

	if r.nx == blockSize || r.remain == 0 {
		copy(r.cell, r.hasher.Sum(nil))
		r.hasher = r.block.Hasher()
		r.cx = 0
		r.nx = 0
	}
	return
}

// decodeRead the parameter p is handler's memory location
func (r *edBody) decodeRead(p []byte) (nn int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remain <= 0 {
		return 0, io.EOF
	}

	if r.cx >= 0 {
		if _, err = io.ReadFull(r.Body, r.cell[r.cx:]); err != nil {
			r.err = err
			return 0, err
		}

		act := r.hasher.Sum(nil)
		if !bytes.Equal(r.cell, act) {
			r.err = checksumError(r.block, r.cell, act)
			return 0, r.err
		}

		r.cx = -1
		r.hasher = r.block.Hasher()
	}

	blockSize := int(r.block.BlockSize)
	tryRead := blockSize - r.nx
	if tryRead > r.remain {
		tryRead = r.remain
	}

	var n int
	if len(p) < tryRead {
		n, err = r.Body.Read(p)
	} else {
		n, err = r.Body.Read(p[:tryRead])
	}
	r.hasher.Write(p[:n])
	nn += n
	r.nx += n
	r.remain -= n

	if r.nx == blockSize || r.remain == 0 {
		_, err = io.ReadFull(r.Body, r.cell)
		if err != nil {
			r.err = err
			return 0, err
		}

		act := r.hasher.Sum(nil)
		if !bytes.Equal(r.cell, act) {
			r.err = checksumError(r.block, r.cell, act)
			return 0, r.err
		}

		r.hasher = r.block.Hasher()
		r.nx = 0
	}
	return
}

func (r *edBody) Read(p []byte) (nn int, err error) {
	var n int
	for len(p) > 0 {
		if r.encode {
			n, err = r.encodeRead(p)
		} else {
			n, err = r.decodeRead(p)
		}
		nn += n
		p = p[n:]
		if n == 0 || err != nil {
			break
		}
	}
	return
}

func (r *edBody) WriteTo(w io.Writer) (int64, error) {
	if r.encode {
		return r.Body.WriteTo(w)
	}
	return r.Body.WriteTo(&edBodyWriter{edBody: r, w: w})
}

type edBodyWriter struct {
	*edBody
	w io.Writer
}

// Write the parameter p is sumx.FreamRead.data
func (r *edBodyWriter) Write(p []byte) (nn int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	var n int
	if r.cx >= 0 {
		n = copy(r.cell[r.cx:], p)
		nn += n
		r.cx += n
		if r.cx < r.hasher.Size() {
			return
		}

		act := r.hasher.Sum(nil)
		if !bytes.Equal(r.cell, act) {
			r.err = checksumError(r.block, r.cell, act)
			return 0, r.err
		}

		r.cx = -1
		r.hasher = r.block.Hasher()
		p = p[n:]
	}
	if r.remain <= 0 {
		if nn == 0 {
			err = io.EOF
		}
		return
	}

	blockSize := int(r.block.BlockSize)
	tryRead := blockSize - r.nx
	if tryRead > r.remain {
		tryRead = r.remain
	}
	if len(p) > tryRead {
		p = p[:tryRead]
	}
	n, err = r.w.Write(p)
	r.hasher.Write(p[:n])
	nn += n
	r.nx += n
	r.remain -= n

	if r.nx == blockSize || r.remain == 0 {
		r.cx = 0
		r.nx = 0
	}
	return
}

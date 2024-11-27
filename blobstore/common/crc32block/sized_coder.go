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
	"bytes"
	"hash"
	"hash/crc32"
	"io"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

const (
	ModeEncode uint8 = 1
	ModeCheck  uint8 = 2
	ModeDecode uint8 = 3

	_alignment     = transport.Alignment
	_alignmentMask = _alignment - 1
)

// sizedCoder sized encoder and decoder
type sizedCoder struct {
	payload int
	mode    uint8
	section bool // decode only
	crc32   hash.Hash

	padhead int // pad head of aligned buffer
	remain  int // actual content remained
	padtail int // pad tail of aligned buffer
	nx      int // block index
	cx      int // cell index, -1 means no sum cached
	cell    [crc32.Size]byte
	err     error

	io.ReadCloser
}

type sizedCoderWt struct {
	*sizedCoder
	io.WriterTo
}

// NewSizedEncoder returns sized crc32 encoder.
func NewSizedEncoder(rc io.ReadCloser, actualSize int64) io.ReadCloser {
	return NewSizedCoder(rc, actualSize, 0, gBlockSize, ModeEncode, false)
}

// NewSizedDecoder returns sized crc32 decoder.
func NewSizedDecoder(rc io.ReadCloser, actualSize int64) io.ReadCloser {
	return NewSizedCoder(rc, actualSize, 0, gBlockSize, ModeDecode, false)
}

// NewSizedSectionDecoder returns section crc32 decoder.
func NewSizedSectionDecoder(rc io.ReadCloser, actualSize int64) io.ReadCloser {
	return NewSizedCoder(rc, actualSize, 0, gBlockSize, ModeDecode, true)
}

// NewSizedRangeDecoder returns ranged crc32 decoder, [from, to].
func NewSizedRangeDecoder(ra io.ReaderAt, actualSize, from, to int64) (int, int, io.ReadCloser) {
	blockSize := gBlockSize
	payload := BlockPayload(gBlockSize)

	blockOffset := (from / payload) * blockSize
	encodedSize, _ := PartialEncodeSizeWith(actualSize, 0, blockSize)
	encodedSize -= blockOffset

	head := from % payload
	tail := (payload - (to+1)%payload) % payload
	if more := (to + 1) + tail; more > actualSize {
		tail -= more - actualSize
	}
	actualSize = (to + 1) - from + head + tail

	rawReader := io.NewSectionReader(ra, blockOffset, encodedSize)
	rc := NewSizedCoder(io.NopCloser(rawReader), actualSize, 0, blockSize, ModeDecode, true)
	return int(head), int(tail), rc
}

// NewPartialEncoder returns partial crc32 encoder, added the padding buffer,
// actual size is next content size, stable size was written content size.
func NewPartialEncoder(rc io.ReadCloser, actualSize, stableSize int64) io.ReadCloser {
	return NewSizedCoder(rc, actualSize, stableSize, gBlockSize, ModeEncode, false)
}

// NewPartialDecoder returns partial crc32 decoder.
func NewPartialDecoder(rc io.ReadCloser, actualSize, stableSize int64) io.ReadCloser {
	return NewSizedCoder(rc, actualSize, stableSize, gBlockSize, ModeDecode, false)
}

// NewSizedCoder returns sized alignment crc32 en-decoder.
func NewSizedCoder(rc io.ReadCloser, actualSize, stableSize, blockSize int64, mode uint8, section bool) io.ReadCloser {
	if !isValidBlockLen(blockSize) {
		panic(ErrInvalidBlock)
	}
	payload := BlockPayload(blockSize)
	_, tail := PartialEncodeSizeWith(actualSize, stableSize, blockSize)
	sc := &sizedCoder{
		payload: int(payload),
		mode:    mode,
		section: section,
		crc32:   crc32.NewIEEE(),

		padhead: int((stableSize % payload) % _alignment),
		remain:  int(actualSize),
		padtail: int(tail),
		nx:      int((stableSize % payload) &^ _alignmentMask),
		cx:      -1,

		ReadCloser: rc,
	}

	wt, ok := rc.(io.WriterTo)
	if !ok {
		return sc
	}
	return &sizedCoderWt{sizedCoder: sc, WriterTo: wt}
}

// decodeRead writes origin data and crc to the parameter p.
func (r *sizedCoder) encodeRead(p []byte) (nn int, err error) {
	var n int
	if r.padhead > 0 { // has head padding
		if n = len(p); r.padhead > n {
			nn += n
			r.nx += n
			r.padhead -= n
			return
		}
		n, r.padhead = r.padhead, 0
		nn += n
		r.nx += n
		p = p[n:]
	}

	if r.cx >= 0 { // has remaining checksum
		n = copy(p, r.cell[r.cx:])
		nn += n
		r.cx += n
		if r.cx < len(r.cell) {
			return
		}
		r.cx = -1
		p = p[n:]
	}

	if r.remain <= 0 {
		if r.padtail > 0 { // has tail padding
			if n = len(p); r.padtail > n {
				nn += n
				r.padtail -= n
				return
			}
			n, r.padtail = r.padtail, 0
			nn += n
		}
		if nn == 0 {
			err = io.EOF
		}
		return
	}

	tryRead := r.payload - r.nx
	if tryRead > r.remain {
		tryRead = r.remain
	}

	if len(p) < tryRead {
		n, err = r.ReadCloser.Read(p)
	} else {
		n, err = r.ReadCloser.Read(p[:tryRead])
	}
	r.crc32.Write(p[:n])
	nn += n
	r.nx += n
	r.remain -= n

	if r.nx == r.payload || r.remain == 0 {
		copy(r.cell[:], r.crc32.Sum(nil))
		r.crc32.Reset()
		r.cx = 0
		r.nx = 0
	}
	return
}

// decodeRead writes origin data to the parameter p.
func (r *sizedCoder) decodeRead(p []byte) (nn int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remain <= 0 {
		return 0, io.EOF
	}
	if r.padhead > 0 {
		_, err = io.CopyN(io.Discard, r.ReadCloser, int64(r.padhead))
		if err != nil {
			r.err = err
			return 0, err
		}
		r.nx += r.padhead
		r.padhead = 0
	}

	tryRead := r.payload - r.nx
	if tryRead > r.remain {
		tryRead = r.remain
	}

	var n int
	if len(p) < tryRead {
		n, err = r.ReadCloser.Read(p)
	} else {
		n, err = r.ReadCloser.Read(p[:tryRead])
	}
	r.crc32.Write(p[:n])
	nn += n
	r.nx += n
	r.remain -= n

	if r.nx == r.payload || r.remain == 0 {
		_, err = io.ReadFull(r.ReadCloser, r.cell[:])
		if err != nil {
			r.err = err
			return 0, err
		}

		act := r.crc32.Sum(nil)
		if !bytes.Equal(r.cell[:], act) {
			r.err = ErrMismatchedCrc
			return 0, r.err
		}

		r.crc32.Reset()
		r.nx = 0

		if r.section && r.remain > 0 { // return sectioned error
			err = transport.ErrFrameContinue
		}

		if r.remain == 0 && r.padtail > 0 {
			_, err = io.CopyN(io.Discard, r.ReadCloser, int64(r.padtail))
			if err != nil {
				r.err = err
				return 0, err
			}
			r.padtail = 0
		}
	}
	return
}

func (r *sizedCoder) Read(p []byte) (nn int, err error) {
	var n int
	for len(p) > 0 {
		switch r.mode {
		case ModeEncode:
			n, err = r.encodeRead(p)
		case ModeDecode:
			n, err = r.decodeRead(p)
		case ModeCheck:
			panic("crc32block: implement checker with WriterTo")
		default:
			panic("crc32block: unknow mode with Reader")
		}
		nn += n
		p = p[n:]
		if n == 0 || err != nil {
			return
		}
	}
	return
}

func (wt *sizedCoderWt) WriteTo(w io.Writer) (int64, error) {
	if wt.mode == ModeEncode {
		return wt.WriterTo.WriteTo(w)
	}
	return wt.WriterTo.WriteTo(&sizedCoderWriter{sizedCoder: wt.sizedCoder, w: w})
}

type sizedCoderWriter struct {
	*sizedCoder
	w io.Writer
}

func (r *sizedCoderWriter) Write(p []byte) (nn int, err error) {
	switch r.mode {
	case ModeDecode:
		return r.decodeWrite(p, false)
	case ModeCheck:
		return r.checkWrite(p)
	default:
		panic("crc32block: unknow mode with WriterTo")
	}
}

func (r *sizedCoderWriter) decodeWrite(p []byte, check bool) (nn int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	var n int
	if r.padhead > 0 { // has head padding
		if n = len(p); r.padhead > n {
			nn += n
			r.nx += n
			r.padhead -= n
			return
		}
		n, r.padhead = r.padhead, 0
		nn += n
		r.nx += n
		p = p[n:]
	}

	if r.cx >= 0 { // has remaining checksum
		n = copy(r.cell[r.cx:], p)
		nn += n
		r.cx += n
		if r.cx < len(r.cell) {
			return
		}

		act := r.crc32.Sum(nil)
		if !bytes.Equal(r.cell[:], act) {
			r.err = ErrMismatchedCrc
			return 0, r.err
		}

		r.cx = -1
		r.crc32.Reset()
		p = p[n:]
	}

	if r.remain <= 0 {
		if r.padtail > 0 { // has tail padding
			if n = len(p); r.padtail > n {
				nn += n
				r.padtail -= n
				return
			}
			n, r.padtail = r.padtail, 0
			nn += n
		}
		if nn == 0 {
			err = io.EOF
		}
		return
	}

	n = r.payload - r.nx
	if n > r.remain {
		n = r.remain
	}
	if check {
		if n > len(p) {
			n = len(p)
		}
	} else {
		if len(p) > n {
			p = p[:n]
		}
		n, err = r.w.Write(p)
	}
	r.crc32.Write(p[:n])
	nn += n
	r.nx += n
	r.remain -= n

	if r.nx == r.payload || r.remain == 0 {
		r.cx = 0
		r.nx = 0
		if r.section && err == nil && r.remain > 0 { // return sectioned error
			err = transport.ErrFrameContinue
		}
	}
	return
}

func (r *sizedCoderWriter) checkWrite(p []byte) (nn int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	pp := p

	var n int
	for len(p) > 0 && err == nil {
		n, err = r.decodeWrite(p, true)
		nn += n
		p = p[n:]
		if err == io.EOF {
			break
		}
	}
	if r.err != nil {
		return 0, r.err
	}
	err = r.checkWriteBuffer(pp[:nn])
	r.err = err
	return
}

func (r *sizedCoderWriter) checkWriteBuffer(p []byte) (err error) {
	var n int
	for len(p) > 0 {
		n, err = r.w.Write(p)
		if err != nil {
			return
		}
		p = p[n:]
	}
	return nil
}

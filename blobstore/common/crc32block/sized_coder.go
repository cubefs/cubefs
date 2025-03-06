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
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"sync"
	// "github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

// Notice: All closer in this file must close once at most.
// like:
// func example() {
// 	rc := NewSizedCoder(...)
// 	buf, err := io.ReadAll(rc)
// 	...
// 	err = rc.Close()
// 	rc = nil
// }

const (
	// write crc after payload
	ModeEncode uint8 = 1 // encode mode
	ModeAppend uint8 = 2 // append, append buffer to stable, just fix first block
	ModeCheck  uint8 = 3 // check, return buffer with head, crc and tail
	ModeDecode uint8 = 4 // decode, return data buffer
	ModeLoad   uint8 = 5 // load, return buffer with head, should with section
	ModeFix    uint8 = 6 // fix, return buffer with fixed first and last block crc

	// write crc before payload
	ModeBlockEncode uint8 = 11 // encode mode, crc at head of block
	ModeBlockDecode uint8 = 12 // decode, return buffer with crc size and head

	// TODO: transport
	// _alignment     = transport.Alignment
	_alignment     = 512
	_alignmentMask = _alignment - 1
)

var ErrFrameContinue = errors.New("crc32block: add back later")

var (
	be         = binary.BigEndian
	ieeeBinary = getIEEE()

	poolCoder    = sync.Pool{New: func() interface{} { return &sizedCoder{crc32: crc32.NewIEEE()} }}
	poolCoderWt  = sync.Pool{New: func() interface{} { return &sizedCoderWt{lastcrc32: crc32.NewIEEE()} }}
	poolWriterTo = sync.Pool{New: func() interface{} { return &sizedCoderWriter{} }}
	poolFixer    = sync.Pool{New: func() interface{} { return &sizedFixer{crc32: crc32.NewIEEE()} }}
)

func getIEEE() []byte {
	crc := crc32.NewIEEE()
	b, _ := crc.(encoding.BinaryMarshaler).MarshalBinary()
	return b[: len(b)-crc32.Size : len(b)-crc32.Size]
}

// sizedCoder sized encoder and decoder
type sizedCoder struct {
	payload int
	mode    uint8
	section bool // decode only
	crc32   hash.Hash32

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

	lastremain int    // first block's remain, lastpad + data + crc
	lastpad    []byte // if ispadsum, means that lastpad is checksum
	ispadsum   bool
	lastcrc32  hash.Hash32
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

// NewSizedRangeDecoder returns ranged crc32 decoder, [from, to).
// The from should be in the first block of reader.
func NewSizedRangeDecoder(rc io.ReadCloser, actualSize, from, to, blockSize int64) (int64, int64, io.ReadCloser) {
	return newSizedRangeDecoder(rc, actualSize, from, to, blockSize, ModeDecode, true)
}

func newSizedRangeDecoder(rc io.ReadCloser,
	actualSize, from, to, blockSize int64,
	mode uint8, section bool,
) (int64, int64, io.ReadCloser) {
	payload := BlockPayload(blockSize)

	head := from % payload
	tail := (payload - to%payload) % payload
	if more := to + tail; more > actualSize {
		tail -= more - actualSize
	}
	actualSize = to - from + head + tail

	rc = NewSizedCoder(rc, actualSize, 0, blockSize, mode, section)
	return head, tail, rc
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
	sc := poolCoder.Get().(*sizedCoder)
	sc.payload = int(payload)
	sc.mode = mode
	sc.section = section
	sc.crc32.Reset()

	sc.padhead = int((stableSize % payload) % _alignment)
	sc.remain = int(actualSize)
	sc.padtail = int(tail)
	sc.nx = int((stableSize % payload) &^ _alignmentMask)
	sc.cx = -1

	sc.err = nil
	sc.ReadCloser = rc

	wt, ok := rc.(io.WriterTo)
	if !ok {
		return sc
	}
	scwt := poolCoderWt.Get().(*sizedCoderWt)
	scwt.sizedCoder = sc
	scwt.WriterTo = wt
	return scwt
}

// NewSizedAppend returns sized alignment crc32 append encoder.
func NewSizedAppend(rc io.ReadCloser,
	actualSize, stableSize, blockSize int64, section bool,
	lastpad, lastcrc []byte,
) (io.ReadCloser, error) {
	rc = NewSizedCoder(rc, actualSize, stableSize, blockSize, ModeAppend, section)
	sc, ok := rc.(*sizedCoderWt)
	if !ok {
		return nil, fmt.Errorf("crc32block: should io.WriteTo reader")
	}
	if sc.padhead == 0 && sc.nx == 0 {
		return sc, nil
	}
	if sc.padhead > len(lastpad) {
		return nil, fmt.Errorf("crc32block: last pad should %d, but %d", sc.padhead, len(lastpad))
	}
	sc.lastpad = lastpad[:sc.padhead]
	if sc.nx+sc.padhead+sc.remain > sc.payload {
		sc.lastremain = sc.payload - sc.nx + crc32.Size
	} else {
		sc.lastremain = sc.padhead + sc.remain + crc32.Size
	}
	sc.ispadsum = false
	sc.lastcrc32.Reset()
	if err := sc.lastcrc32.(encoding.BinaryUnmarshaler).
		UnmarshalBinary(append(ieeeBinary[:], lastcrc...)); err != nil {
		return nil, fmt.Errorf("crc32block: last block crc unmarshal %s", err.Error())
	}
	return sc, nil
}

// encodeRead writes origin data and crc to the parameter p.
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
		r.crc32.Sum(r.cell[:0])
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

		act := r.crc32.Sum32()
		if be.Uint32(r.cell[:]) != act {
			r.err = ErrMismatchedCrc
			return 0, r.err
		}

		r.crc32.Reset()
		r.nx = 0

		if r.section && r.remain > 0 { // return sectioned error
			err = ErrFrameContinue
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

// decodeLoad writes origin head and data to the parameter p.
func (r *sizedCoder) decodeLoad(p []byte) (nn int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remain <= 0 {
		return 0, io.EOF
	}
	if !r.section {
		return 0, fmt.Errorf("crc32block: should load with sectioned")
	}
	if len(p) == 0 || len(p)%_alignment != 0 {
		return 0, fmt.Errorf("crc32block: should aligned buffer, but %d", len(p))
	}

	tryRead := r.payload + crc32.Size - r.nx
	extra := tryRead - r.padhead - crc32.Size - r.padtail - r.remain
	if extra >= 0 {
		tryRead -= extra
	}
	if len(p) < tryRead {
		return 0, fmt.Errorf("crc32block: should enough buffer %d, but %d", tryRead, len(p))
	}

	var n int
	n, err = r.ReadCloser.Read(p[:tryRead])
	if n != tryRead {
		return 0, fmt.Errorf("crc32block: short of read should %d, buf %d", tryRead, n)
	}
	if r.padhead > 0 {
		p = p[r.padhead:]
		nn += r.padhead // notice to caller, the buffer has head
		r.nx += r.padhead
		n -= r.padhead
		r.padhead = 0
	}

	n -= crc32.Size
	if extra >= 0 { // last block
		n -= r.padtail
		r.padtail = 0
	}
	copy(r.cell[:], p[n:])

	nn += n
	r.crc32.Write(p[:n])
	r.nx += n
	r.remain -= n

	if r.nx == r.payload || r.remain == 0 {
		act := r.crc32.Sum32()
		if be.Uint32(r.cell[:]) != act {
			r.err = ErrMismatchedCrc
			return 0, r.err
		}

		r.crc32.Reset()
		r.nx = 0
		r.cx = -1

		if r.section && r.remain > 0 { // return sectioned error
			err = ErrFrameContinue
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
		case ModeLoad:
			return r.decodeLoad(p)
		case ModeAppend, ModeCheck:
			panic("crc32block: implement checker with WriterTo")
		case ModeBlockEncode:
			n, err = r.encodeBlock(p)
		case ModeBlockDecode:
			return r.decodeBlock(p)
		default:
			panic(fmt.Sprintf("crc32block: unknow mode %d with Reader", r.mode))
		}
		nn += n
		p = p[n:]
		if n == 0 || err != nil {
			if err == io.EOF && nn > 0 {
				err = nil
			}
			return
		}
	}
	return
}

func (r *sizedCoder) Close() (err error) {
	err = r.ReadCloser.Close()
	poolCoder.Put(r) // nolint: staticcheck
	return
}

func (wt *sizedCoderWt) WriteTo(w io.Writer) (int64, error) {
	if wt.mode == ModeEncode {
		return wt.WriterTo.WriteTo(w)
	}
	cachewt := poolWriterTo.Get().(*sizedCoderWriter)
	cachewt.sizedCoder = wt.sizedCoder
	cachewt.w = w
	cachewt.wt = wt
	nn, err := wt.WriterTo.WriteTo(cachewt)
	*cachewt = sizedCoderWriter{}
	poolWriterTo.Put(cachewt) // nolint: staticcheck
	return nn, err
}

func (wt *sizedCoderWt) Close() (err error) {
	err = wt.sizedCoder.Close()
	wt.sizedCoder = nil
	wt.WriterTo = nil
	poolCoderWt.Put(wt) // nolint: staticcheck
	return
}

type sizedCoderWriter struct {
	*sizedCoder
	w  io.Writer
	wt *sizedCoderWt
}

func (r *sizedCoderWriter) Write(p []byte) (nn int, err error) {
	switch r.mode {
	case ModeDecode:
		return r.decodeWrite(p, false)
	case ModeAppend:
		return r.appendWrite(p)
	case ModeCheck:
		return r.checkWrite(p)
	default:
		panic(fmt.Sprintf("crc32block: unknow mode %d with WriterTo", r.mode))
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

		act := r.crc32.Sum32()
		if be.Uint32(r.cell[:]) != act {
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
			err = ErrFrameContinue
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

	written, errw := r.checkWriteBuffer(pp[:nn])
	if written != nn && errw == nil {
		errw = io.ErrShortWrite
	}
	if errw != nil {
		if errw != ErrFrameContinue {
			r.err = errw
		}
		return written, errw
	}
	return
}

func (r *sizedCoderWriter) appendWrite(p []byte) (nn int, err error) {
	pp := p
	written := r.wt.lastremain
	if written > len(pp) {
		written = len(pp)
	}
	var n int
	for r.wt.lastremain > 0 && len(pp) > 0 {
		if len(r.wt.lastpad) > 0 && !r.wt.ispadsum {
			n = copy(pp, r.wt.lastpad)
			r.wt.lastremain -= n
			r.wt.lastpad = r.wt.lastpad[n:]
			pp = pp[n:]
		}
		if len(r.wt.lastpad) == 0 && !r.wt.ispadsum {
			n = r.wt.lastremain - crc32.Size
			if len(pp) < n {
				n = len(pp)
			}
			r.wt.lastcrc32.Write(pp[:n])
			pp = pp[n:]
			if r.wt.lastremain -= n; r.wt.lastremain == crc32.Size {
				r.wt.lastpad = r.wt.lastcrc32.Sum(nil)
				r.wt.ispadsum = true
			}
		}
		if len(r.wt.lastpad) > 0 && r.wt.ispadsum {
			n = copy(pp, r.wt.lastpad)
			r.wt.lastremain -= n
			r.wt.lastpad = r.wt.lastpad[n:]
			pp = pp[n:]
		}
	}

	nn, err = r.w.Write(p)
	if nn < written && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil && err != ErrFrameContinue {
		r.err = err
	}
	return
}

func (r *sizedCoderWriter) checkWriteBuffer(p []byte) (nn int, err error) {
	var n int
	for len(p) > 0 {
		n, err = r.w.Write(p)
		nn += n
		if n == 0 || err != nil {
			return
		}
		p = p[n:]
	}
	return
}

// sizedFixer sized fixer for range
type sizedFixer struct {
	mode  uint8
	crc32 hash.Hash32

	first, firstlen int // first start block index
	last, lastlen   int // last end block index
	index           int // read index
	remain          int // actual encoded content remained

	cx   int // cell index, -1 means no sum cached
	cell [crc32.Size]byte
	err  error

	io.ReadCloser
}

func (r *sizedFixer) Read(p []byte) (nn int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remain <= 0 {
		return 0, io.EOF
	}
	if len(p) == 0 || len(p)%_alignment != 0 {
		return 0, fmt.Errorf("crc32block: should aligned buffer, but %d", len(p))
	}
	nn, err = r.ReadCloser.Read(p)
	if r.err = err; err != nil {
		return 0, err
	}
	if nn == 0 || nn%_alignment != 0 {
		return 0, fmt.Errorf("crc32block: short of read buffer, buf %d", nn)
	}
	if nn > r.remain {
		nn = r.remain
	}
	r.remain -= nn
	p = p[:nn]

	var n int
	if r.cx >= 0 { // has remaining checksum
		n = copy(p, r.cell[r.cx:])
		r.cx = -1
		p = p[n:]
	}

	var s, e int
	if r.firstlen > 0 && r.index+len(p) > r.first { // first block
		s = r.first - r.index
		if e = len(p); e > s+r.firstlen {
			e = s + r.firstlen
		}
		r.crc32.Write(p[s:e])
		r.first += e - s
		r.firstlen -= e - s
		if r.firstlen == 0 {
			r.crc32.Sum(p[e:e])
			r.crc32.Reset()
			r.first = 0
		}
	}

	if r.lastlen > 0 && r.index+len(p) > r.last { // last block
		s = r.last - r.index
		if e = len(p); e > s+r.lastlen {
			e = s + r.lastlen
		}
		r.crc32.Write(p[s:e])
		r.last += e - s
		r.lastlen -= e - s
		if r.lastlen == 0 {
			r.crc32.Sum(r.cell[:0])
			n = copy(p[e:], r.cell[:])
			if n < len(r.cell) {
				r.cx = n
			}
			r.crc32.Reset()
			r.last = 0
		}
	}

	r.index += nn
	return
}

func (r *sizedFixer) Close() (err error) {
	err = r.ReadCloser.Close()
	poolFixer.Put(r) // nolint: staticcheck
	return
}

// NewSizedFixer returns fixed-range crc32 data, [from, to).
// The from should be in the first block of reader.
// Only fix the first and last block if needed, do not check other blocks.
// Returns head and tail are aligned.
func NewSizedFixer(rc io.ReadCloser, actualSize, from, to, blockSize int64) (int64, int64, io.ReadCloser) {
	if !isValidBlockLen(blockSize) {
		panic(ErrInvalidBlock)
	}
	payload := BlockPayload(blockSize)

	head := from % payload
	tail := (payload - to%payload) % payload
	if more := to + tail; more > actualSize {
		tail -= more - actualSize
	}
	actualSize = to - from + head + tail

	encodedSize, padtail := PartialEncodeSizeWith(actualSize, 0, blockSize)

	sf := poolFixer.Get().(*sizedFixer)
	sf.mode = ModeFix
	sf.crc32.Reset()
	sf.first, sf.firstlen = 0, 0
	sf.last, sf.lastlen = 0, 0
	sf.index = 0
	sf.remain = int(encodedSize)
	sf.cx = -1
	sf.err = nil
	sf.ReadCloser = rc

	lastdata := (encodedSize - padtail - crc32.Size) % blockSize
	if tail > 0 {
		sf.last = int(encodedSize - padtail - crc32.Size - lastdata)
		sf.lastlen = int(lastdata - tail)
	}
	if encodedSize > blockSize && head > 0 {
		sf.first = int(head)
		sf.firstlen = int(payload - head)
	} else if encodedSize <= blockSize && head > 0 {
		if tail > 0 {
			sf.last += int(head)
			sf.lastlen -= int(head)
		} else {
			sf.last = int(encodedSize - padtail - crc32.Size - lastdata + head)
			sf.lastlen = int(lastdata - head)
		}
	}

	head = head - head%_alignment
	tail = tail + padtail - (tail+padtail)%_alignment
	return head, tail, sf
}

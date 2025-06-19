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
	"bytes"
	crand "crypto/rand"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	mrand "math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const k32 = 32 << 10

type randReadWriter struct {
	rhasher hash.Hash32
	whasher hash.Hash32
}

func (r *randReadWriter) Read(p []byte) (int, error) {
	crand.Read(p)
	r.rhasher.Write(p)
	return len(p), nil
}

func (r *randReadWriter) Write(p []byte) (int, error) {
	r.whasher.Write(p)
	return len(p), nil
}

func (*randReadWriter) WriteTo(io.Writer) (int64, error) { panic("should not call WriteTo") }
func (*randReadWriter) Close() error                     { return nil }

type transReadWriter struct {
	step int
	off  int
	data []byte
}

func (t *transReadWriter) Read(p []byte) (int, error) {
	if t.off >= len(t.data) {
		return 0, io.EOF
	}
	n := copy(p, t.data[t.off:])
	t.off += n
	return n, nil
}

func (t *transReadWriter) ReadFrom(r io.Reader) (nn int64, err error) {
	var n int
	for t.off < len(t.data) {
		p := t.data[t.off:]
		if len(p) >= t.step {
			p = p[:t.step]
		}
		n, err = r.Read(p)
		nn += int64(n)
		t.off += n
		if err != nil {
			break
		}
	}
	return
}

func (t *transReadWriter) WriteTo(w io.Writer) (nn int64, err error) {
	if t.off >= len(t.data) {
		return 0, io.EOF
	}
	var n int
	for {
		p := t.data[t.off:]
		if len(p) >= t.step {
			p = p[:t.step]
		}
		n, err = w.Write(p)
		nn += int64(n)
		t.off += n
		if t.off == len(t.data) {
			return nn, nil
		}
		if n == 0 || err != nil {
			return nn, err
		}
	}
}

func (t *transReadWriter) Close() error { return nil }

func newRc(b []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(b))
}

func TestSizedCoderBase(t *testing.T) {
	for _, step := range []int{1, 2, 7, 16, 1 << 10, 64 << 10} {
		for _, blockSize := range []int64{4 << 10, 32 << 10, 64 << 10} {
			for idx := range [10]struct{}{} {
				size := mrand.Int63n(9<<14) + mrand.Int63n(11) + 1
				if idx == 0 {
					size = blockSize - 1
				}
				logName := fmt.Sprintf("step:%d block:%d size:%d", step, blockSize, size)

				clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
				encodeBody := NewSizedCoder(clientBody, size, 0, blockSize, ModeEncode, false)

				encodeSize, _ := PartialEncodeSizeWith(size, 0, blockSize)
				transBody := &transReadWriter{step: step, data: make([]byte, encodeSize)}
				nn, err := transBody.ReadFrom(encodeBody)
				require.NoError(t, err, logName)
				require.Equal(t, int64(len(transBody.data)), nn)
				_, err = encodeBody.Read(make([]byte, 1))
				require.Equal(t, io.EOF, err, logName)

				transBody.off = 0
				decodeBody := NewSizedCoder(transBody, size, 0, blockSize, ModeDecode, false)
				serverBody := &randReadWriter{whasher: crc32.NewIEEE()}

				if idx%2 == 0 {
					decodeBodyWt := decodeBody.(io.WriterTo)
					nn, err = decodeBodyWt.WriteTo(serverBody)
					require.NoError(t, err, logName)
					require.Equal(t, encodeSize, nn, logName)
					require.Equal(t, clientBody.rhasher.Sum32(), serverBody.whasher.Sum32(), logName)
					_, err = decodeBodyWt.WriteTo(serverBody)
					require.ErrorIs(t, err, io.EOF, logName)
				} else {
					b := make([]byte, size)
					n, err := io.ReadFull(decodeBody, b)
					require.NoError(t, err, logName)
					require.Equal(t, int(size), n, logName)
					serverBody.Write(b)
					require.Equal(t, clientBody.rhasher.Sum32(), serverBody.whasher.Sum32(), logName)
					_, err = decodeBody.Read(make([]byte, 1))
					require.ErrorIs(t, err, io.EOF, logName)
				}
				decodeBody.Close()
				decodeBody = nil
			}
		}
	}
}

type appendWriter struct {
	short bool
	off   int
	buff  []byte
}

func (aw *appendWriter) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(aw.buff[aw.off : aw.off+1])
	aw.off += n
	return int64(n), err
}

func (aw *appendWriter) Write(p []byte) (int, error) {
	if aw.short {
		return len(p) - 1, nil
	}
	aw.buff = append(aw.buff, p...)
	return len(p), nil
}
func (*appendWriter) Read(p []byte) (int, error) { return 0, io.EOF }
func (*appendWriter) Close() error               { return nil }

// TODO: replace with io.NopCloser in higher golang version.
func nopCloser(r io.Reader) io.ReadCloser {
	if _, ok := r.(io.WriterTo); ok {
		return nopCloserWriterTo{r}
	}
	return io.NopCloser(r)
}

type nopCloserWriterTo struct {
	io.Reader
}

func (nopCloserWriterTo) Close() error { return nil }

func (c nopCloserWriterTo) WriteTo(w io.Writer) (n int64, err error) {
	return c.Reader.(io.WriterTo).WriteTo(w)
}

func TestSizedCoderAppend(t *testing.T) {
	{
		_, err := NewSizedAppend(nil, 1, 0, k32, false, nil, nil)
		require.Error(t, err)
		_, err = NewSizedAppend(&randReadWriter{}, 1, 1, k32, false, nil, nil)
		require.Error(t, err)
		_, err = NewSizedAppend(&randReadWriter{}, 1, 1, k32, false, []byte{' '}, nil)
		require.Error(t, err)
	}
	{
		buff := make([]byte, 3)
		crand.Read(buff)
		encodeBody := NewSizedCoder(newRc(buff), 3, 0, k32, ModeEncode, false)
		buf, err := io.ReadAll(encodeBody)
		require.NoError(t, err)
		buf = buf[:7]

		ra, err := NewSizedAppend(&appendWriter{buff: buf}, 3, 1, k32, false,
			[]byte{' '}, []byte{1, 2, 3, 4})
		require.NoError(t, err)
		_, err = ra.(io.WriterTo).WriteTo(&appendWriter{short: true})
		require.Error(t, err)
	}
	{
		var size int64 = 513
		buff := make([]byte, size)
		crand.Read(buff)
		encodeBody := NewSizedCoder(newRc(buff), size, 1, k32, ModeEncode, false)
		buf, err := io.ReadAll(encodeBody)
		require.NoError(t, err)

		ra, err := NewSizedAppend(&appendWriter{buff: buf}, size, 1, k32, false,
			[]byte{11}, []byte{69, 208, 54, 5})
		require.NoError(t, err)
		w := &appendWriter{}
		var nn, n int64
		for nn < 2*_alignment {
			n, err = ra.(io.WriterTo).WriteTo(w)
			require.NoError(t, err)
			nn += n
		}
		require.Equal(t, 2*int64(_alignment), nn)
		require.Equal(t, uint8(11), w.buff[0])

		crc := crc32.NewIEEE()
		decodeBody := NewSizedCoder(newRc(w.buff), size+1, 0, k32, ModeDecode, false)
		_, err = io.CopyN(crc, decodeBody, size+1)
		require.NoError(t, err)
		require.Equal(t, crc32.ChecksumIEEE(append([]byte{11}, buff...)), crc.Sum32())
	}

	size := int64(3<<20) + 27
	buff := make([]byte, size)
	crand.Read(buff)
	payload := BlockPayload(k32)
	logsb := bytes.NewBuffer(nil)

	run := func(size int64) {
		rest := buff[:size:size]
		buffs := make([]byte, 0, len(rest)+_alignment)

		logsb.Reset()
		logsb.WriteString(fmt.Sprintf("size:%d ", size))

		var stable int64
		var lastpad []byte
		var lastcrc []byte
		for idx := range [5]struct{}{} {
			if size == stable {
				break
			}
			actual := mrand.Int63n(size-stable) + 1
			if idx == 4 {
				actual = size - stable
			}
			logsb.WriteString(fmt.Sprintf("(idx:%d actual:%d)", idx, actual))
			encodeBody := NewSizedCoder(newRc(rest[stable:stable+actual]),
				actual, stable, k32, ModeEncode, false)
			encodeSize, pad := PartialEncodeSizeWith(actual, stable, k32)

			pbuf := bytes.NewBuffer(nil)
			nn, err := pbuf.ReadFrom(encodeBody)
			require.NoError(t, err)
			require.Equal(t, encodeSize, nn)

			b := pbuf.Bytes()
			l := int64(len(b))

			ra, err := NewSizedAppend(nopCloser(pbuf), actual, stable, k32, false, lastpad, lastcrc)
			require.NoError(t, err)

			stable += actual
			padhead := int(((stable) % payload) % _alignment)
			lastpad = rest[:stable][len(rest[:stable])-padhead : len(rest[:stable])]
			lastcrc = b[l-pad-crc32.Size : l-pad]

			abuf := bytes.NewBuffer(nil)
			nn, err = ra.(io.WriterTo).WriteTo(abuf)
			require.NoError(t, err)
			require.Equal(t, encodeSize, nn)
			if size != stable {
				offset := abuf.Len() - int(pad) - len(lastpad)
				if stable%payload != 0 {
					offset -= crc32.Size
				}
				buffs = append(buffs, abuf.Bytes()[:offset]...)
			} else {
				buffs = append(buffs, abuf.Bytes()...)
			}
		}

		crc := crc32.NewIEEE()
		decodeBody := NewSizedCoder(newRc(buffs), size, 0, k32, ModeDecode, false)
		_, err := io.CopyN(crc, decodeBody, size)
		require.NoError(t, err, logsb.String())
		require.Equal(t, crc32.ChecksumIEEE(rest), crc.Sum32())
	}

	run(1)
	run(10)
	run(511)
	run(k32 - 4)
	run(k32 + 2)
	for range [100]struct{}{} {
		run(size)
	}
}

func TestSizedCoderMissmatch(t *testing.T) {
	size := int64(12)
	clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
	encodeBody := NewSizedEncoder(clientBody, size)
	require.Panics(t, func() { encodeBody.(io.WriterTo).WriteTo(nil) })
	encodeSize, _ := PartialEncodeSize(size, 0)
	transBody := &transReadWriter{step: 32, data: make([]byte, encodeSize)}
	_, err := transBody.ReadFrom(encodeBody)
	require.NoError(t, err)

	transBody.off = 0
	transBody.data[0] += 1
	decodeBody := NewSizedDecoder(transBody, size)

	b := make([]byte, size)
	for range [3]struct{}{} {
		_, err = io.ReadFull(decodeBody, b)
		require.ErrorIs(t, err, ErrMismatchedCrc)
	}
}

func TestSizedCoderSection(t *testing.T) {
	payload, left := int64(gBlockSize-crc32Len), int64(133)
	size := payload*4 + left
	{
		clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
		encodeBody := NewSizedEncoder(clientBody, size)
		encodeSize, _ := PartialEncodeSize(size, 0)
		transBody := &transReadWriter{step: int(gBlockSize), data: make([]byte, encodeSize)}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.off = 0
		decodeBody := NewSizedSectionDecoder(transBody, size)

		b := make([]byte, gBlockSize)
		var n int
		for range [4]struct{}{} {
			n, err = decodeBody.Read(b)
			require.ErrorIs(t, err, ErrFrameContinue)
			require.Equal(t, int(payload), n)
		}
		n, err = decodeBody.Read(b)
		require.NoError(t, err)
		require.Equal(t, int(left), n)
	}
	{
		clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
		encodeBody := NewSizedEncoder(clientBody, size)
		encodeSize, _ := PartialEncodeSize(size, 0)
		transBody := &transReadWriter{step: int(gBlockSize), data: make([]byte, encodeSize)}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.off = 0
		decodeBody := NewSizedSectionDecoder(transBody, size)
		decodeBodyWt := decodeBody.(io.WriterTo)
		serverBody := &randReadWriter{whasher: crc32.NewIEEE()}

		var n, nn int64
		for range [4]struct{}{} {
			n, err = decodeBodyWt.WriteTo(serverBody)
			nn += n
			require.ErrorIs(t, err, ErrFrameContinue)
		}
		n, err = decodeBodyWt.WriteTo(serverBody)
		nn += n
		require.NoError(t, err)
		require.Equal(t, encodeSize, nn)

		require.Equal(t, clientBody.rhasher.Sum32(), serverBody.whasher.Sum32())
	}
}

func TestSizedCoderRange(t *testing.T) {
	for _, size := range []int64{_alignment - 1, _alignment, _alignment + 1} {
		buf := make([]byte, size)
		re := NewSizedCoder(newRc(buf), size, 0, k32, ModeEncode, false)
		ebuf, err := io.ReadAll(re)
		require.NoError(t, err)
		head, tail, rd := NewSizedRangeDecoder(newRc(ebuf), size, 1, size-2, k32)
		require.Equal(t, int64(1), head)
		require.Equal(t, int64(2), tail)
		n, err := rd.Read(make([]byte, _alignment*2))
		require.NoError(t, err)
		require.Equal(t, int(size), n)
	}

	size := int64(1<<20) + 13
	buff := make([]byte, size)
	crand.Read(buff)

	run := func(from, to int64) {
		encodeBody := NewSizedEncoder(newRc(buff), size)
		encodeSize, _ := PartialEncodeSize(size, 0)
		transBody := &transReadWriter{step: int(gBlockSize), data: make([]byte, encodeSize)}
		nn, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)
		require.Equal(t, int64(len(transBody.data)), nn)
		crcExp := crc32.ChecksumIEEE(buff[from:to])

		payload := BlockPayload(gBlockSize)
		blocks := from / payload
		seek := blocks * payload
		size, from, to = size-seek, from-seek, to-seek
		blockOffset := blocks * gBlockSize
		rc := newRc(transBody.data[blockOffset:])

		head, tail, decodeBody := NewSizedRangeDecoder(rc, size, from, to, gBlockSize)
		var buf []byte
		for {
			b, err := io.ReadAll(decodeBody)
			buf = append(buf, b...)
			if len(b) == 0 || (err != nil && err != ErrFrameContinue) {
				break
			}
		}
		require.Equal(t, crcExp, crc32.ChecksumIEEE(buf[head:len(buf)-int(tail)]))
	}

	run(0, 1)
	run(size-1, size)
	run(0, size)
	run(1, size)
	for range [100]struct{}{} {
		from := mrand.Int63n(size / 2)
		var add int64
		for add == 0 {
			add = mrand.Int63n(size / 2)
		}
		run(from, from+add)
	}
}

func TestSizedCoderPartial(t *testing.T) {
	size := int64(1<<20) + 17
	buff := make([]byte, size)
	crand.Read(buff)

	run := func(actual, stable int64) {
		buf := buff[0:actual:actual]
		encodeBody := NewPartialEncoder(newRc(buf), actual, stable)
		encodeSize, _ := PartialEncodeSize(actual, stable)
		transBody := &transReadWriter{step: int(gBlockSize), data: make([]byte, encodeSize)}
		if actual <= 2000 {
			transBody.step = 1
		}
		nn, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)
		require.Equal(t, int64(len(transBody.data)), nn)

		transBody.off = 0

		if actual%2 == 0 {
			rc := NewSizedCoder(transBody, actual, stable, gBlockSize, ModeCheck, false)
			decodeBody := NewPartialDecoder(rc, actual, stable)
			serverBody := &randReadWriter{whasher: crc32.NewIEEE()}
			decodeBodyWt := decodeBody.(io.WriterTo)
			nn, err = decodeBodyWt.WriteTo(serverBody)
			require.NoError(t, err)
			require.Equal(t, encodeSize, nn)
			require.Equal(t, crc32.ChecksumIEEE(buf), serverBody.whasher.Sum32())
		} else {
			decodeBody := NewPartialDecoder(transBody, actual, stable)
			serverBody := &randReadWriter{whasher: crc32.NewIEEE()}
			b := make([]byte, len(buf))
			_, err = io.ReadFull(decodeBody, b)
			require.NoError(t, err)
			serverBody.Write(b)
			require.Equal(t, crc32.ChecksumIEEE(b), serverBody.whasher.Sum32())
		}
	}

	run(size, 0)
	run(1, size-1)
	run(1999, 817374)
	run(2000, 11223344)
	for range [100]struct{}{} {
		run(mrand.Int63n(size), mrand.Int63n(1<<20))
	}
}

type limitedWritter struct{ n int }

func (w *limitedWritter) Write(p []byte) (int, error) {
	n := len(p)
	if n > w.n {
		n = w.n
	}
	w.n -= n
	return n, nil
}

func TestSizedCoderChecker(t *testing.T) {
	require.Panics(t, func() { NewSizedCoder(nil, 0, 0, 1, ModeEncode, false) })
	require.Panics(t, func() {
		rc := NewSizedCoder(nil, 0, 0, gBlockSize, 0, false)
		rc.Read(make([]byte, 1))
	})
	require.Panics(t, func() {
		clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
		encodeBody := NewSizedEncoder(clientBody, 1)
		encodeSize, _ := PartialEncodeSize(1, 0)
		transBody := &transReadWriter{step: 1 << 10, data: make([]byte, encodeSize)}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.off = 0
		rc := NewSizedCoder(transBody, 1, 0, gBlockSize, 0, false)
		rc.(io.WriterTo).WriteTo(io.Discard)
	})
	{
		clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
		encodeBody := NewSizedEncoder(clientBody, 11)
		encodeSize, _ := PartialEncodeSize(11, 0)
		transBody := &transReadWriter{step: 1 << 10, data: make([]byte, encodeSize)}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.off = 0
		rc := NewSizedCoder(transBody, 11, 0, gBlockSize, ModeCheck, false)
		nn, err := rc.(io.WriterTo).WriteTo(&limitedWritter{511})
		require.ErrorIs(t, err, io.ErrShortWrite)
		require.Equal(t, int64(511), nn)
	}

	size := int64(gBlockSize) + 11
	{
		clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
		encodeBody := NewSizedEncoder(clientBody, size)
		encodeSize, tail := PartialEncodeSize(size, 0)
		transBody := &transReadWriter{step: 1 << 10, data: make([]byte, encodeSize)}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.off = 0
		encodeData := bytes.NewBuffer(make([]byte, 0, len(transBody.data)))
		decodeBody := NewSizedDecoder(io.NopCloser(io.TeeReader(transBody, encodeData)), size)

		b := make([]byte, size)
		_, err = io.ReadFull(decodeBody, b)
		require.NoError(t, err)

		require.Equal(t, transBody.data[:encodeSize-tail], encodeData.Bytes()[:encodeSize-tail])
		require.Equal(t, len(transBody.data), encodeData.Len())
		require.Equal(t, int(size), len(b))
	}
	{
		clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
		encodeBody := NewSizedEncoder(clientBody, size)
		encodeSize, _ := PartialEncodeSize(size, 0)
		transBody := &transReadWriter{step: 1, data: make([]byte, encodeSize)}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.off = 0
		encodeData := bytes.NewBuffer(make([]byte, 0, len(transBody.data)))
		rc := NewSizedCoder(transBody, size, 0, gBlockSize, ModeCheck, false)

		_, err = rc.(io.WriterTo).WriteTo(encodeData)
		require.NoError(t, err)
		require.Equal(t, transBody.data, encodeData.Bytes())

		transBody.off = 0
		encodeData.Reset()
		rc = NewSizedCoder(transBody, size, 0, gBlockSize, ModeCheck, false)
		decodeBody := NewSizedDecoder(rc, size)

		require.Panics(t, func() { io.ReadAll(decodeBody) })

		_, err = decodeBody.(io.WriterTo).WriteTo(encodeData)
		require.NoError(t, err)
		require.Equal(t, clientBody.rhasher.Sum32(), crc32.ChecksumIEEE(encodeData.Bytes()))
	}
	{
		clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
		encodeBody := NewSizedEncoder(clientBody, size)
		encodeSize, _ := PartialEncodeSize(size, 0)
		transBody := &transReadWriter{step: 1 << 10, data: make([]byte, encodeSize)}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.data[size-1] += 1
		transBody.off = 0
		rc := NewSizedCoder(transBody, size, 0, gBlockSize, ModeCheck, false)

		_, err = rc.(io.WriterTo).WriteTo(io.Discard)
		require.ErrorIs(t, err, ErrMismatchedCrc)
	}
}

func TestSizedCoderLoader(t *testing.T) {
	{
		rc := NewSizedCoder(nil, 0, 0, k32, ModeLoad, true)
		_, err := rc.Read(make([]byte, 1))
		require.ErrorIs(t, err, io.EOF)
	}
	{
		rc := NewSizedCoder(nil, 1, 0, k32, ModeLoad, false)
		_, err := rc.Read(make([]byte, 1))
		require.Error(t, err)
	}
	{
		rc := NewSizedCoder(newRc(make([]byte, 10)), 1, 0, k32, ModeLoad, true)
		_, err := rc.Read(make([]byte, 1))
		require.Error(t, err)
		_, err = rc.Read(make([]byte, _alignment))
		require.Error(t, err)
	}
	{
		size := int64(_alignmentMask)
		buf := make([]byte, size)
		re := NewSizedCoder(newRc(buf), size, 0, k32, ModeEncode, false)
		ebuf, err := io.ReadAll(re)
		require.NoError(t, err)
		ebuf[_alignmentMask+2]++
		rd := NewSizedCoder(newRc(ebuf), size, 0, k32, ModeLoad, true)
		_, err = rd.Read(make([]byte, _alignment))
		require.Error(t, err)
		_, err = rd.Read(make([]byte, _alignment*2))
		require.ErrorIs(t, err, ErrMismatchedCrc)
		_, err = rd.Read(make([]byte, _alignment*2))
		require.ErrorIs(t, err, ErrMismatchedCrc)
	}
	for _, size := range []int64{_alignment - 1, _alignment, _alignment + 1} {
		buf := make([]byte, size)
		re := NewSizedCoder(newRc(buf), size, 0, k32, ModeEncode, false)
		ebuf, err := io.ReadAll(re)
		require.NoError(t, err)
		rd := NewSizedCoder(newRc(ebuf), size, 0, k32, ModeLoad, true)
		n, err := rd.Read(make([]byte, _alignment*2))
		require.NoError(t, err)
		require.Equal(t, int(size), n)
	}

	size := int64(1<<20) + 19
	buff := make([]byte, size)
	crand.Read(buff)
	rbuff := make([]byte, gBlockSize)

	run := func(actual, stable int64) {
		buf := buff[0:actual:actual]

		re := NewSizedCoder(newRc(buf), actual, stable, gBlockSize, ModeEncode, false)
		ebuf, err := io.ReadAll(re)
		require.NoError(t, err)

		rd := NewSizedCoder(newRc(ebuf), actual, stable, gBlockSize, ModeLoad, true)
		var nn int
		crc := crc32.NewIEEE()
		head := int((stable % BlockPayload(gBlockSize)) % _alignment)
		for {
			n, err := rd.Read(rbuff)
			crc.Write(rbuff[head:n])
			nn += n - head
			head = 0
			if err == ErrFrameContinue {
				continue
			}
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		require.Equal(t, len(buf), nn)
		require.Equal(t, crc32.ChecksumIEEE(buf), crc.Sum32())
	}

	run(size, 0)
	run(1, size-1)
	run(1999, 817374)
	run(2000, 11223344)
	for range [100]struct{}{} {
		run(mrand.Int63n(size), mrand.Int63n(1<<20))
	}
}

func TestSizedCoderFixer(t *testing.T) {
	{
		_, _, rc := NewSizedFixer(nil, 0, 0, 0, k32)
		_, err := rc.Read(make([]byte, 1))
		require.ErrorIs(t, err, io.EOF)
	}
	{
		_, _, rc := NewSizedFixer(nil, 1, 0, 1, k32)
		_, err := rc.Read(make([]byte, 1))
		require.Error(t, err)
	}
	{
		_, _, rc := NewSizedFixer(newRc(make([]byte, 10)), 1, 0, 1, k32)
		_, err := rc.Read(make([]byte, 1))
		require.Error(t, err)
		_, err = rc.Read(make([]byte, _alignment))
		require.Error(t, err)
	}
	{
		size := int64(_alignmentMask)
		_, _, rc := NewSizedFixer(newRc(nil), size, 0, size, k32)
		_, err := rc.Read(make([]byte, _alignment*2))
		require.ErrorIs(t, err, io.EOF)
		_, err = rc.Read(make([]byte, _alignment*2))
		require.ErrorIs(t, err, io.EOF)
	}
	for _, size := range []int64{_alignment - 1, _alignment, _alignment + 1} {
		es, _ := PartialEncodeSizeWith(size, 0, k32)
		buf := make([]byte, size)
		crand.Read(buf)
		re := NewSizedCoder(newRc(buf), size, 0, k32, ModeEncode, false)
		ebuf, err := io.ReadAll(re)
		require.NoError(t, err)
		head, tail, rd := NewSizedFixer(newRc(ebuf), size, 1, size-3, k32)
		abuf := make([]byte, _alignment*2)
		n, err := io.ReadFull(rd, abuf)
		require.NoError(t, err)
		require.Equal(t, int(es), n)

		rd = NewSizedCoder(newRc(abuf[head:len(abuf)-int(tail)]), size-4, 1, k32, ModeDecode, false)
		dbuf, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.Equal(t, buf[1:size-3], dbuf)
	}

	size := int64(1<<20) + 23
	buff := make([]byte, size)
	crand.Read(buff)
	payload := BlockPayload(k32)

	run := func(from, to int64) {
		encodeBody := NewSizedCoder(newRc(buff), size, 0, k32, ModeEncode, false)
		encodeSize, _ := PartialEncodeSizeWith(size, 0, k32)
		var step int
		switch from % 3 {
		case 1:
			step = _alignment
		case 2:
			step = k32
		default:
			step = int(gBlockSize)
		}
		transBody := &transReadWriter{step: step, data: make([]byte, encodeSize)}
		nn, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)
		require.Equal(t, int64(len(transBody.data)), nn)

		data := buff[from:to]

		blockOffset := (from / payload) * k32
		rc := newRc(transBody.data[blockOffset:])
		head, tail, fixedBody := NewSizedFixer(rc, size, from, to, k32)

		buf := make([]byte, encodeSize)
		n, err := fixedBody.Read(buf)
		require.NoError(t, err)
		require.True(t, n > int(head+tail))
		fixedBody.Close()
		fixedBody = nil
		buf = buf[:n]

		deBody := NewSizedCoder(newRc(buf[head:len(buf)-int(tail)]), to-from, from, k32, ModeDecode, false)
		buf, err = io.ReadAll(deBody)
		deBody.Close()
		deBody = nil
		require.NoError(t, err)
		require.Equal(t, len(data), len(buf))
		require.Equal(t, crc32.ChecksumIEEE(data), crc32.ChecksumIEEE(buf))
	}

	run(0, 1)
	run(4, 511)
	run(size-1, size)
	run(0, size)
	run(1, size)
	for range [100]struct{}{} {
		from := mrand.Int63n(size / 2)
		var add int64
		for add == 0 {
			add = mrand.Int63n(size / 2)
		}
		run(from, from+add)
	}
}

func TestSizedBlockCoder(t *testing.T) {
	{
		rc := NewSizedBlockEncoder(nil, 0, k32)
		_, err := rc.Read(make([]byte, 1))
		require.ErrorIs(t, err, io.EOF)
		_, _, rc = NewSizedRangeBlockDecoder(nil, 0, 0, 0, k32)
		_, err = rc.Read(make([]byte, 1))
		require.ErrorIs(t, err, io.EOF)
	}
	{
		rc := NewSizedBlockEncoder(newRc(make([]byte, 10)), 11, k32)
		_, err := rc.Read(make([]byte, 11))
		require.Error(t, err)
		_, err = rc.Read(make([]byte, 15))
		require.Error(t, err)
		rc = NewSizedBlockDecoder(newRc(make([]byte, 10)), 11, k32)
		_, err = rc.Read(make([]byte, 11))
		require.Error(t, err)
		_, err = rc.Read(make([]byte, 15))
		require.Error(t, err)
	}
	{
		rc := NewSizedBlockDecoder(newRc(make([]byte, 14)), 10, k32)
		_, err := rc.Read(make([]byte, 14))
		require.Error(t, err)
		_, err = rc.Read(make([]byte, 14))
		require.Error(t, err)
	}

	size := int64(1<<20) + 31
	buff := make([]byte, size)
	crand.Read(buff)
	rbuff := make([]byte, gBlockSize)
	rrbuff := make([]byte, gBlockSize*2)
	payload := BlockPayload(gBlockSize)

	run := func(size int64) {
		buf := buff[0:size:size]

		re := NewSizedBlockEncoder(newRc(buf), size, gBlockSize)
		encodeSize, tail := PartialEncodeSize(size, 0)
		ebuf := make([]byte, encodeSize-tail)
		_, err := io.ReadFull(re, ebuf)
		require.NoError(t, err)

		rd := NewSizedBlockDecoder(newRc(ebuf), size, gBlockSize)
		crc := crc32.NewIEEE()
		var nn int
		for {
			n, err := rd.Read(rbuff)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			crc.Write(rbuff[crc32.Size:n])
			nn += n - crc32.Size
		}
		require.Equal(t, len(buf), nn)
		require.Equal(t, crc32.ChecksumIEEE(buf), crc.Sum32())

		if size < gBlockSize*2 {
			return
		}

		for range [7]struct{}{} {
			from := mrand.Int63n(size-10) + 1
			to := mrand.Int63n(size-from-1) + 1
			to = from + to

			blocks := from / payload
			seek := blocks * payload
			buffOffset := blocks * gBlockSize

			head, tail, rr := NewSizedRangeBlockDecoder(newRc(ebuf[buffOffset:]),
				size-seek, from-seek, to-seek, gBlockSize)
			rrbuff = rrbuff[:0]
			for {
				n, err := rr.Read(rbuff)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				rrbuff = append(rrbuff, rbuff[crc32.Size:n]...)
			}
			crc.Reset()
			crc.Write(rrbuff[head : len(rrbuff)-int(tail)])
			require.Equal(t, crc32.ChecksumIEEE(buf[from:to]), crc.Sum32())
		}
	}

	run(1)
	run(817374)
	run(1000001)
	run(size)
	for range [100]struct{}{} {
		run(mrand.Int63n(size) + 1)
	}
}

type noneReadWriter struct{}

func (noneReadWriter) Read(p []byte) (int, error)       { return len(p), nil }
func (noneReadWriter) Write(p []byte) (int, error)      { return len(p), nil }
func (noneReadWriter) WriteTo(io.Writer) (int64, error) { panic("should not call WriteTo") }
func (noneReadWriter) Close() error                     { return nil }

func BenchmarkSizedCoder(b *testing.B) {
	cases := []struct {
		step            int
		blockSize, size int64
	}{
		{32 << 10, 4 << 10, 1 << 20},
		{32 << 10, 32 << 10, 1 << 20},
		{32 << 10, 64 << 10, 1 << 20},
		{1 << 20, 64 << 10, 8 << 20},
		{1 << 20, 512 << 10, 8 << 20},
		{4 << 20, 1 << 20, 16 << 20},
	}

	for _, cs := range cases {
		b.Run(fmt.Sprintf("step(%d)-block(%d)-size(%d)", cs.step, cs.blockSize, cs.size),
			func(b *testing.B) {
				clientBody := noneReadWriter{}
				encodeBody := NewSizedCoder(clientBody, cs.size, 0, cs.blockSize, ModeEncode, false)
				encodeSize, _ := PartialEncodeSize(cs.size, 0)
				buff := make([]byte, encodeSize)
				transBody := &transReadWriter{step: cs.step, data: buff}

				b.ResetTimer()
				for ii := 0; ii <= b.N; ii++ {
					transBody.off = 0
					transBody.data = buff
					transBody.ReadFrom(encodeBody)
					transBody.off = 0
					decodeBody := NewSizedCoder(transBody, cs.size, 0, cs.blockSize, ModeDecode, false)
					serverBody := noneReadWriter{}
					decodeBody.(io.WriterTo).WriteTo(serverBody)
					decodeBody.Close()
					decodeBody = nil
				}
			},
		)
	}
}

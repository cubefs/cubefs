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

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

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
					require.ErrorIs(t, io.EOF, err, logName)
				} else {
					b := make([]byte, size)
					n, err := io.ReadFull(decodeBody, b)
					require.NoError(t, err, logName)
					require.Equal(t, int(size), n, logName)
					serverBody.Write(b)
					require.Equal(t, clientBody.rhasher.Sum32(), serverBody.whasher.Sum32(), logName)
					_, err = decodeBody.Read(make([]byte, 1))
					require.ErrorIs(t, io.EOF, err, logName)
				}
			}
		}
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
			require.ErrorIs(t, err, transport.ErrFrameContinue)
			require.Equal(t, int(payload), n)
		}
		n, err = decodeBody.Read(b)
		require.ErrorIs(t, err, io.EOF)
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
			require.ErrorIs(t, err, transport.ErrFrameContinue)
		}
		n, err = decodeBodyWt.WriteTo(serverBody)
		nn += n
		require.NoError(t, err)
		require.Equal(t, encodeSize, nn)

		require.Equal(t, clientBody.rhasher.Sum32(), serverBody.whasher.Sum32())
	}
}

func TestSizedCoderRange(t *testing.T) {
	size := int64(1<<20) + 13
	buff := make([]byte, size)
	crand.Read(buff)

	run := func(from, to int64) {
		encodeBody := NewSizedEncoder(io.NopCloser(bytes.NewReader(buff)), size)
		encodeSize, _ := PartialEncodeSize(size, 0)
		transBody := &transReadWriter{step: int(gBlockSize), data: make([]byte, encodeSize)}
		nn, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)
		require.Equal(t, int64(len(transBody.data)), nn)

		head, tail, decodeBody := NewSizedRangeDecoder(bytes.NewReader(transBody.data), size, from, to)
		var buf []byte
		for {
			b, err := io.ReadAll(decodeBody)
			buf = append(buf, b...)
			if len(b) == 0 || (err != nil && err != transport.ErrFrameContinue) {
				break
			}
		}
		require.True(t, bytes.Equal(buff[from:to+1], buf[head:len(buf)-tail]))
	}

	run(0, 0)
	run(size-1, size-1)
	run(0, size-1)
	run(1, size-1)
	for range [100]struct{}{} {
		from := mrand.Int63n(size / 2)
		run(from, from+mrand.Int63n(size/2))
	}
}

func TestSizedCoderPartial(t *testing.T) {
	size := int64(1<<20) + 17
	buff := make([]byte, size)
	crand.Read(buff)

	run := func(actual, stable int64) {
		buf := buff[0:actual:actual]
		encodeBody := NewPartialEncoder(io.NopCloser(bytes.NewReader(buf)), actual, stable)
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
		rc := NewSizedCoder(nil, 0, 0, 32<<10, ModeLoad, true)
		_, err := rc.Read(make([]byte, 1))
		require.ErrorIs(t, io.EOF, err)
	}
	{
		rc := NewSizedCoder(nil, 1, 0, 32<<10, ModeLoad, false)
		_, err := rc.Read(make([]byte, 1))
		require.Error(t, err)
	}
	{
		rc := NewSizedCoder(io.NopCloser(bytes.NewReader(make([]byte, 10))),
			1, 0, 32<<10, ModeLoad, true)
		_, err := rc.Read(make([]byte, 1))
		require.Error(t, err)
		_, err = rc.Read(make([]byte, _alignment))
		require.Error(t, err)
	}
	{
		size := int64(_alignmentMask)
		buf := make([]byte, size)
		re := NewSizedCoder(io.NopCloser(bytes.NewReader(buf)),
			size, 0, 32<<10, ModeEncode, false)
		ebuf, err := io.ReadAll(re)
		require.NoError(t, err)
		ebuf[_alignmentMask+2]++
		rd := NewSizedCoder(io.NopCloser(bytes.NewReader(ebuf)),
			size, 0, 32<<10, ModeLoad, true)
		_, err = rd.Read(make([]byte, _alignment))
		require.Error(t, err)
		_, err = rd.Read(make([]byte, _alignment*2))
		require.ErrorIs(t, err, ErrMismatchedCrc)
		_, err = rd.Read(make([]byte, _alignment*2))
		require.ErrorIs(t, err, ErrMismatchedCrc)
	}
	for _, size := range []int64{_alignment - 1, _alignment, _alignment + 1} {
		buf := make([]byte, size)
		re := NewSizedCoder(io.NopCloser(bytes.NewReader(buf)),
			size, 0, 32<<10, ModeEncode, false)
		ebuf, err := io.ReadAll(re)
		require.NoError(t, err)
		rd := NewSizedCoder(io.NopCloser(bytes.NewReader(ebuf)),
			size, 0, 32<<10, ModeLoad, true)
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

		re := NewSizedCoder(io.NopCloser(bytes.NewReader(buf)),
			actual, stable, gBlockSize, ModeEncode, false)
		ebuf, err := io.ReadAll(re)
		require.NoError(t, err)

		rd := NewSizedCoder(io.NopCloser(bytes.NewReader(ebuf)),
			actual, stable, gBlockSize, ModeLoad, true)

		var nn int
		crc := crc32.NewIEEE()
		head := int((stable % BlockPayload(gBlockSize)) % _alignment)
		for {
			n, err := rd.Read(rbuff)
			crc.Write(rbuff[head:n])
			nn += n - head
			head = 0
			if err == transport.ErrFrameContinue {
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

				b.ResetTimer()
				for ii := 0; ii <= b.N; ii++ {
					transBody := &transReadWriter{step: cs.step, data: buff}
					transBody.ReadFrom(encodeBody)
					transBody.off = 0
					decodeBody := NewSizedCoder(transBody, cs.size, 0, cs.blockSize, ModeDecode, false)
					serverBody := noneReadWriter{}
					decodeBody.(io.WriterTo).WriteTo(serverBody)
				}
			},
		)
	}
}

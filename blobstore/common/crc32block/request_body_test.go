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
	"hash/crc32"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBodyEncoderDecoder(t *testing.T) {
	cases := []int64{
		0,
		64*1024 - 4,
		64 * 1024,
		64*1024 + 4,
		1024 * 1024,
	}
	for idx, size := range cases {
		wb := genRandData(size)
		{
			encoder := NewBodyEncoder(io.NopCloser(bytes.NewReader(wb)))
			defer encoder.Close()
			decoder := NewBodyDecoder(encoder)
			defer decoder.Close()
			require.Equal(t, size, decoder.CodeSize(encoder.CodeSize(size)))

			rb := make([]byte, size)
			_, err := io.ReadFull(decoder, rb)
			require.NoError(t, err, "index is %d", idx)
			require.Equal(t, crc32.ChecksumIEEE(wb), crc32.ChecksumIEEE(rb), "index is %d", idx)
		}
		{
			encoder := NewBodyEncoder(io.NopCloser(bytes.NewReader(wb)))
			defer encoder.Close()
			decoder := NewDecoderReader(encoder)
			require.Equal(t, size, DecodeSizeWithDefualtBlock(encoder.CodeSize(size)))

			rb := make([]byte, size)
			_, err := io.ReadFull(decoder, rb)
			require.NoError(t, err, "index is %d", idx)
			require.Equal(t, crc32.ChecksumIEEE(wb), crc32.ChecksumIEEE(rb), "index is %d", idx)
		}
		{
			encoder := NewEncoderReader(bytes.NewReader(wb))
			decoder := NewBodyDecoder(io.NopCloser(encoder))
			defer decoder.Close()
			require.Equal(t, size, decoder.CodeSize(EncodeSizeWithDefualtBlock(size)))

			rb := make([]byte, size)
			_, err := io.ReadFull(decoder, rb)
			require.NoError(t, err, "index is %d", idx)
			require.Equal(t, crc32.ChecksumIEEE(wb), crc32.ChecksumIEEE(rb), "index is %d", idx)
		}
	}
}

func TestNilEncoderDecoder(t *testing.T) {
	cases := []int64{
		0,
		64*1024 - 4,
		64 * 1024,
		64*1024 + 4,
		1024 * 1024,
	}
	for _, size := range cases {
		{
			encoder := NewBodyEncoder(nil)
			defer encoder.Close()
			decoder := NewBodyDecoder(nil)
			require.Equal(t, size, decoder.CodeSize(encoder.CodeSize(size)))

			rb := make([]byte, 1024)
			_, err := io.ReadFull(decoder, rb)
			require.Error(t, err)
		}
	}
}

type brokenReader struct {
	io.ReadCloser
}

func (r brokenReader) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	p[10]++
	return
}

func TestBodyDecoderMissmatch(t *testing.T) {
	var size int64 = 1 << 12
	wb := genRandData(size)

	encoder := NewBodyEncoder(io.NopCloser(bytes.NewReader(wb)))
	defer encoder.Close()
	decoder := NewBodyDecoder(brokenReader{encoder})
	defer decoder.Close()
	require.Equal(t, size, decoder.CodeSize(encoder.CodeSize(size)))

	rb := make([]byte, size)
	_, err := io.ReadFull(decoder, rb)
	require.ErrorIs(t, ErrMismatchedCrc, err)
}

type closedReader struct {
	io.ReadCloser
	waiter chan struct{}
}

func (r *closedReader) Read(p []byte) (n int, err error) {
	<-r.waiter
	n, err = r.ReadCloser.Read(p)
	return
}

func TestBodyReadClosed(t *testing.T) {
	var size int64 = 1 << 12
	wb := genRandData(size)

	encoder := NewBodyEncoder(io.NopCloser(bytes.NewReader(wb)))
	defer encoder.Close()
	rc := &closedReader{
		ReadCloser: encoder,
		waiter:     make(chan struct{}),
	}
	decoder := NewBodyDecoder(rc)
	defer decoder.Close()
	require.Equal(t, size, decoder.CodeSize(encoder.CodeSize(size)))

	decoder.Close()
	rb := make([]byte, size)
	_, err := io.ReadFull(decoder, rb)
	require.ErrorIs(t, ErrReadOnClosed, err)
}

func benchmarkCode(b *testing.B, newFunc func(io.Reader) io.ReadCloser) {
	const kb = 1 << 10
	const mb = 1 << 20

	cases := []struct {
		name string
		size int64
	}{
		{"1__B", 1},
		{"128B", 128},
		{"2__K", kb * 2},
		{"64_K", kb * 64},
		{"512K", kb * 512},
		{"1__M", mb},
		{"2__M", mb * 2},
	}

	for _, cs := range cases {
		b.ResetTimer()
		b.Run(cs.name, func(b *testing.B) {
			src := make([]byte, cs.size)
			b.ResetTimer()
			for ii := 0; ii <= b.N; ii++ {
				rc := newFunc(bytes.NewBuffer(src))
				io.Copy(io.Discard, rc)
				rc.Close()
			}
		})
	}
}

func BenchmarkBodyEncodeOnly(b *testing.B) {
	benchmarkCode(b, func(r io.Reader) io.ReadCloser {
		return NewBodyEncoder(io.NopCloser(r))
	})
}

type coder struct {
	en, de RequestBody
}

func (c *coder) Read(p []byte) (n int, err error) {
	return c.de.Read(p)
}

func (c *coder) Close() error {
	c.en.Close()
	return c.de.Close()
}

func BenchmarkBodyEncodeDecode(b *testing.B) {
	benchmarkCode(b, func(r io.Reader) io.ReadCloser {
		encoder := NewBodyEncoder(io.NopCloser(r))
		decoder := NewBodyDecoder(encoder)
		return &coder{encoder, decoder}
	})
}

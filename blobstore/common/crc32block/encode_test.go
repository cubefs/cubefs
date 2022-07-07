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
	"crypto/rand"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	_KiB = int64(1024)
)

func genRandData(size int64) []byte {
	b := make([]byte, size)
	rand.Read(b)
	return b
}

func TestEncodeData(t *testing.T) {
	data := genRandData(128*1024 + 80)
	fsize := int64(len(data)) // 128k+80
	r := bytes.NewReader(data)
	w := bytes.NewBuffer(nil)

	encoder, err := NewEncoder(nil)
	require.NoError(t, err)
	require.NotNil(t, encoder)

	// encode
	n, err := encoder.Encode(r, fsize, w)
	require.NoError(t, err)
	require.Equal(t, n, EncodeSize(int64(fsize), defaultCrc32BlockSize))
}

func TestDecodeData(t *testing.T) {
	data := genRandData(128*1024 + 80)
	fsize := int64(len(data)) // 128k+80
	r := bytes.NewReader(data)
	w := bytes.NewBuffer(nil)

	encoder, err := NewEncoder(nil)
	require.NoError(t, err)
	require.NotNil(t, encoder)

	// encode
	n, err := encoder.Encode(r, fsize, w)
	require.NoError(t, err)
	require.Equal(t, n, EncodeSize(fsize, defaultCrc32BlockSize))

	// decode
	testDatas := []struct {
		off     int64
		from    int64
		to      int64
		fsize   int64
		padding int64
	}{
		{from: 0, to: 0, fsize: 0},
		{from: fsize, to: fsize, fsize: fsize},
		{from: 0, to: fsize, fsize: fsize},

		{from: 64*_KiB - 4, to: fsize, fsize: fsize},
		{from: 64*_KiB + 0, to: fsize, fsize: fsize},
		{from: 64*_KiB + 4, to: fsize, fsize: fsize},
		{from: 64*_KiB + 5, to: fsize, fsize: fsize},

		{from: 64*_KiB - 4, to: fsize - 1, fsize: fsize},
		{from: 64*_KiB + 0, to: fsize - 1, fsize: fsize},
		{from: 64*_KiB + 4, to: fsize - 1, fsize: fsize},
		{from: 64*_KiB + 5, to: fsize - 1, fsize: fsize},

		{from: 64*_KiB + 4, to: fsize - 64*_KiB, fsize: fsize},
		{from: 64*_KiB + 4, to: fsize - 64*_KiB - 4, fsize: fsize},
		{from: 64*_KiB + 4, to: fsize - 64*_KiB - 5, fsize: fsize},
		{from: 0, to: fsize - 64*_KiB - 4 - 64*_KiB - 4, fsize: fsize},
	}

	for _, tb := range testDatas {
		disk := append(make([]byte, tb.off), w.Bytes()...)
		disk = append(disk, make([]byte, tb.padding)...)

		decoder, err := NewDecoder(bytes.NewReader(disk), int64(tb.off), tb.fsize)
		require.NoError(t, err)
		require.NotNil(t, decoder)

		reader, err := decoder.Reader(tb.from, tb.to)
		require.NoError(t, err)
		require.NotNil(t, reader)

		cont, err := ioutil.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, data[tb.from:tb.to], cont)
	}
}

func TestLimitEncoderReader(t *testing.T) {
	data := genRandData(128*1024 + 80)
	fsize := int64(len(data)) // 128k+80
	r := bytes.NewReader(data)
	w := bytes.NewBuffer(nil)

	encReader := NewLimitEncoderReader(r, fsize)
	io.Copy(w, encReader)

	// decode
	testDatas := []struct {
		off     int64
		from    int64
		to      int64
		fsize   int64
		padding int64
	}{
		{from: 0, to: 0, fsize: 0},
		{from: fsize, to: fsize, fsize: fsize},
		{from: 0, to: fsize, fsize: fsize},

		{from: 0, to: 64, fsize: fsize},
		{from: 64*_KiB - 4, to: fsize, fsize: fsize},
		{from: 64*_KiB + 0, to: fsize, fsize: fsize},
		{from: 64*_KiB + 4, to: fsize, fsize: fsize},
		{from: 64*_KiB + 5, to: fsize, fsize: fsize},

		{from: 64*_KiB - 4, to: fsize - 1, fsize: fsize},
		{from: 64*_KiB + 0, to: fsize - 1, fsize: fsize},
		{from: 64*_KiB + 4, to: fsize - 1, fsize: fsize},
		{from: 64*_KiB + 5, to: fsize - 1, fsize: fsize},

		{from: 64*_KiB + 4, to: fsize - 64*_KiB, fsize: fsize},
		{from: 64*_KiB + 4, to: fsize - 64*_KiB - 4, fsize: fsize},
		{from: 64*_KiB + 4, to: fsize - 64*_KiB - 5, fsize: fsize},
		{from: 0, to: fsize - 64*_KiB - 4 - 64*_KiB - 4, fsize: fsize},
	}

	for _, tb := range testDatas {
		disk := append(make([]byte, tb.off), w.Bytes()...)
		disk = append(disk, make([]byte, tb.padding)...)

		decoder, err := NewDecoder(bytes.NewReader(disk), int64(tb.off), tb.fsize)
		require.NoError(t, err)
		require.NotNil(t, decoder)

		reader, err := decoder.Reader(tb.from, tb.to)
		require.NoError(t, err)
		require.NotNil(t, reader)

		cont, err := ioutil.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, data[tb.from:tb.to], cont)
	}
}

func TestLimitEncoderReader2(t *testing.T) {
	testDatas := []struct {
		offset   int64
		fsize    int64
		from, to int64
		padding  int64
		err      error
	}{
		{offset: 0, fsize: 1, from: 0, to: 1},
		{offset: 0, fsize: 64*_KiB - 4, from: 0, to: 64},
		{offset: 0, fsize: 64*_KiB - 4, from: 0, to: 64*_KiB - 4},
		{offset: 10, fsize: 64 * _KiB, from: 0, to: 64*_KiB - 4},
		{offset: 0, fsize: 64 * _KiB, from: 0, to: 64 * _KiB},
		{offset: 10, fsize: 64 * _KiB, from: 0, to: 64 * _KiB},
		{offset: 10, fsize: 64*_KiB + 5, from: 0, to: 64 * _KiB},

		{offset: 10, fsize: 128 * _KiB, from: 0, to: 128 * _KiB},
		{offset: 10, fsize: 128 * _KiB, from: 128*_KiB - 4, to: 128 * _KiB},
		{offset: 10, fsize: 128 * _KiB, from: 64*_KiB - 4, to: 64 * _KiB},
		{offset: 10, fsize: 128 * _KiB, from: 64*_KiB + 4, to: 64*_KiB + 4},
		{offset: 10, fsize: 128 * _KiB, from: 64*_KiB + 4, to: 64*_KiB + 8},
		{offset: 10, fsize: 128 * _KiB, from: 64*_KiB + 7, to: 64*_KiB + 10},
		{offset: 10, fsize: 128 * _KiB, from: 64*_KiB - 4, to: 64*_KiB - 1},
		{offset: 10, fsize: 128 * _KiB, from: 4, to: 64*_KiB - 1},
		{offset: 10, fsize: 128 * _KiB, from: 4, to: 64*_KiB - 4},
		{offset: 10, fsize: 128 * _KiB, from: 4, to: 64 * _KiB},
		{offset: 10, fsize: 128 * _KiB, from: 64 * _KiB, to: 128 * _KiB},
		{offset: 10, fsize: 128*_KiB + 7, from: 64 * _KiB, to: 64 * _KiB},
		{offset: 10, fsize: 128*_KiB + 7, from: 64*_KiB + 4, to: 128 * _KiB},
		{offset: 10, fsize: 128*_KiB + 7, from: 64*_KiB - 4, to: 128*_KiB + 7},
	}

	for _, tb := range testDatas {
		// encoder
		data := genRandData(tb.fsize)
		fsize := int64(len(data))
		r := bytes.NewReader(data)
		w := bytes.NewBuffer(nil)

		encReader := NewLimitEncoderReader(r, fsize)
		io.Copy(w, encReader)

		// decoder
		content := append(make([]byte, tb.offset), w.Bytes()...)
		content = append(content, make([]byte, tb.padding)...)

		decoder, err := NewDecoder(bytes.NewReader(content), int64(tb.offset), tb.fsize)
		require.NoError(t, err)
		require.NotNil(t, decoder)

		reader, err := decoder.Reader(tb.from, tb.to)
		require.NoError(t, err)
		require.NotNil(t, reader)

		cont, err := ioutil.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, data[tb.from:tb.to], cont)
	}
}

func TestLimitEncoderReaderFailed(t *testing.T) {
	// encoder
	data := genRandData(64 * _KiB)
	fsize := int64(len(data))
	r := bytes.NewReader(data)
	w := bytes.NewBuffer(nil)

	enc, err := NewEncoder(make([]byte, 1))
	require.Error(t, err)
	require.Nil(t, enc)

	enc, err = NewEncoder(nil)
	require.NoError(t, err)
	require.NotNil(t, enc)

	n, err := enc.Encode(r, fsize, w)
	require.NoError(t, err)
	require.Equal(t, n, EncodeSize(fsize, defaultCrc32BlockSize))

	r = bytes.NewReader(data)
	_, err = enc.Encode(r, fsize+1, ioutil.Discard)
	require.Error(t, err)
	_, ok := err.(ReaderError)
	require.Equal(t, ok, true)
}

func BenchmarkEncode(b *testing.B) {
	benchmarkCode(b, func(r io.Reader) io.ReadCloser {
		return io.NopCloser(NewEncoderReader(r))
	})
}

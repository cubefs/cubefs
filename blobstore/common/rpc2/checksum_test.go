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

import (
	crand "crypto/rand"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	mrand "math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func newBlock(blockSize uint32) ChecksumBlock {
	return ChecksumBlock{
		Algorithm: ChecksumAlgorithm_Crc_IEEE,
		Direction: ChecksumDirection_Duplex,
		BlockSize: blockSize,
	}
}

var defBlock = newBlock(DefaultBlockSize)

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

func TestChecksumDirection(t *testing.T) {
	require.True(t, ChecksumDirection_Duplex.IsUpload())
	require.True(t, ChecksumDirection_Duplex.IsDownload())
	require.True(t, ChecksumDirection_Upload.IsUpload())
	require.False(t, ChecksumDirection_Upload.IsDownload())
	require.False(t, ChecksumDirection_Download.IsUpload())
	require.True(t, ChecksumDirection_Download.IsDownload())
	invalid := ChecksumDirection(111)
	require.False(t, invalid.IsUpload())
	require.False(t, invalid.IsDownload())
}

func TestChecksumUnmarshal(t *testing.T) {
	t.Log(defBlock.String())
	b, err := defBlock.Marshal()
	require.NoError(t, err)
	_, err = unmarshalBlock(b[:len(b)-1])
	require.Error(t, err)
	block, err := unmarshalBlock(b)
	require.NoError(t, err)
	require.Equal(t, defBlock.Algorithm, block.Algorithm)

	block.BlockSize = 0
	b, err = block.Marshal()
	require.NoError(t, err)
	_, err = unmarshalBlock(b)
	require.Error(t, err)
}

func TestEncodeDecodeBodyBase(t *testing.T) {
	for _, step := range []int{1, 2, 3, 7, 16, 1 << 10, 64 << 10} {
		for _, blockSize := range []int{128, 513, 1 << 10, 64 << 10} {
			for idx := range [10]struct{}{} {
				size := mrand.Intn(9<<14) + mrand.Intn(11) + 1
				if idx == 0 {
					size = blockSize - 1
				}

				logName := fmt.Sprintf("step: %d block:%d size:%d", step, blockSize, size)
				block := newBlock(uint32(blockSize))
				clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
				encodeBody := newEdBody(block, clientNopBody(io.NopCloser(clientBody)), size, true)

				transBody := &transReadWriter{step: step, data: make([]byte, block.EncodeSize(int64(size)))}
				nn, err := transBody.ReadFrom(encodeBody)
				require.NoError(t, err, logName)
				require.Equal(t, int64(len(transBody.data)), nn)
				_, err = encodeBody.Read(make([]byte, 1))
				require.Equal(t, io.EOF, err, logName)

				transBody.off = 0
				decodeBody := newEdBody(block, transBody, size, false)
				serverBody := &randReadWriter{whasher: crc32.NewIEEE()}

				if idx%2 == 0 {
					nn, err = decodeBody.WriteTo(serverBody)
					require.NoError(t, err, logName)
					require.Equal(t, block.EncodeSize(int64(size)), nn, logName)
					require.Equal(t, clientBody.rhasher.Sum32(), serverBody.whasher.Sum32(), logName)
					_, err = decodeBody.WriteTo(serverBody)
					require.ErrorIs(t, io.EOF, err, logName)
				} else {
					b := make([]byte, size)
					n, err := io.ReadFull(decodeBody, b)
					require.NoError(t, err, logName)
					require.Equal(t, size, n, logName)
					serverBody.Write(b)
					require.Equal(t, clientBody.rhasher.Sum32(), serverBody.whasher.Sum32(), logName)
					_, err = decodeBody.Read(make([]byte, 1))
					require.ErrorIs(t, io.EOF, err, logName)
				}
			}
		}
	}
}

func TestEncodeDecodeBodyMissmatch(t *testing.T) {
	{
		size := 12
		clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
		encodeBody := newEdBody(defBlock, clientNopBody(io.NopCloser(clientBody)), size, true)
		require.Panics(t, func() { encodeBody.WriteTo(nil) })
		transBody := &transReadWriter{step: 32, data: make([]byte, defBlock.EncodeSize(int64(size)))}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.off = 0
		transBody.data[10] += 1
		decodeBody := newEdBody(defBlock, transBody, size, false)

		b := make([]byte, size)
		for range [3]struct{}{} {
			_, err = io.ReadFull(decodeBody, b)
			require.Error(t, err)
			status, code, detail := DetectError(err)
			require.Equal(t, 400, status, detail)
			require.Equal(t, "Checksum", code, detail)
		}
	}
	{
		block := newBlock(1)
		size := 12
		clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
		encodeBody := newEdBody(block, clientNopBody(io.NopCloser(clientBody)), size, true)
		transBody := &transReadWriter{step: 1, data: make([]byte, block.EncodeSize(int64(size)))}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.off = 0
		transBody.data[10] += 1
		decodeBody := newEdBody(block, transBody, size, false)
		serverBody := &randReadWriter{whasher: crc32.NewIEEE()}

		for range [3]struct{}{} {
			_, err = decodeBody.WriteTo(serverBody)
			require.Error(t, err)
			status, code, detail := DetectError(err)
			require.Equal(t, 400, status, detail)
			require.Equal(t, "Checksum", code, detail)
		}
	}
}

func TestEncodeDecodeBodyNodata(t *testing.T) {
	size := 12
	clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
	encodeBody := newEdBody(defBlock, clientNopBody(io.NopCloser(clientBody)), size, true)
	transBody := &transReadWriter{step: 32, data: make([]byte, defBlock.EncodeSize(int64(size)))}
	_, err := transBody.ReadFrom(encodeBody)
	require.NoError(t, err)

	transBody.off = 0
	transBody.data = transBody.data[1:]
	decodeBody := newEdBody(defBlock, transBody, size, false)

	b := make([]byte, size)
	for range [3]struct{}{} {
		_, err = io.ReadFull(decodeBody, b)
		require.Error(t, err)
	}

	var block *ChecksumBlock
	require.Equal(t, int64(10), block.EncodeSize(10))
	block = &ChecksumBlock{}
	require.Nil(t, block.Readable(nil))
}

type oneByte struct {
	done bool
}

func (b *oneByte) Write(p []byte) (int, error) {
	if b.done {
		return 0, io.ErrShortWrite
	}
	b.done = true
	return 1, io.ErrShortWrite
}

func TestEncodeDecodeBodyReadWriteto(t *testing.T) {
	block := newBlock(3)
	size := 12
	clientBody := &randReadWriter{rhasher: crc32.NewIEEE()}
	{
		encodeBody := newEdBody(block, clientNopBody(io.NopCloser(clientBody)), size, true)
		transBody := &transReadWriter{step: 1, data: make([]byte, block.EncodeSize(int64(size)))}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.off = 0
		decodeBody := newEdBody(block, transBody, size, false)
		for range [12 / 2]struct{}{} {
			decodeBody.WriteTo(&oneByte{})
			_, err = io.ReadFull(decodeBody, make([]byte, 1))
			require.NoError(t, err)
		}
	}
	{
		encodeBody := newEdBody(block, clientNopBody(io.NopCloser(clientBody)), size, true)
		transBody := &transReadWriter{step: 1, data: make([]byte, block.EncodeSize(int64(size)))}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.off = 0
		transBody.data = transBody.data[:3]
		decodeBody := newEdBody(block, transBody, size, false)
		decodeBody.WriteTo(&oneByte{})
		decodeBody.WriteTo(&oneByte{})
		decodeBody.WriteTo(&oneByte{})
		_, err = io.ReadFull(decodeBody, make([]byte, 1))
		require.Error(t, err)
	}
	{
		encodeBody := newEdBody(block, clientNopBody(io.NopCloser(clientBody)), size, true)
		transBody := &transReadWriter{step: 1, data: make([]byte, block.EncodeSize(int64(size)))}
		_, err := transBody.ReadFrom(encodeBody)
		require.NoError(t, err)

		transBody.off = 0
		transBody.data[1] += 1
		decodeBody := newEdBody(block, transBody, size, false)
		decodeBody.WriteTo(&oneByte{})
		decodeBody.WriteTo(&oneByte{})
		decodeBody.WriteTo(&oneByte{})
		_, err = io.ReadFull(decodeBody, make([]byte, 1))
		require.Error(t, err)
	}
}

type noneReadWriter struct{}

func (r *noneReadWriter) Read(p []byte) (int, error)  { return len(p), nil }
func (r *noneReadWriter) Write(p []byte) (int, error) { return len(p), nil }

func BenchmarkEncodeDecodeBody(b *testing.B) {
	cases := []struct {
		step, blockSize, size int
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
				block := newBlock(uint32(cs.blockSize))
				clientBody := &noneReadWriter{}
				encodeBody := newEdBody(block, clientNopBody(io.NopCloser(clientBody)), cs.size, true)
				buff := make([]byte, block.EncodeSize(int64(cs.size)))

				b.ResetTimer()
				for ii := 0; ii <= b.N; ii++ {
					transBody := &transReadWriter{step: cs.step, data: buff}
					transBody.ReadFrom(encodeBody)
					transBody.off = 0
					decodeBody := newEdBody(block, transBody, cs.size, false)
					serverBody := &noneReadWriter{}
					decodeBody.WriteTo(serverBody)
				}
			},
		)
	}
}

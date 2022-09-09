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
	"sync"

	"github.com/cubefs/blobstore/util/bytespool"
)

// RequestBody is implemented http request's body.
// always io.ReadCloser.
//
// For client requests, The HTTP Client's Transport is
// responsible for calling the Close method. Necessarily call
// the Close method if the body's life-cycle control by yourself.
//
// For server requests, the Server will close the request body.
// The ServeHTTP Handler does not need to.
//
// The Body must allow Read to be called concurrently with Close.
// In particular, calling Close should unblock a Read waiting
// for input.
type RequestBody interface {
	io.ReadCloser

	// CodeSize returns encoded whole body size for encoding,
	// or origin body size for decoding.
	CodeSize(int64) int64
}

type requestBody struct {
	encode bool
	offset int
	err    error
	block  blockUnit
	rc     io.ReadCloser

	blockLock chan struct{} // safely free the block
	closeCh   chan struct{}
	closeOnce sync.Once
}

func (r *requestBody) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	for len(p) > 0 {
		if r.offset < 0 || r.offset == r.block.length() {
			if r.err = r.nextBlock(); r.err != nil {
				if n > 0 {
					return n, nil
				}
				return n, r.err
			}
		}

		read := copy(p, r.block[r.offset:])
		r.offset += read

		p = p[read:]
		n += read
	}
	return n, nil
}

func (r *requestBody) nextBlock() error {
	var (
		n     int
		err   error
		block blockUnit
	)
	if r.encode {
		block = r.block[crc32Len:]
	} else {
		block = r.block
	}

	readCh := make(chan struct{})
	go func() {
		if _, ok := <-r.blockLock; !ok {
			// closed
			return
		}
		n, err = readFullOrToEnd(r.rc, block)
		close(readCh)
		r.blockLock <- struct{}{}
	}()

	select {
	case <-r.closeCh:
		return ErrReadOnClosed
	case <-readCh:
	}
	if err != nil {
		return err
	}

	if r.encode {
		r.offset = 0
		r.block = r.block[:crc32Len+n]
		r.block.writeCrc()
		return nil
	}

	if n <= crc32Len {
		return ErrMismatchedCrc
	}

	r.offset = crc32Len
	r.block = r.block[:n]
	if err = r.block.check(); err != nil {
		return ErrMismatchedCrc
	}
	return nil
}

func (r *requestBody) Close() error {
	r.closeOnce.Do(func() {
		block := r.block
		r.block = nil
		close(r.closeCh)

		go func(buf []byte) {
			<-r.blockLock
			close(r.blockLock)
			bytespool.Free(buf)
		}(block)
	})
	return r.rc.Close()
}

func (r *requestBody) CodeSize(size int64) int64 {
	if r.encode {
		return EncodeSize(size, int64(r.block.length()))
	}
	return DecodeSize(size, int64(r.block.length()))
}

type codeSizeBody struct {
	encode      bool
	blockLength int64
}

func (c *codeSizeBody) Read(p []byte) (n int, err error) { return 0, io.EOF }
func (c *codeSizeBody) Close() error                     { return nil }
func (c *codeSizeBody) CodeSize(size int64) int64 {
	if c.encode {
		return EncodeSize(size, c.blockLength)
	}
	return DecodeSize(size, c.blockLength)
}

// TODO: using resourcepool's chan-pool if block size greater than 64K.
func newRequestBody(rc io.ReadCloser, encode bool) RequestBody {
	if rc == nil {
		return &codeSizeBody{
			encode:      encode,
			blockLength: gBlockSize,
		}
	}

	lock := make(chan struct{}, 1)
	lock <- struct{}{}
	return &requestBody{
		encode:    encode,
		block:     bytespool.Alloc(int(gBlockSize)),
		offset:    -1,
		rc:        rc,
		blockLock: lock,
		closeCh:   make(chan struct{}),
	}
}

// NewBodyEncoder returns encoder with crc32.
//
// If rc == nil, the encoder is called just with CodeSize,
// you need not to Close it at all.
func NewBodyEncoder(rc io.ReadCloser) RequestBody {
	return newRequestBody(rc, true)
}

// NewBodyDecoder returns decoder with crc32.
//
// If rc == nil, the decoder is called just with CodeSize,
// you need not to Close it at all.
func NewBodyDecoder(rc io.ReadCloser) RequestBody {
	return newRequestBody(rc, false)
}

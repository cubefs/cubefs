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

package iostat

import (
	"io"
	"time"
)

type iostatReader struct {
	underlying io.Reader
	ios        *StatMgr
}

type iostatReadCloser struct {
	underlying io.ReadCloser
	ios        *StatMgr
}

type iostatReaderAt struct {
	underlying io.ReaderAt
	ios        *StatMgr
}

func (ior *iostatReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	ior.ios.ReadBegin(uint64(len(p)))
	defer ior.ios.ReadEnd(time.Now())

	n, err = ior.underlying.ReadAt(p, off)
	return
}

func (ior *iostatReader) Read(p []byte) (n int, err error) {
	ior.ios.ReadBegin(uint64(len(p)))
	defer ior.ios.ReadEnd(time.Now())

	n, err = ior.underlying.Read(p)
	return
}

func (ior *iostatReadCloser) Read(p []byte) (n int, err error) {
	ior.ios.ReadBegin(uint64(len(p)))
	defer ior.ios.ReadEnd(time.Now())

	n, err = ior.underlying.Read(p)

	return
}

func (ior *iostatReadCloser) Close() error {
	return ior.underlying.Close()
}

func (sm *StatMgr) Reader(underlying io.Reader) io.Reader {
	return &iostatReader{
		underlying: underlying,
		ios:        sm,
	}
}

func (sm *StatMgr) ReaderAt(underlying io.ReaderAt) io.ReaderAt {
	return &iostatReaderAt{
		underlying: underlying,
		ios:        sm,
	}
}

func (sm *StatMgr) ReaderCloser(underlying io.ReadCloser) io.ReadCloser {
	return &iostatReadCloser{
		underlying: underlying,
		ios:        sm,
	}
}

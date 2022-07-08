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

type iostatWriter struct {
	underlying io.Writer
	ios        *StatMgr
}

type iostatWriteCloser struct {
	underlying io.WriteCloser
	ios        *StatMgr
}

type iostatWriterAt struct {
	underlying io.WriterAt
	ios        *StatMgr
}

func (iow *iostatWriter) Write(p []byte) (written int, err error) {
	iow.ios.WriteBegin(uint64(len(p)))
	defer iow.ios.WriteEnd(time.Now())

	written, err = iow.underlying.Write(p)

	return
}

func (iow *iostatWriterAt) WriteAt(p []byte, off int64) (written int, err error) {
	iow.ios.WriteBegin(uint64(len(p)))
	defer iow.ios.WriteEnd(time.Now())

	written, err = iow.underlying.WriteAt(p, off)

	return
}

func (iow *iostatWriteCloser) Write(p []byte) (written int, err error) {
	iow.ios.WriteBegin(uint64(len(p)))
	defer iow.ios.WriteEnd(time.Now())

	written, err = iow.underlying.Write(p)

	return
}

func (iow *iostatWriteCloser) Close() error {
	return iow.underlying.Close()
}

func (sm *StatMgr) Writer(underlying io.Writer) io.Writer {
	return &iostatWriter{
		underlying: underlying,
		ios:        sm,
	}
}

func (sm *StatMgr) WriterAt(underlying io.WriterAt) io.WriterAt {
	return &iostatWriterAt{
		underlying: underlying,
		ios:        sm,
	}
}

func (sm *StatMgr) WriteCloser(underlying io.WriteCloser) io.WriteCloser {
	return &iostatWriteCloser{
		underlying: underlying,
		ios:        sm,
	}
}

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

package qos

import (
	"context"
	"io"
	"sync"

	"github.com/cubefs/cubefs/blobstore/blobnode/base/limitio"
)

const (
	_limitedBpsRead  = "limited_bps_r"
	_limitedBpsWrite = "limited_bps_w"
)

type bpsReader struct {
	underlying io.Reader
	c          limitio.Controller
	ctx        context.Context
	once       sync.Once
}

type bpsReaderAt struct {
	underlying io.ReaderAt
	c          limitio.Controller
	ctx        context.Context
	once       sync.Once
}

type bpsWriter struct {
	underlying io.Writer
	c          limitio.Controller
	ctx        context.Context
	once       sync.Once
}

type bpsWriterAt struct {
	underlying io.WriterAt
	c          limitio.Controller
	ctx        context.Context
	once       sync.Once
}

func NewBpsReader(ctx context.Context, underlying io.Reader, c limitio.Controller) io.Reader {
	if c == nil {
		return underlying
	}
	return &bpsReader{
		ctx:        ctx,
		underlying: underlying,
		c:          c,
	}
}

func NewBpsReaderAt(ctx context.Context, underlying io.ReaderAt, c limitio.Controller) io.ReaderAt {
	if c == nil {
		return underlying
	}
	return &bpsReaderAt{
		ctx:        ctx,
		underlying: underlying,
		c:          c,
	}
}

func NewBpsWriter(ctx context.Context, underlying io.Writer, c limitio.Controller) io.Writer {
	if c == nil {
		return underlying
	}
	return &bpsWriter{
		ctx:        ctx,
		underlying: underlying,
		c:          c,
	}
}

func NewBpsWriterAt(ctx context.Context, underlying io.WriterAt, c limitio.Controller) io.WriterAt {
	if c == nil {
		return underlying
	}
	return &bpsWriterAt{
		ctx:        ctx,
		underlying: underlying,
		c:          c,
	}
}

func (r *bpsReader) Read(p []byte) (n int, err error) {
	size, limited := r.c.Assign(int64(len(p)))
	if limited {
		r.once.Do(func() { limitio.AddTrackTag(r.ctx, _limitedBpsRead) })
	}

	n, err = r.underlying.Read(p[:size])
	r.c.Fill(int64(int(size) - n))

	return
}

func (rt *bpsReaderAt) readAt(p []byte, off int64) (n int, err error) {
	size, limited := rt.c.Assign(int64(len(p)))
	if limited {
		rt.once.Do(func() { limitio.AddTrackTag(rt.ctx, _limitedBpsRead) })
	}

	n, err = rt.underlying.ReadAt(p[:size], off)
	rt.c.Fill(int64(int(size) - n))

	return
}

func (rt *bpsReaderAt) ReadAt(p []byte, off int64) (readn int, err error) {
	var nn int
	for readn < len(p) && err == nil {
		nn, err = rt.readAt(p[readn:], off)
		off += int64(nn)
		readn += nn
	}
	return
}

func (w *bpsWriter) Write(p []byte) (written int, err error) {
write:
	size, limited := w.c.Assign(int64(len(p)))
	n, err := w.underlying.Write(p[:size])

	if limited {
		w.once.Do(func() {
			limitio.AddTrackTag(w.ctx, _limitedBpsWrite)
		})
	}
	w.c.Fill(int64(int(size) - n))
	written += n

	if err != nil {
		return
	}
	// NOTE: n must be equal to size
	if int(size) != len(p) {
		p = p[size:]
		goto write
	}
	return
}

func (wt *bpsWriterAt) WriteAt(p []byte, off int64) (written int, err error) {
write:
	size, limited := wt.c.Assign(int64(len(p)))

	if limited {
		wt.once.Do(func() {
			limitio.AddTrackTag(wt.ctx, _limitedBpsWrite)
		})
	}
	n, err := wt.underlying.WriteAt(p[:size], off)
	wt.c.Fill(int64(int(size) - n))
	off += int64(n)
	written += n

	if err != nil {
		return
	}
	// NOTE: n must be equal to size
	if int(size) != len(p) {
		p = p[size:]
		goto write
	}
	return
}

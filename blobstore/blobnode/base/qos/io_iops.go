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
	_limitedIopsRead  = "limited_iops_r"
	_limitedIopsWrite = "limited_iops_w"
)

type iopsReader struct {
	underlying io.Reader
	c          limitio.Controller
	ctx        context.Context
	once       sync.Once
}

type iopsReaderAt struct {
	underlying io.ReaderAt
	c          limitio.Controller
	ctx        context.Context
	once       sync.Once
}

type iopsWriter struct {
	underlying io.Writer
	c          limitio.Controller
	ctx        context.Context
	once       sync.Once
}

type iopsWriterAt struct {
	underlying io.WriterAt
	c          limitio.Controller
	ctx        context.Context
	once       sync.Once
}

func NewIOPSReader(ctx context.Context, underlying io.Reader, c limitio.Controller) io.Reader {
	return &iopsReader{
		ctx:        ctx,
		underlying: underlying,
		c:          c,
	}
}

func NewIOPSReaderAt(ctx context.Context, underlying io.ReaderAt, c limitio.Controller) io.ReaderAt {
	return &iopsReaderAt{
		ctx:        ctx,
		underlying: underlying,
		c:          c,
	}
}

func NewIOPSWriter(ctx context.Context, underlying io.Writer, c limitio.Controller) io.Writer {
	return &iopsWriter{
		ctx:        ctx,
		underlying: underlying,
		c:          c,
	}
}

func NewIOPSWriterAt(ctx context.Context, underlying io.WriterAt, c limitio.Controller) io.WriterAt {
	return &iopsWriterAt{
		ctx:        ctx,
		underlying: underlying,
		c:          c,
	}
}

func (r *iopsReader) Read(p []byte) (n int, err error) {
	_, limited := r.c.Assign(1)
	if limited {
		r.once.Do(func() { limitio.AddTrackTag(r.ctx, _limitedIopsRead) })
	}
	n, err = r.underlying.Read(p)
	return
}

func (rt *iopsReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	_, limited := rt.c.Assign(1)
	if limited {
		rt.once.Do(func() { limitio.AddTrackTag(rt.ctx, _limitedIopsRead) })
	}
	n, err = rt.underlying.ReadAt(p, off)
	return
}

func (w *iopsWriter) Write(p []byte) (written int, err error) {
	_, limited := w.c.Assign(1)
	if limited {
		w.once.Do(func() { limitio.AddTrackTag(w.ctx, _limitedIopsWrite) })
	}
	written, err = w.underlying.Write(p)
	return
}

func (wt *iopsWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	_, limited := wt.c.Assign(1)
	if limited {
		wt.once.Do(func() { limitio.AddTrackTag(wt.ctx, _limitedIopsWrite) })
	}
	n, err = wt.underlying.WriteAt(p, off)
	return
}

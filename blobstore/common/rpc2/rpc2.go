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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	Version = 0
	Magic   = 0xee

	_headerCell = 4
)

var (
	errLimitedWrite = errors.New("rpc2: body should be limited")

	ErrServerClosed  = errors.New("rpc2: server closed")
	ErrLimitedWriter = errors.New("rpc2: request or response body wrap with LimitedWriter")
	ErrFrameHeader   = errors.New("rpc2: request or response header is not in one frame")
	ErrFrameProtocol = errors.New("rpc2: invalid protocol frame")
)

type (
	Marshaler interface {
		Marshal() ([]byte, error)
	}
	Unmarshaler interface {
		Unmarshal([]byte) error
	}
	Codec interface {
		Marshaler
		Unmarshaler
	}
)

type noneCodec struct{}

func (*noneCodec) Marshal() ([]byte, error) { return nil, nil }
func (*noneCodec) Unmarshal([]byte) error   { return nil }

var _ Codec = (*noneCodec)(nil)

type Body interface {
	io.Reader
	io.WriterTo
	io.Closer
}

var NoBody Body = noBody{}

type noBody struct{}

func (noBody) Read([]byte) (int, error)         { return 0, io.EOF }
func (noBody) Close() error                     { return nil }
func (noBody) WriteTo(io.Writer) (int64, error) { return 0, nil }

type nopBody struct {
	io.ReadCloser
}

func (nopBody) WriteTo(io.Writer) (int64, error) {
	panic("rpc2: should not call WriteTo in client request")
}

func clientNopBody(rc io.ReadCloser) Body {
	return nopBody{rc}
}

var NoParameter Codec = noParameter{}

type noParameter struct{}

func (noParameter) Marshal() ([]byte, error) { return nil, nil }
func (noParameter) Unmarshal([]byte) error   { return nil }

// LimitedWriter wrap Body with WriteTo
type LimitedWriter struct {
	w io.Writer
	a int64
	n int64
	e error
}

func LimitWriter(w io.Writer, limit int64) io.Writer {
	return &LimitedWriter{w, limit, limit, nil}
}

func (lw *LimitedWriter) Write(p []byte) (int, error) {
	if lw.n <= 0 {
		return 0, errLimitedWrite
	}
	if lw.n < int64(len(p)) {
		p = p[:lw.n]
	}
	n, err := lw.w.Write(p)
	lw.n -= int64(n)
	return n, err
}

func getSpan(ctx context.Context) trace.Span {
	return trace.SpanFromContextSafe(ctx)
}

type headerCell [_headerCell]byte

func (h *headerCell) Set(n int) {
	binary.LittleEndian.PutUint32((*h)[:], uint32(n))
}

func (h *headerCell) Get() int {
	return int(binary.LittleEndian.Uint32((*h)[:]))
}

func (h *headerCell) Write(p []byte) (int, error) {
	_ = p[3]
	copy((*h)[:], p)
	return _headerCell, nil
}

func (h headerCell) Reader() io.Reader {
	return bytes.NewReader(h[:])
}

func beforeContextDeadline(ctx context.Context, t time.Time) time.Time {
	d, ok := ctx.Deadline()
	if !ok {
		return t
	}
	return latestTime(t, d)
}

func latestTime(t time.Time, others ...time.Time) time.Time {
	for _, u := range others {
		if u.IsZero() {
			continue
		}
		if t.IsZero() || u.Before(t) {
			t = u
		}
	}
	return t
}

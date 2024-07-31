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
	"context"
	"io"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type headerReader struct {
	once      sync.Once
	marshaler interface {
		MarshalTo(dAtA []byte) (int, error)
	}
}

// Read reader marshal to
func (m *headerReader) Read(p []byte) (n int, err error) {
	n, err = 0, io.EOF
	m.once.Do(func() { n, err = m.marshaler.MarshalTo(p) })
	return
}

func (m *RequestHeader) MarshalToReader() io.Reader {
	return &headerReader{marshaler: m}
}

type Request struct {
	RequestHeader

	ctx  context.Context
	cli  *Client
	conn *transport.Stream

	Body    Body
	GetBody func() (io.ReadCloser, error) // client side

	// fill trailer header
	AfterBody func() error
}

func (req *Request) Span() trace.Span {
	return getSpan(req.ctx)
}

func (req *Request) Context() context.Context {
	return req.ctx
}

func (req *Request) request(deadline time.Time) (*Response, error) {
	reqHeaderSize := req.RequestHeader.Size()
	if _headerCell+reqHeaderSize > req.conn.MaxPayloadSize() {
		return nil, ErrHeaderFrame
	}

	var cell headerCell
	cell.Set(reqHeaderSize)
	size := _headerCell + reqHeaderSize + int(req.ContentLength) + req.Trailer.AllSize()

	req.conn.SetDeadline(deadline)
	_, err := req.conn.SizedWrite(io.MultiReader(cell.Reader(),
		req.RequestHeader.MarshalToReader(),
		io.LimitReader(req.Body, req.ContentLength),
		req.trailerReader(),
	), size)
	if err != nil {
		return nil, err
	}

	resp := &Response{Request: req}
	frame, err := readHeaderFrame(req.conn, &resp.ResponseHeader)
	if err != nil {
		return nil, err
	}
	resp.Body = makeBodyWithTrailer(
		req.conn.NewSizedReader(int(resp.ContentLength)+resp.Trailer.AllSize(), frame),
		resp.ContentLength, req, resp.Trailer)
	return resp, nil
}

type trailerReader struct {
	once sync.Once
	req  *Request
	r    io.Reader
}

func (t *trailerReader) Read(p []byte) (n int, err error) {
	t.once.Do(func() {
		if fn := t.req.AfterBody; fn != nil {
			if err = fn(); err != nil {
				t.r = t.req.Trailer.Reader()
			}
		}
	})
	if err != nil {
		return
	}
	return t.r.Read(p)
}

func (req *Request) trailerReader() io.Reader {
	return &trailerReader{req: req}
}

func (req *Request) WithCrc() *Request {
	return req
}

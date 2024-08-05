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

	stream *serverStream

	Body    Body
	GetBody func() (io.ReadCloser, error) // client side

	// fill trailer header
	AfterBody func() error
}

func (req *Request) ServerStream() ServerStream {
	return req.stream
}

func (req *Request) Span() trace.Span {
	return getSpan(req.ctx)
}

func (req *Request) Context() context.Context {
	return req.ctx
}

func (req *Request) write(deadline time.Time) error {
	reqHeaderSize := req.RequestHeader.Size()
	if _headerCell+reqHeaderSize > req.conn.MaxPayloadSize() {
		return ErrFrameHeader
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
	return err
}

func (req *Request) request(deadline time.Time) (*Response, error) {
	if err := req.write(deadline); err != nil {
		return nil, err
	}
	resp := &Response{Request: req}
	frame, err := readHeaderFrame(req.conn, &resp.ResponseHeader)
	if err != nil {
		return nil, err
	}
	resp.Body = makeBodyWithTrailer(
		req.conn.NewSizedReader(int(resp.ContentLength)+resp.Trailer.AllSize(), frame),
		resp.ContentLength, req, &resp.Trailer)
	return resp, nil
}

func (req *Request) trailerReader() io.Reader {
	return &trailerReader{
		Fn:      req.AfterBody,
		Trailer: req.Trailer,
	}
}

func (req *Request) WithCrc() *Request {
	return req
}

func baseRequest() *Request {
	return &Request{
		RequestHeader: RequestHeader{
			Version: Version,
			Magic:   Magic,
		},
		Body:      NoBody,
		AfterBody: func() error { return nil },
	}
}

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
	"fmt"
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

func (m *RequestHeader) ToString() string {
	return fmt.Sprintf("Version:%d Magic:%d"+
		" StreamCmd:%s RemoteAddr:%s RemoteHandler:%s TraceID:%s"+
		" ContentLength:%d Header:%+v Trailer:%+v Parameter:len(%d)",
		m.Version, m.Magic,
		m.StreamCmd.String(), m.RemoteAddr, m.RemoteHandler, m.TraceID,
		m.ContentLength, m.Header.M, m.Trailer.M, len(m.Parameter))
}

type OptionRequest func(*Request)

type Request struct {
	RequestHeader

	ctx    context.Context
	client *Client // client side
	conn   *transport.Stream

	stream *serverStream

	opts []OptionRequest

	checksum *ChecksumBlock

	Body    Body
	GetBody func() (io.ReadCloser, error) // client side

	// fill trailer
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
	encodeLen := req.checksum.EncodeSize(req.ContentLength)
	size := _headerCell + reqHeaderSize + int(encodeLen) + req.Trailer.AllSize()

	req.conn.SetDeadline(deadline)
	_, err := req.conn.SizedWrite(io.MultiReader(cell.Reader(),
		req.RequestHeader.MarshalToReader(),
		io.LimitReader(req.Body, encodeLen), // the body was encoded
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
	if resp.Status < 200 || resp.Status >= 300 {
		frame.Close()
		return nil, &Error{
			Status: resp.Status,
			Reason: resp.Reason,
			Detail: resp.Error,
		}
	}

	resp.Body = makeBodyWithTrailer(req.conn.NewSizedReader(
		int(req.checksum.EncodeSize(resp.ContentLength))+resp.Trailer.AllSize(), frame),
		req, resp.ContentLength)
	return resp, nil
}

func (req *Request) trailerReader() io.Reader {
	return &trailerReader{
		Fn:      req.AfterBody,
		Trailer: &req.Trailer,
	}
}

func (req *Request) Option(opt OptionRequest) *Request {
	req.opts = append(req.opts, opt)
	return req
}

func (req *Request) OptionCrc() *Request {
	return req.OptionChecksum(ChecksumBlock{
		Algorithm: ChecksumAlgorithm_Crc_IEEE,
		BlockSize: DefaultBlockSize,
	})
}

func (req *Request) OptionChecksum(block ChecksumBlock) *Request {
	if _, exist := algorithms[block.Algorithm]; !exist || block.BlockSize == 0 {
		panic(fmt.Sprintf("rpc2: checksum(%s) not implements", block.String()))
	}
	if req.checksum != nil {
		return req
	}
	cb, err := block.Marshal()
	if err != nil {
		return req
	}

	req.checksum = &block
	req.Header.Set(headerInternalChecksum, string(cb))
	if req.ContentLength == 0 {
		return req
	}

	req.opts = append(req.opts, func(r *Request) {
		r.Body = newEdBody(block, r.Body, int(req.ContentLength), true)
	})
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

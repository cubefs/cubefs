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
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func (m *RequestHeader) MarshalToReader() io.Reader {
	return Codec2Reader(m)
}

func (m *RequestHeader) ToString() string {
	return fmt.Sprintf("Version:%d Magic:%d"+
		" StreamCmd:%s RemotePath:%s TraceID:%s"+
		" ContentLength:%d Header:%+v Trailer:%+v Parameter:len(%d)",
		m.Version, m.Magic,
		m.StreamCmd.String(), m.RemotePath, m.TraceID,
		m.ContentLength, m.Header.M, m.Trailer.M, len(m.Parameter))
}

type OptionRequest func(*Request)

type Request struct {
	RequestHeader
	RemoteAddr string

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

func (req *Request) ParseParameter(para Unmarshaler) error {
	if len(req.Parameter) > 0 {
		return para.Unmarshal(req.Parameter[:])
	}
	_, err := req.Body.WriteTo(LimitWriter(Codec2Writer(para), req.ContentLength))
	return err
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

	decode := req.checksum != nil && req.checksum.Direction.IsDownload()
	payloadSize := resp.Trailer.AllSize()
	if decode {
		payloadSize += int(req.checksum.EncodeSize(resp.ContentLength))
	} else {
		payloadSize += int(resp.ContentLength)
	}
	resp.Body = makeBodyWithTrailer(req.conn.NewSizedReader(payloadSize, frame),
		req, &resp.Trailer, resp.ContentLength, decode)
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

func (req *Request) optionCrc(direction ChecksumDirection) *Request {
	return req.OptionChecksum(ChecksumBlock{
		Algorithm: ChecksumAlgorithm_Crc_IEEE,
		Direction: direction,
		BlockSize: DefaultBlockSize,
	})
}

func (req *Request) OptionCrc() *Request         { return req.optionCrc(ChecksumDirection_Duplex) }
func (req *Request) OptionCrcUpload() *Request   { return req.optionCrc(ChecksumDirection_Upload) }
func (req *Request) OptionCrcDownload() *Request { return req.optionCrc(ChecksumDirection_Download) }

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
	if req.ContentLength == 0 || !block.Direction.IsUpload() {
		return req
	}

	req.opts = append(req.opts, func(r *Request) {
		r.Body = newEdBody(block, r.Body, int(req.ContentLength), true)
	})
	return req
}

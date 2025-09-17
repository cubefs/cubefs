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

type OptionRequest func(*Request)

type Request struct {
	RequestHeader
	RemoteAddr string
	BodyRead   int64 // has read body size

	ctx    context.Context
	client *Client // client side
	opts   []OptionRequest
	conn   *transport.Stream

	checksum    ChecksumBlock
	bodyAligned bool

	// server side
	cancel       context.CancelFunc
	stream       *serverStream
	readablePara bool

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

func (req *Request) WithContext(ctx context.Context) *Request {
	r := new(Request)
	*r = *req
	r.ctx = ctx
	return r
}

// ParseParameter try to parse parameter from Parameter, then body,
// if parameter is readable and in body, copy it to Parameter.
func (req *Request) ParseParameter(para Unmarshaler) error {
	rr, ok := para.(Readable)
	if ok && rr.Readable() {
		req.readablePara = true
		if len(req.Parameter) == 0 && req.ContentLength <= 4<<10 {
			buff := make([]byte, req.ContentLength)
			if _, err := io.ReadFull(req.Body, buff); err != nil {
				return NewError(400, "ParseParameter", err.Error())
			}
			req.Parameter = buff
		}
	}
	if len(req.Parameter) > 0 {
		return para.Unmarshal(req.Parameter[:])
	}
	if req.ContentLength == 0 {
		return para.Unmarshal(nil)
	}
	_, err := req.Body.WriteTo(LimitWriter(Codec2Writer(para, int(req.ContentLength)), req.ContentLength))
	if err != nil {
		return NewError(400, "ParseParameter", err.Error())
	}
	return nil
}

func (req *Request) GetReadableParameter() []byte {
	if req.readablePara {
		return req.Parameter
	}
	return nil
}

func (req *Request) write(deadline time.Time) error {
	reqHeaderSize := req.RequestHeader.Size()
	if _headerCell+reqHeaderSize > req.conn.MaxPayloadSize() {
		return ErrFrameHeader
	}

	var cell headerCell
	cell.Set(reqHeaderSize)
	encodeLen := req.checksum.EncodeSize(req.ContentLength)

	tr := req.trailerReader()
	var mr io.Reader

	req.conn.SetDeadline(deadline)
	if req.bodyAligned {
		if _, err := req.conn.SizedWrite(req.ctx,
			codec2CellReader(cell, &req.RequestHeader),
			_headerCell+reqHeaderSize); err != nil {
			return err
		}
		if tr == nil {
			mr = req.Body
		} else {
			mr = io.MultiReader(io.LimitReader(req.Body, encodeLen), tr)
		}
		if _, err := req.conn.SizedWrite(req.ctx, mr,
			int(encodeLen)+req.Trailer.AllSize()); err != nil {
			return err
		}
		return nil
	}
	if tr == nil {
		mr = io.MultiReader(codec2CellReader(cell, &req.RequestHeader), req.Body)
	} else {
		mr = io.MultiReader(codec2CellReader(cell, &req.RequestHeader),
			io.LimitReader(req.Body, encodeLen), tr) // the body was encoded
	}

	size := _headerCell + reqHeaderSize + int(encodeLen) + req.Trailer.AllSize()
	_, err := req.conn.SizedWrite(req.ctx, mr, size)
	return err
}

func (req *Request) request(deadline time.Time) (*Response, bool, error) {
	if err := req.write(deadline); err != nil {
		return nil, true, err
	}
	resp := &Response{Request: req}
	frame, err := readHeaderFrame(req.ctx, req.conn, &resp.ResponseHeader)
	if err != nil {
		return nil, true, err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		frame.Close()
		// set the stream broken if it has body
		return nil, resp.ContentLength > 0, NewError(resp.Status, resp.Reason, resp.Error)
	}

	decode := req.checksum != ChecksumBlock{} && req.checksum.Direction.IsDownload()
	payloadSize := resp.Trailer.AllSize()
	if decode {
		payloadSize += int(req.checksum.EncodeSize(resp.ContentLength))
	} else {
		payloadSize += int(resp.ContentLength)
	}
	resp.Body = makeBodyWithTrailer(req.conn.NewSizedReader(req.ctx, payloadSize, frame),
		req, &resp.Trailer, resp.ContentLength, decode)
	return resp, false, nil
}

func (req *Request) trailerReader() io.Reader {
	if req.AfterBody == nil && req.Trailer.AllSize() == 0 {
		return nil
	}
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
	if block.BlockSize%transport.Alignment != 0 || algorithmSizes[block.Algorithm] > _checksumAlignment {
		panic(fmt.Sprintf("rpc2: checksum(%s) block size is not aligned", block.String()))
	}
	if _, exist := algorithms[block.Algorithm]; !exist || block.BlockSize == 0 {
		panic(fmt.Sprintf("rpc2: checksum(%s) not implements", block.String()))
	}
	if req.checksum != (ChecksumBlock{}) {
		return req
	}
	cb, err := block.Marshal()
	if err != nil {
		return req
	}

	req.checksum = block
	req.Header.Set(HeaderInternalChecksum, string(cb))
	if req.ContentLength == 0 || !block.Direction.IsUpload() {
		return req
	}

	req.opts = append(req.opts, req.optChecksum)
	return req
}

func (req *Request) optChecksum(r *Request) {
	r.Body = newEdBody(r.checksum, r.Body, int(req.ContentLength), true)
	if getBody := r.GetBody; getBody != nil {
		r.GetBody = func() (io.ReadCloser, error) {
			body, err := getBody()
			if err != nil {
				return nil, err
			}
			return newEdBody(r.checksum, clientNopBody(body), int(req.ContentLength), true), nil
		}
	}
}

func (req *Request) OptionBodyAligned() *Request {
	req.OptionBodyAlignedUpload()
	return req.OptionBodyAlignedDownload()
}

func (req *Request) OptionBodyAlignedUpload() *Request {
	req.bodyAligned = true
	return req
}

func (req *Request) OptionBodyAlignedDownload() *Request {
	req.Header.Set(HeaderInternalBodyAligned, "1")
	return req
}

func (req *Request) LocalAddrString() string {
	if addr := req.conn.LocalAddr(); addr != nil {
		return addr.String()
	}
	return ""
}

func (req *Request) RemoteAddrString() string {
	if addr := req.conn.RemoteAddr(); addr != nil {
		return addr.String()
	}
	return ""
}

func (req *Request) reuse() {
	putRequest(req)
}

var poolRequest = sync.Pool{
	New: func() any {
		return &Request{
			RequestHeader: RequestHeader{
				Version: Version,
				Magic:   Magic,
			},
		}
	},
}

func getRequest() *Request {
	return poolRequest.Get().(*Request)
}

func putRequest(req *Request) {
	req.Header.Renew()
	req.Trailer.Renew()
	*req = Request{
		RequestHeader: RequestHeader{
			Version:   req.Version,
			Magic:     req.Magic,
			Header:    req.Header,
			Trailer:   req.Trailer,
			Parameter: req.Parameter[:0],
		},
		opts: req.opts[:0],
	}
	poolRequest.Put(req) // nolint: staticcheck
}

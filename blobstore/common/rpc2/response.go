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
	"fmt"
	"hash/crc32"
	"io"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

// server side response
type ResponseWriter interface {
	SetContentLength(int64)
	Header() *Header
	Trailer() *FixedHeader

	WriteHeader(status int, obj Marshaler) error
	WriteOK(obj Marshaler) error
	Flush() error
	io.Writer
	io.ReaderFrom

	AfterBody(func() error)
}

func (m *ResponseHeader) MarshalToReader() io.Reader {
	return &headerReader{marshaler: m}
}

func (m *ResponseHeader) ToString() string {
	return fmt.Sprintf("Version:%d Magic:%d Status:%d Reason:%s Error:%s"+
		" ContentLength:%d Header:%+v Trailer:%+v Parameter:len(%d)",
		m.Version, m.Magic, m.Status, m.Reason, m.Error,
		m.ContentLength, m.Header.M, m.Trailer.M, len(m.Parameter))
}

// client side response
type Response struct {
	ResponseHeader

	Body Body

	Request *Request
}

var _ ResponseWriter = &response{}

type response struct {
	hdr ResponseHeader

	conn       *transport.Stream
	connBroken bool

	hasWroteHeader bool

	midWriter io.Writer

	remain    int // body remain
	toWrite   int
	toList    []io.Reader
	afterBody func() error
}

func (resp *response) SetContentLength(l int64) {
	resp.hdr.ContentLength = l
	resp.remain = int(l)
	if l <= 0 {
		resp.hdr.Trailer.Del(headerInternalCrc)
	}
}

func (resp *response) Header() *Header {
	return &resp.hdr.Header
}

func (resp *response) Trailer() *FixedHeader {
	return &resp.hdr.Trailer
}

func (resp *response) WriteOK(obj Marshaler) error {
	return resp.WriteHeader(200, obj)
}

func (resp *response) WriteHeader(status int, obj Marshaler) error {
	if resp.hasWroteHeader {
		return nil
	}
	resp.hdr.Status = int32(status)
	resp.hasWroteHeader = true
	resp.hdr.Header.SetStable()
	resp.hdr.Trailer.SetStable()

	if obj == nil {
		obj = NoParameter
	}
	b, err := obj.Marshal()
	if err != nil {
		return err
	}
	resp.hdr.Parameter = b

	var cell headerCell
	cell.Set(resp.hdr.Size())
	resp.toWrite += _headerCell + resp.hdr.Size()
	resp.toList = append(resp.toList, cell.Reader(), resp.hdr.MarshalToReader())
	return nil
}

func (resp *response) Write(p []byte) (int, error) {
	if !resp.hasWroteHeader {
		if err := resp.WriteHeader(200, NoParameter); err != nil {
			return 0, err
		}
	}
	if resp.remain < len(p) {
		p = p[:resp.remain]
	}
	resp.remain -= len(p)
	resp.toWrite += len(p)
	resp.toList = append(resp.toList, bytes.NewReader(p))
	if resp.remain == 0 {
		resp.toWrite += resp.hdr.Trailer.AllSize()
		resp.toList = append(resp.toList, &trailerReader{
			Fn:      resp.afterBody,
			Trailer: &resp.hdr.Trailer,
		})
	}
	resp.midWriter.Write(p)
	if err := resp.Flush(); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (resp *response) ReadFrom(r io.Reader) (n int64, err error) {
	if !resp.hasWroteHeader {
		if err := resp.WriteHeader(200, NoParameter); err != nil {
			return 0, err
		}
	}
	remain := resp.remain
	resp.toWrite += remain + resp.hdr.Trailer.AllSize()
	resp.toList = append(resp.toList,
		io.TeeReader(io.LimitReader(r, int64(remain)), resp.midWriter),
		&trailerReader{
			Fn:      resp.afterBody,
			Trailer: &resp.hdr.Trailer,
		})
	resp.remain = 0
	if err := resp.Flush(); err != nil {
		return 0, err
	}
	return int64(remain), nil
}

func (resp *response) Flush() error {
	if len(resp.toList) == 0 {
		return nil
	}
	_, err := resp.conn.SizedWrite(io.MultiReader(resp.toList...), resp.toWrite)
	if err != nil {
		resp.connBroken = true
		return err
	}
	resp.toWrite = 0
	resp.toList = resp.toList[:0]
	return nil
}

func (resp *response) AfterBody(fn func() error) {
	afterBody := resp.afterBody
	resp.afterBody = func() error {
		if err := fn(); err != nil {
			return err
		}
		return afterBody()
	}
}

func (resp *response) options(req *Request) {
	resp.midWriter = io.MultiWriter()
	if req.Header.Get(headerInternalCrc) != "" {
		resp.hdr.Trailer.SetLen(headerInternalCrc, 4)
		crc := crc32.NewIEEE()
		resp.midWriter = crc
		resp.afterBody = func() error {
			resp.hdr.Trailer.Set(headerInternalCrc, string(crc.Sum(nil)))
			return nil
		}
	}
}

func baseResponse() *Response {
	return &Response{
		ResponseHeader: ResponseHeader{
			Version: Version,
			Magic:   Magic,
		},
	}
}

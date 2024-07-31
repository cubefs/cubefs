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
	"io"
)

// server side response
type ResponseWriter interface {
	SetContentLength(int64)
	Header() Header
	Trailer() FixedHeader

	WriteHeader(status int)
	io.Writer
	io.ReaderFrom
}

func (m *ResponseHeader) MarshalToReader() io.Reader {
	return &headerReader{marshaler: m}
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
}

func (resp *response) SetContentLength(l int64) {
	resp.hdr.ContentLength = l
}

func (resp *response) Header() Header {
	return resp.hdr.Header
}

func (resp *response) Trailer() FixedHeader {
	return resp.hdr.Trailer
}

func (resp *response) WriteHeader(status int) {
	resp.hdr.Status = int32(status)
}

func (resp *response) Write(p []byte) (int, error) {
	return 0, nil
}

func (resp *response) ReadFrom(r io.Reader) (n int64, err error) {
	return 0, nil
}

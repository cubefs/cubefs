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
	"io"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

// server side response
type ResponseWriter interface {
	SetContentLength(int64)
	Header() *Header
	Trailer() *FixedHeader

	// WriteHeader object in Header's Parameter
	WriteHeader(status int, obj Marshaler) error
	// WriteOK object in body
	WriteOK(obj Marshaler) error
	// SetError fill error's reason to response header
	SetError(err error)
	Flush() error
	// io.Writer
	io.ReaderFrom
	// WriteBody writes body by application.
	WriteBody(func(ChecksumBlock, *transport.Stream) (int64, error)) (int64, error)

	AfterBody(func() error)
}

// client side response
type Response struct {
	ResponseHeader

	Body Body

	Request *Request
}

var _ ResponseWriter = &response{}

func (resp *Response) ParseResult(ret Unmarshaler) error {
	if ret == nil {
		return nil
	}
	if len(resp.Parameter) > 0 {
		return ret.Unmarshal(resp.Parameter[:])
	}
	if resp.ContentLength == 0 {
		return ret.Unmarshal(nil)
	}
	_, err := resp.Body.WriteTo(LimitWriter(Codec2Writer(ret, int(resp.ContentLength)), resp.ContentLength))
	return err
}

type response struct {
	hdr ResponseHeader

	ctx        context.Context
	conn       *transport.Stream
	connBroken bool

	hasWroteHeader bool
	hasWroteBody   bool

	bodyEncoder *edBody
	bodyAligned bool

	remain    int // body remain
	nHeader   int
	nBody     int
	allReader multiReader // first Reader is Header
	afterBody func() error
}

func (resp *response) SetContentLength(l int64) {
	resp.hdr.ContentLength = l
	resp.remain = int(l)
	if resp.bodyEncoder != nil {
		resp.bodyEncoder.remain = int(l)
	}
}

func (resp *response) Header() *Header {
	return &resp.hdr.Header
}

func (resp *response) Trailer() *FixedHeader {
	return &resp.hdr.Trailer
}

func (resp *response) SetError(err error) {
	_, reason, detail := DetectError(err)
	resp.hdr.Reason = reason
	resp.hdr.Error = detail.Error()
}

func (resp *response) WriteOK(obj Marshaler) error {
	if resp.hasWroteHeader {
		return nil
	}
	if obj == nil {
		obj = NoParameter
	}
	size := int64(obj.Size())
	resp.SetContentLength(int64(size))
	_, err := resp.ReadFrom(Codec2Reader(obj))
	return err
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
	resp.nHeader += _headerCell + resp.hdr.Size()
	resp.allReader.Append(codec2CellReader(cell, &resp.hdr))
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
	if resp.remain != len(p) {
		return 0, io.ErrShortWrite
	}
	if resp.hasWroteBody {
		return 0, nil
	}
	resp.hasWroteBody = true

	r, nbody := resp.encodeBody(bytes.NewReader(p))
	resp.nBody += nbody + resp.hdr.Trailer.AllSize()
	resp.addTrailerReader(r)
	resp.remain = 0
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
	if resp.hasWroteBody {
		return 0, nil
	}
	resp.hasWroteBody = true

	remain := resp.remain
	r, nbody := resp.encodeBody(r)
	resp.nBody += nbody + resp.hdr.Trailer.AllSize()
	resp.addTrailerReader(r)
	resp.remain = 0
	if err := resp.Flush(); err != nil {
		return 0, err
	}
	return int64(remain), nil
}

func (resp *response) WriteBody(write func(ChecksumBlock, *transport.Stream) (int64, error)) (n int64, err error) {
	if !resp.hasWroteHeader {
		if err = resp.WriteHeader(200, NoParameter); err != nil {
			return 0, err
		}
	}
	if resp.hasWroteBody {
		return 0, nil
	}
	resp.hasWroteBody = true

	if err = resp.Flush(); err != nil {
		return 0, err
	}
	var cb ChecksumBlock
	if resp.bodyEncoder != nil {
		cb = resp.bodyEncoder.block
	}
	n, err = write(cb, resp.conn)
	resp.remain -= int(n)
	if err != nil {
		resp.connBroken = true
		return
	}
	if resp.remain > 0 {
		err = io.ErrShortWrite
		return
	}

	if resp.afterBody == nil && resp.hdr.Trailer.AllSize() == 0 {
		return
	}
	_, err = resp.conn.SizedWrite(resp.ctx, &trailerReader{
		Fn:      resp.afterBody,
		Trailer: &resp.hdr.Trailer,
	}, resp.hdr.Trailer.AllSize())
	if err != nil {
		resp.connBroken = true
		return
	}
	return
}

func (resp *response) Flush() error {
	all := resp.nHeader + resp.nBody
	if all == 0 {
		return nil
	}
	if resp.connBroken {
		return io.ErrClosedPipe
	}
	var err error
	if resp.bodyAligned {
		_, err = resp.conn.SizedWrite(resp.ctx, resp.allReader.readers[0], resp.nHeader)
		if err != nil {
			resp.connBroken = true
			return err
		}
		resp.allReader.readers = resp.allReader.readers[1:]
		_, err = resp.conn.SizedWrite(resp.ctx, &resp.allReader, resp.nBody)
		if err != nil {
			resp.connBroken = true
			return err
		}
	} else {
		_, err = resp.conn.SizedWrite(resp.ctx, &resp.allReader, all)
		if err != nil {
			resp.connBroken = true
			return err
		}
	}
	resp.nHeader = 0
	resp.nBody = 0
	resp.allReader.Renew()
	return nil
}

func (resp *response) AfterBody(fn func() error) {
	afterBody := resp.afterBody
	resp.afterBody = func() error {
		if err := fn(); err != nil {
			return err
		}
		if afterBody != nil {
			return afterBody()
		}
		return nil
	}
}

func (resp *response) addTrailerReader(r io.Reader) {
	resp.allReader.Append(r)
	if resp.afterBody == nil && resp.hdr.Trailer.AllSize() == 0 {
		return
	}
	resp.allReader.Append(&trailerReader{
		Fn:      resp.afterBody,
		Trailer: &resp.hdr.Trailer,
	})
}

func (resp *response) options(req *Request) {
	if req.checksum != (ChecksumBlock{}) && req.checksum.Direction.IsDownload() {
		resp.bodyEncoder = newEdBody(req.checksum, nil, 0, true)
	}
	resp.bodyAligned = req.bodyAligned
}

func (resp *response) encodeBody(r io.Reader) (io.Reader, int) {
	if resp.bodyEncoder == nil {
		return r, resp.remain
	}
	resp.bodyEncoder.Body = clientNopBody(NopCloser(r))
	return resp.bodyEncoder, int(resp.bodyEncoder.block.EncodeSize(int64(resp.remain)))
}

func (resp *response) reuse() {
	putResponse(resp)
}

var poolResponse = sync.Pool{
	New: func() any {
		return &response{
			hdr: ResponseHeader{
				Version: Version,
				Magic:   Magic,
			},
		}
	},
}

func getResponse() *response {
	return poolResponse.Get().(*response)
}

func putResponse(resp *response) {
	resp.hdr.Header.Renew()
	resp.hdr.Trailer.Renew()
	resp.allReader.Renew()
	*resp = response{
		hdr: ResponseHeader{
			Version:   resp.hdr.Version,
			Magic:     resp.hdr.Magic,
			Header:    resp.hdr.Header,
			Trailer:   resp.hdr.Trailer,
			Parameter: resp.hdr.Parameter[:0],
		},
		allReader: resp.allReader,
	}
	poolResponse.Put(resp) // nolint: staticcheck
}

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }

type multiReader struct {
	origins []io.Reader
	readers []io.Reader
}

func (mr *multiReader) Append(r io.Reader) {
	mr.readers = append(mr.readers, r)
	mr.origins = mr.readers
}

func (mr *multiReader) Renew() {
	mr.readers = mr.origins[:0]
	mr.origins = mr.readers
}

// Read copy of io.MultiReader.
func (mr *multiReader) Read(p []byte) (n int, err error) {
	for len(mr.readers) > 0 {
		if len(mr.readers) == 1 {
			if r, ok := mr.readers[0].(*multiReader); ok {
				mr.readers = r.readers
				continue
			}
		}
		n, err = mr.readers[0].Read(p)
		if err == io.EOF {
			mr.readers[0] = eofReader{} // permit earlier GC
			mr.readers = mr.readers[1:]
		}
		if n > 0 || err != io.EOF {
			if err == io.EOF && len(mr.readers) > 0 {
				err = nil
			}
			return
		}
	}
	return 0, io.EOF
}

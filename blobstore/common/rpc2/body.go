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
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

type bodyAndTrailer struct {
	sr     *transport.SizedReader // smux reader
	br     Body                   // body reader
	remain int                    // body remain, read trailer if remain == 0
	req    *Request
	err    error

	trailerOnce sync.Once
	trailer     *FixedHeader
	closeOnce   sync.Once
}

func (r *bodyAndTrailer) storeError(err error) {
	if r.err == nil {
		r.err = err
	}
}

func (r *bodyAndTrailer) tryReadTrailer() error {
	if r.remain < 0 {
		panic("rpc2: body read too much, remain < 0")
	}
	var err error
	if r.remain == 0 { // try to read trailer
		r.trailerOnce.Do(func() {
			_, err = r.trailer.ReadFrom(r.sr)
		})
	}
	return err
}

func (r *bodyAndTrailer) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remain == 0 {
		return 0, io.EOF
	}

	if len(p) > r.remain {
		p = p[:r.remain]
	}
	n, err := r.br.Read(p)
	r.remain -= n
	if err == nil {
		err = r.tryReadTrailer()
	}
	r.storeError(err)
	return n, err
}

func (r *bodyAndTrailer) WriteTo(w io.Writer) (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remain == 0 {
		return 0, io.EOF
	}

	lw, ok := w.(*LimitedWriter)
	if !ok {
		return 0, ErrLimitedWriter
	}
	if lw.a > int64(r.remain) {
		return 0, io.ErrShortWrite
	}

	_, err := r.br.WriteTo(lw)
	if err == errLimitedWrite {
		err = nil
	}
	n := lw.a - lw.n // actual read bytes
	r.remain -= int(n)
	if err == nil {
		err = r.tryReadTrailer()
	}
	r.storeError(err)
	return n, err
}

func (r *bodyAndTrailer) Close() error {
	r.storeError(r.tryReadTrailer())
	r.closeOnce.Do(func() {
		err := r.sr.Close()
		if cli := r.req.client; cli != nil {
			cli.Connector.Put(r.req.Context(), r.req.conn,
				err != nil || !r.sr.Finished())
			r.req.conn = nil
		}
		r.storeError(err)
		if !r.sr.Finished() {
			r.storeError(io.ErrClosedPipe)
		}
	})
	return r.err
}

// makeBodyWithTrailer body and trailer remain.
func makeBodyWithTrailer(sr *transport.SizedReader, req *Request,
	trailer *FixedHeader, l int64, decode bool,
) Body {
	var br Body
	if decode {
		br = newEdBody(*req.checksum, sr, int(l), false)
	} else {
		br = sr
	}
	r := &bodyAndTrailer{
		sr:      sr,
		br:      br,
		remain:  int(l),
		req:     req,
		trailer: trailer,
	}
	r.tryReadTrailer()
	return r
}

// readHeaderFrame try to read request or response header.
func readHeaderFrame(stream *transport.Stream, hdr Unmarshaler) (*transport.FrameRead, error) {
	frame, err := stream.ReadFrame()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			frame.Close()
		}
	}()

	if frame.Len() < _headerCell {
		err = ErrFrameHeader
		return nil, err
	}
	var cell headerCell
	cell.Write(frame.Bytes(_headerCell))
	headerSize := cell.Get()
	if frame.Len() < headerSize {
		err = ErrFrameHeader
		return nil, err
	}

	if err = hdr.Unmarshal(frame.Bytes(headerSize)); err != nil {
		return nil, err
	}
	return frame, nil
}

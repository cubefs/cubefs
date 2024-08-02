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
	sr *transport.SizedReader
	br io.Reader
	// body remain, read trailer if remain == 0
	remain    int
	closeOnce sync.Once

	req     *Request
	trailer *FixedHeader
}

func (r *bodyAndTrailer) tryReadTrailer() error {
	if r.remain < 0 {
		panic("rpc2: response body read too much")
	}
	if r.remain == 0 { // try to read trailer
		if _, err := r.trailer.ReadFrom(r.sr); err != nil {
			return err
		}
	}
	return nil
}

func (r *bodyAndTrailer) Read(p []byte) (int, error) {
	n, err := r.br.Read(p)
	r.remain -= n
	if err == nil {
		err = r.tryReadTrailer()
	}
	return n, err
}

func (r *bodyAndTrailer) WriteTo(w io.Writer) (int64, error) {
	if _, ok := w.(*LimitedWriter); !ok {
		return 0, ErrLimitedWriter
	}
	n, err := r.sr.WriteTo(w)
	if err == errLimitedWrite {
		err = nil
	}
	r.remain -= int(n)
	if err == nil {
		err = r.tryReadTrailer()
	}
	return n, err
}

func (r *bodyAndTrailer) Close() (err error) {
	r.closeOnce.Do(func() {
		err = r.sr.Close()
		if r.req != nil {
			if err == nil && r.sr.Finished() {
				err = r.req.cli.Connector.Put(r.req.Context(), r.req.conn)
			} else {
				r.req.conn.Close()
			}
		}
	})
	return
}

// makeBodyWithTrailer body and trailer remain.
func makeBodyWithTrailer(sr *transport.SizedReader, l int64, req *Request, trailer *FixedHeader) Body {
	return &bodyAndTrailer{
		sr:      sr,
		br:      io.LimitReader(sr, l),
		remain:  int(l),
		req:     req,
		trailer: trailer,
	}
}

// readHeaderFrame try to read request or response header.
func readHeaderFrame(stream *transport.Stream, hdr interface {
	Unmarshal([]byte) error
},
) (*transport.FrameRead, error) {
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
		err = ErrHeaderFrame
		return nil, err
	}
	var cell headerCell
	cell.Write(frame.Bytes(_headerCell))
	headerSize := cell.Get()
	if frame.Len() < headerSize {
		err = ErrHeaderFrame
		return nil, err
	}

	if err = hdr.Unmarshal(frame.Bytes(headerSize)); err != nil {
		return nil, err
	}
	return frame, nil
}

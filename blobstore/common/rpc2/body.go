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
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/rpc2/transport"
)

type bodyAndTrailer struct {
	sr *transport.SizedReader
	br io.Reader
	// body remain, read trailer if remain == 0
	remain    int
	closeOnce sync.Once

	req *Request

	trailerOnce sync.Once
	trailer     *FixedHeader

	trailerWriter  io.Writer
	trailerChecker func(*FixedHeader) error
}

func (r *bodyAndTrailer) tryReadTrailer() error {
	if r.remain < 0 {
		panic("rpc2: response body read too much")
	}
	var err error
	if r.remain == 0 { // try to read trailer
		r.trailerOnce.Do(func() {
			_, err = r.trailer.ReadFrom(r.sr)
		})
		if err == nil || err == io.EOF {
			err = r.trailerChecker(r.trailer)
		}
	}
	return err
}

func (r *bodyAndTrailer) Read(p []byte) (int, error) {
	n, err := r.br.Read(p)
	r.remain -= n
	r.trailerWriter.Write(p[:n])
	if err == nil {
		err = r.tryReadTrailer()
	}
	return n, err
}

func (r *bodyAndTrailer) WriteTo(w io.Writer) (int64, error) {
	if _, ok := w.(*LimitedWriter); !ok {
		return 0, ErrLimitedWriter
	}
	n, err := r.sr.WriteTo(io.MultiWriter(w, r.trailerWriter))
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
	errx := r.tryReadTrailer()
	r.closeOnce.Do(func() {
		err = r.sr.Close()
		if r.req != nil {
			if err == nil && r.sr.Finished() {
				err = r.req.client.Connector.Put(r.req.Context(), r.req.conn)
			} else {
				r.req.conn.Close()
			}
		}
	})
	if errx != nil {
		err = errx
	}
	return
}

// makeBodyWithTrailer body and trailer remain.
func makeBodyWithTrailer(sr *transport.SizedReader, l int64, req *Request, trailer *FixedHeader) Body {
	writer := io.MultiWriter()
	checker := func(trailer *FixedHeader) error { return nil }
	if trailer.Has(headerInternalCrc) {
		crc := crc32.NewIEEE()
		writer = io.MultiWriter(crc)
		checker = func(trailer *FixedHeader) error {
			exp := []byte(trailer.Get(headerInternalCrc))
			act := crc.Sum(nil)
			if !bytes.Equal(exp, act) {
				return &Error{
					Status: 400,
					Reason: "Checksum",
					Detail: fmt.Sprintf("rpc2: internal crc exp(%v) act(%v)", exp, act),
				}
			}
			return nil
		}
	}
	r := &bodyAndTrailer{
		sr:      sr,
		br:      io.LimitReader(sr, l),
		remain:  int(l),
		req:     req,
		trailer: trailer,

		trailerWriter:  writer,
		trailerChecker: checker,
	}
	r.tryReadTrailer()
	return r
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

func writeHeaderFrame(stream *transport.Stream, hdr interface {
	Size() int
	MarshalToReader() io.Reader
},
) error {
	var cell headerCell
	cell.Set(hdr.Size())
	_, err := stream.SizedWrite(io.MultiReader(cell.Reader(),
		hdr.MarshalToReader()), _headerCell+hdr.Size())
	return err
}

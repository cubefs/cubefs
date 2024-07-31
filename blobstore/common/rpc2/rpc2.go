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
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	Version = 0
	Magic   = 0xee

	_headerCell = 4
)

var (
	errLimitedWrite = errors.New("rpc2: body should be limited")

	ErrServerClosed  = errors.New("rpc2: server closed")
	ErrLimitedWriter = errors.New("rpc2: request or response body wrap with LimitedWriter")
	ErrHeaderFrame   = errors.New("rpc2: request or response header is not in one frame")
)

type Body interface {
	io.Reader
	io.WriterTo
	io.Closer
}

var NoBody Body = noBody{}

type noBody struct{}

func (noBody) Read([]byte) (int, error)         { return 0, io.EOF }
func (noBody) Close() error                     { return nil }
func (noBody) WriteTo(io.Writer) (int64, error) { return 0, nil }

type nopBody struct {
	io.ReadCloser
}

func (nopBody) WriteTo(io.Writer) (int64, error) {
	panic("rpc2: should not call WriteTo in client request")
}

func clientNopBody(rc io.ReadCloser) Body {
	return nopBody{rc}
}

// LimitedWriter wrap Body with WriteTo
type LimitedWriter struct {
	w io.Writer
	n int64
	e error
}

func LimitWriter(w io.Writer, limit int64) io.Writer {
	return &LimitedWriter{w, limit, nil}
}

func (lw *LimitedWriter) Write(p []byte) (int, error) {
	if lw.n <= 0 {
		return 0, errLimitedWrite
	}
	if lw.n < int64(len(p)) {
		p = p[:lw.n]
	}
	n, err := lw.w.Write(p)
	lw.n -= int64(n)
	return n, err
}

func getSpan(ctx context.Context) trace.Span {
	return trace.SpanFromContextSafe(ctx)
}

type headerCell [_headerCell]byte

func (h *headerCell) Set(n int) {
	binary.LittleEndian.PutUint32((*h)[:], uint32(n))
}

func (h *headerCell) Get() int {
	return int(binary.LittleEndian.Uint32((*h)[:]))
}

func (h *headerCell) Write(p []byte) (int, error) {
	_ = p[3]
	copy((*h)[:], p)
	return _headerCell, nil
}

func (h headerCell) Reader() io.Reader {
	return bytes.NewReader(h[:])
}

func beforeContextDeadline(ctx context.Context, t time.Time) time.Time {
	d, ok := ctx.Deadline()
	if !ok {
		return t
	}
	return latestTime(t, d)
}

func latestTime(t time.Time, others ...time.Time) time.Time {
	for _, u := range others {
		if u.IsZero() {
			continue
		}
		if t.IsZero() || u.Before(t) {
			t = u
		}
	}
	return t
}

// func SimpleClient() {
// 	var r io.Reader
// 	req := NewRequest(context.Background(), "localhost:9999", "/put/file", r)
// 	req.WithCrc()
// 	req.TraceID = "client-trace-id"
// 	req.ContentLength = 1 << 30

// 	req.Trailer.Set("value-before-body", "before send body")
// 	req.Trailer.SetSize("value-after-body", 4)
// 	req.AfterBody = func() error {
// 		req.Trailer.Set("value-after-body", "send")
// 		return nil
// 	}

// 	var cli *Client
// 	resp, _ := cli.Do(req)
// 	_ = resp.Status
// 	var w io.Writer
// 	resp.Body.WriteTo(w)
// 	resp.Body.Close()
// }

// func SimpleServer(req *Request, resp ResponseWriter) {
// 	_ = req.Header

// 	var w io.Writer
// 	req.Body.WriteTo(w)

// 	resp.SetTraceID("server-trace-id")
// 	resp.SetContentLength(1 << 30)
// 	resp.Trailer().Set("a", "b")
// 	resp.Trailer().SetSize("server-side", 4)

// 	resp.WriteHeader(200)
// 	var r io.Reader // 1G content
// 	resp.ReadFrom(r)

// 	resp.Trailer().Set("server-side", "server")
// 	resp.WriteTrailer()
// }

// type (
// 	reqMsg  struct{}
// 	respMsg struct{}
// )

// func streamClieng() {
// 	var cli *Client
// 	stream, _ := cli.BidiStreaming(context.TODO(), "", "reading")
// 	stream.Header()

// 	waitc := make(chan struct{})
// 	go func() {
// 		for {
// 			resp, err := stream.Recv()
// 			if err == io.EOF {
// 				close(waitc)
// 				return
// 			}
// 			_ = resp.(respMsg)
// 		}
// 	}()

// 	for range [100]struct{}{} {
// 		stream.Send(reqMsg{})
// 	}
// 	stream.CloseSend()
// 	<-waitc
// 	stream.Trailer()
// }

// func streamServer(conn Stream, stream BidiStreamingServer) {
// 	stream.SetHeader(Header{})
// 	for {
// 		req, err := stream.Recv()
// 		if err == io.EOF {
// 			break
// 		}
// 		_ = req.(reqMsg)
// 		stream.Send(respMsg{})
// 	}
// 	stream.SetTrailer(Header{})
// }

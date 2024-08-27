// Copyright 2022 The CubeFS Authors.
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

package auditlog

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type responseWriter struct {
	module     string
	statusCode int
	n          int
	bodyLimit  int
	// bodyWritten record how much bytes has been written to client
	bodyWritten int64
	// body hold some data buffer of response body, like json or form
	// audit log will record body buffer into log file
	body           []byte
	extra          http.Header // extra header
	span           trace.Span
	startTime      time.Time
	hasRecordCost  bool
	hasWroteHeader bool
	no2xxBody      bool

	http.ResponseWriter
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if !w.hasWroteHeader {
		w.WriteHeader(http.StatusOK)
		w.hasWroteHeader = true
	}
	if !(w.statusCode/100 == 2 && w.no2xxBody) && w.n < w.bodyLimit {
		n := copy(w.body[w.n:], b)
		w.n += n
	}
	n, err := w.ResponseWriter.Write(b)
	w.bodyWritten += int64(n)
	return n, err
}

// ReadFrom implement io.ReaderFrom when io.Copy.
// Response with this function will not hold first body bytes in local buffer.
func (w *responseWriter) ReadFrom(src io.Reader) (n int64, err error) {
	if !w.hasWroteHeader {
		w.WriteHeader(http.StatusOK)
		w.hasWroteHeader = true
	}
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		n, err = rf.ReadFrom(src)
		w.bodyWritten += int64(n)
		return
	}
	n, err = io.Copy(w.ResponseWriter, src)
	w.bodyWritten += int64(n)
	return
}

func (w *responseWriter) WriteHeader(code int) {
	if w.hasWroteHeader {
		return
	}

	if !w.hasRecordCost {
		w.span.AppendTrackLog(w.module, w.startTime, nil)
		traceLog := w.span.TrackLog()
		for i := range traceLog {
			w.Header().Add(rpc.HeaderTraceLog, traceLog[i])
		}
		tags := w.span.Tags().ToSlice()
		for i := range tags {
			w.Header().Add(rpc.HeaderTraceTags, tags[i])
		}
		w.Header().Set(trace.GetTraceIDKey(), w.span.TraceID())
		w.hasRecordCost = true
	}
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
	w.hasWroteHeader = true
}

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}

func (w *responseWriter) Flush() {
	w.ResponseWriter.(http.Flusher).Flush()
}

func (w *responseWriter) ExtraHeader() http.Header {
	if w.extra == nil {
		w.extra = make(http.Header)
	}

	return w.extra
}

func (w *responseWriter) getBody() []byte {
	header := w.ResponseWriter.Header()
	length, _ := strconv.ParseInt(header.Get(rpc.HeaderContentLength), 10, 64)
	if (w.statusCode/100 == 2 && w.no2xxBody) || length > int64(w.n) {
		return nil
	}
	return w.body[:w.n]
}

func (w *responseWriter) getStatusCode() int {
	return w.statusCode
}

func (w *responseWriter) getHeader() M {
	header := w.ResponseWriter.Header()
	headerM := make(M)
	for k := range header {
		if k == rpc.HeaderTraceLog || k == rpc.HeaderTraceTags {
			headerM[k] = header[k]
		} else if len(header[k]) == 1 {
			headerM[k] = header[k][0]
		} else {
			headerM[k] = header[k]
		}
	}

	for k := range w.extra {
		if len(w.extra[k]) == 1 {
			headerM[k] = w.extra[k][0]
		} else {
			headerM[k] = w.extra[k]
		}
	}

	return headerM
}

func (w *responseWriter) getBodyWritten() int64 {
	return w.bodyWritten
}

type responseWriter2 struct {
	module     string
	statusCode int

	hasWroteHeader bool
	no2xxBody      bool

	bodyLimit     int
	bodyWritten   int64
	n             int
	body          []byte
	contentLength int64

	startTime  time.Time
	span       trace.Span
	spanTraces int
	spanTags   int
	extra      http.Header

	rpc2.ResponseWriter
}

func (resp *responseWriter2) getBody() []byte {
	return resp.body[:resp.n]
}

func (resp *responseWriter2) writeTrace() {
	resp.span.AppendTrackLog(resp.module, resp.startTime, nil)
	header := resp.ResponseWriter.Header()
	if traceLogs := resp.span.TrackLog(); len(traceLogs) > 0 {
		resp.spanTraces = len(traceLogs)
		header.Set(rpc.HeaderTraceLog, strings.Join(traceLogs, "|"))
	}
	if tags := resp.span.Tags().ToSlice(); len(tags) > 0 {
		resp.spanTags = len(tags)
		header.Set(rpc.HeaderTraceTags, strings.Join(tags, "|"))
	}
	header.Set(trace.GetTraceIDKey(), resp.span.TraceID())
}

func (resp *responseWriter2) getHeader(header *rpc2.Header) M {
	headerM := make(M)
	if resp.contentLength > 0 {
		headerM[rpc.HeaderContentLength] = strconv.FormatInt(resp.contentLength, 10)
	}
	for k, v := range header.M {
		headerM[k] = v
	}
	trailer := resp.ResponseWriter.Trailer()
	for k, v := range trailer.M {
		headerM[k] = v.Value
	}
	for k := range resp.extra {
		if len(resp.extra[k]) == 1 {
			headerM[k] = resp.extra[k][0]
		} else {
			headerM[k] = resp.extra[k]
		}
	}
	return headerM
}

func (resp *responseWriter2) SetContentLength(l int64) {
	resp.contentLength = l
	resp.ResponseWriter.SetContentLength(l)
}

func (resp *responseWriter2) WriteOK(obj rpc2.Marshaler) error {
	if resp.hasWroteHeader {
		return nil
	}
	resp.hasWroteHeader = true
	resp.writeTrace()
	if obj == nil {
		obj = rpc2.NoParameter
	}

	resp.statusCode = 200
	size := obj.Size()
	ra, ok := obj.(rpc2.Readable)
	if ok && ra.Readable() && !resp.no2xxBody && size <= resp.bodyLimit {
		resp.n, _ = obj.MarshalTo(resp.body)
	}
	resp.contentLength = int64(size)
	err := resp.ResponseWriter.WriteOK(obj)
	if err == nil {
		resp.bodyWritten += int64(size)
	}
	return err
}

func (resp *responseWriter2) WriteHeader(status int, obj rpc2.Marshaler) error {
	if resp.hasWroteHeader {
		return nil
	}
	resp.hasWroteHeader = true
	resp.writeTrace()
	if obj == nil {
		obj = rpc2.NoParameter
	}

	resp.statusCode = status
	size := obj.Size()
	ra, ok := obj.(rpc2.Readable)
	if ok && ra.Readable() && size < resp.bodyLimit &&
		!(resp.statusCode/100 == 2 && resp.no2xxBody) {
		resp.n, _ = obj.MarshalTo(resp.body)
	}
	return resp.ResponseWriter.WriteHeader(status, obj)
}

func (resp *responseWriter2) ReadFrom(src io.Reader) (n int64, err error) {
	if !resp.hasWroteHeader {
		if err = resp.WriteHeader(200, rpc2.NoParameter); err != nil {
			return 0, err
		}
	}
	n, err = resp.ResponseWriter.ReadFrom(src)
	resp.bodyWritten += n
	return
}

func (resp *responseWriter2) ExtraHeader() http.Header {
	if resp.extra == nil {
		resp.extra = make(http.Header)
	}
	return resp.extra
}

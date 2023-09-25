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
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
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
	span           trace.Span
	startTime      time.Time
	hasRecordCost  bool
	hasWroteHeader bool

	http.ResponseWriter
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if !w.hasWroteHeader {
		w.WriteHeader(http.StatusOK)
		w.hasWroteHeader = true
	}
	if w.n < w.bodyLimit {
		n := copy(w.body[w.n:], b)
		w.n += n
	}
	n, err := w.ResponseWriter.Write(b)
	w.bodyWritten += int64(n)
	return n, err
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

func (w *responseWriter) getBody() []byte {
	header := w.ResponseWriter.Header()
	length, _ := strconv.ParseInt(header.Get(rpc.HeaderContentLength), 10, 64)
	if length > int64(w.n) {
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
		} else {
			headerM[k] = header.Get(k)
		}
	}
	return headerM
}

func (w *responseWriter) getBodyWritten() int64 {
	return w.bodyWritten
}

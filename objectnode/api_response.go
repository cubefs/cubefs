// Copyright 2023 The CubeFS Authors.
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

package objectnode

import (
	"net/http"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
)

type ResponseStater struct {
	StatusCode     int
	Written        int64
	StartTime      time.Time
	hasWroteHeader bool

	http.ResponseWriter
}

func NewResponseStater(w http.ResponseWriter) *ResponseStater {
	return &ResponseStater{
		ResponseWriter: w,
		StatusCode:     http.StatusOK,
		StartTime:      time.Now().UTC(),
	}
}

func (w *ResponseStater) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *ResponseStater) Write(b []byte) (int, error) {
	if !w.hasWroteHeader {
		w.WriteHeader(http.StatusOK)
		w.hasWroteHeader = true
	}
	n, err := w.ResponseWriter.Write(b)
	w.Written += int64(n)

	return n, err
}

func (w *ResponseStater) WriteHeader(code int) {
	if w.hasWroteHeader {
		return
	}
	w.StatusCode = code
	w.ResponseWriter.WriteHeader(code)
	w.hasWroteHeader = true
}

func (w *ResponseStater) ExtraHeader() http.Header {
	h := make(http.Header)
	if eh, ok := w.ResponseWriter.(auditlog.ResponseExtraHeader); ok {
		h = eh.ExtraHeader()
	}

	return h
}

func writeResponse(w http.ResponseWriter, code int, response []byte, mime string) {
	if mime != "" {
		w.Header().Set(ContentType, mime)
	}
	w.Header().Set(ContentLength, strconv.Itoa(len(response)))
	w.WriteHeader(code)
	if response != nil {
		_, _ = w.Write(response)
	}
}

func writeSuccessResponseXML(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, ValueContentTypeXML)
}

func writeSuccessResponseJSON(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, ValueContentTypeJSON)
}

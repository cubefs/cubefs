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

package base

import (
	"io"
	"time"
)

type Writer struct {
	io.WriterAt
	Offset int64
}

type Reader struct {
	io.ReaderAt
	Offset int64
}

type TimeReader struct {
	r io.Reader
	t int64
}

type TimeWriter struct {
	w io.Writer
	t int64
}

func (p *Writer) Write(val []byte) (n int, err error) {
	n, err = p.WriteAt(val, p.Offset)
	p.Offset += int64(n)
	return
}

func (p *Reader) Read(val []byte) (n int, err error) {
	n, err = p.ReadAt(val, p.Offset)
	p.Offset += int64(n)
	return
}

func accumulateLatency(total *int64, begin int64) {
	t := time.Now().UnixNano() - begin
	*total = *total + t
}

func (r *TimeReader) Read(p []byte) (n int, err error) {
	defer accumulateLatency(&r.t, time.Now().UnixNano())
	n, err = r.r.Read(p)
	return
}

func (r *TimeReader) Duration() time.Duration {
	return time.Duration(r.t)
}

func (r *TimeWriter) Write(p []byte) (n int, err error) {
	defer accumulateLatency(&r.t, time.Now().UnixNano())
	n, err = r.w.Write(p)
	return
}

func (r *TimeWriter) Duration() time.Duration {
	return time.Duration(r.t)
}

func NewTimeReader(reader io.Reader) *TimeReader {
	return &TimeReader{r: reader}
}

func NewTimeWriter(writer io.Writer) *TimeWriter {
	return &TimeWriter{w: writer}
}

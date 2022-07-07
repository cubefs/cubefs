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

package example

import (
	"io"
	"net/http"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// Filer file interface
type Filer interface {
	Write(*rpc.Context)
	Read(*rpc.Context)
}

type file struct{}

// NewFile new file
func NewFile() Filer {
	return &file{}
}

type noopReader struct {
	size int
	io.Reader
}

func (r *noopReader) Read(p []byte) (int, error) {
	if len(p) >= r.size {
		size := r.size
		r.size = 0
		return size, io.EOF
	}
	r.size -= len(p)
	return len(p), nil
}

type noopWriter struct {
	size int
	io.Writer
}

func (w *noopWriter) Write(p []byte) (int, error) {
	if len(p) >= w.size {
		size := w.size
		w.size = 0
		return size, io.EOF
	}
	w.size -= len(p)
	return len(p), nil
}

// ArgsWrite args upload
type ArgsWrite struct {
	Size int `flag:"size"`
}

func (f *file) Write(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	args := new(ArgsWrite)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("receive file write request, args: %#v", args)

	if args.Size <= 0 {
		c.RespondError(errBadRequst)
		return
	}

	// do some trace tags
	span.SetTag("limit-key", "limit-write")

	start := time.Now()
	w := &noopWriter{size: args.Size}
	if _, err := io.CopyN(w, c.Request.Body, int64(args.Size)); err != nil {
		span.AppendTrackLog("copy", start, err)
		c.RespondError(rpc.NewError(http.StatusGone, "ReadBody", err))
		return
	}
	span.AppendTrackLog("copy", start, nil)

	c.Respond()
}

// ArgsRead args read
type ArgsRead struct {
	Size int
}

func (*file) Read(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())

	args := new(ArgsRead)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("receive file read request, args: %#v", args)

	if args.Size < 0 {
		c.RespondError(errBadRequst)
		return
	}

	// do something
	span.SetTag("test-for-nil", nil)

	c.RespondWithReader(http.StatusOK, args.Size, rpc.MIMEStream,
		&noopReader{size: args.Size}, nil)
}

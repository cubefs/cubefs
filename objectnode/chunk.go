// Copyright 2019 The CubeFS Authors.
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
	"io"
	"net/http/httputil"
)

// ClosableChunkReader wraps the chunked reader from the "httputil" package provided by Go
// and provides a close method.
type closableChunkedReader struct {
	io.Reader
	src io.ReadCloser
}

func (r *closableChunkedReader) Read(p []byte) (n int, err error) {
	return r.Reader.Read(p)
}

func (r *closableChunkedReader) Close() error {
	return r.src.Close()
}

// NewClosableChunkedReader returns an instance of the io.ReadCloser interface
// used to parse chunk data.
func NewClosableChunkedReader(source io.ReadCloser) io.ReadCloser {
	return &closableChunkedReader{
		src:    source,
		Reader: httputil.NewChunkedReader(source),
	}
}

// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package compressor

import (
	"bytes"
	"compress/gzip"
	"io"
)

// TODO: reuse bytes.Buffer

type gzipCompressor struct{}

func (gzipCompressor) Compress(pb []byte) ([]byte, error) {
	buffer := new(bytes.Buffer)
	gw := gzip.NewWriter(buffer)
	if _, err := gw.Write(pb); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (gzipCompressor) Decompress(cb []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewBuffer(cb))
	if err != nil {
		return nil, err
	}
	buffer := new(bytes.Buffer)
	if _, err := io.Copy(buffer, gr); err != nil {
		return nil, err
	}
	if err := gr.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

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

const EncodingGzip = "gzip"

// Compressor bytes compressor.
// TODO: add stream Compressor.
type Compressor interface {
	Compress([]byte) ([]byte, error)
	Decompress([]byte) ([]byte, error)
}

type none struct{}

func (none) Compress(pb []byte) ([]byte, error)   { return pb, nil }
func (none) Decompress(cb []byte) ([]byte, error) { return cb, nil }

var compressors = make(map[string]func() Compressor)

func init() {
	compressors[""] = func() Compressor { return none{} }
	compressors[EncodingGzip] = func() Compressor { return gzipCompressor{} }
}

func New(encoding string) Compressor {
	if newCompressor, ok := compressors[encoding]; ok {
		return newCompressor()
	}
	return compressors[""]()
}

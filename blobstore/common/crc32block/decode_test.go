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

package crc32block

import (
	"bytes"
	"hash/crc32"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleEncoderDecoder(t *testing.T) {
	size1 := int64(64*1024 - 4)
	size2 := int64(64 * 1024)
	size3 := int64(64*1024 + 4)
	size4 := int64(1024 * 1024)
	size5 := int64(0)
	data := []struct {
		bodysize int64
	}{
		{bodysize: size1},
		{bodysize: size2},
		{bodysize: size3},
		{bodysize: size4},
		{bodysize: size5},
	}

	for index, d := range data {
		b1 := genRandData(d.bodysize)
		body := bytes.NewReader(b1)
		enc := NewEncoderReader(body)

		dec := NewDecoderReader(enc)
		b2 := make([]byte, d.bodysize)
		_, err := io.ReadFull(dec, b2)
		require.NoError(t, err, "index is %v", index)
		require.Equal(t, crc32.ChecksumIEEE(b1), crc32.ChecksumIEEE(b2), "index is %v", index)
	}
}

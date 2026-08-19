// Copyright 2026 The CubeFS Authors.
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyDeleteObjectsChecksum(t *testing.T) {
	body := []byte(`<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>example</Key></Object></Delete>`)
	header := func(values ...string) http.Header {
		h := make(http.Header)
		for i := 0; i < len(values); i += 2 {
			h.Set(values[i], values[i+1])
		}
		return h
	}

	tests := []struct {
		name      string
		header    http.Header
		errorCode *ErrorCode
	}{
		{
			name:   "content md5",
			header: header(ContentMD5, GetMD5(body)),
		},
		{
			name:   "boto3 crc32",
			header: header("x-amz-sdk-checksum-algorithm", "CRC32", XAmzChecksumCRC32, "upiFyg=="),
		},
		{
			name:      "invalid content md5",
			header:    header(ContentMD5, "invalid"),
			errorCode: BadDigest,
		},
		{
			name:      "invalid crc32",
			header:    header(XAmzChecksumCRC32, "invalid"),
			errorCode: BadDigest,
		},
		{
			name:      "missing checksum",
			header:    make(http.Header),
			errorCode: MissingContentMD5,
		},
		{
			name:      "content md5 takes precedence",
			header:    header(ContentMD5, "invalid", XAmzChecksumCRC32, "upiFyg=="),
			errorCode: BadDigest,
		},
		{
			name:   "valid content md5 ignores invalid crc32",
			header: header(ContentMD5, GetMD5(body), XAmzChecksumCRC32, "invalid"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.errorCode, verifyDeleteObjectsChecksum(test.header, body))
		})
	}
}

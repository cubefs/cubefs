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
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

func TestDefaultDecoder_DecodeReq(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, "/test", strings.NewReader("test"))
	require.NoError(t, err)

	decoder := defaultDecoder{}

	request.Header.Set("Content-Type", rpc.MIMEJSON)
	request.Header.Set("Content-MD5", "1")
	decodeReq := decoder.DecodeReq(request)
	require.NotNil(t, decodeReq)

	request.Header.Set("Content-Type", rpc.MIMEPOSTForm)
	decodeReq = decoder.DecodeReq(request)
	require.NotNil(t, decodeReq)
}

func TestDefaultDecoder_RequestCompact(t *testing.T) {
	dst := []byte(`{"str":"aaa\nbbb","idx":10}`)
	for _, src := range [][]byte{
		[]byte(`{"str":"aaa\nbbb","idx":10}`),
		[]byte(`{"str": "aaa\nbbb",
                 "idx": 10}`),
		[]byte(`{
	"str": "aaa\nbbb",
"idx": 10
}`),
		[]byte(`    {
                "str": "aaa\nbbb",
                "idx": 10
            }`),
	} {
		require.True(t, bytes.Equal(compactNewline(src), dst))
	}
}

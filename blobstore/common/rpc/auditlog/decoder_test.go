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
	{
		request, err := http.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"name":"test"}`))
		require.NoError(t, err)

		decoder := defaultDecoder{}

		request.Header.Set(rpc.HeaderContentType, rpc.MIMEJSON)
		request.Header.Set(rpc.HeaderContentMD5, "1")
		decodeReq := decoder.DecodeReq(request)
		require.Equal(t, "/test", decodeReq.Path)
		require.Equal(t, rpc.MIMEJSON, decodeReq.Header[rpc.HeaderContentType])
		require.Equal(t, "1", decodeReq.Header[rpc.HeaderContentMD5])
		require.True(t, len(decodeReq.Params) > 0)

		request.Header.Set(rpc.HeaderContentType, rpc.MIMEPOSTForm)
		decodeReq = decoder.DecodeReq(request)
		require.NotNil(t, decodeReq)
		require.Equal(t, "/test", decodeReq.Path)
		require.Equal(t, rpc.MIMEPOSTForm, decodeReq.Header[rpc.HeaderContentType])
		require.True(t, len(decodeReq.Params) > 0)
	}
	{
		body := bytes.NewReader(make([]byte, maxSeekableBodyLength+1))
		request, err := http.NewRequest(http.MethodPut, "/nobody", body)
		require.NoError(t, err)

		decoder := defaultDecoder{}
		request.Header.Set(rpc.HeaderContentType, rpc.MIMEJSON)
		request.Header.Set(rpc.HeaderCrcEncoded, "1")
		decodeReq := decoder.DecodeReq(request)
		require.Equal(t, "/nobody", decodeReq.Path)
		require.True(t, len(decodeReq.Params) == 0)
	}
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

func Benchmark_Decode(b *testing.B) {
	request, err := http.NewRequest(http.MethodPost, "/test", strings.NewReader(`{"name":"test"}`))
	require.NoError(b, err)
	request.Header.Set(rpc.HeaderContentType, rpc.MIMEJSON)
	request.Header.Set(rpc.HeaderContentMD5, "1")
	request.Header.Set(rpc.HeaderContentMD5, "2")

	for ii := 0; ii < b.N; ii++ {
		decoder := defaultDecoder{}
		decoder.DecodeReq(request)
	}
}

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
	"bytes"
	"mime/multipart"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSecurityToken(t *testing.T) {
	token := "X-AMZ-SECURITY-TOKEN-EXAMPLE"
	// header
	request := httptest.NewRequest("GET", "https://s3.cubefs.com/bucket/key", nil)
	request.Header.Set(XAmzSecurityToken, token)
	require.Equal(t, token, getSecurityToken(request))
	// query
	queries := url.Values{}
	queries.Set(XAmzSecurityToken, token)
	request = httptest.NewRequest("GET", "https://s3.cubefs.com/bucket/key?"+queries.Encode(), nil)
	require.Equal(t, token, getSecurityToken(request))
	// post form upload
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, _ := w.CreateFormField(XAmzSecurityToken)
	fw.Write([]byte(token))
	fw, _ = w.CreateFormField("key")
	fw.Write([]byte("test.txt"))
	w.Close()
	request = httptest.NewRequest("POST", "https://s3.cubefs.com/bucket", &buf)
	request.Header.Set(ContentType, w.FormDataContentType())
	require.NoError(t, request.ParseMultipartForm(64))
	require.Equal(t, token, getSecurityToken(request))

	// no X-Amz-Security-Token
	request = httptest.NewRequest("GET", "https://s3.cubefs.com/bucket/key", nil)
	require.Equal(t, "", getSecurityToken(request))
	request = httptest.NewRequest("GET", "https://s3.cubefs.com/bucket/key?no-token=test", nil)
	require.Equal(t, "", getSecurityToken(request))
	buf.Reset()
	w = multipart.NewWriter(&buf)
	fw, _ = w.CreateFormField("key")
	fw.Write([]byte("test.txt"))
	w.Close()
	request = httptest.NewRequest("POST", "https://s3.cubefs.com/bucket", &buf)
	request.Header.Set(ContentType, w.FormDataContentType())
	require.NoError(t, request.ParseMultipartForm(64))
	require.Equal(t, "", getSecurityToken(request))
}

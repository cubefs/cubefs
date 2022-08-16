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
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

func TestDefaultDecoder_DecodeReq(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, "/test", strings.NewReader("test"))
	assert.NoError(t, err)

	decoder := defaultDecoder{}

	request.Header.Set("Content-Type", rpc.MIMEJSON)
	request.Header.Set("Content-MD5", "1")
	decodeReq := decoder.DecodeReq(request)
	assert.NotNil(t, decodeReq)

	request.Header.Set("Content-Type", rpc.MIMEPOSTForm)
	decodeReq = decoder.DecodeReq(request)
	assert.NotNil(t, decodeReq)
}

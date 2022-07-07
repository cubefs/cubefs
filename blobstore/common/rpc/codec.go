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

package rpc

import (
	"net/http"

	"github.com/cubefs/cubefs/blobstore/common/crc32block"
)

type crcDecoder struct{}

var _ ProgressHandler = (*crcDecoder)(nil)

func (*crcDecoder) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	if req.Header.Get(HeaderCrcEncoded) != "" && w.Header().Get(HeaderAckCrcEncoded) == "" {
		if size := req.ContentLength; size > 0 && req.Body != nil {
			decoder := crc32block.NewBodyDecoder(req.Body)
			req.ContentLength = decoder.CodeSize(size)
			req.Body = decoder
		}
		w.Header().Set(HeaderAckCrcEncoded, "1")
	}
	f(w, req)
}

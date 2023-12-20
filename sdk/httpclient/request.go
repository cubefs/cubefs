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

package httpclient

import (
	"fmt"
	"net/url"

	"github.com/cubefs/cubefs/proto"
)

type request struct {
	method string
	path   string
	params url.Values
	header map[string]string
	body   []byte
}

var ReqHeaderUA = fmt.Sprintf("cubefs-sdk/%v (commit %v)", proto.Version, proto.CommitID)

func newRequest(method string, path string) *request {
	req := &request{
		method: method,
		path:   path,
		params: make(url.Values),
		header: make(map[string]string),
	}
	req.header["User-Agent"] = ReqHeaderUA
	return req
}

func (r *request) url() string {
	if len(r.params) == 0 {
		return r.path
	}
	return r.path + "?" + r.params.Encode()
}

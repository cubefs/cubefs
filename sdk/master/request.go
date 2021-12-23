// Copyright 2018 The Chubao Authors.
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

package master

import (
	"fmt"

	"github.com/chubaofs/chubaofs/proto"
)

type request struct {
	method string
	path   string
	params map[string]string
	header map[string]string
	body   []byte
}

var (
	ReqHeaderUA = fmt.Sprintf("chubaofs-sdk/%v (commit %v)", proto.Version, proto.CommitID)
)

func (r *request) addParam(key, value string) {
	r.params[key] = value
}

func (r *request) addHeader(key, value string) {
	r.header[key] = value
}

func (r *request) addBody(body []byte) {
	r.body = body
}

func newAPIRequest(method string, path string) *request {
	req := &request{
		method: method,
		path:   path,
		params: make(map[string]string),
		header: make(map[string]string),
	}

	req.header["User-Agent"] = ReqHeaderUA

	return req
}

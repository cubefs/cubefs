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

package auth

import (
	"net/http"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc/auth/proto"
)

type transport struct {
	Secret []byte
	Tr     http.RoundTripper
}

func New(tr http.RoundTripper, cfg *proto.Config) http.RoundTripper {
	if cfg.EnableAuth {
		if cfg.Secret != "" {
			return &transport{
				Secret: []byte(cfg.Secret),
				Tr:     tr,
			}
		}
	}
	return tr
}

// a simple auth token
func (t *transport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	now := time.Now().Unix()
	param := proto.ParamFromRequest(req)
	req.Header.Set(proto.TokenHeaderKey, proto.Encode(now, param, t.Secret))
	return t.Tr.RoundTrip(req)
}

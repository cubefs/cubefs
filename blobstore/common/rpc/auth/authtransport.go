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
)

type AuthTransport struct {
	Secret []byte
	Tr     http.RoundTripper
}

func NewAuthTransport(tr http.RoundTripper, cfg *Config) http.RoundTripper {
	if cfg.EnableAuth {
		if cfg.Secret == "" {
			panic("auth secret can not be nil")
		}
		return &AuthTransport{
			Secret: []byte(cfg.Secret),
			Tr:     tr,
		}
	}
	return nil
}

// a simple auth token
func (self *AuthTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	now := time.Now().Unix()
	if err != nil {
		return self.Tr.RoundTrip(req)
	}

	info := &authInfo{timestamp: now, others: genEncodeStr(req)}

	err = calculate(info, self.Secret)
	if err != nil {
		return self.Tr.RoundTrip(req)
	}

	token, err := encodeAuthInfo(info)
	if err != nil {
		return self.Tr.RoundTrip(req)
	}

	req.Header.Set(TokenHeaderKey, token)
	return self.Tr.RoundTrip(req)
}

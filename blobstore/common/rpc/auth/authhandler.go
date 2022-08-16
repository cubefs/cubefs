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
)

type AuthHandler struct {
	Secret []byte
}

func NewAuthHandler(cfg *Config) *AuthHandler {
	if cfg.EnableAuth {
		if cfg.Secret == "" {
			panic("auth secret can not be nil")
		}
		return &AuthHandler{
			Secret: []byte(cfg.Secret),
		}
	}
	return nil
}

func (self *AuthHandler) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	token := req.Header.Get(TokenHeaderKey)
	if token == "" {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	info, err := decodeAuthInfo(token)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	info.others = genEncodeStr(req)

	err = verify(info, self.Secret)
	if err != nil && err == errMismatchToken {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	f(w, req)
}

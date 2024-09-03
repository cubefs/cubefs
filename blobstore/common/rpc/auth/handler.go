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

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auth/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
)

type handler struct {
	Secret []byte
}

func New(cfg *proto.Config) interface {
	rpc2.Interceptor
	rpc.ProgressHandler
} {
	if !cfg.EnableAuth || cfg.Secret == "" {
		panic("auth secret can not be empty")
	}
	return &handler{Secret: []byte(cfg.Secret)}
}

func (h *handler) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	if err := proto.Decode(req.Header.Get(proto.TokenHeaderKey), proto.ParamFromRequest(req), h.Secret); err != nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	f(w, req)
}

func (h *handler) Handle(w rpc2.ResponseWriter, req *rpc2.Request, f rpc2.Handle) error {
	if err := proto.Decode(req.Header.Get(proto.TokenHeaderKey), []byte(req.RemotePath), h.Secret); err != nil {
		return rpc2.NewError(http.StatusForbidden, "Auth", err.Error())
	}
	return f(w, req)
}

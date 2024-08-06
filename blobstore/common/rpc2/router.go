// Copyright 2024 The CubeFS Authors.
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

package rpc2

import "fmt"

type Router struct {
	maps map[string]Handle
}

var _ Handler = (*Router)(nil)

func (r *Router) Register(handler string, handle Handle) {
	if r.maps == nil {
		r.maps = make(map[string]Handle)
	}
	if _, exist := r.maps[handler]; exist {
		panic(fmt.Sprintf("rpc2: handle(%s) has registered", handler))
	}
	r.maps[handler] = handle
}

func (r *Router) Handle(w ResponseWriter, req *Request) error {
	handle, exist := r.maps[req.RemoteHandler]
	if !exist {
		return &Error{
			Status: 404,
			Reason: "NoRouter",
			Detail: fmt.Sprintf("no router for handler(%s)", req.RemoteHandler),
		}
	}
	return handle(w, req)
}

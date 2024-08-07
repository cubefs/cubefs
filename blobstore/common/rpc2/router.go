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

import (
	"fmt"
	"runtime/debug"
)

var defaultPanicHandler = func(_ ResponseWriter, req *Request, err interface{}, stack []byte) error {
	if err != nil {
		span := req.Span()
		span.Errorf("panic fired in handle:%s -> %v\n", req.RemoteHandler, err)
		span.Error(string(stack))
		return &Error{
			Status: 597,
			Reason: "HandlePanic",
			Detail: fmt.Sprintf("panic(%v)", err),
		}
	}
	return nil
}

type Router struct {
	PanicHandler func(w ResponseWriter, req *Request, err interface{}, stack []byte) error

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

	if r.PanicHandler == nil {
		r.PanicHandler = defaultPanicHandler
	}
}

func (r *Router) Handle(w ResponseWriter, req *Request) (err error) {
	handle, exist := r.maps[req.RemoteHandler]
	if !exist {
		err = &Error{
			Status: 404,
			Reason: "NoRouter",
			Detail: fmt.Sprintf("no router for handler(%s)", req.RemoteHandler),
		}
		return
	}

	defer func() {
		if p := recover(); p != nil {
			stack := debug.Stack()
			if errp := r.PanicHandler(w, req, p, stack); err == nil {
				err = errp
			}
		}
	}()

	err = handle(w, req)
	return
}

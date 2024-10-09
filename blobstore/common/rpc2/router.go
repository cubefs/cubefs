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

const DefaultStatusPanic = 597

type Handle func(ResponseWriter, *Request) error

// Handle calls f(w, r) to Handler
func (f Handle) Handle(w ResponseWriter, r *Request) error {
	return f(w, r)
}

type Handler interface {
	Handle(ResponseWriter, *Request) error
}

// Interceptor intercept handler's request and response
type Interceptor interface {
	Handle(ResponseWriter, *Request, Handle) error
}

var defaultPanicHandler = func(_ ResponseWriter, req *Request, err interface{}, stack []byte) error {
	if err != nil {
		span := req.Span()
		span.Errorf("panic fired in path:%s -> %v\n", req.RemotePath, err)
		span.Error(string(stack))
		return NewErrorf(DefaultStatusPanic, "HandlePanic", "panic(%v)", err)
	}
	return nil
}

type Router struct {
	PanicHandler func(w ResponseWriter, req *Request, err interface{}, stack []byte) error

	interceptors []Interceptor
	middlewares  []Handle
	handlers     map[string]Handle
}

var _ Handler = (&Router{}).MakeHandler()

func (r *Router) Interceptor(its ...Interceptor) {
	if len(r.interceptors)+len(r.middlewares)+len(its) > 1<<10 {
		panic("rpc2: too much interceptors (>1024)")
	}
	r.interceptors = append(r.interceptors, its...)
}

func (r *Router) Middleware(mws ...Handle) {
	if len(r.interceptors)+len(r.middlewares)+len(mws) > 1<<10 {
		panic("rpc2: too much middlewares (>1024)")
	}
	r.middlewares = append(r.middlewares, mws...)
}

func (r *Router) Register(path string, handle Handle) {
	if r.handlers == nil {
		r.handlers = make(map[string]Handle)
	}
	if _, exist := r.handlers[path]; exist {
		panic(fmt.Sprintf("rpc2: path(%s) has registered", path))
	}
	r.handlers[path] = handle

	if r.PanicHandler == nil {
		r.PanicHandler = defaultPanicHandler
	}
}

func (r *Router) MakeHandler() Handle {
	if len(r.interceptors) == 0 {
		return r.handleWithPanic(r.handle)
	}
	return r.handleWithPanic(r.makeInterceptor(r.handle, r.interceptors[:]))
}

func (r *Router) makeInterceptor(h Handle, its []Interceptor) Handle {
	if len(its) == 0 {
		return h
	}
	last := len(its) - 1
	ph := its[last]
	return r.makeInterceptor(
		func(w ResponseWriter, req *Request) error {
			return ph.Handle(w, req, h)
		}, its[:last])
}

func (r *Router) handleWithPanic(h Handle) Handle {
	if r.PanicHandler == nil {
		return h
	}
	return func(w ResponseWriter, req *Request) (err error) {
		defer func() {
			if p := recover(); p != nil {
				stack := debug.Stack()
				if errp := r.PanicHandler(w, req, p, stack); err == nil {
					err = errp
				}
			}
		}()
		err = h(w, req)
		return
	}
}

func (r *Router) handle(w ResponseWriter, req *Request) (err error) {
	handle, exist := r.handlers[req.RemotePath]
	if !exist {
		err = NewErrorf(404, "NoRouter", "no router for path(%s)", req.RemotePath)
		return
	}

	for idx := range r.middlewares {
		if err = r.middlewares[idx](w, req); err != nil {
			return
		}
	}
	err = handle(w, req)
	if req.stream != nil { // stream
		return
	}

	if err != nil {
		status, _, _ := DetectError(err)
		w.SetError(err)
		w.WriteHeader(status, NoParameter)
	}
	if err = w.WriteOK(nil); err != nil {
		return
	}
	if err = w.Flush(); err != nil {
		return
	}
	return
}
